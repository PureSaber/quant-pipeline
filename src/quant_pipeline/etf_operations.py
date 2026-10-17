"""Versioned operations controller around an independently frozen ETF runtime."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import runpy
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from quant_lab.research import study_lock
from quant_workspace.local_control import admission_lock

from quant_pipeline import etf_schedule
from quant_pipeline.etf_frozen_worker import process_token

SCHEMA = "quant.etf-operations/v1"
WORKER = Path(__file__).with_name("etf_frozen_worker.py")
RETRY = {"collection_retryable", "publication_delay"}


def digest(value):
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    ).hexdigest()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text("utf-8"))


def workers_running(config):
    for path in Path(config["output_root"]).glob("*/worker-*.json"):
        row = read(path)
        if row["state"] == "running":
            actual = process_token(row["pid"])
            if actual is not None and (
                row.get("process_token") is None or actual == row["process_token"]
            ):
                return True
    return False


def invoke(argv, *, input, timeout, process_directory=None, **options):
    """Timeout stops only this live child's process tree, before draining its pipes."""
    with subprocess.Popen(argv, stdin=subprocess.PIPE, **options) as process:
        marker = None
        if process_directory:
            marker = Path(process_directory) / f"worker-launcher-{uuid.uuid4().hex}.json"
            owner = {
                "pid": process.pid,
                "process_token": process_token(process.pid),
                "state": "running",
            }
            save(marker, owner)
        try:
            stdout, stderr = process.communicate(input, timeout=timeout)
        except subprocess.TimeoutExpired:
            if os.name == "nt" and process.poll() is None:
                subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    capture_output=True,
                    timeout=10,
                    check=False,
                    creationflags=subprocess.CREATE_NO_WINDOW,
                )
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=10)
            raise
        finally:
            if marker:
                save(marker, {**owner, "state": "finished"})
        return subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + uuid.uuid4().hex + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8"
    )
    temporary.replace(path)


def load(path):
    path = Path(path).resolve()
    value = read(path)
    required = {
        "schema",
        "daily_profile",
        "daily_profile_sha256",
        "protected_files",
        "frozen_python",
        "frozen_root",
        "frozen_stack_sha256",
        "output_root",
        "runs_root",
        "scheduler",
        "execution_enabled",
        "continuation",
    }
    if set(value) != required or value["schema"] != SCHEMA:
        raise ValueError("Invalid operations configuration")
    for key in ("daily_profile", "frozen_python", "frozen_root", "output_root", "runs_root"):
        if not Path(value[key]).is_absolute():
            raise ValueError("Operations paths must be absolute")
    if type(value["execution_enabled"]) is not bool:
        raise ValueError("execution_enabled must be boolean")
    profile = read(value["daily_profile"])
    etf_schedule.validate(profile.get("schedule"))
    if not profile.get("schedule"):
        raise ValueError("Operations require an explicit bounded schedule")
    if sha(value["daily_profile"]) != value["daily_profile_sha256"]:
        raise ValueError("FROZEN_IDENTITY: daily profile changed")
    for file, expected in value["protected_files"].items():
        if not Path(file).is_absolute() or sha(file) != expected:
            raise ValueError("FROZEN_IDENTITY: protected file changed")
    import yaml

    forward = yaml.safe_load(Path(profile["forward_config"]).read_text("utf-8"))
    required_paths = {
        str(Path(p).resolve())
        for p in [
            value["daily_profile"],
            profile["recipe"],
            profile["forward_config"],
            profile["master_refresh"],
            str(Path(forward["account"]) / "account.json"),
        ]
    }
    if not required_paths <= {str(Path(p).resolve()) for p in value["protected_files"]}:
        raise ValueError("All frozen inputs must be pinned")
    output = Path(value["output_root"]).resolve()
    for source in [
        profile["output_root"],
        profile["dataset_root"],
        forward["account"],
        value["frozen_root"],
        value["runs_root"],
    ]:
        source = Path(source).resolve()
        if output == source or output in source.parents or source in output.parents:
            raise ValueError("Operations evidence must have a separate root")
    runtime = Path(value["scheduler"]["execute"]).resolve().parents[2]
    if Path(sys.prefix).resolve() != runtime / "env":
        raise ValueError("Operations must use the explicitly installed environment")
    pins = [
        runtime / "installed-stack.json",
        runtime / "profile/stack.json",
        runtime / "profile/bootstrap.py",
    ]
    if not {str(p) for p in pins} <= set(value["protected_files"]):
        raise ValueError("Operations installation identity must be pinned")
    verifier = runpy.run_path(str(runtime / "profile/bootstrap.py"))
    if verifier["installed_identity"](read(runtime / "profile/stack.json")) != read(
        runtime / "installed-stack.json"
    ):
        raise ValueError("Operations installed package tree changed")
    return value, profile, sha(path)


def worker(config, action, *, directory=None, as_of=None, forward_sha256=None, timeout=120):
    payload = {"action": action, "config": config}
    if directory:
        payload.update(directory=str(directory), as_of=as_of)
    if forward_sha256:
        payload["forward_sha256"] = forward_sha256
    try:
        result = invoke(
            [config["frozen_python"], "-I", "-B", "-X", "utf8", str(WORKER)],
            input=json.dumps(payload),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            timeout=timeout,
            process_directory=directory if action in {"collect", "prepare", "execute"} else None,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
    except subprocess.TimeoutExpired:
        return {
            "state": "failed",
            "stage": "collection" if action == "collect" else action,
            "error": "Worker exceeded its time budget; inspect before recovery",
            "exceptions": [{"type": "TimeoutError", "module": "builtins"}],
        }
    if directory:
        (Path(directory) / f"{action}-{uuid.uuid4().hex}-stderr.txt").write_text(
            result.stderr, encoding="utf-8"
        )
    try:
        value = json.loads(result.stdout)
        if result.returncode or not isinstance(value, dict):
            raise ValueError("Worker process failed")
        return value
    except ValueError as exc:
        raise ValueError(
            "Frozen worker returned no valid result: " + result.stderr[-1000:]
        ) from exc


def classify(value, stage):
    if stage == "collection" and value.get("publication_delay") is True:
        return "publication_delay", True
    rows = value.get("exceptions", [])
    text = " ".join(str(r.get(k, "")) for r in rows for k in ("message", "reason")).lower()
    text += " " + str(value.get("error", "")).lower()
    types = {r.get("type") for r in rows}
    if types & {"SSLCertVerificationError", "CertificateError"} or any(
        s in text
        for s in ("certificate_verify_failed", "certificate verify failed", "hostname mismatch")
    ):
        return "certificate_invalid", False
    if "timely_order_missing" in text or "historical_backfill" in text:
        return "historical_fill_blocked", False
    if "identity" in text or "protected file" in text:
        return "identity_changed", False
    if stage in {"master_refresh", "collection"}:
        if types & {"SSLError"} and not any(
            s in text for s in ("record layer failure", "eof occurred")
        ):
            return "tls_unverified", False
        if types & {
            "TimeoutError",
            "Timeout",
            "ConnectTimeout",
            "ReadTimeout",
            "ConnectionError",
            "ConnectionResetError",
            "SSLEOFError",
        } or (
            types & {"SSLError", "SSLEOFError"}
            and any(
                s in text for s in ("record layer failure", "eof occurred in violation of protocol")
            )
        ):
            return "transport_temporary", True
    return "native_check_failed", False


def history(config):
    rows = []
    for path in Path(config["output_root"]).glob("*/operation.json"):
        row = read(path)
        if row.get("sha256") != digest({k: v for k, v in row.items() if k != "sha256"}):
            raise ValueError("Operations receipt changed")
        rows.append(row)
    return sorted(rows, key=lambda r: (r.get("sequence", 0), r["started_at"]))


def continuation(config):
    entry = config["continuation"]
    if not entry or sha(entry["path"]) != entry["sha256"]:
        return {"allowed": False, "reason": "原账户续跑核查尚未通过"}
    proof = read(entry["path"])
    return {
        "allowed": proof.get("continuation_permitted") is True,
        "reason": proof.get("reason", "等待原生续跑核查"),
        "evidence": entry,
    }


def task_status(config):
    from quant_workspace.etf_scheduler import inspect_task

    return inspect_task(config["scheduler"])


def status(path, *, now=None, _lock_owned=False):
    config, profile, config_sha = load(path)
    now = now or datetime.now(timezone.utc)
    native = worker(config, "inspect")
    if native.get("schema") != "quant.etf-daily-status/v1":
        raise ValueError(native.get("error", "Native inspection failed"))
    rows = history(config)
    expected = native["expected_session"]
    local_date = now.astimezone(etf_schedule.ZONE).date().isoformat()
    today = [r for r in rows if r["as_of"] == local_date]
    latest = today[-1] if today else None
    schedule = etf_schedule.window(profile, now, native["today_is_session"], today, expected)
    scheduler = task_status(config)
    permitted = continuation(config)
    enabled = config["execution_enabled"] and permitted["allowed"]
    active = enabled and scheduler.get("verified") is True and scheduler.get("enabled") is True
    execution = native["execution"]
    complete = bool(
        expected == local_date
        and latest
        and latest["state"] == "completed"
        and execution.get("available")
        and execution.get("as_of") == expected
        and native["data"]["fresh"]
    )
    state = "completed" if complete else "waiting"
    if not native["today_is_session"]:
        state = "market_closed"
    elif latest and latest["state"] == "running":
        from quant_pipeline.forward_daily import _lock_held

        state = (
            "running"
            if workers_running(config)
            or (not _lock_owned and _lock_held(Path(profile["output_root"])))
            else "interrupted"
        )
    elif latest and latest["state"] not in RETRY | {"completed"}:
        state = "needs_attention"
    elif latest and latest["state"] in RETRY:
        state = "retry_wait" if active and schedule["next_attempt_at"] else "needs_attention"
    elif schedule["state"] == "collection_exhausted":
        state = "needs_attention"
    if state not in {"completed", "market_closed", "running", "interrupted"} and not active:
        state = "paused" if not enabled or scheduler.get("verified") else "unknown"
    next_at = schedule["next_attempt_at"] if active and state in {"waiting", "retry_wait"} else None
    # Native data/account projection remains authoritative. Legacy history stays separate.
    native["legacy_latest"] = native["latest"]
    native.update(
        latest=latest,
        last_success=next((r for r in reversed(rows) if r["state"] == "completed"), None),
    )
    native["research_input"] = next(
        (r for r in reversed(rows) if r.get("research_recipe")), native.get("research_input")
    )
    missed = (
        [
            s
            for i, s in enumerate(schedule["slots"])
            if now.astimezone(etf_schedule.ZONE)
            >= datetime.fromisoformat(local_date + "T" + s + ":00+08:00")
            + timedelta(seconds=profile["schedule"]["collection_timeout_seconds"])
            and not any(r.get("slot") == i for r in today)
        ]
        if native["today_is_session"] and not complete
        else []
    )
    schedule["missed_slots"] = missed
    native["schedule"] = {**schedule, "next_attempt_at": next_at}
    native["operations"] = {
        "schema": SCHEMA,
        "state": state,
        "enabled": enabled,
        "scheduler": scheduler,
        "continuation": permitted,
        "config_sha256": config_sha,
        "worker_sha256": sha(WORKER),
        "latest_receipt_sha256": rows[-1]["sha256"] if rows else None,
        "next_attempt_at": next_at,
        "manual_action_required": state in {"paused", "unknown", "needs_attention", "interrupted"},
        "last_account_session": execution.get("as_of"),
        "history": rows,
        "acceptance": acceptance(
            native,
            [
                r
                for r in rows
                if r.get("config_sha256") == config_sha and r.get("worker_sha256") == sha(WORKER)
            ],
        ),
    }
    return native


def acceptance(native, rows):
    """Derive, never invent, consecutive successes from verified operation evidence."""
    if rows:
        newest = rows[-1]
        rows = [
            r
            for r in rows
            if (r.get("config_sha256"), r.get("worker_sha256"))
            == (newest.get("config_sha256"), newest.get("worker_sha256"))
        ]
    valid = {
        r["as_of"]: r
        for r in rows
        if r["state"] == "completed"
        and r.get("trigger") == "windows_task"
        and r.get("scheduler", {}).get("verified") is True
        and r.get("scheduler", {}).get("natural_trigger") is True
        and r.get("verified_at")
        and r.get("natural_credit") is True
    }
    attempts = sorted({r["as_of"] for r in rows})
    start = attempts[0] if attempts else None
    sessions = [d for d in native["calendar"] if start and start <= d <= native["expected_session"]]
    segments, current, gaps = [], [], []
    for day in sessions:
        if day in valid:
            current.append(
                {"session": day, "operation_id": valid[day]["id"], "sha256": valid[day]["sha256"]}
            )
        else:
            if current:
                segments.append(current)
            current = []
            gaps.append(day)
    if current:
        segments.append(current)
    longest = max((len(s) for s in segments), default=0)
    return {
        "schema": "quant.etf-operations-acceptance/v1",
        "required_sessions": 6,
        "state": "passed" if longest >= 6 else "pending",
        "longest_consecutive": longest,
        "segments": segments,
        "gaps": gaps,
        "strategy_validated": False,
        "scope": "new operations software; original account gaps remain unchanged",
    }


def binding(value):
    return digest(
        {
            "operations": {
                k: value["operations"][k]
                for k in (
                    "config_sha256",
                    "worker_sha256",
                    "latest_receipt_sha256",
                    "enabled",
                    "scheduler",
                )
            },
            "account": {k: v for k, v in value["account"].items() if k != "checked_at"},
            "data": value["data"],
            "expected_session": value["expected_session"],
        }
    )


def recover_plan(path, operation_id, *, _lock_owned=False):
    if not re.fullmatch(r"[a-f0-9]{32}", operation_id):
        raise ValueError("Invalid operation identifier")
    value = status(path, _lock_owned=_lock_owned)
    config, _, _ = load(path)
    row = next((r for r in value["operations"]["history"] if r["id"] == operation_id), None)
    action, reason = "none", "该记录不满足恢复条件"
    current = value["expected_session"]
    if row and row["as_of"] == current and value["operations"]["enabled"]:
        if value["operations"]["state"] == "running" or workers_running(config):
            reason = "仍有程序持有执行锁，不能重复启动"
        elif row.get("forward_config_sha256"):
            assessment = worker(
                config,
                "assess",
                directory=Path(row["forward_config"]).parent,
                as_of=current,
                forward_sha256=row["forward_config_sha256"],
            )
            if assessment["state"] == "completed":
                action, reason = "reconcile", "账户已完成，只核验并关联结果，不重复记账"
            elif assessment["state"] in {"ready", "interrupted", "retryable_failure"}:
                action, reason = "resume", "原生规则允许继续；确认时将重新核验"
            else:
                reason = assessment.get("reason", assessment.get("error", "原生规则阻断"))
        elif row["state"] in RETRY:
            action, reason = "retry", "仅在下一个有效窗口重新获取数据"
        elif row["state"] == "running" and not workers_running(config):
            action, reason = "retry", "上次采集已中断且没有活动进程；仅在下一个有效窗口重试"
    plan = {
        "schema": "quant.etf-recovery-plan/v1",
        "operation_id": operation_id,
        "action": action,
        "reason": reason,
        "binding": binding(value),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "ttl_seconds": 900,
    }
    plan["sha256"] = digest(plan)
    return plan


def execute(path, *, trigger="manual", recovery=None):
    if trigger not in {"manual", "windows_task"}:
        raise ValueError("Unknown trigger")
    config, profile, config_sha = load(path)
    root = Path(config["output_root"])
    # The common admission lock prevents maintenance racing with publication of running evidence.
    with admission_lock(config["runs_root"]):
        if (Path(config["runs_root"]) / ".local-maintenance.json").exists():
            raise ValueError("Maintenance in progress")
        root.mkdir(parents=True, exist_ok=True)
        with study_lock(Path(profile["output_root"])):
            return _execute_locked(path, config, profile, config_sha, trigger, recovery)


def _execute_locked(path, config, profile, config_sha, trigger, recovery):
    value = status(path, _lock_owned=True)
    now = datetime.now(timezone.utc)
    expected = value["expected_session"]
    ops = value["operations"]
    if not ops["enabled"]:
        return {"state": "paused", "read_only": True, "reason": ops["continuation"]["reason"]}
    if trigger == "windows_task" and not (
        ops["scheduler"].get("verified")
        and ops["scheduler"].get("enabled")
        and ops["scheduler"].get("state") == "Running"
    ):
        raise ValueError("Scheduled task identity or enabled state could not be verified")
    window = etf_schedule.window(profile, now, value["today_is_session"], [], expected)
    if workers_running(config):
        return {
            "state": "running",
            "read_only": True,
            "reason": "A native worker still owns this operation",
        }
    action = "tick"
    previous = value["latest"]
    if recovery:
        content = {k: v for k, v in recovery.items() if k != "sha256"}
        created = datetime.fromisoformat(content["created_at"])
        if recovery.get("sha256") != digest(content) or not timedelta(
            0
        ) <= now - created <= timedelta(seconds=900):
            raise ValueError("Recovery preview expired or changed")
        if recovery["binding"] != binding(value):
            raise ValueError("State changed after recovery preview")
        fresh = recover_plan(path, recovery["operation_id"], _lock_owned=True)
        if fresh["action"] != recovery["action"] or fresh["action"] == "none":
            raise ValueError("Recovery no longer allowed")
        if not previous or previous["id"] != recovery["operation_id"]:
            raise ValueError("Only the latest current-day attempt may be recovered")
        action = recovery["action"]
    if action != "reconcile" and window["state"] != "ready":
        return {"state": window["state"], "read_only": True}
    if (
        not recovery
        and previous
        and (previous["state"] not in RETRY or previous["slot"] >= window["slot"])
    ):
        return {"state": "already_attempted", "read_only": True}
    if action == "retry" and previous["slot"] >= window["slot"]:
        return {"state": "already_attempted", "read_only": True}
    identifier = uuid.uuid4().hex
    directory = Path(config["output_root"]) / identifier
    directory.mkdir()
    operation = {
        "schema": "quant.etf-operation/v1",
        "id": identifier,
        "as_of": expected,
        "sequence": max((r.get("sequence", 0) for r in ops["history"]), default=0) + 1,
        "started_at": now.isoformat(),
        "slot": window["slot"],
        "state": "running",
        "trigger": trigger,
        "action": action,
        "config_sha256": config_sha,
        "worker_sha256": sha(WORKER),
        "scheduler": ops["scheduler"],
        "stages": [],
        "natural_credit": False,
        "previous_operation": previous["id"] if previous else None,
    }

    def publish():
        operation.pop("sha256", None)
        operation["sha256"] = digest(operation)
        save(directory / "operation.json", operation)

    def stage(name, state, **evidence):
        operation["stages"].append(
            {
                "stage": name,
                "state": state,
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                **evidence,
            }
        )
        publish()

    stage("trigger", "completed")
    if recovery and previous["state"] == "running":
        original_path = Path(config["output_root"]) / previous["id"] / "operation.json"
        save(
            original_path.with_name("interrupted-original-" + previous["sha256"] + ".json"),
            previous,
        )
        interrupted = {
            **previous,
            "state": "interrupted",
            "resolved_by": identifier,
            "replaces_sha256": previous["sha256"],
        }
        interrupted.pop("sha256", None)
        interrupted["sha256"] = digest(interrupted)
        save(original_path, interrupted)
    current_stage = "identity"
    try:
        if recovery and action in {"resume", "reconcile"}:
            source = Path(previous["forward_config"]).parent
            staged = {k: previous[k] for k in ("forward_config_sha256", "forward_config")}
            operation.update(staged)
            operation["recovered_from"] = previous["id"]
        else:
            current_stage = "collection"
            stage(current_stage, "running")
            collected = worker(
                config,
                "collect",
                directory=directory,
                as_of=expected,
                timeout=window["collection_timeout_seconds"],
            )
            operation["stages"].extend(collected.get("stages", []))
            if collected["state"] != "collected":
                code, retry = classify(collected, collected.get("stage", "collection"))
                operation.update(
                    state="collection_retryable" if retry else "blocked",
                    reason_code=code,
                    error=collected.get("error"),
                    exceptions=collected.get("exceptions", []),
                )
                return operation
            current_stage = "preflight"
            stage("collection", "completed")
            staged = worker(config, "prepare", directory=directory, as_of=expected, timeout=300)
            operation.update({k: v for k, v in staged.items() if k != "state"})
            if staged["state"] not in {"ready", "completed"}:
                operation["state"] = (
                    "collection_retryable" if staged["state"] == "publication_delay" else "blocked"
                )
                operation["reason_code"] = (
                    "publication_delay"
                    if staged["state"] == "publication_delay"
                    else "native_check_failed"
                )
                operation["error"] = (
                    staged.get("error")
                    or staged.get("forward", {}).get("reason")
                    or "Data preflight did not pass"
                )
                return operation
            stage("preflight", "completed")
            source = directory
        current_stage = "observe"
        stage("observe", "running")
        # Real clock only; the unchanged native run rechecks timing before commitment.
        result = worker(
            config,
            "execute",
            directory=source,
            as_of=expected,
            forward_sha256=staged["forward_config_sha256"],
            timeout=1800,
        )
        operation["native_result"] = result
        operation["state"] = result["state"] if result["state"] == "completed" else "blocked"
        if result["state"] != "completed":
            code, _ = classify(result, "observe")
            operation.update(
                reason_code=code, error=result.get("error", "Native execution incomplete")
            )
            return operation
        if (
            not result.get("execution", {}).get("available")
            or result["execution"]["as_of"] != expected
        ):
            raise ValueError("Native ledger does not cover the current session")
        stage("observe", "completed")
        stage("verify", "completed")
        operation["verified_at"] = datetime.now(timezone.utc).isoformat()
        native_receipt = result.get("native_receipt") or {}
        operation["natural_credit"] = (
            trigger == "windows_task"
            and recovery is None
            and ops["scheduler"].get("natural_trigger") is True
            # Native run returns completed for a verified pre-existing observation too.
            # Only a fresh execution receipt proves this trigger performed the observation.
            and native_receipt.get("plan_state") == "ready"
            and bool(native_receipt.get("native_attempt_id"))
            and bool(native_receipt.get("native_result_sha256"))
        )
        return operation
    except Exception as exc:  # noqa: BLE001 - preserve a failed attempt without faking completion
        operation.update(state="blocked", error=str(exc), reason_code="native_check_failed")
        stage(current_stage, "blocked", reason=str(exc))
        return operation
    finally:
        if window["last_slot"] and operation["state"] == "collection_retryable":
            operation["state"] = "collection_exhausted"
        operation["finished_at"] = datetime.now(timezone.utc).isoformat()
        publish()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["status", "tick", "recover-plan", "recover-execute"])
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--operation")
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--trigger", choices=["manual", "windows_task"], default="manual")
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            result = status(args.config)
        elif args.command == "recover-plan":
            result = recover_plan(args.config, args.operation or "")
        else:
            plan = (
                (read(args.plan) if args.plan else json.load(sys.stdin))
                if args.command == "recover-execute"
                else None
            )
            result = execute(args.config, trigger=args.trigger, recovery=plan)
            if args.trigger == "windows_task":
                config, _, _ = load(args.config)
                save(
                    Path(config["output_root"]) / "triggers" / (uuid.uuid4().hex + ".json"),
                    {
                        "schema": "quant.etf-trigger/v1",
                        "checked_at": datetime.now(timezone.utc).isoformat(),
                        "result": result,
                        "natural_credit": False,
                        "note": "Trigger evidence alone never counts as a successful session",
                    },
                )
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))
        return 0 if result.get("state") not in {"blocked", "failed", "collection_exhausted"} else 3
    except Exception as exc:  # noqa: BLE001 - CLI fail-closed boundary
        print(
            json.dumps(
                {"schema": SCHEMA, "state": "verification_failed", "error": str(exc)},
                ensure_ascii=False,
            )
        )
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
