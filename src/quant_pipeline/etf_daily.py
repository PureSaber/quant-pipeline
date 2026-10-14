"""Four-ETF daily adapter: immutable QDK refresh, native preflight and forward receipts.

The scheduler calls tick; the module has no background loop or broker interface.
Profile paths are operator configuration, never browser supplied commands.
"""

from __future__ import annotations

import argparse
import json
import uuid
from copy import deepcopy
from datetime import datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import yaml
from quant_lab.research import canonical, file_hash, load_recipe, study_lock

from quant_pipeline import forward_daily, research_paper

SYMBOLS = ["510300", "510500", "159915", "588000"]
SLOTS = (time(16, 30), time(17), time(18))
SCHEMA = "quant.etf-daily/v1"


def load_profile(path):
    path = Path(path).resolve()
    value = json.loads(path.read_text(encoding="utf-8"))
    required = {"schema", "dataset_root", "recipe", "output_root", "forward_config"}
    if not isinstance(value, dict) or set(value) != required or value["schema"] != SCHEMA:
        raise ValueError("ETF daily profile keys do not match v1")
    for key in required - {"schema"}:
        if key == "forward_config" and value[key] is None:
            continue
        if not isinstance(value[key], str) or not Path(value[key]).is_absolute():
            raise ValueError(f"{key} must be an absolute operator-configured path")
        value[key] = Path(value[key]).resolve()
    output = value["output_root"]
    for source in (value["dataset_root"], value["recipe"].parent):
        if output == source or output in source.parents or source in output.parents:
            raise ValueError("Daily output must be separate from dataset and frozen recipe")
    if value["forward_config"]:
        config = forward_daily.load_config(value["forward_config"])
        forward_daily._require_separate_path(output, config.account, "daily output")
        forward_daily._require_separate_path(value["dataset_root"], config.account, "dataset")
    value["profile_path"] = path
    value["profile_sha256"] = file_hash(path)
    value["recipe_sha256"] = file_hash(value["recipe"])
    value["operator_sha256"] = (
        file_hash(value["forward_config"]) if value["forward_config"] else None
    )
    recipe = load_recipe(value["recipe"])
    if recipe["backend"] != "equity" or recipe["strategy"]["frequency"] != "weekly":
        raise ValueError("Four-ETF baseline must retain the native weekly equity recipe")
    return value


def _snapshot(profile):
    from quant_data_kit.research_dataset import load_research_snapshot

    manifest, frames = load_research_snapshot(profile["dataset_root"])
    if sorted(manifest["symbols"]) != sorted(SYMBOLS):
        raise ValueError("Configured dataset differs from the fixed four-ETF universe")
    return manifest, frames


def closed_session(frames, now):
    if now.tzinfo is None:
        raise ValueError("Current time must carry a timezone")
    local = now.astimezone(ZoneInfo("Asia/Shanghai"))
    calendar = pd.DatetimeIndex(pd.to_datetime(frames["calendar"]["date"])).normalize()
    today = pd.Timestamp(local.date())
    if calendar.empty or calendar.min() > today or calendar.max() < today:
        raise ValueError("Exchange calendar does not cover the actual current date")
    closed = calendar[(calendar < today) | ((calendar == today) & (local.time() >= time(15)))]
    if closed.empty:
        raise ValueError("Exchange calendar has no closed session")
    return str(closed.max().date()), today in calendar


def _identity_unchanged(profile):
    if file_hash(profile["profile_path"]) != profile["profile_sha256"]:
        raise ValueError("Daily profile changed during execution")
    if file_hash(profile["recipe"]) != profile["recipe_sha256"]:
        raise ValueError("Frozen baseline recipe changed during execution")


def _read_receipt(path):
    value = json.loads(path.read_text(encoding="utf-8"))
    from quant_lab.research import digest

    if value.get("sha256") != digest({k: v for k, v in value.items() if k != "sha256"}):
        raise ValueError("Daily operation receipt identity mismatch")
    return value


def _history(profile):
    return sorted(
        (_read_receipt(p) for p in profile["output_root"].glob("*/operation.json")),
        key=lambda row: row["started_at"],
    )


def _current_history(profile):
    return [
        r
        for r in _history(profile)
        if r["profile_sha256"] == profile["profile_sha256"]
        and r["recipe_sha256"] == profile["recipe_sha256"]
        and r.get("operator_sha256") == profile["operator_sha256"]
    ]


def status(path, *, now=None):
    """Read-only live verification, including staleness of the last successful receipt."""
    now = now or datetime.now(timezone.utc)
    profile = load_profile(path)
    manifest, frames = _snapshot(profile)
    expected, trading = closed_session(frames, now)
    history = _current_history(profile)
    latest = history[-1] if history else None
    successful = [r for r in history if r["state"] == "completed"]
    value = {
        "schema": "quant.etf-daily-status/v1",
        "read_only": True,
        "checked_at": now.isoformat(),
        "expected_session": expected,
        "today_is_session": trading,
        "symbols": SYMBOLS,
        "data": {
            "snapshot_id": manifest["snapshot_id"],
            "covered_through": manifest["requested_end"],
            "captured_at": manifest["captured_at"],
            "fresh": (
                manifest["requested_end"] == expected
                and pd.Timestamp(manifest["captured_at"])
                >= pd.Timestamp(expected, tz="Asia/Shanghai") + pd.Timedelta(hours=15)
            ),
        },
        "latest": latest,
        "last_success": successful[-1] if successful else None,
        "forward": None,
        "account": None,
        "execution": {
            "basis": "simulated",
            "available": False,
            "reason": "No verified native observation",
            "tables": {},
        },
        "limitations": [
            "Simulation positions and modeled costs, not broker executions",
            "Operational acceptance does not certify strategy effectiveness",
        ],
    }
    if profile["forward_config"]:
        config = forward_daily.load_config(profile["forward_config"])
        value["account"] = research_paper.inspect_account(config.account, now=now)
        # Assess the actual last staged input, not an unrelated newer data catalog.
        observed = next((r for r in reversed(history) if r.get("forward_config")), None)
        selected = Path(observed["forward_config"]) if observed else profile["forward_config"]
        if observed and file_hash(selected) != observed["forward_config_sha256"]:
            raise ValueError("Staged forward configuration changed")
        value["forward"] = forward_daily.assess(selected, expected, now=now)
        if config.approval != "approved":
            value["forward"].update(state="paused", action="none", reason="Operator paused")
        value["execution"] = execution_view(config.account)
    return value


def execution_view(account):
    """Only expose verified native tables; never synthesize target/position values."""
    definition = research_paper.load_account(account)
    saved = research_paper.observations(account, definition)
    if not saved:
        return {
            "basis": "simulated",
            "available": False,
            "tables": {},
            "reason": "No completed observation",
        }
    result, directory = saved[-1]
    from quant_lab import load_and_validate_standard_run

    # Validate native exact-ledger contracts before projecting any table.
    manifest = load_and_validate_standard_run(directory)
    if manifest.schema_version != "2.0.0":
        raise ValueError("ETF daily view requires the native exact-ledger v2 contract")
    from quant_pipeline.etf_projection import project_execution

    diagnostics = directory / "execution_diagnostics.json"
    evidence = json.loads(diagnostics.read_text(encoding="utf-8"))
    projection = project_execution(directory, manifest, evidence, result["artifacts"])
    return {
        "basis": "simulated",
        "available": True,
        "as_of": result["as_of"],
        **projection,
        "metrics": result["metrics"],
        "diagnostics": evidence,
        "cost_basis": "Native modeled commissions and slippage; no independent live quotes",
        "target_difference": "Use native orders/fills and rejection reasons; no inferred zero",
    }


def _stage_recipe(profile, manifest, directory):
    recipe = load_recipe(profile["recipe"])
    snapshot = profile["dataset_root"] / "snapshots" / manifest["snapshot_id"]
    inputs = deepcopy(recipe["inputs"])
    inputs.update(bundle=str(snapshot), catalog=str(snapshot / "catalog.csv"))
    if "history" in inputs:
        inputs["history"] = str(snapshot / "history")
    recipe["inputs"] = inputs
    path = directory / "inputs-recipe.yaml"
    path.write_text(yaml.safe_dump(recipe, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return path, recipe


def run(path, *, mode="tick", now=None):
    """One bounded operation. Retries apply only to pre-observation acquisition failures."""
    from a_share_multifactor.research_workbench import preflight_recipe
    from quant_data_kit.research_dataset import inspect_dataset, update_dataset
    from quant_lab.research import digest

    if mode not in {"tick", "refresh", "observe"}:
        raise ValueError("Unsupported ETF operation")
    now = now or datetime.now(timezone.utc)
    profile = load_profile(path)
    manifest, frames = _snapshot(profile)
    as_of, trading = closed_session(frames, now)
    local = now.astimezone(ZoneInfo("Asia/Shanghai"))
    if mode == "tick" and (not trading or local.time() < SLOTS[0]):
        return {"state": "not_due" if trading else "market_closed", "read_only": True}
    if mode == "tick" and not profile["forward_config"]:
        return {
            "state": "candidate_required",
            "read_only": True,
            "reason": "Confirm and register a future paper candidate before enabling scheduling",
        }
    root = profile["output_root"]
    root.mkdir(parents=True, exist_ok=True)
    with study_lock(root):
        _identity_unchanged(profile)
        history = _current_history(profile)
        slot = max((i for i, boundary in enumerate(SLOTS) if local.time() >= boundary), default=-1)
        today = [r for r in history if r["as_of"] == as_of and r["mode"] == "tick"]
        if mode == "tick" and today:
            previous = today[-1]
            if previous["state"] != "collection_retryable" or slot <= previous["slot"]:
                return {
                    "state": "already_attempted",
                    "read_only": True,
                    "previous_state": previous["state"],
                    "operation_id": previous["id"],
                }
        if profile["forward_config"]:
            config = forward_daily.load_config(profile["forward_config"])
            if config.approval != "approved":
                return {"state": "paused", "read_only": True}
            research_paper.inspect_account(config.account, now=now)
        identifier = uuid.uuid4().hex
        directory = root / identifier
        directory.mkdir()
        operation = {
            "schema": "quant.etf-daily-operation/v1",
            "id": identifier,
            "started_at": now.isoformat(),
            "as_of": as_of,
            "mode": mode,
            "slot": slot,
            "state": "running",
            "stages": [],
            "profile_sha256": profile["profile_sha256"],
            "recipe_sha256": profile["recipe_sha256"],
            "operator_sha256": profile["operator_sha256"],
            "notify": False,
        }

        def publish():
            operation.pop("sha256", None)
            operation["sha256"] = digest(operation)
            research_paper.atomic_json(directory / "operation.json", operation)

        def stage(name, state, **evidence):
            operation["stages"].append({"stage": name, "state": state, **evidence})
            publish()

        publish()
        try:
            _identity_unchanged(profile)
            stage("collection", "running")
            if mode != "observe":
                update_dataset(profile["dataset_root"], end=as_of, overlap_sessions=5)
            manifest, _ = _snapshot(profile)
            quality = inspect_dataset(profile["dataset_root"], manifest["snapshot_id"])
            operation["data"] = {
                "snapshot_id": manifest["snapshot_id"],
                "covered_through": manifest["requested_end"],
                "captured_at": manifest["captured_at"],
                "quality": quality,
            }
            changes = quality.get("update_evidence", {})
            revisions = changes.get("revised_rows", {})
            actions = changes.get("action_changes", {})
            if any(revisions.get(k, 0) for k in ("raw", "adjusted", "benchmark")) or any(
                actions.get(k, 0) for k in ("removed", "revised")
            ):
                raise ValueError("Historical inputs revised; retain both snapshots for review")
            if manifest["requested_end"] != as_of:
                operation["state"] = "collection_retryable"
                stage(
                    "collection",
                    "data_missing",
                    reason="Provider has not published the target session",
                )
                return operation
            stage("collection", "completed")
            recipe_path, recipe = _stage_recipe(profile, manifest, directory)
            operation["research_recipe"] = str(recipe_path)
            operation["research_recipe_sha256"] = file_hash(recipe_path)
            check_recipe = deepcopy(recipe)
            if profile["forward_config"] and mode != "refresh":
                current = forward_daily.load_config(profile["forward_config"])
                account = research_paper.load_account(current.account)
                check_recipe["interval"] = {
                    "start": account["definition"]["holdout_start"],
                    "end": as_of,
                }
                check_recipe.pop("validation", None)
            check = preflight_recipe(check_recipe)
            stage("preflight", "completed" if check["passed"] else "blocked", evidence=check)
            if not check["passed"]:
                operation["state"] = "preflight_blocked"
                return operation
            _identity_unchanged(profile)
            if mode == "refresh" or not profile["forward_config"]:
                operation["state"] = "research_ready" if mode == "refresh" else "candidate_required"
                return operation
            original = forward_daily.load_config(profile["forward_config"])
            staged = yaml.safe_load(original.path.read_text(encoding="utf-8"))
            staged.update(
                account=str(original.account),
                receipt_dir=str(original.receipt_dir),
                input_recipe=str(recipe_path),
                input_recipe_sha256=file_hash(recipe_path),
            )
            forward_path = directory / "forward.yaml"
            forward_path.write_text(yaml.safe_dump(staged, sort_keys=False), encoding="utf-8")
            operation.update(
                forward_config=str(forward_path), forward_config_sha256=file_hash(forward_path)
            )
            plan = forward_daily.assess(forward_path, as_of, now=now)
            stage("frozen_identity", plan["state"], evidence=plan)
            allowed = {"ready", "completed"}
            if mode == "observe":
                allowed |= {"interrupted", "retryable_failure"}
            if plan["state"] not in allowed:
                operation["state"] = plan["state"]
                return operation
            _identity_unchanged(profile)
            if file_hash(original.path) != original.sha256:
                raise ValueError("Forward operator configuration changed")

            def operator_gate():
                _identity_unchanged(profile)
                if file_hash(original.path) != original.sha256:
                    raise ValueError("Forward operator configuration changed")

            receipt = forward_daily.run(forward_path, as_of, now=now, operation_gate=operator_gate)
            stage("observe", receipt["state"], evidence=receipt)
            operation["state"] = receipt["state"]
            if receipt["state"] == "completed":
                operation["execution"] = execution_view(original.account)
                stage("verify", "completed")
            return operation
        except Exception as exc:  # noqa: BLE001 - persist unexpected native failure evidence
            collecting = (
                bool(operation["stages"]) and operation["stages"][-1]["stage"] == "collection"
            )
            # Only transport failures have automatic retries. Data conflicts and unknown failures block.
            transport = isinstance(exc, (TimeoutError, ConnectionError)) or (
                type(exc).__module__.startswith(("requests", "urllib3"))
                and type(exc).__name__
                in {"Timeout", "ConnectTimeout", "ReadTimeout", "ConnectionError"}
            )
            operation.update(
                state="collection_retryable"
                if collecting and (transport or _publication_delay(exc, as_of))
                else "blocked",
                error=str(exc),
                error_type=type(exc).__name__,
            )
            stage(
                operation["stages"][-1]["stage"] if operation["stages"] else "preflight",
                operation["state"],
                reason=str(exc),
            )
            return operation
        finally:
            operation["finished_at"] = datetime.now(timezone.utc).isoformat()
            previous = history[-1] if history else None

            def signature(record):
                return (
                    record["state"],
                    record.get("error"),
                    [(s["stage"], s["state"]) for s in record["stages"]],
                )

            changed = previous is None or signature(previous) != signature(operation)
            new_completion = operation["state"] == "completed" and not any(
                r["state"] == "completed" and r["as_of"] == as_of for r in history
            )
            operation["notify"] = (
                new_completion
                or changed
                and (
                    operation["state"] not in {"research_ready", "completed"}
                    or (previous and previous["state"] not in {"research_ready", "completed"})
                )
            )
            publish()


def _publication_delay(exc, as_of):
    """Only a missing final bar may be retried; earlier gaps remain blocking."""
    prefix = "ETF daily-price coverage failed: "
    if not isinstance(exc, ValueError) or not str(exc).startswith(prefix):
        return False
    try:
        evidence = json.loads(str(exc)[len(prefix) :])
        missing = evidence["missing_after_first_available_bar"]
        return (
            not evidence["pre_listing_or_provider_history_unavailable"]
            and bool(missing)
            and all(dates == [as_of] for dates in missing.values())
        )
    except (ValueError, KeyError, TypeError, AttributeError):
        return False


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("status", "tick", "refresh", "observe"))
    parser.add_argument("--profile", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = (
            status(args.profile)
            if args.command == "status"
            else run(args.profile, mode=args.command)
        )
        print(canonical(result))
        return (
            0
            if result.get("state")
            in {
                None,
                "completed",
                "research_ready",
                "market_closed",
                "not_due",
                "already_attempted",
            }
            else 3
        )
    except (OSError, ValueError, KeyError) as exc:
        print(
            json.dumps(
                {
                    "schema": "quant.etf-daily-error/v1",
                    "state": "configuration_failed",
                    "error": str(exc),
                },
                ensure_ascii=False,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
