"""Standalone adapter executed by the unchanged frozen interpreter (-I -B).

Only orchestration lives here. Financial calculations, input policy, timing guards,
account writes and ledger verification always come from the installed native stack.
The protocol accepts no clock, strategy, account override or shell command.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import runpy
import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def process_token(pid):
    """Read process creation identity without importing a newer package into this runtime."""
    if os.name != "nt":
        path = Path(f"/proc/{pid}/stat")
        return path.read_text().split(")", 1)[1].split()[19] if path.exists() else None
    import ctypes
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    handle = kernel.OpenProcess(0x1000, False, pid)
    if not handle:
        return None
    try:
        code = wintypes.DWORD()
        if not kernel.GetExitCodeProcess(handle, ctypes.byref(code)) or code.value != 259:
            return None
        times = [wintypes.FILETIME() for _ in range(4)]
        if not kernel.GetProcessTimes(handle, *(ctypes.byref(t) for t in times)):
            raise OSError("Cannot verify worker process identity")
        return f"{pid}:{times[0].dwHighDateTime}:{times[0].dwLowDateTime}"
    finally:
        kernel.CloseHandle(handle)


def execute_request(payload):
    marker = None
    if payload.get("action") in {"collect", "prepare", "execute"}:
        directory = Path(payload["directory"]).resolve()
        if directory.parent != Path(payload["config"]["output_root"]).resolve():
            raise ValueError("Worker output must be one direct operation directory")
        marker = directory / f"worker-{os.getpid()}.json"
        running = {
            "pid": os.getpid(),
            "process_token": process_token(os.getpid()),
            "action": payload["action"],
            "state": "running",
        }
        marker.write_text(json.dumps(running), encoding="utf-8")
    try:
        return main(payload)
    finally:
        if marker:
            running.update(state="finished", finished_at=datetime.now(timezone.utc).isoformat())
            marker.write_text(json.dumps(running), encoding="utf-8")


def verify(config):
    root = Path(config["frozen_root"]).resolve()
    if Path(sys.prefix).resolve() != root / "env":
        raise ValueError("FROZEN_IDENTITY: wrong interpreter")
    if sha(root / "installed-stack.json") != config["frozen_stack_sha256"]:
        raise ValueError("FROZEN_IDENTITY: installation receipt changed")
    for path, expected in config["protected_files"].items():
        if sha(path) != expected:
            raise ValueError("FROZEN_IDENTITY: protected file changed: " + path)
    if sha(config["daily_profile"]) != config["daily_profile_sha256"]:
        raise ValueError("FROZEN_IDENTITY: daily profile changed")
    module = runpy.run_path(str(root / "profile/bootstrap.py"))
    stack = json.loads((root / "profile/stack.json").read_text("utf-8"))
    actual = module["installed_identity"](stack)
    if actual != json.loads((root / "installed-stack.json").read_text("utf-8")):
        raise ValueError("FROZEN_IDENTITY: imported package tree changed")
    from quant_pipeline import etf_daily, forward_daily, research_paper

    profile = etf_daily.load_profile(config["daily_profile"])
    forward = forward_daily.load_config(profile["forward_config"])
    account = research_paper.load_account(forward.account)
    if account["definition_sha256"] != forward.definition_sha256:
        raise ValueError("FROZEN_IDENTITY: account definition changed")
    if research_paper.equity_code_identity() != account["definition"]["code_identity"]:
        raise ValueError("FROZEN_IDENTITY: native code differs from account")
    research_paper.inspect_account(forward.account)
    return profile, forward


def exception_chain(exc):
    pending, seen, rows = [exc], set(), []
    while pending and len(rows) < 16:
        current = pending.pop(0)
        if not isinstance(current, BaseException) or id(current) in seen:
            continue
        seen.add(id(current))
        rows.append(
            {
                "type": type(current).__name__,
                "module": type(current).__module__,
                "message": str(current),
                "reason": str(getattr(current, "reason", "")),
            }
        )
        pending.extend([current.__cause__, current.__context__, getattr(current, "reason", None)])
        pending.extend(arg for arg in current.args if isinstance(arg, BaseException))
    return rows


def main(request):
    if set(request) - {"action", "config", "directory", "as_of", "forward_sha256"}:
        raise ValueError("Unknown frozen worker fields")
    action, config = request["action"], request["config"]
    if action not in {"inspect", "collect", "prepare", "execute", "assess"}:
        raise ValueError("Unknown frozen worker action")
    profile, forward = verify(config)
    from quant_pipeline import etf_daily, forward_daily, research_paper

    if action == "inspect":
        value = etf_daily.status(profile["profile_path"])
        _, frames = etf_daily._snapshot(profile)
        value["calendar"] = [str(x)[:10] for x in frames["calendar"]["date"]]
        value["worker_sha256"] = sha(__file__)
        return value
    directory = Path(request["directory"]).resolve()
    if directory.parent != Path(config["output_root"]).resolve():
        raise ValueError("Worker output must be one direct operation directory")
    as_of = request["as_of"]
    _, frames = etf_daily._snapshot(profile)
    expected, trading = etf_daily.closed_session(frames, datetime.now(timezone.utc))
    if (
        as_of != expected
        or not trading
        or as_of
        != datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()
    ):
        raise ValueError("HISTORICAL_BACKFILL: only the actual current closed session is allowed")
    if action == "collect":
        from quant_pipeline.etf_collection import collect

        stages = []

        def stage(name, state, **evidence):
            stages.append(
                {
                    "stage": name,
                    "state": state,
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                    **evidence,
                }
            )
            research_paper.atomic_json(directory / "collection-progress.json", stages)

        try:
            collect(profile, directory, as_of, stage)
        except Exception as exc:  # noqa: BLE001 - retain typed nested transport evidence
            return {
                "state": "failed",
                "stage": stages[-1]["stage"] if stages else "identity",
                "stages": stages,
                "error": str(exc),
                "exceptions": exception_chain(exc),
                "publication_delay": etf_daily._publication_delay(exc, as_of),
            }
        return {"state": "collected", "stages": stages}
    if action == "prepare":
        import pandas as pd
        import yaml
        from a_share_multifactor.research_workbench import preflight_recipe
        from quant_data_kit.research_dataset import inspect_dataset
        from quant_lab.research import file_hash

        manifest, _ = etf_daily._snapshot(profile)
        quality = inspect_dataset(profile["dataset_root"], manifest["snapshot_id"])
        changes = quality.get("update_evidence", {})
        if any(
            changes.get("revised_rows", {}).get(k, 0) for k in ("raw", "adjusted", "benchmark")
        ) or any(changes.get("action_changes", {}).get(k, 0) for k in ("removed", "revised")):
            raise ValueError("DATA_REVISED: Historical inputs revised; retain snapshots for review")
        data = {
            "snapshot_id": manifest["snapshot_id"],
            "covered_through": manifest["requested_end"],
            "captured_at": etf_daily.market_capture(profile, manifest),
            "quality": quality,
        }
        if data["covered_through"] != as_of:
            return {"state": "publication_delay", "data": data}
        if pd.Timestamp(data["captured_at"]) < pd.Timestamp(
            as_of, tz="Asia/Shanghai"
        ) + pd.Timedelta(hours=15):
            raise ValueError("DATA_STALE: market data captured before close")
        path, recipe = etf_daily._stage_recipe(profile, manifest, directory)
        check_recipe = deepcopy(recipe)
        account = research_paper.load_account(forward.account)
        check_recipe["interval"] = {"start": account["definition"]["holdout_start"], "end": as_of}
        check_recipe.pop("validation", None)
        check = preflight_recipe(check_recipe)
        value = {
            "state": "preflight_blocked",
            "data": data,
            "preflight": check,
            "research_recipe": str(path),
            "research_recipe_sha256": file_hash(path),
        }
        if not check["passed"]:
            return value
        staged = yaml.safe_load(forward.path.read_text("utf-8"))
        staged.update(
            account=str(forward.account),
            receipt_dir=str(forward.receipt_dir),
            input_recipe=str(path),
            input_recipe_sha256=file_hash(path),
        )
        staged_path = directory / "forward.yaml"
        staged_path.write_text(yaml.safe_dump(staged, sort_keys=False), encoding="utf-8")
        assessment = forward_daily.assess(staged_path, as_of)
        value.update(
            state=assessment["state"],
            forward=assessment,
            forward_config=str(staged_path),
            forward_config_sha256=file_hash(staged_path),
        )
        return value
    path = directory / "forward.yaml"
    if sha(path) != request["forward_sha256"]:
        raise ValueError("FROZEN_IDENTITY: staged forward configuration changed")
    staged = forward_daily.load_config(path)
    if (staged.account, staged.account_id, staged.definition_sha256, staged.receipt_dir) != (
        forward.account,
        forward.account_id,
        forward.definition_sha256,
        forward.receipt_dir,
    ):
        raise ValueError("FROZEN_IDENTITY: staged operator binding differs")
    if action == "assess":
        return forward_daily.assess(path, as_of)
    receipt = forward_daily.run(path, as_of, operation_gate=lambda: verify(config))
    result = {"state": receipt["state"], "native_receipt": receipt}
    if receipt["state"] == "completed":
        result["execution"] = etf_daily.execution_view(forward.account)
        research_paper.inspect_account(forward.account)
    else:
        result["error"] = receipt.get("reason", "Native observation did not complete")
    return result


if __name__ == "__main__":
    try:
        payload = json.load(sys.stdin)
        # Provider/library progress must never corrupt the JSON protocol.
        with contextlib.redirect_stdout(sys.stderr):
            response = execute_request(payload)
    except Exception as error:  # noqa: BLE001 - boundary returns evidence, never success
        response = {"state": "failed", "error": str(error), "exceptions": exception_chain(error)}
    print(json.dumps(response, ensure_ascii=False, allow_nan=False))
