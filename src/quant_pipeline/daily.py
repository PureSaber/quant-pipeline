"""Close-of-day research orchestration over the existing producer CLIs."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml


def save(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8"
    )
    temporary.replace(path)


def _input_request(decision_config: Path, as_of: str | None, now: datetime) -> dict:
    settings = yaml.safe_load(decision_config.read_text(encoding="utf-8"))
    if not isinstance(settings, dict):
        raise TypeError("Decision configuration must be a mapping")
    watchlist = settings.get("watchlist")
    if not isinstance(watchlist, list) or not watchlist:
        raise ValueError("Decision configuration requires a non-empty watchlist")
    symbols = []
    for item in watchlist:
        if not isinstance(item, dict) or not str(item.get("symbol", "")).strip():
            raise ValueError("Every watchlist item requires a symbol")
        symbols.append(str(item["symbol"]).strip())
    local = now.astimezone(ZoneInfo("Asia/Shanghai"))
    cutoff = datetime.fromisoformat(as_of).date() if as_of else local.date()
    if as_of is None and local.hour < 16:
        cutoff = (local - timedelta(days=1)).date()
    history_days = int(settings.get("history_days", 730))
    if history_days < 1:
        raise ValueError("history_days must be positive")
    start = cutoff - timedelta(days=history_days)
    return {
        "symbols": symbols,
        "start": start.isoformat(),
        "end": cutoff.isoformat(),
    }


def run_daily(config: Path, *, inputs: Path | None = None, as_of: str | None = None) -> dict:
    settings = yaml.safe_load(config.read_text(encoding="utf-8"))
    root = (config.parent / settings.get("root", "..")).resolve()
    output = (root / settings["output"]).resolve()
    output.mkdir(parents=True, exist_ok=True)
    invoked_at = datetime.now(timezone.utc)
    invocation = invoked_at.strftime("%Y%m%dT%H%M%S%fZ")
    log = output / "operations" / invocation
    log.mkdir(parents=True)
    lock = output / ".pipeline.lock"
    with lock.open("x", encoding="utf-8") as handle:
        handle.write(invocation)
    status = {
        "invocation": invocation,
        "status": "running",
        "steps": [],
        "decision_status": "blocked",
    }
    try:
        python = (
            str((root / settings["python"]).resolve()) if settings.get("python") else sys.executable
        )

        def invoke(name: str, arguments: list[str]) -> int:
            command = [python, *arguments]
            try:
                result = subprocess.run(
                    command,
                    cwd=root,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=settings.get("timeout_seconds", 900),
                    check=False,
                )
                stdout, stderr, code = result.stdout, result.stderr, result.returncode
            except (OSError, subprocess.TimeoutExpired) as exc:
                stdout, stderr, code = "", str(exc), 124
            (log / f"{name}.stdout.txt").write_text(stdout, encoding="utf-8")
            (log / f"{name}.stderr.txt").write_text(stderr, encoding="utf-8")
            status["steps"].append(
                {
                    "name": name,
                    "exit_code": code,
                    "stdout_sha256": hashlib.sha256(stdout.encode()).hexdigest(),
                    "stderr_sha256": hashlib.sha256(stderr.encode()).hexdigest(),
                }
            )
            return code

        selected_inputs = inputs.resolve() if inputs else None
        data_config = settings.get("data_config")
        if selected_inputs is None and data_config:
            decision_config = (root / settings["decision_config"]).resolve()
            request = _input_request(decision_config, as_of, invoked_at)
            selected_inputs = output / "input-snapshots" / invocation
            status["inputs"] = str(selected_inputs)
            invoke(
                "inputs",
                [
                    "-m",
                    "quant_data_kit.research_inputs",
                    "--config",
                    str((root / data_config).resolve()),
                    "--output",
                    str(selected_inputs),
                    "--symbols",
                    *request["symbols"],
                    "--start",
                    request["start"],
                    "--end",
                    request["end"],
                    "--captured-at",
                    invoked_at.isoformat(),
                ],
            )
        elif selected_inputs is not None:
            status["inputs"] = str(selected_inputs)

        producer = [
            "-m",
            "a_share_multifactor.decision_workflow",
            "--config",
            str(root / settings["decision_config"]),
            "--output",
            str(output),
        ]
        if selected_inputs:
            producer += ["--inputs", str(selected_inputs)]
        if as_of:
            producer += ["--as-of", as_of]
        pointer = output / "latest.json"
        old_pointer = pointer.read_bytes() if pointer.exists() else None
        code = invoke("decision", producer)
        if not pointer.exists() or pointer.read_bytes() == old_pointer:
            # A process crash may happen before the producer publishes its own blocked card.
            card = {
                "schema_version": "quant.decision/v1",
                "run_id": invocation,
                "status": "blocked",
                "as_of": as_of or datetime.now(timezone.utc).date().isoformat(),
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "valid_until": None,
                "scope": "paper_simulation_only",
                "data_quality": {"passed": False},
                "validation": {"passed": False},
                "current_positions": [],
                "targets": [],
                "proposed_trades": [],
                "estimated_cost": {},
                "risk": {},
                "evidence": {"operation_log": str(log)},
                "reasons": [f"decision process did not publish; exit={code}"],
            }
            failed_run = output / invocation
            save(failed_run / "decision.json", card)
            save(
                pointer,
                {
                    "run_id": invocation,
                    "status": "blocked",
                    "decision": str(failed_run / "decision.json"),
                },
            )
        latest = json.loads(pointer.read_text(encoding="utf-8"))
        status["decision_status"] = latest["status"]
        decision = Path(latest["decision"])
        status["decision"] = str(decision)
        # Scan failed/blocked attempts too. The registry is maintained by the producer.
        invoke(
            "index",
            [
                "-m",
                "quant_lab.cli",
                "--db",
                str(output / "experiments.db"),
                "scan",
                "--root",
                str(output),
                "--project",
                "a-share-multifactor",
            ],
        )
        if settings.get("account_config"):
            invoke(
                "account",
                [
                    "-m",
                    "quant_portfolio.account_import",
                    "--config",
                    str(root / settings["account_config"]),
                    "--decision",
                    str(decision),
                    "--output",
                    str(log / "account.json"),
                ],
            )
        report_command = settings.get("report_command")
        if report_command:
            args = [
                str(arg)
                .replace("{output}", str(output))
                .replace("{db}", str(output / "experiments.db"))
                for arg in report_command
            ]
            invoke("report", args)
        else:
            invoke(
                "report",
                [
                    "-m",
                    "quant_lab.cli",
                    "--db",
                    str(output / "experiments.db"),
                    "export",
                    "html",
                    "--out",
                    str(output / "experiments.html"),
                ],
            )
        status["status"] = (
            "completed"
            if all(step["exit_code"] == 0 for step in status["steps"])
            and latest["status"] != "blocked"
            else "failed"
        )
    except (OSError, ValueError, KeyError, TypeError) as exc:
        status.update(status="failed", error=f"{type(exc).__name__}: {exc}")
    finally:
        save(log / "operation.json", status)
        save(output / "operation-latest.json", status)
        lock.unlink()
    return status


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the close-of-day decision, account, index and report flow"
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--inputs", type=Path)
    parser.add_argument("--as-of")
    args = parser.parse_args()
    status = run_daily(args.config.resolve(), inputs=args.inputs, as_of=args.as_of)
    print(json.dumps(status, ensure_ascii=False, indent=2))
    return 0 if status["status"] == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
