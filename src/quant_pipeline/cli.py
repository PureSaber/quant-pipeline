from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from quant_pipeline.runner import run_pipeline


def cmd_run(args: argparse.Namespace) -> int:
    result = run_pipeline(Path(args.config), dry_run=args.dry_run, stop_on_error=not args.continue_on_error)
    payload = {
        "name": result.name,
        "ok": result.ok,
        "steps": [
            {
                "name": s.name,
                "command": s.command,
                "exit_code": s.exit_code,
                "duration_s": s.duration_s,
                "stderr": s.stderr[-500:] if s.stderr else "",
            }
            for s in result.steps
        ],
    }
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        for step in result.steps:
            status = "OK" if step.exit_code == 0 else "FAIL"
            print(f"[{status}] {step.name} ({step.duration_s}s) exit={step.exit_code}")
            if step.exit_code != 0 and step.stderr:
                print(step.stderr.strip())
    return 0 if result.ok else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="quant-pipe", description="Run quant stack YAML pipelines")
    sub = p.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Execute a pipeline config")
    run.add_argument("--config", required=True)
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--continue-on-error", action="store_true")
    run.add_argument("--json", action="store_true")
    run.set_defaults(func=cmd_run)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
