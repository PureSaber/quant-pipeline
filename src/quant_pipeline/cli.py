from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from quant_pipeline.dag_runner import DagRunner
from quant_pipeline.dag_schema import load_pipeline_spec
from quant_pipeline.runner import load_pipeline_config, run_pipeline
from quant_pipeline.v2_models import PipelineV2Error


def _print_v2_result(result, *, as_json: bool) -> None:
    payload = {
        "run_id": result.run_id,
        "ok": result.ok,
        "topology": list(result.topology),
        "step_status": {key: value.value for key, value in result.step_status.items()},
        "artifact_index": result.artifact_index,
        "checkpoint_path": str(result.checkpoint_path),
        "checkpoint_hash": result.checkpoint_hash,
    }
    if as_json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return
    for step_id in result.topology:
        print(f"[{result.step_status[step_id].value.upper()}] {step_id}")
    print(f"checkpoint={result.checkpoint_path} sha256={result.checkpoint_hash}")


def cmd_run(args: argparse.Namespace) -> int:
    config_path = Path(args.config)
    config = load_pipeline_config(config_path)
    if config.get("schema_version") == "2.0.0":
        if not args.stack_manifest:
            print("v2 requires --stack-manifest", file=sys.stderr)
            return 2
        try:
            spec = load_pipeline_spec(config_path)
            runner = DagRunner(
                stack_manifest=Path(args.stack_manifest), seed=args.seed, dry_run=args.dry_run
            )
            result = runner.run(spec, args.run_id or spec.name, resume=args.resume)
        except (PipelineV2Error, OSError, ValueError) as exc:
            print(f"v2 pipeline rejected: {exc}", file=sys.stderr)
            return 2
        _print_v2_result(result, as_json=args.json)
        return 0 if result.ok else 1
    if args.resume:
        print("--resume is only available for schema_version 2.0.0", file=sys.stderr)
        return 2
    result = run_pipeline(
        Path(args.config), dry_run=args.dry_run, stop_on_error=not args.continue_on_error
    )
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
    run.add_argument("--stack-manifest")
    run.add_argument("--run-id")
    run.add_argument("--seed", type=int, default=0)
    run.add_argument("--resume", action="store_true")
    run.set_defaults(func=cmd_run)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
