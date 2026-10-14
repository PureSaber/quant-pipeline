"""Studio adapter for the existing native weekly ETF research recipe."""

import argparse
import json
from pathlib import Path

from quant_lab.research import canonical, load_recipe

from quant_pipeline.etf_daily import SYMBOLS


def preflight(path):
    from a_share_multifactor.decision_workflow import load_inputs
    from a_share_multifactor.research_workbench import preflight_recipe

    recipe = load_recipe(path)
    if recipe["backend"] != "equity" or recipe["strategy"]["frequency"] != "weekly":
        raise ValueError("ETF workflow requires the frozen weekly equity baseline")
    _, frames = load_inputs(Path(recipe["inputs"]["bundle"]))
    if sorted(frames["raw"].symbol.unique()) != sorted(SYMBOLS):
        raise ValueError("ETF workflow requires the fixed four-symbol universe")
    check = preflight_recipe(recipe)
    return {
        **check,
        "schema_version": "quant.etf-research-preflight/v1",
        "software_preflight": "pass" if check["passed"] else "fail",
        "read_only": True,
        "investable": False,
        "evidence_kind": "synthetic"
        if frames["raw"].source.astype(str).str.contains("synthetic").any()
        else "retrospective",
    }


def inspect_study(output):
    from quant_report_hub.research_workbench import load_study

    study = load_study(output / "study.json")
    records = []
    for row in study["results"]:
        records.append(
            {
                "candidate": row["candidate"]["name"],
                "status": row["status"],
                "metrics": row.get("metrics"),
                "comparison": row.get("comparison"),
                "error": row.get("error"),
                "scope": row.get("scope"),
                "parameters": row["candidate"],
                "code_identity": row.get("identity"),
                "input_identity": row.get("data_identity"),
            }
        )
    return {
        "schema_version": "quant.etf-research-view/v1",
        "read_only": True,
        "investable": False,
        "verification": "native_study_artifacts",
        "evidence_kind": "synthetic"
        if any("synthetic" in str(r.get("scope")) for r in records)
        else "retrospective",
        "study_id": study["recipe"]["study_id"],
        "definition_sha256": study["definition_sha256"],
        "recipe": study["recipe"],
        "attempt_dates": [
            {key: event.get(key) for key in ("attempt_id", "status", "recorded_at")}
            for event in study.get("attempts", [])
        ],
        "comparison_basis": {
            key: study["recipe"].get(key) for key in ("interval", "costs", "validation", "inputs")
        },
        "family_complete": all(row["status"] == "completed" for row in study["results"]),
        "results": records,
        "limitations": [
            "All candidates retained; no automatic winner selection",
            "Historical results are not untouched forward evidence",
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "run", "inspect"))
    parser.add_argument("--recipe", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "inspect":
            value = inspect_study(args.output)
        else:
            value = preflight(args.recipe)
            if args.command == "run" and value["software_preflight"] == "pass":
                from quant_pipeline.research_workbench import run_research

                run_research(args.recipe, args.output)
                value = inspect_study(args.output)
                (args.output / "studio-view.json").write_text(canonical(value), encoding="utf-8")
        print(canonical(value))
        return 3 if value.get("software_preflight") == "fail" else 0
    except (ValueError, OSError, KeyError) as exc:
        print(json.dumps({"state": "failed", "error": str(exc)}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
