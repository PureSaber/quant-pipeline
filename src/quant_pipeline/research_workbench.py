"""Run preregistered research recipes through explicit local strategy adapters."""

from __future__ import annotations

import argparse
import importlib
import json
import subprocess
import sys
from pathlib import Path

from quant_lab.research import canonical, execute_study, file_hash, load_recipe


def code_identity() -> dict:
    """Record actual clean source revisions; no floating version labels."""
    revisions = {}
    for name in ("quant_pipeline", "quant_lab", "quant_data_kit", "quant_factors"):
        module = importlib.import_module(name)
        root = Path(module.__file__).resolve().parents[2]
        status = subprocess.check_output(["git", "status", "--porcelain"], cwd=root, text=True)
        if status.strip():
            raise ValueError(f"Research execution requires a clean {name} checkout")
        revisions[name] = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip()
    return revisions


def input_identity(recipe: dict) -> dict:
    evidence = {}
    for key, value in recipe["inputs"].items():
        if key == "source":
            evidence[key] = value
            continue
        path = Path(value)
        if path.is_file():
            evidence[key] = file_hash(path)
        elif path.is_dir():
            manifest = path / "manifest.json"
            payload = json.loads(manifest.read_text(encoding="utf-8"))
            evidence[key] = {"manifest": file_hash(manifest)}
            entries = (
                payload.get("files", {})
                if key == "bundle"
                else {k: payload[k] for k in ("history", "original")}
            )
            for name, item in entries.items():
                target = (path / item["file"]).resolve()
                if target.parent != path.resolve() or file_hash(target) != item["sha256"]:
                    raise ValueError(f"Input integrity failure: {key}/{name}")
                evidence[key][name] = item["sha256"]
        else:
            raise FileNotFoundError(path)
    return evidence


class FixtureExecutor:
    """Run old certified fixture backends in their separately frozen environment."""

    def __init__(self, python: Path):
        self.python = python.resolve()
        if not self.python.is_file():
            raise FileNotFoundError(python)
        self.worker = Path(__file__).with_name("research_fixture_worker.py")

    def inspect(self, backend: str) -> dict:
        completed = subprocess.run(
            [str(self.python), "-I", str(self.worker), "inspect", backend],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
        return json.loads(completed.stdout)

    def __call__(self, recipe, candidate, output):
        if candidate["cost_multiplier"] != 1 or candidate["signal_delay"] != 0:
            raise ValueError("Fixture backends support explicit backend parameters only")
        if candidate["factors"] != recipe["factors"] or candidate["strategy"] != recipe["strategy"]:
            raise ValueError("Equity factor/allocation variants do not apply to fixture backends")
        request = output / "request.json"
        request.write_text(canonical({"recipe": recipe, "candidate": candidate}), encoding="utf-8")
        completed = subprocess.run(
            [str(self.python), "-I", str(self.worker), "run", str(request)],
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
        )
        (output / "worker.log").write_text(completed.stdout + completed.stderr, encoding="utf-8")
        if completed.returncode:
            raise RuntimeError("Frozen fixture worker failed; inspect worker.log")
        return json.loads((output / "worker-result.json").read_text(encoding="utf-8"))


def run_research(recipe_path: Path, output: Path, *, fixture_python: Path | None = None) -> dict:
    recipe = load_recipe(recipe_path)
    identity = code_identity()
    data = input_identity(recipe)
    if recipe["backend"] == "equity":
        import a_share_multifactor
        from a_share_multifactor.research_workbench import EquityResearchExecutor
        from a_share_multifactor.run_contract import _code_version, _installed_internal_dependencies

        identity["a_share_multifactor"] = _code_version(
            Path(a_share_multifactor.__file__).resolve().parents[2]
        )
        identity.update(_installed_internal_dependencies())
        executor = EquityResearchExecutor()
    else:
        if fixture_python is None:
            raise ValueError(
                "Fixture backends require --fixture-python with their frozen dependencies"
            )
        executor = FixtureExecutor(fixture_python)
        identity["fixture_runtime"] = executor.inspect(recipe["backend"])
    result = execute_study(recipe, output, identity=identity, data_identity=data, executor=executor)
    from quant_report_hub.research_workbench import render_study

    render_study(output / "study.json", output / "research.html")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("recipe", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fixture-python", type=Path)
    args = parser.parse_args()
    result = run_research(args.recipe, args.output, fixture_python=args.fixture_python)
    print(
        json.dumps(
            {
                "study": result["study_id"],
                "completed": result["completed"],
                "failed": result["failed"],
                "report": str(args.output / "research.html"),
            },
            ensure_ascii=False,
        )
    )
    return 2 if result["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
