import json

from quant_lab import load_and_validate_standard_run
from quant_lab.research import candidates, file_hash, load_recipe

from quant_pipeline.etf_projection import project_execution
from quant_pipeline.research_demo import create_demo


def test_real_native_parquet_projection_and_liquidated_positions(tmp_path, monkeypatch):
    from a_share_multifactor import run_contract
    from a_share_multifactor.research_workbench import EquityResearchExecutor

    monkeypatch.setattr(run_contract, "_code_version", lambda *_: "a" * 40)
    monkeypatch.setattr(
        run_contract, "_installed_internal_dependencies", lambda: {"test": "a" * 40}
    )

    recipe = load_recipe(create_demo(tmp_path / "demo"))
    recipe["interval"] = {"start": "2023-04-03", "end": "2023-05-30"}
    recipe.pop("validation", None)
    output = tmp_path / "run"
    output.mkdir()
    EquityResearchExecutor()(recipe, candidates(recipe)[0], output)
    manifest = load_and_validate_standard_run(output)
    diagnostics = json.loads((output / "execution_diagnostics.json").read_text())
    hashes = {
        p.relative_to(output).as_posix(): file_hash(p) for p in output.rglob("*") if p.is_file()
    }
    view = project_execution(output, manifest, diagnostics, hashes)
    assert view["tables"]["positions"]["total_rows"] > 0
    assert "quantity" in view["tables"]["positions"]["columns"]
    assert view["cost_totals"]
    assert view["cost_assumptions"] == recipe["costs"]
    # A risk liquidation is an explicit empty target, not missing target data.
    diagnostics["risk_checks"].append({"execution_target": {}, "session": "2023-05-30"})
    view = project_execution(output, manifest, diagnostics, hashes)
    assert all(row[1] == "0" for row in view["position_comparison"])
