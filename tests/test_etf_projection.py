import json

import pandas as pd
from quant_lab import load_and_validate_standard_run
from quant_lab.research import candidates, file_hash, load_recipe

from quant_pipeline.etf_projection import cost_window, project_execution
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


def test_cost_window_excludes_prior_orders_even_when_their_fills_arrive_later():
    old, new = "2026-10-13T07:00:00Z", "2026-10-14T07:00:00Z"
    frames = {
        "orders": pd.DataFrame(
            [
                {
                    "event_time": at,
                    "order_id": identifier,
                    "instrument_id": "510300",
                    "side": "buy",
                    "quantity_units": 1000,
                    "quantity_scale": 0,
                }
                for at, identifier in ((old, "old"), (new, "new"))
            ]
        ),
        "fills": pd.DataFrame(
            [
                {
                    "event_time": new,
                    "order_id": identifier,
                    "fill_id": identifier,
                    "instrument_id": "510300",
                    "side": "buy",
                    "quantity_units": 600,
                    "quantity_scale": 0,
                }
                for identifier in ("old", "new")
            ]
        ),
        "costs": pd.DataFrame(
            [
                {
                    "event_time": new,
                    "fill_id": identifier,
                    "currency": "CNY",
                    "cost_type": "taker",
                    "amount_units": 500,
                    "amount_scale": 2,
                }
                for identifier in ("old", "new")
            ]
        ),
    }
    window = cost_window(
        frames, {"checked_at": new, "cost_forecast": {"estimated_cost_amount": "12"}}, new
    )
    assert window["fees"] == [["CNY", "taker", "5.00"]]
    assert window["quantities"] == [["510300", "buy", "1000", "600", "400"]]
