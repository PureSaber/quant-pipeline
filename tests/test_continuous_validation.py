import json

import pandas as pd
import pytest
from quant_lab.research import candidates, execute_study
from test_walk_forward_research import FakeLedger, setup_recipe

from quant_pipeline.continuous_validation import ContinuousWalkForwardExecutor, replay_selected_path
from quant_pipeline.family_evidence import nested_research, run_family_evidence
from quant_pipeline.research_validation import (
    paired_bootstrap,
    summarize_validation,
    write_validation_report,
)


class ContinuousFake(FakeLedger):
    def continuous(self, recipe, selections, out):
        assert (out.parent / "selection.json").exists() or out.name.startswith("selected")
        self.selections = selections
        return self(recipe, selections[0]["candidate"], out)


@pytest.mark.parametrize("delay", [0, 1])
@pytest.mark.parametrize("mutation", [None, "missing_delay", "representation", "neutralization"])
def test_continuous_direction_uses_executed_neutralized_signal(tmp_path, delay, mutation):
    recipe, dates = setup_recipe(tmp_path)
    recipe["validation"].update(account_policy="continuous", direction_policy="train_ic")
    recipe["neutralization"] = ["industry"]
    recipe["required_history"] = {"industry": "classification"}
    candidate = candidates(recipe)[0]
    candidate["signal_delay"] = delay

    class Ledger(ContinuousFake):
        def __call__(self, recipe, selected, out):
            result = super().__call__(recipe, selected, out)
            evidence = result["factor_evidence"]
            evidence["neutralization"] = [
                {
                    "factor": name,
                    "horizon": 1,
                    "sessions": 3,
                    "applied_by": ["industry"] if mutation != "neutralization" else [],
                    "neutralized_rank_ic": 0.5,
                }
                for name in selected["factors"]
            ]
            if mutation == "missing_delay":
                evidence.pop("signal_delay")
            if mutation == "representation":
                evidence["signal_representation"] = "unrelated-signal"
            return result

    output = tmp_path / "candidate"
    output.mkdir()
    executor = ContinuousWalkForwardExecutor(Ledger(dates), dates)
    if mutation:
        with pytest.raises(ValueError, match="Training"):
            executor(recipe, candidate, output)
        assert not (output / "continuous").exists()
    else:
        result = executor(recipe, candidate, output)
        assert all(
            set(fold["candidate"]["factors"].values()) == {1}
            for fold in result["validation"]["folds"]
        )


def test_training_updates_feed_one_account_and_selected_path_is_replayed_and_verified(tmp_path):
    recipe, dates = setup_recipe(tmp_path)
    recipe["validation"].update(account_policy="continuous", direction_policy="train_ic")
    ledger = ContinuousFake(dates)
    result = execute_study(
        recipe,
        tmp_path / "study",
        identity={"code": "fixed"},
        data_identity={},
        executor=ContinuousWalkForwardExecutor(ledger, dates),
    )
    assert not result["failed"]
    validation = summarize_validation(result)
    assert "selected_oos_metrics" not in validation and validation["selected_path_requires_replay"]
    path = replay_selected_path(result, validation, ledger, tmp_path / "selected")
    assert path["available"] and path["metrics"]["total_return"] < 0
    assert all(r["candidate"]["name"] == "alternative" for r in path["selections"])
    assert all(set(r["candidate"]["factors"].values()) == {-1} for r in path["selections"])
    assert replay_selected_path(result, validation, ledger, tmp_path / "selected") == path
    write_validation_report(result, tmp_path / "study", selected_path=path)
    assert "continuous" in (tmp_path / "study/validation.html").read_text(encoding="utf-8")
    (tmp_path / "selected/returns.csv").write_text("tampered")
    with pytest.raises(ValueError, match="artifact"):
        replay_selected_path(result, validation, ledger, tmp_path / "selected")


def test_continuous_coverage_guard_and_incomplete_family(tmp_path):
    recipe, dates = setup_recipe(tmp_path)
    recipe["validation"]["account_policy"] = "continuous"
    with pytest.raises(ValueError, match="ordered"):
        ContinuousWalkForwardExecutor(FakeLedger(dates), dates[::-1])
    assert not replay_selected_path({}, {"selection_complete": False}, None, None)["available"]
    out = tmp_path / "candidate"
    out.mkdir()

    class Bad(ContinuousFake):
        def continuous(self, recipe, selections, out):
            value = super().continuous(recipe, selections, out)
            pd.Series([0.0], index=dates[:1]).to_csv(out / "returns.csv")
            return value

    with pytest.raises(ValueError, match="coverage"):
        ContinuousWalkForwardExecutor(Bad(dates), dates)(recipe, candidates(recipe)[0], out)


def test_stationary_sensitivity_and_family_entrypoint(tmp_path):
    import numpy as np
    from quant_lab.trials import TrialRegistry

    result = paired_bootstrap(
        np.random.default_rng(4).normal(size=60),
        method="stationary",
        block_lengths=[3, 6],
        repetitions=100,
    )
    assert result["available"] and len(result["dependence_sensitivity"]["sensitivity"]) == 2
    registry = TrialRegistry(tmp_path / "r.db")
    registry.register_family(
        "f",
        {
            "study_ids": ["not-started"],
            "hypothesis": "h",
            "basis": {
                "currency": "USD",
                "benchmark_id": "cash",
                "cost_policy": "net",
                "periods_per_year": 252,
                "sample_kind": "historical",
            },
        },
    )
    output = tmp_path / "evidence.json"
    result = run_family_evidence(registry.path, "f", output)
    assert not result["audit"]["available"] and json.loads(output.read_text())["methods"] == {}
    data = pd.DataFrame({"value": np.arange(40)}, index=pd.date_range("2024-01-01", periods=40))
    nested = nested_research(
        data,
        {"a": {"window": 3}},
        fit=lambda frame, spec: frame.value.mean(),
        evaluate=lambda model, frame: model,
        outer_train=15,
        outer_test=5,
        inner_train=5,
        inner_test=3,
    )
    assert nested["folds"]


def test_workbench_continuous_entrypoint_shared_registry_and_resume(tmp_path, monkeypatch):
    import yaml

    from quant_pipeline.research_workbench import run_research

    recipe, dates = setup_recipe(tmp_path)
    recipe["validation"]["account_policy"] = "continuous"
    ledger = ContinuousFake(dates)
    monkeypatch.setattr(
        "quant_pipeline.research_workbench.code_identity", lambda: {"code": "frozen"}
    )
    monkeypatch.setattr(
        "quant_pipeline.research_workbench.equity_code_identity", lambda: {"code": "frozen"}
    )
    monkeypatch.setattr(
        "quant_pipeline.research_workbench.input_identity", lambda _: {"input": "frozen"}
    )
    monkeypatch.setattr(
        "a_share_multifactor.research_workbench.EquityResearchExecutor", lambda: ledger
    )
    monkeypatch.setattr(
        "a_share_multifactor.decision_workflow.load_inputs",
        lambda _: ({}, {"calendar": pd.DataFrame({"date": dates})}),
    )
    source = tmp_path / "recipe.yaml"
    source.write_text(yaml.safe_dump(recipe), encoding="utf-8")
    output, registry = tmp_path / "run", tmp_path / "shared.db"
    result = run_research(source, output, registry_path=registry)
    assert not result["failed"]
    assert "连续账户" in (output / "research.html").read_text(encoding="utf-8")
    calls = len(ledger.calls)
    assert run_research(source, output, registry_path=registry) == result
    assert len(ledger.calls) == calls
