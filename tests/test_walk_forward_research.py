from copy import deepcopy

import pandas as pd
import pytest
from quant_lab.research import candidates, execute_study, load_recipe
from quant_lab.research_options import validate_validation

from quant_pipeline.research_demo import create_demo
from quant_pipeline.research_validation import (
    WalkForwardExecutor,
    paired_bootstrap,
    return_metrics,
    summarize_validation,
    write_validation_report,
)


def setup_recipe(tmp_path):
    recipe = load_recipe(create_demo(tmp_path / "data"))
    dates = pd.bdate_range("2024-01-01", periods=25)
    recipe["interval"] = {"start": str(dates[0].date()), "end": str(dates[-1].date())}
    recipe["validation"] = {
        "train_sessions": 6,
        "test_sessions": 5,
        "selection_metric": "total_return",
    }
    recipe["diagnostics"] = {}
    recipe["variants"] = [{"name": "alternative", "strategy": {"frequency": "monthly"}}]
    return recipe, dates


class FakeLedger:
    def __init__(self, dates):
        self.dates = dates
        self.calls = []

    def __call__(self, recipe, candidate, out):
        self.calls.append((deepcopy(recipe), deepcopy(candidate)))
        interval = recipe["interval"]
        dates = self.dates[(self.dates >= interval["start"]) & (self.dates <= interval["end"])]
        # Training favours the alternative; its test performance is deliberately worse.
        training = "train" in out.name
        rate = (0.03 if training else -0.02) if candidate["name"] == "alternative" else 0.01
        returns = pd.Series([0.0] + [rate] * (len(dates) - 1), index=dates)
        returns.to_csv(out / "returns.csv", header=["net_return"])
        signal_delay = candidate["signal_delay"]
        return {
            "metrics": {**return_metrics(returns), "fills": 1, "cost_total": 5},
            "factor_evidence": {
                "signal_delay": signal_delay,
                "signal_representation": (
                    "lagged-cross-sectional-percentile-rank" if signal_delay else "factor-value"
                ),
                "ic_decay": [
                    {"factor": name, "horizon": 1, "sessions": 3, "rank_ic": -0.5}
                    for name in candidate["factors"]
                ],
            },
            "comparison": {"start": interval["start"], "end": interval["end"], "currency": "CNY"},
            "scope": "synthetic-test",
        }


def test_train_only_selection_and_complete_nonoverlapping_test_windows(tmp_path):
    recipe, dates = setup_recipe(tmp_path)
    ledger = FakeLedger(dates)
    root = tmp_path / "study"
    result = execute_study(
        recipe,
        root,
        identity={"code": "fixed"},
        data_identity={},
        executor=WalkForwardExecutor(ledger, dates),
    )
    assert result["failed"] == 0
    report = summarize_validation(result)
    assert report["selection_complete"]
    assert {r["candidate"] for r in report["selection"]} == {"alternative"}
    assert report["selected_oos_metrics"]["total_return"] < 0
    for candidate in result["results"]:
        evaluation = candidate["validation"]
        assert evaluation["unused_tail_sessions"] == 3
        days = [r["date"] for fold in evaluation["folds"] for r in fold["returns"]]
        assert len(days) == len(set(days)) == 15
        assert all(f["train"]["end"] < f["test"]["start"] for f in evaluation["folds"])
    write_validation_report(result, root)
    assert "alternative" in (root / "validation.html").read_text(encoding="utf-8")
    original_count = len(ledger.calls)
    execute_study(
        recipe,
        root,
        identity={"code": "fixed"},
        data_identity={},
        executor=WalkForwardExecutor(ledger, dates),
    )
    assert len(ledger.calls) == original_count


def test_direction_learning_uses_only_training_evidence_and_is_frozen_before_test(tmp_path):
    recipe, dates = setup_recipe(tmp_path)
    recipe["validation"]["direction_policy"] = "train_ic"
    candidate = candidates(recipe)[0]
    out = tmp_path / "candidate"
    out.mkdir()
    ledger = FakeLedger(dates)
    result = WalkForwardExecutor(ledger, dates)(recipe, candidate, out)
    assert all(
        set(f["candidate"]["factors"].values()) == {-1} for f in result["validation"]["folds"]
    )
    for path in out.glob("fold-*/selection.json"):
        assert path.exists()
    assert candidate["factors"]["momentum_20d"] == 1


def test_direction_learning_matches_actual_neutralized_signal(tmp_path):
    recipe, dates = setup_recipe(tmp_path)
    recipe["validation"]["direction_policy"] = "train_ic"
    recipe["neutralization"] = ["industry"]
    recipe["required_history"] = {"industry": "classification"}
    ledger = FakeLedger(dates)

    def executor(spec, candidate, out):
        result = ledger(spec, candidate, out)
        result["factor_evidence"]["neutralization"] = [
            {
                "factor": name,
                "horizon": 1,
                "sessions": 3,
                "applied_by": ["industry"],
                "neutralized_rank_ic": 0.5,
            }
            for name in candidate["factors"]
        ]
        return result

    out = tmp_path / "neutralized"
    out.mkdir()
    result = WalkForwardExecutor(executor, dates)(recipe, candidates(recipe)[0], out)
    assert all(
        set(fold["candidate"]["factors"].values()) == {1} for fold in result["validation"]["folds"]
    )


@pytest.mark.parametrize(
    "signal_delay,mutation,message",
    [
        (1, "missing", "delay evidence"),
        (1, "wrong_delay", "delay evidence"),
        (1, "wrong_representation", "signal representation"),
        (0, "wrong_representation", "signal representation"),
    ],
)
def test_direction_learning_rejects_mismatched_signal_evidence(
    tmp_path, signal_delay, mutation, message
):
    recipe, dates = setup_recipe(tmp_path)
    recipe["validation"]["direction_policy"] = "train_ic"
    candidate = candidates(recipe)[0]
    candidate["signal_delay"] = signal_delay
    ledger = FakeLedger(dates)

    def executor(spec, selected, out):
        result = ledger(spec, selected, out)
        evidence = result["factor_evidence"]
        if mutation == "missing":
            evidence.pop("signal_delay")
            evidence.pop("signal_representation")
        elif mutation == "wrong_delay":
            evidence["signal_delay"] = 0
        else:
            evidence["signal_representation"] = (
                "factor-value"
                if evidence["signal_representation"] != "factor-value"
                else "lagged-cross-sectional-percentile-rank"
            )
        return result

    output = tmp_path / "mismatched-evidence"
    output.mkdir()
    with pytest.raises(ValueError, match=message):
        WalkForwardExecutor(executor, dates)(recipe, candidate, output)


def test_incomplete_family_disables_fdr_and_no_failed_candidate_is_dropped(tmp_path):
    recipe, dates = setup_recipe(tmp_path)
    root = tmp_path / "study"
    result = execute_study(
        recipe,
        root,
        identity={"code": "x"},
        data_identity={},
        executor=WalkForwardExecutor(FakeLedger(dates), dates),
    )
    result["results"][-1]["status"] = "failed"
    report = summarize_validation(result)
    assert not report["selection_complete"] and not report["all_candidates_completed"]
    assert "selected_oos_metrics" not in report
    assert "fdr_unavailable" in report


@pytest.mark.parametrize(
    "value",
    [
        {"train_sessions": 2, "test_sessions": 2, "step_sessions": 1},
        {"train_sessions": True, "test_sessions": 2},
        {"train_sessions": 5, "test_sessions": 2, "direction_horizon": 20},
        {"train_sessions": 5, "test_sessions": 2, "fdr_alpha": float("nan")},
        {
            "train_sessions": 5,
            "test_sessions": 2,
            "direction_policy": "train_ic",
            "direction_horizon": 20,
            "embargo_sessions": 20,
        },
    ],
)
def test_validation_contract_rejects_ambiguous_splits(value):
    with pytest.raises(ValueError):
        validate_validation(value)


def test_bootstrap_and_return_guards():
    assert not paired_bootstrap([0.1] * 3)["available"]
    assert paired_bootstrap([0.001] * 100)["ci_95"][0] > 0
    with pytest.raises(ValueError):
        return_metrics(pd.Series([float("nan")]))


def test_insufficient_windows_and_missing_sessions_fail(tmp_path):
    recipe, dates = setup_recipe(tmp_path)
    recipe["validation"]["train_sessions"] = 25
    with pytest.raises(ValueError, match="complete folds"):
        WalkForwardExecutor(FakeLedger(dates), dates)(recipe, candidates(recipe)[0], tmp_path)
