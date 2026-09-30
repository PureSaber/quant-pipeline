import json
import multiprocessing
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from quant_lab.research import candidates, digest, execute_study
from test_continuous_validation import ContinuousFake
from test_walk_forward_research import setup_recipe

from quant_pipeline.continuous_validation import ContinuousWalkForwardExecutor, replay_selected_path
from quant_pipeline.research_validation import summarize_validation


def _paused_replay(summary, validation, dates, output, stage, ready, release, results):
    """Run in a separate process; pause only to make the race deterministic."""
    original_exists, original_rename = Path.exists, Path.rename

    def exists(path):
        answer = original_exists(path)
        if stage == "checked" and path == output / "selected-path.json":
            ready.set()
            if not release.wait(30):
                raise TimeoutError("test did not release cache check")
        return answer

    def rename(path, target):
        result = original_rename(path, target)
        if stage == "published" and target == output:
            ready.set()
            if not release.wait(30):
                raise TimeoutError("test did not release publication")
        return result

    try:
        with patch.object(Path, "exists", exists), patch.object(Path, "rename", rename):
            result = replay_selected_path(summary, validation, ContinuousFake(dates), output)
        results.put({"available": result["available"]})
    except (OSError, ValueError, RuntimeError, AssertionError) as exc:
        results.put({"error": repr(exc)})


@pytest.mark.parametrize("stage", ["checked", "published"])
def test_concurrent_replay_cannot_move_or_replace_completed_output(study, tmp_path, stage):
    summary, validation, ledger = study
    output = tmp_path / "selected"
    context = multiprocessing.get_context("spawn")
    ready, release, results = context.Event(), context.Event(), context.Queue()
    process = context.Process(
        target=_paused_replay,
        args=(summary, validation, ledger.dates, output, stage, ready, release, results),
    )
    process.start()
    calls = len(ledger.calls)
    try:
        assert ready.wait(20), "worker did not reach the race window"
        # A competing process fails closed without touching the active output.
        with pytest.raises(OSError):
            replay_selected_path(summary, validation, ledger, output)
        assert len(ledger.calls) == calls
    finally:
        release.set()
        process.join(timeout=30)
        if process.is_alive():
            process.terminate()
            process.join(timeout=10)
    assert process.exitcode == 0
    assert results.get(timeout=5) == {"available": True}
    cached = output / "selected-path.json"
    before = cached.read_bytes()
    assert replay_selected_path(summary, validation, ledger, output)["available"]
    assert cached.read_bytes() == before and len(ledger.calls) == calls
    assert not list(tmp_path.glob("selected-interrupted-*"))


@pytest.fixture
def study(tmp_path):
    recipe, dates = setup_recipe(tmp_path)
    recipe["validation"]["account_policy"] = "continuous"
    ledger = ContinuousFake(dates)
    summary = execute_study(
        recipe,
        tmp_path / "study",
        identity={"code": "fixed"},
        data_identity={},
        executor=ContinuousWalkForwardExecutor(ledger, dates),
    )
    assert summary["failed"] == 0
    return summary, summarize_validation(summary), ledger


@pytest.mark.parametrize("field", ["metrics", "selections", "artifact_hashes", "available"])
def test_modified_summary_is_never_resumed(study, tmp_path, field):
    summary, validation, ledger = study
    output = tmp_path / "selected"
    replay_selected_path(summary, validation, ledger, output)
    cached = output / "selected-path.json"
    payload = json.loads(cached.read_text())
    if field == "metrics":
        payload[field]["total_return"] = 99.0
    elif field == "selections":
        payload[field][0]["candidate"]["name"] = "forged"
    elif field == "artifact_hashes":
        payload[field] = {}
    else:
        payload[field] = False
    cached.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="selected path"):
        replay_selected_path(summary, validation, ledger, output)


def test_metrics_are_checked_against_returns_even_with_recomputed_summary_hash(study, tmp_path):
    summary, validation, ledger = study
    output = tmp_path / "selected"
    replay_selected_path(summary, validation, ledger, output)
    cached = output / "selected-path.json"
    payload = json.loads(cached.read_text())
    payload["metrics"]["total_return"] = 99.0
    payload.pop("evidence_sha256", None)
    payload["evidence_sha256"] = digest(payload)
    cached.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="metrics"):
        replay_selected_path(summary, validation, ledger, output)


def test_interruption_keeps_evidence_and_next_call_retries(study, tmp_path):
    summary, validation, ledger = study
    original = ledger.continuous
    calls = []

    def interrupted(recipe, selections, out):
        calls.append(out)
        if len(calls) == 1:
            (out / "partial.txt").write_text("keep this diagnostic")
            raise RuntimeError("temporary failure")
        return original(recipe, selections, out)

    ledger.continuous = interrupted
    output = tmp_path / "selected"
    with pytest.raises(RuntimeError, match="temporary"):
        replay_selected_path(summary, validation, ledger, output)
    result = replay_selected_path(summary, validation, ledger, output)
    assert result["available"] and len(calls) == 2
    assert (calls[0] / "partial.txt").read_text() == "keep this diagnostic"
    assert replay_selected_path(summary, validation, ledger, output) == result
    assert len(calls) == 2


def test_old_incomplete_directory_is_preserved_and_retried(study, tmp_path):
    summary, validation, ledger = study
    output = tmp_path / "selected"
    output.mkdir()
    (output / "partial.txt").write_text("old interrupted work")
    assert replay_selected_path(summary, validation, ledger, output)["available"]
    assert any(
        p.read_text() == "old interrupted work" for p in tmp_path.glob("selected-*/partial.txt")
    )


def test_selected_replay_requires_full_planned_return_coverage(study, tmp_path):
    summary, validation, ledger = study
    original = ledger.continuous

    def missing_session(recipe, selections, out):
        result = original(recipe, selections, out)
        path = out / "returns.csv"
        pd.read_csv(path).iloc[1:].to_csv(path, index=False)
        return result

    ledger.continuous = missing_session
    with pytest.raises(ValueError, match="coverage"):
        replay_selected_path(summary, validation, ledger, tmp_path / "selected")


def test_training_and_execution_limitations_survive_wrapper(tmp_path):
    recipe, dates = setup_recipe(tmp_path)
    recipe["validation"]["account_policy"] = "continuous"

    class Ledger(ContinuousFake):
        def __call__(self, recipe, candidate, out):
            result = super().__call__(recipe, candidate, out)
            result["limitations"] = [
                "training limitation" if "train" in out.name else "execution limitation"
            ]
            return result

    output = tmp_path / "candidate"
    output.mkdir()
    result = ContinuousWalkForwardExecutor(Ledger(dates), dates)(
        deepcopy(recipe), candidates(recipe)[0], output
    )
    assert {"training limitation", "execution limitation"} <= set(result["limitations"])
