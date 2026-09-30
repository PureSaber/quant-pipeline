"""Training-only parameter updates feeding one uninterrupted OOS execution account."""

import json
import os
import tempfile
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import numpy as np
import pandas as pd
from quant_factors.validation import walk_forward_splits
from quant_lab.research import canonical, digest, file_hash, study_lock
from quant_lab.research_options import validate_validation

from quant_pipeline.research_validation import return_metrics, training_directions


class ContinuousWalkForwardExecutor:
    def __init__(self, executor, dates):
        self.executor = executor
        self.dates = pd.DatetimeIndex(pd.to_datetime(dates))
        if not self.dates.is_unique or not self.dates.is_monotonic_increasing:
            raise ValueError("unique ordered validation dates required")

    def __call__(self, recipe, candidate, output):
        settings = validate_validation(recipe["validation"])
        dates = self.dates[
            (self.dates >= recipe["interval"]["start"]) & (self.dates <= recipe["interval"]["end"])
        ]
        splits = walk_forward_splits(
            dates,
            train_size=settings["train_sessions"],
            test_size=settings["test_sessions"],
            embargo_size=settings["embargo_sessions"],
            expanding=settings["expanding"],
        )
        if not splits or len(splits) > 64:
            raise ValueError("continuous validation requires 1–64 complete folds")
        decisions, training, limitations = [], [], set()
        for split in splits:
            train = deepcopy(recipe)
            train.pop("validation", None)
            train["interval"] = {
                "start": str(split.train_start.date()),
                "end": str(split.train_end.date()),
            }
            selected = deepcopy(candidate)
            folder = output / f"train-{split.fold:03d}"
            folder.mkdir()
            result = self.executor(train, selected, folder)
            limitations.update(result.get("limitations", []))
            if settings["direction_policy"] == "train_ic" and candidate["name"] != "buy_hold":
                selected["factors"] = training_directions(
                    recipe, candidate, settings, result.get("factor_evidence")
                )
                if selected != candidate:
                    folder = output / f"train-selected-{split.fold:03d}"
                    folder.mkdir()
                    result = self.executor(train, selected, folder)
                    limitations.update(result.get("limitations", []))
            decision = {
                "fold": split.fold,
                "train": train["interval"],
                "test": {"start": str(split.test_start.date()), "end": str(split.test_end.date())},
                "candidate": selected,
                "training_metrics": result["metrics"],
            }
            decisions.append(decision)
            training.append(result.get("factor_evidence", {}))
        (output / "selection.json").write_text(canonical(decisions), encoding="utf-8")
        oos_recipe = deepcopy(recipe)
        oos_recipe.pop("validation", None)
        oos_recipe["interval"] = {
            "start": decisions[0]["test"]["start"],
            "end": decisions[-1]["test"]["end"],
        }
        path = output / "continuous"
        path.mkdir()
        result = self.executor.continuous(oos_recipe, decisions, path)
        limitations.update(result.get("limitations", []))
        returns = pd.read_csv(path / "returns.csv", index_col=0, parse_dates=True).iloc[:, 0]
        expected = pd.DatetimeIndex(np.concatenate([dates[s.test_indices].values for s in splits]))
        if not returns.index.equals(expected):
            raise ValueError("continuous OOS return coverage differs from planned folds")
        returns.to_csv(output / "returns.csv", header=["net_return"])
        for decision in decisions:
            window = returns.loc[decision["test"]["start"] : decision["test"]["end"]]
            decision["test_metrics"] = return_metrics(window)
            decision["returns"] = [
                {"date": str(day.date()), "net_return": float(value)}
                for day, value in window.items()
            ]
        evaluation = {
            "schema_version": "quant.walk-forward/v2",
            "settings": settings,
            "folds": decisions,
            "training_only_selection": True,
            "account_policy": "single-continuous-oos-account",
            "unused_tail_sessions": int((dates > splits[-1].test_end).sum()),
        }
        (output / "validation.json").write_text(canonical(evaluation), encoding="utf-8")
        result.update(
            validation=evaluation,
            factor_evidence={"scope": "training-only-by-fold", "folds": training},
            limitations=[
                *sorted(limitations),
                "one continuous account; no resets between folds",
                "historical train-only selection is not an untouched prospective holdout",
            ],
        )
        return result


def replay_selected_path(summary, validation, executor, output):
    """Do not splice returns from separately funded candidate accounts after selection."""
    if not validation["selection_complete"]:
        return {"available": False, "reason": "candidate family incomplete"}
    completed = {
        r["candidate"]["name"]: r for r in summary["results"] if r["status"] == "completed"
    }
    decisions, expected_dates = [], []
    for chosen in validation["selection"]:
        fold = completed[chosen["candidate"]]["validation"]["folds"][chosen["fold"]]
        decisions.append(
            {k: deepcopy(fold[k]) for k in ("train", "test", "candidate", "training_metrics")}
        )
        expected_dates.extend(row["date"] for row in fold["returns"])
    recipe = deepcopy(summary["recipe"])
    recipe.pop("validation", None)
    recipe["interval"] = {
        "start": decisions[0]["test"]["start"],
        "end": decisions[-1]["test"]["end"],
    }
    request_sha = digest(
        {"recipe": recipe, "decisions": decisions, "definition": summary["definition_sha256"]}
    )
    output = Path(output)
    # The lock is a stable sibling: never move it when preserving an interrupted
    # output, and hold it across the cache check, execution and final publication.
    with study_lock(output.parent / f".{output.name}-lock"):
        return _replay_locked(recipe, decisions, expected_dates, request_sha, executor, output)


def _replay_locked(recipe, decisions, expected_dates, request_sha, executor, output):
    if output.is_symlink():
        raise ValueError("continuous selected path must not be a symlink")
    cached = output / "selected-path.json"
    if cached.exists():
        return _load_selected(output, request_sha, decisions, expected_dates)
    if output.exists():
        # Preserve legacy interrupted work; never treat directory existence as completion.
        archived = output.with_name(f"{output.name}-interrupted-{uuid4().hex}")
        if archived.resolve().parent != output.resolve().parent:
            raise ValueError("continuous selected path archive escaped its parent")
        output.rename(archived)
    attempt = Path(tempfile.mkdtemp(prefix=f"{output.name}-attempt-", dir=output.parent))
    try:
        result = executor.continuous(recipe, decisions, attempt)
        _durable_json(
            attempt / "execution-result.json",
            {
                "metrics": result["metrics"],
                "limitations": result.get("limitations", []),
            },
        )
        evidence = {
            "available": True,
            "account_policy": "single-continuous-selected-path",
            "metrics": _selected_metrics(attempt, expected_dates),
            "limitations": result.get("limitations", []),
            "selections": decisions,
            "artifacts": str(output),
            "request_sha256": request_sha,
            "artifact_hashes": _selected_artifacts(attempt),
        }
        evidence["evidence_sha256"] = digest(evidence)
        _durable_json(attempt / "selected-path.json", evidence)
        # A completed directory is published only after all artifacts and checks succeed.
        # Concurrent attempts cannot replace a nonempty completed directory.
        attempt.rename(output)
    except Exception as exc:
        _durable_json(attempt / "failure.json", {"type": type(exc).__name__, "message": str(exc)})
        raise
    return evidence


def _durable_json(path, payload):
    with path.open("w", encoding="utf-8") as stream:
        stream.write(canonical(payload))
        stream.flush()
        os.fsync(stream.fileno())


def _selected_artifacts(output):
    artifacts = {}
    for path in sorted(output.rglob("*")):
        if path.is_symlink() or output.resolve() not in path.resolve().parents:
            raise ValueError("continuous selected path artifact escaped its directory")
        if path.is_file() and path != output / "selected-path.json":
            artifacts[path.relative_to(output).as_posix()] = file_hash(path)
    return artifacts


def _selected_metrics(output, expected_dates):
    returns = pd.read_csv(output / "returns.csv", index_col=0, parse_dates=True).iloc[:, 0]
    expected = pd.DatetimeIndex(pd.to_datetime(expected_dates))
    if (
        not expected.is_unique
        or not expected.is_monotonic_increasing
        or not returns.index.equals(expected)
    ):
        raise ValueError("continuous selected path return coverage differs from planned folds")
    execution = json.loads((output / "execution-result.json").read_text(encoding="utf-8"))
    return {**execution["metrics"], **return_metrics(returns)}


def _load_selected(output, request_sha, decisions, expected_dates):
    try:
        previous = json.loads((output / "selected-path.json").read_text(encoding="utf-8"))
        payload = {k: v for k, v in previous.items() if k != "evidence_sha256"}
        if previous.get("evidence_sha256") != digest(payload):
            raise ValueError(
                "continuous selected path summary integrity changed or legacy cache; use a new output"
            )
        if previous["request_sha256"] != request_sha or previous["selections"] != decisions:
            raise ValueError("continuous selected path request changed")
        if previous["artifact_hashes"] != _selected_artifacts(output):
            raise ValueError("continuous selected path artifact changed")
        if previous["metrics"] != _selected_metrics(output, expected_dates):
            raise ValueError("continuous selected path metrics differ from verified returns")
        execution = json.loads((output / "execution-result.json").read_text(encoding="utf-8"))
        if previous["limitations"] != execution["limitations"] or previous["available"] is not True:
            raise ValueError("continuous selected path evidence changed")
        return previous
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("continuous selected path cache is incomplete or invalid") from exc
