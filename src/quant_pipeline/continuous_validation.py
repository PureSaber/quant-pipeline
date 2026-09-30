"""Training-only parameter updates feeding one uninterrupted OOS execution account."""

import json
from copy import deepcopy

import numpy as np
import pandas as pd
from quant_factors.validation import walk_forward_splits
from quant_lab.research import canonical, digest, file_hash
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
        decisions, training = [], []
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
            if settings["direction_policy"] == "train_ic" and candidate["name"] != "buy_hold":
                selected["factors"] = training_directions(
                    recipe, candidate, settings, result.get("factor_evidence")
                )
                if selected != candidate:
                    folder = output / f"train-selected-{split.fold:03d}"
                    folder.mkdir()
                    result = self.executor(train, selected, folder)
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
    decisions = []
    for chosen in validation["selection"]:
        fold = completed[chosen["candidate"]]["validation"]["folds"][chosen["fold"]]
        decisions.append(
            {k: deepcopy(fold[k]) for k in ("train", "test", "candidate", "training_metrics")}
        )
    recipe = deepcopy(summary["recipe"])
    recipe.pop("validation", None)
    recipe["interval"] = {
        "start": decisions[0]["test"]["start"],
        "end": decisions[-1]["test"]["end"],
    }
    request_sha = digest(
        {"recipe": recipe, "decisions": decisions, "definition": summary["definition_sha256"]}
    )
    cached = output / "selected-path.json"
    if output.exists():
        previous = json.loads(cached.read_text(encoding="utf-8"))
        if previous["request_sha256"] != request_sha:
            raise ValueError("continuous selected path request changed")
        for name, sha in previous["artifact_hashes"].items():
            path = (output / name).resolve()
            if output.resolve() not in path.parents or file_hash(path) != sha:
                raise ValueError("continuous selected path artifact changed")
        return previous
    output.mkdir()
    result = executor.continuous(recipe, decisions, output)
    evidence = {
        "available": True,
        "account_policy": "single-continuous-selected-path",
        "metrics": result["metrics"],
        "selections": decisions,
        "artifacts": str(output),
        "request_sha256": request_sha,
        "artifact_hashes": {
            str(p.relative_to(output)).replace("\\", "/"): file_hash(p)
            for p in sorted(output.rglob("*"))
            if p.is_file()
        },
    }
    cached.write_text(canonical(evidence), encoding="utf-8")
    return evidence
