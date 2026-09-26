"""Preregistered walk-forward evaluation with train-only candidate selection."""

from __future__ import annotations

from copy import deepcopy
from html import escape
from pathlib import Path

import numpy as np
import pandas as pd
from quant_factors.validation import benjamini_hochberg, walk_forward_splits
from quant_lab.research import canonical, file_hash
from quant_lab.research_options import validate_validation


def return_metrics(returns: pd.Series) -> dict:
    values = np.asarray(returns, dtype=float)
    if not len(values) or not np.isfinite(values).all() or (values < -1).any():
        raise ValueError("Returns must be nonempty, finite and no less than -1")
    nav = np.r_[1.0, np.cumprod(1 + values)]
    standard_deviation = float(values.std(ddof=1)) if len(values) > 1 else 0.0
    return {
        "total_return": float(nav[-1] - 1),
        "max_drawdown": float(np.min(nav / np.maximum.accumulate(nav) - 1)),
        "sharpe": float(values.mean() / standard_deviation * np.sqrt(252))
        if standard_deviation > 0
        else None,
        "sessions": len(values),
    }


def _validate_training_signal_evidence(factor_evidence: object, candidate: dict) -> None:
    if not isinstance(factor_evidence, dict):
        raise TypeError("Training factor evidence must be a mapping")
    expected_delay = candidate.get("signal_delay", 0)
    if type(expected_delay) is not int or expected_delay < 0:
        raise ValueError("Candidate signal_delay must be a non-negative integer")
    actual_delay = factor_evidence.get("signal_delay")
    if type(actual_delay) is not int or actual_delay != expected_delay:
        raise ValueError("Training signal delay evidence does not match the candidate")
    expected_representation = (
        "lagged-cross-sectional-percentile-rank" if expected_delay else "factor-value"
    )
    if factor_evidence.get("signal_representation") != expected_representation:
        raise ValueError("Training signal representation does not match the candidate")


class WalkForwardExecutor:
    """Run each fold in isolated ledgers; all fold accounts start with the same capital.

    Training data ends before the embargo. Factor diagnostics discard immature labels.
    OOS returns include the initial cash session and each fold's entry costs. Fold
    concatenation represents repeated deployment, not one continuously held account.
    """

    def __init__(self, executor, dates):
        self.executor = executor
        self.dates = pd.DatetimeIndex(pd.to_datetime(dates)).sort_values().unique()

    def __call__(self, recipe, candidate, output):
        settings = validate_validation(recipe["validation"])
        dates = self.dates[
            (self.dates >= recipe["interval"]["start"]) & (self.dates <= recipe["interval"]["end"])
        ]
        if (
            not len(dates)
            or str(dates[0].date()) != recipe["interval"]["start"]
            or str(dates[-1].date()) != recipe["interval"]["end"]
        ):
            raise ValueError("Validation endpoints must be observed sessions")
        splits = walk_forward_splits(
            dates,
            train_size=settings["train_sessions"],
            test_size=settings["test_sessions"],
            embargo_size=settings["embargo_sessions"],
            expanding=settings["expanding"],
        )
        if not splits or len(splits) > 64:
            raise ValueError("Validation requires between 1 and 64 complete folds")
        records, series, training, results = [], [], [], []
        for split in splits:
            fold_root = output / f"fold-{split.fold:03d}"
            fold_root.mkdir()
            selected = deepcopy(candidate)
            train_recipe = deepcopy(recipe)
            train_recipe.pop("validation", None)
            train_recipe["interval"] = {
                "start": str(split.train_start.date()),
                "end": str(split.train_end.date()),
            }
            train_dir = fold_root / "train"
            train_dir.mkdir()
            train_result = self.executor(train_recipe, selected, train_dir)
            if settings["direction_policy"] == "train_ic" and candidate["name"] != "buy_hold":
                neutralized = bool(recipe.get("neutralization"))
                _validate_training_signal_evidence(train_result.get("factor_evidence"), selected)
                evidence = {
                    row["factor"]: row
                    for row in train_result["factor_evidence"][
                        "neutralization" if neutralized else "ic_decay"
                    ]
                    if row["horizon"] == settings["direction_horizon"]
                }
                for name in selected["factors"]:
                    row = evidence.get(name, {})
                    value = row.get("neutralized_rank_ic" if neutralized else "rank_ic")
                    if neutralized and set(row.get("applied_by", [])) != set(
                        recipe["neutralization"]
                    ):
                        raise ValueError("Training neutralization evidence is incomplete")
                    if value is None or not np.isfinite(value) or row.get("sessions", 0) < 2:
                        raise ValueError(f"Training factor direction unavailable: {name}")
                    selected["factors"][name] = 1 if value >= 0 else -1
                if selected["factors"] != candidate["factors"]:
                    train_dir = fold_root / "train-selected-directions"
                    train_dir.mkdir()
                    train_result = self.executor(train_recipe, selected, train_dir)
            test_recipe = deepcopy(train_recipe)
            test_recipe["interval"] = {
                "start": str(split.test_start.date()),
                "end": str(split.test_end.date()),
            }
            test_dir = fold_root / "test"
            test_dir.mkdir()
            # Freeze the training decision before the executor can see any test result.
            decision = {
                "fold": split.fold,
                "train": train_recipe["interval"],
                "test": test_recipe["interval"],
                "candidate": selected,
                "training_metrics": train_result["metrics"],
            }
            (fold_root / "selection.json").write_text(canonical(decision), encoding="utf-8")
            test_result = self.executor(test_recipe, selected, test_dir)
            returns = pd.read_csv(test_dir / "returns.csv", index_col=0, parse_dates=True).iloc[
                :, 0
            ]
            expected = dates[split.test_indices]
            if not returns.index.equals(expected):
                raise ValueError("OOS returns must cover every declared test session exactly")
            return_metrics(returns)
            series.append(returns)
            training.append(train_result)
            results.append(test_result)
            records.append(
                {
                    **decision,
                    "test_metrics": test_result["metrics"],
                    "returns": [
                        {"date": str(day.date()), "net_return": float(value)}
                        for day, value in returns.items()
                    ],
                }
            )
        oos = pd.concat(series)
        if oos.index.has_duplicates:
            raise ValueError("OOS folds overlap")
        oos.to_csv(output / "returns.csv", header=["net_return"])
        evaluation = {
            "schema_version": "quant.walk-forward/v1",
            "settings": settings,
            "folds": records,
            "unused_tail_sessions": int((dates > splits[-1].test_end).sum()),
            "training_only_selection": True,
            "account_policy": "independent-initial-capital-per-fold",
        }
        (output / "validation.json").write_text(canonical(evaluation), encoding="utf-8")
        metrics = return_metrics(oos)
        metrics.update(
            fills=sum(r["metrics"].get("fills", 0) for r in results),
            cost_total=sum(r["metrics"].get("cost_total", 0) for r in results),
        )
        source_scope = results[0]["scope"]
        return {
            "metrics": metrics,
            "validation": evaluation,
            "risk_summary": {"test_folds": [r.get("risk_summary", {}) for r in results]},
            "segments": [
                {
                    "window": row["test"]["start"],
                    "sessions": len(row["returns"]),
                    "net_return": row["test_metrics"]["total_return"],
                }
                for row in records
            ],
            "comparison": {
                **results[0]["comparison"],
                "start": str(oos.index[0].date()),
                "end": str(oos.index[-1].date()),
                "validation": settings,
            },
            "scope": source_scope + "/walk-forward-oos",
            "factor_evidence": {
                "scope": "training-only-by-fold",
                "correlations": [],
                "folds": [r.get("factor_evidence", {}) for r in training],
            },
            "limitations": [
                *sorted({text for result in results for text in result.get("limitations", [])}),
                "folds restart from cash; entry costs charged each fold",
                "retrospective walk-forward is not an untouched prospective holdout",
                "incomplete final test window is reported but not evaluated",
            ],
        }


def paired_bootstrap(values, *, seed=17, repetitions=1000):
    """Moving-block bootstrap of paired daily excess return; no IID daily claim."""
    values = np.asarray(values, dtype=float)
    if len(values) < 10 or not np.isfinite(values).all():
        return {"available": False, "reason": "fewer than 10 finite paired sessions"}
    block = max(2, int(np.sqrt(len(values))))
    rng = np.random.default_rng(seed)
    offsets = np.arange(block)
    starts = rng.integers(0, len(values), size=(repetitions, int(np.ceil(len(values) / block))))
    indices = ((starts[..., None] + offsets) % len(values)).reshape(repetitions, -1)[
        :, : len(values)
    ]
    boot = values[indices].mean(axis=1)
    observed = float(values.mean())
    p = float((1 + np.sum(np.abs(boot - observed) >= abs(observed))) / (repetitions + 1))
    return {
        "available": True,
        "mean_daily_excess": observed,
        "block_sessions": block,
        "ci_95": np.quantile(boot, [0.025, 0.975]).tolist(),
        "p_value": p,
        "assumption": "moving-block stationary approximation; exploratory evidence",
    }


def summarize_validation(summary: dict) -> dict:
    settings = validate_validation(summary["recipe"]["validation"])
    results = {r["candidate"]["name"]: r for r in summary["results"] if r["status"] == "completed"}
    eligible = {"base", *(v["name"] for v in summary["recipe"].get("variants", []))}
    planned = {r["candidate"]["name"] for r in summary["results"]}
    result = {
        "schema_version": "quant.validation-summary/v1",
        "study_id": summary["study_id"],
        "definition_sha256": summary["definition_sha256"],
        "settings": settings,
        "selection_eligible": sorted(eligible),
        "selection": [],
        "paired_tests": [],
        "selection_complete": eligible <= results.keys(),
        "all_candidates_completed": planned <= results.keys(),
    }
    if result["selection_complete"]:
        fold_count = len(results["base"]["validation"]["folds"])
        chosen_returns = []
        for fold in range(fold_count):
            choices = []
            for name in sorted(eligible):
                record = results[name]["validation"]["folds"][fold]
                metric = record["training_metrics"].get(settings["selection_metric"])
                if metric is not None and np.isfinite(metric):
                    choices.append((float(metric), name, record))
            if not choices:
                result["selection_complete"] = False
                result["selection_error"] = f"No finite training metric for fold {fold}"
                break
            choices.sort(key=lambda row: (-row[0], row[1]))
            score, name, record = choices[0]
            chosen_returns += [r["net_return"] for r in record["returns"]]
            result["selection"].append(
                {
                    "fold": fold,
                    "candidate": name,
                    "train_score": score,
                    "train": record["train"],
                    "test": record["test"],
                    "test_metrics": record["test_metrics"],
                }
            )
        if result["selection_complete"]:
            result["selected_oos_metrics"] = return_metrics(pd.Series(chosen_returns))
    benchmark = results.get("buy_hold")
    if benchmark:
        baseline = {
            r["date"]: r["net_return"]
            for f in benchmark["validation"]["folds"]
            for r in f["returns"]
        }
        for name, candidate in sorted(results.items()):
            if name == "buy_hold":
                continue
            values = {
                r["date"]: r["net_return"]
                for f in candidate["validation"]["folds"]
                for r in f["returns"]
            }
            if values.keys() != baseline.keys():
                raise ValueError("Candidates do not share OOS test dates")
            evidence = paired_bootstrap([values[d] - baseline[d] for d in sorted(values)])
            result["paired_tests"].append({"candidate": name, **evidence})
        valid = [r for r in result["paired_tests"] if r["available"]]
        # A missing candidate invalidates family-wide FDR claims; never drop failures silently.
        if valid and result["all_candidates_completed"] and len(valid) == len(planned) - 1:
            corrected = benjamini_hochberg([r["p_value"] for r in valid], settings["fdr_alpha"])
            for row, (_, correction) in zip(valid, corrected.iterrows()):
                row.update(
                    adjusted_p_value=float(correction.adjusted_p_value),
                    reject=bool(correction.reject),
                )
        else:
            result["fdr_unavailable"] = "incomplete candidate family or insufficient samples"
    return result


def write_validation_report(summary: dict, output: Path) -> dict:
    result = summarize_validation(summary)
    result["study_sha256"] = file_hash(output / "study.json")
    (output / "validation.json").write_text(canonical(result), encoding="utf-8")
    rows = "".join(
        f"<tr><td>{row['fold']}</td><td>{escape(row['candidate'])}</td>"
        f"<td>{escape(str(row['train']))}</td><td>{escape(str(row['test']))}</td>"
        f"<td>{row['test_metrics']['total_return']:.2%}</td></tr>"
        for row in result["selection"]
    )
    page = (
        '<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>样本外验证</title>'
        "<style>body{font:16px system-ui;max-width:1100px;margin:40px auto;padding:20px;"
        "background:#f4f6fa;color:#172b43}td,th{padding:12px;border-bottom:1px solid #ccd} "
        "pre{white-space:pre-wrap;background:white;padding:20px}</style>"
        f"<h1>{escape(summary['study_id'])} · 样本外验证</h1>"
        "<p>每折仅按训练结果选择候选；测试区间不参与本折选择。每折账户从现金重新开始，计入入场费用。</p>"
        "<p>滚动样本外结果属于历史研究；不等同于未触碰的前向观察。</p>"
        f"<p>选择完整：{result['selection_complete']}；全部候选完成：{result['all_candidates_completed']}</p>"
        "<table><tr><th>折</th><th>训练所选候选</th><th>训练区间</th><th>测试区间</th><th>测试收益</th></tr>"
        + rows
        + "</table><h2>完整证据与多重比较</h2><pre>"
        + escape(canonical(result))
        + '</pre><a href="research.html">返回研究报告</a></html>'
    )
    (output / "validation.html").write_text(page, encoding="utf-8")
    return result
