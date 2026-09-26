"""Create clearly labelled synthetic equity/ETF examples for workbench installation checks."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


def create_demo(output: Path, *, asset: str = "etf", advanced: bool = False) -> Path:
    if asset not in {"etf", "equity"}:
        raise ValueError("asset must be equity or etf")
    output.mkdir(parents=True, exist_ok=False)
    inputs = output / "inputs"
    inputs.mkdir()
    dates = pd.bdate_range("2023-01-02", periods=360)
    rng = np.random.default_rng(17)
    symbols = (
        ["510300", "159919", "512000", "159915"]
        if asset == "etf"
        else ["000001", "000333", "600036", "601318"]
    )
    rows = []
    for i, symbol in enumerate(symbols):
        prices = (3 if asset == "etf" else 20) * np.exp(
            np.cumsum(rng.normal(0.0002 * (i + 1), 0.009, len(dates)))
        )
        for j, (date, close) in enumerate(zip(dates, prices)):
            open_price = prices[j - 1] if j else close
            rows.append(
                {
                    "symbol": symbol,
                    "date": date,
                    "open": open_price,
                    "high": max(close, open_price) * 1.01,
                    "low": min(close, open_price) * 0.99,
                    "close": close,
                    "volume": 10000000,
                    "adjustment": "none",
                    "volume_unit": "share",
                    "source": "synthetic-fixture-not-market-data",
                }
            )
    raw = pd.DataFrame(rows)
    frames = {
        "raw": raw,
        "adjusted": raw.assign(adjustment="qfq"),
        "calendar": pd.DataFrame({"date": pd.bdate_range(dates[0], periods=361)}),
        "benchmark": pd.DataFrame({"date": dates, "benchmark_return": np.zeros(len(dates))}),
    }
    manifest = {"origin": "synthetic_fixture", "scope": "software-demonstration-only", "files": {}}
    for name, frame in frames.items():
        target = inputs / f"{name}.parquet"
        frame.to_parquet(target, index=False)
        manifest["files"][name] = {
            "file": target.name,
            "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
        }
    (inputs / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    catalog = pd.DataFrame(
        [
            {
                "symbol": s,
                "asset_class": asset,
                "product_type": "etf" if asset == "etf" else "stock",
                "venue": "SSE" if s in {"510300", "512000", "600036", "601318"} else "SZSE",
                "price_scale": 3 if asset == "etf" else 2,
                "price_tick": ".001" if asset == "etf" else ".01",
                "quantity_step": 1,
                "lot_size": 100,
                "commission_rate": 0,
                "stamp_duty_rate": 0,
                "effective_from": dates[0].strftime("%Y-%m-%dT00:00:00Z"),
                "effective_to": (dates[-1] + pd.Timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z"),
                "available_at": dates[0].strftime("%Y-%m-%dT00:00:00Z"),
            }
            for s in symbols
        ]
    )
    catalog.to_csv(inputs / "catalog.csv", index=False)
    recipe = {
        "schema_version": "quant.research-recipe/v1",
        "study_id": f"synthetic-{asset}-example",
        "hypothesis": "Synthetic installation example: compare fixed signals and perturbations; no market claim.",
        "mode": "exploratory",
        "backend": "equity",
        "inputs": {"bundle": "inputs", "catalog": "inputs/catalog.csv"},
        "interval": {"start": str(dates[100].date()), "end": str(dates[-1].date())},
        "factors": {"momentum_20d": 1, "volatility_20d": -1},
        "strategy": {
            "family": "etf_trend" if asset == "etf" else "rank",
            "frequency": "weekly",
            "top_n": 2,
            "max_weight": 0.25,
            "cash_buffer": 0.5,
            "trend_window": 60,
        },
        "costs": {
            "initial_capital": 100000,
            "commission": 0.0003,
            "min_commission": 5,
            "stamp_tax": 0 if asset == "etf" else 0.0005,
            "slippage": 0.001,
            "participation_rate": 0.01,
        },
        "risk": {"max_drawdown": 0.3, "max_single_weight": 0.4},
        "diagnostics": {
            "single_factors": True,
            "ablations": True,
            "cost_multipliers": [2],
            "signal_delays": [1],
            "frequencies": ["monthly"],
        },
        "variants": [],
        "source": {"scope": "synthetic-software-demonstration"},
    }
    if advanced:
        from quant_data_kit.research_coverage import import_history

        records = []
        states = {
            "listed": "true",
            "delisted": "false",
            "tradable": "true",
            "limit_up": "false",
            "limit_down": "false",
            "member": "true",
        }

        def state(symbol, day, field, value):
            stamp = (
                pd.Timestamp(day).tz_localize("Asia/Shanghai") + pd.Timedelta(hours=8)
            ).isoformat()
            records.append(
                {
                    "symbol": symbol,
                    "domain": "universe" if field == "member" else "status",
                    "field": field,
                    "value": value,
                    "effective_at": stamp,
                    "available_at": stamp,
                }
            )

        for symbol in symbols:
            for field, value in states.items():
                state(symbol, dates[0], field, value)
        state(symbols[0], dates[220], "member", "false")
        state(symbols[0], dates[245], "member", "true")
        state(symbols[1], dates[255], "tradable", "false")
        state(symbols[1], dates[260], "tradable", "true")
        state(symbols[2], dates[280], "limit_up", "true")
        state(symbols[2], dates[282], "limit_up", "false")
        source = output / "synthetic-history.csv"
        pd.DataFrame(records).to_csv(source, index=False)
        import_history(
            source,
            output / "history",
            provider="synthetic-fixture",
            source_uri="fixture://advanced-research-demo",
            license_note="Generated software test data",
        )
        recipe.update(
            study_id=f"synthetic-{asset}-advanced",
            factors={"risk_adjusted_momentum": 1},
            factor_expressions={
                "risk_adjusted_momentum": "momentum_20d / clip(volatility_20d, 0.005, 1)"
            },
            allocation={"mode": "equal", "lookback": 20, "min_observations": 10, "max_turnover": 2},
            execution={
                "mode": "dynamic",
                "universe_field": "member",
                "max_retry_sessions": 5,
                "status_fields": {name: name for name in states if name != "member"},
            },
            required_history={
                name: "universe" if name == "member" else "status" for name in states
            },
            validation={
                "method": "walk_forward",
                "train_sessions": 80,
                "test_sessions": 40,
                "embargo_sessions": 5,
                "selection_metric": "total_return",
            },
            diagnostics={"cost_multipliers": [2], "signal_delays": [1]},
            variants=[
                {"name": "plain_momentum", "factors": {"momentum_20d": 1}},
                {"name": "inverse_vol", "allocation": {"mode": "inverse_vol", "max_turnover": 2}},
                {"name": "cost_aware", "allocation": {"mode": "cost_aware", "max_turnover": 2}},
            ],
        )
        recipe["inputs"]["history"] = "history"
    path = output / "recipe.yaml"
    path.write_text(yaml.safe_dump(recipe, sort_keys=False), encoding="utf-8")
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--asset", choices=["etf", "equity"], default="etf")
    parser.add_argument(
        "--advanced",
        action="store_true",
        help="Include expressions, rolling validation, allocation and synthetic trading states",
    )
    args = parser.parse_args()
    print(create_demo(args.output, asset=args.asset, advanced=args.advanced))
