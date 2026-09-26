"""Standalone subprocess adapter. Import only the frozen legacy backend runtime."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from dataclasses import replace
from decimal import Decimal
from importlib.metadata import distribution
from pathlib import Path

PINS = {
    "quant-data-kit": "8f258f11be8e4d8edddcd41b79b817bd6c925970",
    "quant-execution": "15e4e5c9dbaf2fe9b438732b2e94db295d5ea58c",
    "quant-lab": "27489d270e132adbec1bced93eb2ae84ad5e1a9b",
}


def inspect(backend):
    import importlib

    revisions = {}
    for name, expected in PINS.items():
        metadata = json.loads(distribution(name).read_text("direct_url.json") or "{}")
        revision = metadata.get("vcs_info", {}).get("commit_id")
        if revision != expected:
            raise ValueError(f"Fixture runtime requires {name}@{expected}")
        revisions[name] = revision
    package = "qfs_certified" if backend == "futures_fixture" else "quant_crypto_basis"
    module = importlib.import_module(package)
    root = Path(module.__file__).resolve().parents[1 if package == "qfs_certified" else 2]
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=root, text=True).strip():
        raise ValueError("Fixture strategy checkout must be clean")
    revisions[package] = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()
    revisions["worker_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    return revisions


def run(request):
    import pandas as pd
    import yaml
    from quant_lab import load_and_validate_standard_run

    payload = json.loads(request.read_text(encoding="utf-8"))
    recipe, candidate = payload["recipe"], payload["candidate"]
    inspect(recipe["backend"])
    out = request.parent
    parameters = candidate["backend_parameters"]
    if recipe["backend"] == "crypto_fixture":
        from quant_crypto_basis.artifacts import write_certified_standard_run
        from quant_crypto_basis.runner import run_fixture_backtest
        from quant_crypto_basis.strategy import BasisFundingConfig
        from quant_data_kit import FixedPoint

        config = BasisFundingConfig()
        changes = {
            k: Decimal(str(v))
            for k, v in parameters.items()
            if k in {"entry_basis_bps", "exit_basis_bps", "minimum_funding_rate"}
        }
        if "quantity" in parameters:
            changes["quantity"] = FixedPoint.from_decimal(str(parameters["quantity"]), 3)
        if "passive_limits" in parameters:
            changes["passive_limits"] = parameters["passive_limits"]
        replay = run_fixture_backtest(
            source=recipe["inputs"].get("source", "binance"),
            run_id=candidate["candidate_id"],
            strategy_config=replace(config, **changes),
            initial_cash=str(recipe["costs"]["initial_capital"]),
        )
        run_dir = out / "ledger"
        write_certified_standard_run(replay, run_dir)
    else:
        from qfs_certified.runner import run_certified_backtest

        config = yaml.safe_load(Path(recipe["inputs"]["config"]).read_text(encoding="utf-8"))
        if Decimal(str(config["account"]["initial_cash"])) != Decimal(
            str(recipe["costs"]["initial_capital"])
        ):
            raise ValueError("Recipe capital differs from frozen futures account")
        config["run_id"] = candidate["candidate_id"]
        for signal in config["signals"]:
            signal["quantity"] = str(
                Decimal(signal["quantity"]) * Decimal(str(parameters.get("quantity_multiplier", 1)))
            )
        config_path = out / "fixture-config.yaml"
        config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
        run_dir = run_certified_backtest(config_path, out / "ledger").run_dir
    manifest = load_and_validate_standard_run(run_dir)
    snapshots = pd.read_parquet(run_dir / "standard/v2/portfolio_snapshots.parquet")
    nav = snapshots.nav_units.astype(float) / 10.0 ** snapshots.nav_scale.astype(float)
    fills = pd.read_parquet(run_dir / "standard/v2/fills.parquet")
    actual_start = pd.to_datetime(snapshots.event_time, utc=True).min().date().isoformat()
    actual_end = pd.to_datetime(snapshots.event_time, utc=True).max().date().isoformat()
    if recipe["interval"]["start"] > actual_start or recipe["interval"]["end"] < actual_end:
        raise ValueError("Requested interval does not contain the frozen fixture window")
    result = {
        "scope": "fixture-only-not-market-performance",
        "metrics": {
            "total_return": float(nav.iloc[-1] / float(recipe["costs"]["initial_capital"]) - 1),
            "max_drawdown": float((nav / nav.cummax() - 1).min()),
            "sharpe": None,
            "fills": len(fills),
        },
        "comparison": {
            "start": actual_start,
            "end": actual_end,
            "currency": manifest.base_currency,
            "dataset_snapshots": manifest.dataset_snapshots,
            "costs": "frozen fixture contract",
            "frequency": "event",
        },
        "segments": [],
        "limitations": [
            "fixture returns are software tests, not alpha evidence",
            "fees, margin, funding and rolls use frozen backend contracts",
        ],
    }
    (out / "worker-result.json").write_text(json.dumps(result, allow_nan=False), encoding="utf-8")


if __name__ == "__main__":
    if sys.argv[1] == "inspect":
        print(json.dumps(inspect(sys.argv[2])))
    else:
        run(Path(sys.argv[2]))
