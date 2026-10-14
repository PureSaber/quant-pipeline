"""Presentation of already validated native ledger values (no return calculation)."""

from decimal import Decimal

import pandas as pd


def quantity(row, key):
    return Decimal(int(row[key + "_units"])).scaleb(-int(row[key + "_scale"]))


def display(frame):
    value = frame.tail(100).copy()
    for column in list(value.columns):
        if column.endswith("_units") and column[:-6] + "_scale" in value:
            key = column[:-6]
            value[key] = value.apply(
                lambda r, c=column, k=key: "" if pd.isna(r[c]) else str(quantity(r, k)), axis=1
            )
            value = value.drop(columns=[column, key + "_scale"])
    return {
        "columns": list(value.columns),
        "rows": value.fillna("").astype(str).values.tolist(),
        "total_rows": len(frame),
    }


def project_execution(directory, manifest, diagnostics, hashes):
    frames, tables = {}, {}
    for artifact in manifest.artifacts:
        if artifact.name not in {
            "positions",
            "orders",
            "fills",
            "costs",
            "order_events",
            "portfolio_snapshots",
        }:
            continue
        path = directory / "standard" / "v2" / artifact.path
        relative = path.relative_to(directory).as_posix()
        if hashes.get(relative) != artifact.sha256:
            raise ValueError("Native observation and standard-run artifact identity differ")
        frame = pd.read_parquet(path)
        frames[artifact.name] = frame
        tables[artifact.name] = {**display(frame), "file": relative, "sha256": artifact.sha256}
    required = {"positions", "orders", "fills", "costs", "portfolio_snapshots"}
    if required - frames.keys():
        raise ValueError("Native ledger omits required operational tables")
    # Use the final common ledger instant: an older nonzero position must never
    # survive a later liquidation merely because that instrument has no new row.
    latest = frames["portfolio_snapshots"].event_time.max()
    positions = frames["positions"]
    held = {
        r["instrument_id"]: quantity(r, "quantity")
        for r in positions[positions.event_time.eq(latest)].to_dict("records")
    }
    targets = [r for r in diagnostics.get("risk_checks", []) if "execution_target" in r]
    pending = {}
    for order in frames["orders"].to_dict("records"):
        if order["status"] in {"created", "accepted", "partially_filled"}:
            remaining = quantity(order, "quantity") - quantity(order, "filled_quantity")
            symbol = order["instrument_id"]
            pending[symbol] = pending.get(symbol, Decimal(0)) + remaining * (
                1 if order["side"] == "buy" else -1
            )
    comparison = []
    if targets:
        target = targets[-1]["execution_target"]
        for symbol in sorted(set(target) | set(held) | set(pending)):
            intended, actual = Decimal(target.get(symbol, 0)), held.get(symbol, Decimal(0))
            comparison.append(
                [
                    symbol,
                    str(intended),
                    str(actual),
                    str(intended - actual),
                    str(pending.get(symbol, Decimal(0))),
                ]
            )
    # Totals keep cost type and currency separate. Slippage is a price assumption,
    # never relabelled as a broker-measured execution difference.
    cost_totals = {}
    for row in frames["costs"].to_dict("records"):
        key = (row["currency"], row["cost_type"])
        cost_totals[key] = cost_totals.get(key, Decimal(0)) + quantity(row, "amount")
    return {
        "tables": tables,
        "position_comparison": comparison,
        "target_evidence": targets[-1] if targets else None,
        "ledger_time": str(latest),
        "cost_assumptions": diagnostics.get("cost_assumptions"),
        "cost_totals": [[*key, str(amount)] for key, amount in sorted(cost_totals.items())],
    }
