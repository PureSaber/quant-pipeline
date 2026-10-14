"""Native paper guard: a simulated opening fill needs a previously saved order."""

import pandas as pd
from quant_lab.research import digest

POLICY = "prior-observation-before-open/v1"
IMMUTABLE = (
    "event_time",
    "order_id",
    "idempotency_key",
    "account_id",
    "strategy_id",
    "instrument_id",
    "side",
    "quantity_units",
    "quantity_scale",
    "order_type",
    "limit_price_units",
    "limit_price_scale",
    "stop_price_units",
    "stop_price_scale",
    "time_in_force",
    "reduce_only",
)


def tables(directory):
    from quant_lab import load_and_validate_standard_run

    manifest = load_and_validate_standard_run(directory)
    if manifest.schema_version != "2.0.0":
        raise ValueError("Timely-order policy requires native exact-ledger v2 artifacts")
    result = {
        artifact.name: pd.read_parquet(directory / "standard/v2" / artifact.path)
        for artifact in manifest.artifacts
        if artifact.name in {"orders", "fills"}
    }
    if set(result) != {"orders", "fills"}:
        raise ValueError("Timely-order evidence lacks native orders or fills")
    return result


def fingerprint(row):
    value = {key: row[key] for key in IMMUTABLE}
    value["event_time"] = pd.Timestamp(value["event_time"]).isoformat()
    return digest({key: None if pd.isna(item) else item for key, item in value.items()})


def verify(directory, previous, commit_times):
    """Do not mutate the ledger. An unproved historical fill rejects the whole observation."""
    witnessed = {}
    for result, source in previous:
        if result.get("timing_evidence", {}).get("policy") != POLICY:
            raise ValueError("Prior observation lacks the frozen timely-order policy")
        completed = pd.Timestamp(result["completed_at"])
        if completed.tzinfo is None or completed < pd.Timestamp(result["observed_at"]):
            raise ValueError("Invalid observation completion timestamp")
        # A replay's start/completion timestamp alone is not proof of publication.
        committed = pd.Timestamp(commit_times[str(source.resolve())])
        if committed.tzinfo is None:
            raise ValueError("Invalid native commit timestamp")
        completed = max(completed, committed)
        for row in tables(source)["orders"].to_dict("records"):
            key = fingerprint(row)
            witnessed[key] = min(witnessed.get(key, completed), completed)
    current = tables(directory)
    orders = {row["order_id"]: fingerprint(row) for row in current["orders"].to_dict("records")}
    checked = []
    for fill in current["fills"].to_dict("records"):
        opening = pd.Timestamp(fill["event_time"]).tz_convert("Asia/Shanghai").normalize()
        opening += pd.Timedelta(hours=9, minutes=30)
        key = orders[fill["order_id"]]
        completed = witnessed.get(key)
        if completed is None or completed >= opening:
            raise ValueError(
                f"TIMELY_ORDER_MISSING: {fill['order_id']} has no completed observation "
                f"before simulated opening {opening.isoformat()}; historical fills cannot be invented"
            )
        checked.append(
            {
                "fill_id": fill["fill_id"],
                "order_id": fill["order_id"],
                "opening_at": opening.isoformat(),
                "witness_completed_at": completed.isoformat(),
            }
        )
    return {"policy": POLICY, "verified_fills": checked, "orders_recorded": len(orders)}
