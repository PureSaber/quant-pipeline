from pathlib import Path

import pandas as pd
import pytest

from quant_pipeline import research_timing as timing


def order():
    return {
        "event_time": pd.Timestamp("2026-10-14T15:00:00+08:00"),
        "order_id": "o1",
        "idempotency_key": "k1",
        "account_id": "a1",
        "strategy_id": "s1",
        "instrument_id": "510300",
        "side": "buy",
        "quantity_units": 100,
        "quantity_scale": 0,
        "order_type": "market",
        "limit_price_units": None,
        "limit_price_scale": None,
        "stop_price_units": None,
        "stop_price_scale": None,
        "time_in_force": "gtc",
        "reduce_only": False,
    }


def inputs(monkeypatch, *, finished="2026-10-14T22:05:00+08:00", committed=None, changed=False):
    earlier, current = Path("earlier"), Path("current")
    old = pd.DataFrame([order()])
    new = old.copy()
    if changed:
        new.loc[0, "quantity_units"] = 200
    fills = pd.DataFrame(
        [
            {
                "order_id": "o1",
                "fill_id": "f1",
                "event_time": pd.Timestamp("2026-10-15T15:00:00+08:00"),
            }
        ]
    )
    monkeypatch.setattr(
        timing,
        "tables",
        lambda path: {
            "orders": old if path == earlier else new,
            "fills": pd.DataFrame() if path == earlier else fills,
        },
    )
    result = {
        "timing_evidence": {"policy": timing.POLICY},
        "observed_at": "2026-10-14T21:00:00+08:00",
        "completed_at": finished,
    }
    return current, [(result, earlier)], {str(earlier.resolve()): committed or finished}


def test_evening_order_can_fill_next_open(monkeypatch):
    evidence = timing.verify(*inputs(monkeypatch))
    assert evidence["orders_recorded"] == 1
    assert evidence["verified_fills"][0]["opening_at"] == "2026-10-15T09:30:00+08:00"


@pytest.mark.parametrize(
    "options",
    [
        {"finished": "2026-10-15T09:31:00+08:00"},
        {"committed": "2026-10-15T09:30:00+08:00"},
        {"changed": True},
    ],
)
def test_late_or_changed_orders_cannot_be_backfilled(monkeypatch, options):
    with pytest.raises(ValueError, match="TIMELY_ORDER_MISSING"):
        timing.verify(*inputs(monkeypatch, **options))


def test_missed_observation_cannot_invent_past_fill(monkeypatch):
    current, _, _ = inputs(monkeypatch)
    with pytest.raises(ValueError, match="TIMELY_ORDER_MISSING"):
        timing.verify(current, [], {})


def test_first_session_without_fills_is_valid(monkeypatch):
    monkeypatch.setattr(
        timing, "tables", lambda _: {"orders": pd.DataFrame([order()]), "fills": pd.DataFrame()}
    )
    assert timing.verify(Path("current"), [], {})["verified_fills"] == []
