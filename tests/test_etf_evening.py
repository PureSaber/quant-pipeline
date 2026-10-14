import json
import subprocess
from datetime import datetime, timezone

import pytest
from test_etf_daily import candidate, setup

from quant_pipeline import etf_collection, etf_daily, etf_schedule


def scheduled(profile, monkeypatch):
    value = json.loads(profile.read_text())
    value["schedule"] = {
        "timezone": "Asia/Shanghai",
        "slots": ["16:30", "17:00", "18:00", "21:00", "22:00"],
        "collection_timeout_seconds": 600,
    }
    profile.write_text(json.dumps(value))
    monkeypatch.setattr(
        etf_collection,
        "bounded",
        lambda p, d, a, s, timeout, fetch=True: etf_collection.collect(p, d, a, s, fetch=fetch),
    )
    return profile


def at(hour, minute=0, day=14):
    return datetime(2026, 10, day, hour - 8, minute, tzinfo=timezone.utc)


def test_evening_retries_share_manual_and_automatic_slots(tmp_path, monkeypatch):
    import quant_data_kit.research_dataset as qdk

    profile = scheduled(candidate(tmp_path, monkeypatch), monkeypatch)
    calls = []

    def delay(*args, **kwargs):
        calls.append("collect")
        raise TimeoutError("delayed")

    monkeypatch.setattr(qdk, "update_dataset", delay)
    for hour, minute in [(16, 30), (17, 0), (18, 0), (21, 0)]:
        assert (
            etf_daily.run(profile, mode="refresh", now=at(hour, minute))["state"]
            == "collection_retryable"
        )
        assert etf_daily.run(profile, now=at(hour, minute))["state"] == "already_attempted"
    final = etf_daily.run(profile, now=at(22))
    assert final["state"] == "collection_exhausted" and final["notify"]
    assert final["schedule"]["next_attempt_at"] is None
    assert etf_daily.run(profile, mode="refresh", now=at(22, 1))["state"] == "already_attempted"
    assert len(calls) == 5
    state = etf_daily.status(profile, now=at(22, 30))
    assert state["schedule"]["state"] == "collection_exhausted"
    assert state["last_success"] is None


@pytest.mark.parametrize(
    "hour,minute,state",
    [(14, 0, "not_due"), (20, 59, "waiting_for_slot"), (22, 10, "collection_exhausted")],
)
def test_outside_windows_is_read_only(tmp_path, monkeypatch, hour, minute, state):
    profile, _, _ = setup(tmp_path, monkeypatch)
    scheduled(profile, monkeypatch)
    before = set(tmp_path.rglob("*"))
    for mode in ("tick", "refresh", "observe"):
        result = etf_daily.run(profile, mode=mode, now=at(hour, minute))
        assert result["state"] == state and result["read_only"]
    assert set(tmp_path.rglob("*")) == before
    assert etf_daily.run(profile, mode="refresh", now=at(17, day=15))["state"] == "market_closed"


def test_completed_manual_refresh_can_feed_tick_without_recollection(tmp_path, monkeypatch):
    import quant_data_kit.research_dataset as qdk

    profile = scheduled(candidate(tmp_path, monkeypatch), monkeypatch)
    calls = []
    monkeypatch.setattr(qdk, "update_dataset", lambda *a, **k: calls.append("collect"))
    monkeypatch.setattr(
        etf_daily.forward_daily,
        "run",
        lambda *a, **k: calls.append("observe") or {"state": "completed"},
    )
    first = etf_daily.run(profile, mode="refresh", now=at(21))
    assert first["preflight_passed_at"] and first["state"] == "research_ready"
    assert etf_daily.run(profile, now=at(21, 1))["state"] == "completed"
    assert etf_daily.run(profile, mode="refresh", now=at(22))["state"] == "already_attempted"
    assert calls == ["collect", "observe"]


def test_historical_change_never_retries_at_a_later_slot(tmp_path, monkeypatch):
    import quant_data_kit.research_dataset as qdk

    profile, _, _ = setup(tmp_path, monkeypatch)
    scheduled(profile, monkeypatch)
    monkeypatch.setattr(
        qdk, "update_dataset", lambda *a, **k: (_ for _ in ()).throw(ValueError("history changed"))
    )
    assert etf_daily.run(profile, mode="refresh", now=at(21))["state"] == "blocked"
    assert etf_daily.run(profile, mode="refresh", now=at(22))["state"] == "already_attempted"
    assert etf_daily.status(profile, now=at(22))["schedule"]["state"] == "needs_attention"


def test_concurrent_refresh_and_tick_share_one_writer(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    import quant_data_kit.research_dataset as qdk

    profile = scheduled(candidate(tmp_path, monkeypatch), monkeypatch)
    entered, release = Event(), Event()
    calls = []

    def collect(*args, **kwargs):
        calls.append("collect")
        entered.set()
        assert release.wait(5)

    monkeypatch.setattr(qdk, "update_dataset", collect)
    monkeypatch.setattr(
        etf_daily.forward_daily,
        "run",
        lambda *a, **k: calls.append("observe") or {"state": "completed"},
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(etf_daily.run, profile, mode="refresh", now=at(21))
        assert entered.wait(5)
        second = pool.submit(etf_daily.run, profile, now=at(21))
        release.set()
        assert first.result(timeout=10)["state"] == "research_ready"
        try:
            assert second.result(timeout=10)["state"] == "completed"
        except (OSError, RuntimeError, ValueError) as exc:
            assert (
                "lock" in str(exc).lower()
                or "already" in str(exc).lower()
                or isinstance(exc, PermissionError)
                and exc.errno == 13
            )
            assert etf_daily.run(profile, now=at(21))["state"] == "completed"
    assert calls == ["collect", "observe"]


def test_killed_collection_keeps_partial_evidence_and_is_retryable(tmp_path, monkeypatch):
    import sys
    from pathlib import Path

    profile = {"profile_path": tmp_path / "profile.json"}
    progress = [{"name": "collection", "state": "running"}]
    (tmp_path / "collection-progress.json").write_text(json.dumps(progress))
    original = subprocess.run

    def slow(command, **kwargs):
        return original([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)

    monkeypatch.setattr(subprocess, "run", slow)
    stages = []
    with pytest.raises(etf_collection.CollectionFailure) as caught:
        etf_collection.bounded(
            profile, tmp_path, "2026-10-14", lambda **r: stages.append(r), timeout=0.2
        )
    assert caught.value.retryable and stages == progress
    assert Path(tmp_path / "collection-progress.json").exists()
    assert not (tmp_path / "collection-result.json").exists()


@pytest.mark.parametrize(
    "change",
    [
        {"slots": ["22:00", "21:00"]},
        {"slots": ["14:00"]},
        {"timezone": "UTC"},
        {"collection_timeout_seconds": 601},
    ],
)
def test_schedule_rejects_invalid_configuration(change):
    with pytest.raises(ValueError):
        etf_schedule.validate(
            {
                "timezone": "Asia/Shanghai",
                "slots": ["21:00"],
                "collection_timeout_seconds": 600,
                **change,
            }
        )
