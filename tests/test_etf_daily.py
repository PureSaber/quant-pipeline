import json
from datetime import datetime, timezone

import pandas as pd
import pytest

from quant_pipeline import etf_daily
from quant_pipeline.research_demo import create_demo


def setup(tmp_path, monkeypatch):
    import a_share_multifactor.research_workbench as equity
    import quant_data_kit.research_dataset as qdk

    recipe = create_demo(tmp_path / "seed")
    profile = tmp_path / "profile.json"
    profile.write_text(
        json.dumps(
            {
                "schema": etf_daily.SCHEMA,
                "dataset_root": str(tmp_path / "data"),
                "recipe": str(recipe),
                "output_root": str(tmp_path / "operations"),
                "forward_config": None,
            }
        )
    )
    frames = {
        "calendar": pd.DataFrame({"date": ["2026-10-13", "2026-10-14", "2026-10-16", "2026-10-19"]})
    }
    manifest = {
        "symbols": etf_daily.SYMBOLS,
        "snapshot_id": "sha256-" + "a" * 64,
        "requested_end": "2026-10-14",
        "captured_at": "2026-10-14T16:30:00+08:00",
    }
    monkeypatch.setattr(etf_daily, "_snapshot", lambda _: (manifest, frames))
    monkeypatch.setattr(qdk, "update_dataset", lambda *a, **k: manifest)
    monkeypatch.setattr(qdk, "inspect_dataset", lambda *a: {"passed": True})
    monkeypatch.setattr(equity, "preflight_recipe", lambda _: {"passed": True})
    return profile, manifest, frames


def test_calendar_uses_exchange_sessions_not_weekdays():
    frames = {"calendar": pd.DataFrame({"date": ["2026-10-13", "2026-10-14", "2026-10-16"]})}
    assert etf_daily.closed_session(frames, datetime(2026, 10, 15, 10, tzinfo=timezone.utc)) == (
        "2026-10-14",
        False,
    )
    assert etf_daily.closed_session(frames, datetime(2026, 10, 14, 6, tzinfo=timezone.utc)) == (
        "2026-10-13",
        True,
    )
    with pytest.raises(ValueError, match="actual current"):
        etf_daily.closed_session(frames, datetime(2026, 10, 20, 10, tzinfo=timezone.utc))


def test_binding_master_does_not_claim_a_new_market_capture(tmp_path, monkeypatch):
    import quant_data_kit.research_dataset as qdk

    profile, manifest, _ = setup(tmp_path, monkeypatch)
    manifest.update(
        parent_snapshot_id="sha256-parent",
        update_evidence={"market_data_refetched": False},
    )
    monkeypatch.setattr(
        qdk,
        "load_research_snapshot",
        lambda root, snapshot_id: (
            {"captured_at": "2026-10-14T10:00:00+08:00"},
            {},
        ),
    )
    value = etf_daily.status(profile, now=datetime(2026, 10, 14, 9, tzinfo=timezone.utc))
    assert value["data"]["captured_at"] == "2026-10-14T10:00:00+08:00"
    assert not value["data"]["fresh"]


def test_refresh_writes_receipt_but_status_has_no_side_effects(tmp_path, monkeypatch):
    profile, _, _ = setup(tmp_path, monkeypatch)
    now = datetime(2026, 10, 14, 9, tzinfo=timezone.utc)
    before = set(tmp_path.rglob("*"))
    assert etf_daily.status(profile, now=now)["last_success"] is None
    assert set(tmp_path.rglob("*")) == before
    value = etf_daily.run(profile, mode="refresh", now=now)
    assert value["state"] == "research_ready"
    assert not value["notify"]
    state = etf_daily.status(profile, now=now)
    assert state["latest"]["id"] == value["id"]
    assert state["last_success"] is None
    assert not state["execution"]["available"]


@pytest.mark.parametrize(
    "error,state",
    [(TimeoutError("delayed"), "collection_retryable"), (ValueError("history revised"), "blocked")],
)
def test_only_transport_failure_is_automatically_retryable(tmp_path, monkeypatch, error, state):
    profile, _, _ = setup(tmp_path, monkeypatch)
    import quant_data_kit.research_dataset as qdk

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(qdk, "update_dataset", fail)
    value = etf_daily.run(
        profile, mode="refresh", now=datetime(2026, 10, 14, 9, tzinfo=timezone.utc)
    )
    assert value["state"] == state and value["notify"]
    repeated = etf_daily.run(
        profile, mode="refresh", now=datetime(2026, 10, 14, 10, tzinfo=timezone.utc)
    )
    assert repeated["state"] == state and not repeated["notify"]


def test_no_automatic_candidate_creation_and_no_calendar_guess(tmp_path, monkeypatch):
    profile, _, _ = setup(tmp_path, monkeypatch)
    assert (
        etf_daily.run(profile, now=datetime(2026, 10, 14, 7, tzinfo=timezone.utc))["state"]
        == "not_due"
    )
    assert (
        etf_daily.run(profile, now=datetime(2026, 10, 14, 9, tzinfo=timezone.utc))["state"]
        == "candidate_required"
    )
    assert (
        etf_daily.run(profile, now=datetime(2026, 10, 15, 9, tzinfo=timezone.utc))["state"]
        == "market_closed"
    )
    assert not (tmp_path / "operations").exists()


def test_preflight_and_profile_mutation_block_observation(tmp_path, monkeypatch):
    profile, _, _ = setup(tmp_path, monkeypatch)
    import a_share_multifactor.research_workbench as equity

    monkeypatch.setattr(
        equity, "preflight_recipe", lambda _: {"passed": False, "issues": ["master missing"]}
    )
    value = etf_daily.run(
        profile, mode="refresh", now=datetime(2026, 10, 14, 9, tzinfo=timezone.utc)
    )
    assert value["state"] == "preflight_blocked"
    receipt = tmp_path / "operations" / value["id"] / "operation.json"
    payload = json.loads(receipt.read_text())
    payload["state"] = "completed"
    receipt.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="identity"):
        etf_daily.status(profile, now=datetime(2026, 10, 14, 9, tzinfo=timezone.utc))


def test_publication_delay_only_retries_final_day():
    value = {
        "pre_listing_or_provider_history_unavailable": {},
        "missing_after_first_available_bar": {"510300": ["2026-10-14"]},
    }
    error = lambda: ValueError("ETF daily-price coverage failed: " + json.dumps(value))
    assert etf_daily._publication_delay(error(), "2026-10-14")
    value["missing_after_first_available_bar"]["510300"].append("2026-10-13")
    assert not etf_daily._publication_delay(error(), "2026-10-14")


def test_historical_revision_blocks_and_new_profile_does_not_inherit_success(tmp_path, monkeypatch):
    profile, _, _ = setup(tmp_path, monkeypatch)
    import quant_data_kit.research_dataset as qdk

    monkeypatch.setattr(
        qdk, "inspect_dataset", lambda *a: {"update_evidence": {"revised_rows": {"raw": 1}}}
    )
    now = datetime(2026, 10, 14, 9, tzinfo=timezone.utc)
    outcome = etf_daily.run(profile, mode="refresh", now=now)
    assert outcome["state"] == "blocked" and "Historical" in outcome["error"]
    profile.write_text(profile.read_text() + " ")
    assert etf_daily.status(profile, now=now)["latest"] is None


def candidate(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from quant_lab.research import file_hash

    profile, _, _ = setup(tmp_path, monkeypatch)
    config = tmp_path / "operator.yaml"
    config.write_text("approval: approved\n")
    value = json.loads(profile.read_text())
    value["forward_config"] = str(config)
    profile.write_text(json.dumps(value))
    original = SimpleNamespace(
        account=tmp_path / "account",
        approval="approved",
        path=config,
        sha256=file_hash(config),
        receipt_dir=tmp_path / "receipts",
    )
    monkeypatch.setattr(etf_daily.forward_daily, "load_config", lambda _: original)
    monkeypatch.setattr(
        etf_daily.research_paper, "inspect_account", lambda *a, **k: {"latest": None}
    )
    monkeypatch.setattr(
        etf_daily.research_paper,
        "load_account",
        lambda *a: {"definition": {"holdout_start": "2026-10-14"}},
    )
    monkeypatch.setattr(etf_daily.forward_daily, "assess", lambda *a, **k: {"state": "ready"})
    monkeypatch.setattr(etf_daily, "execution_view", lambda *a: {"available": True})
    return profile


def test_retry_slots_and_same_day_idempotency(tmp_path, monkeypatch):
    import quant_data_kit.research_dataset as qdk

    profile = candidate(tmp_path, monkeypatch)
    calls = []

    def delayed(*a, **k):
        calls.append("collect")
        if len(calls) == 1:
            raise TimeoutError("publication delayed")

    def observe(*a, **k):
        k["operation_gate"]()
        calls.append("observe")
        return {"state": "completed"}

    monkeypatch.setattr(qdk, "update_dataset", delayed)
    monkeypatch.setattr(etf_daily.forward_daily, "run", observe)

    def tick(hour, minute=0):
        return etf_daily.run(profile, now=datetime(2026, 10, 14, hour, minute, tzinfo=timezone.utc))

    assert tick(8, 30)["state"] == "collection_retryable"
    assert tick(8, 45)["state"] == "already_attempted"
    assert tick(9)["state"] == "completed"
    assert tick(10)["state"] == "already_attempted"
    assert calls == ["collect", "collect", "observe"]


def test_interrupted_account_needs_explicit_native_retry(tmp_path, monkeypatch):
    profile = candidate(tmp_path, monkeypatch)
    monkeypatch.setattr(
        etf_daily.forward_daily, "assess", lambda *a, **k: {"state": "interrupted", "action": "run"}
    )
    calls = []
    monkeypatch.setattr(
        etf_daily.forward_daily,
        "run",
        lambda *a, **k: calls.append("native recovery") or {"state": "completed"},
    )
    now = datetime(2026, 10, 14, 9, tzinfo=timezone.utc)
    assert etf_daily.run(profile, now=now)["state"] == "interrupted"
    assert not calls
    assert etf_daily.run(profile, mode="observe", now=now)["state"] == "completed"
    assert calls == ["native recovery"]
