import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from quant_pipeline import etf_frozen_worker as frozen
from quant_pipeline import etf_operations as ops


@pytest.mark.parametrize(
    "error,stage,code,retry",
    [
        ({"exceptions": [{"type": "TimeoutError"}]}, "collection", "transport_temporary", True),
        ({"exceptions": [{"type": "TimeoutError"}]}, "observe", "native_check_failed", False),
        (
            {"exceptions": [{"type": "SSLError", "message": "record layer failure"}]},
            "master_refresh",
            "transport_temporary",
            True,
        ),
        (
            {"exceptions": [{"type": "SSLError", "message": "unknown SSL failure"}]},
            "collection",
            "tls_unverified",
            False,
        ),
        (
            {"exceptions": [{"type": "ConnectionError"}, {"type": "SSLCertVerificationError"}]},
            "collection",
            "certificate_invalid",
            False,
        ),
        (
            {"error": "TIMELY_ORDER_MISSING: cannot invent fills"},
            "observe",
            "historical_fill_blocked",
            False,
        ),
        ({"error": "FROZEN_IDENTITY changed"}, "identity", "identity_changed", False),
        ({"publication_delay": True}, "collection", "publication_delay", True),
    ],
)
def test_error_classification_fails_closed(error, stage, code, retry):
    assert ops.classify(error, stage) == (code, retry)


def test_nested_exception_evidence():
    cause = ConnectionResetError("reset")
    wrapped = RuntimeError(cause)
    wrapped.__cause__ = cause
    assert [r["type"] for r in frozen.exception_chain(wrapped)] == [
        "RuntimeError",
        "ConnectionResetError",
    ]


@pytest.fixture
def harness(tmp_path, monkeypatch):
    now = datetime(2026, 10, 16, 8, 30, tzinfo=timezone.utc)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return state["now"]

    state = {
        "now": now,
        "calls": [],
        "collect": {"state": "collected"},
        "prepare": {
            "state": "ready",
            "forward_config_sha256": "a" * 64,
            "forward_config": "staged",
        },
        "execute": {"state": "completed", "execution": {"available": True, "as_of": "2026-10-16"}},
        "assess": {"state": "interrupted"},
    }
    config = {
        "output_root": str(tmp_path / "new"),
        "runs_root": str(tmp_path / "runs"),
        "execution_enabled": True,
        "continuation": None,
    }
    profile = {
        "output_root": str(tmp_path / "old"),
        "schedule": {
            "timezone": "Asia/Shanghai",
            "slots": ["16:30", "17:00", "18:00", "21:00", "22:00"],
            "collection_timeout_seconds": 600,
        },
    }
    Path(profile["output_root"]).mkdir()
    native = {
        "schema": "quant.etf-daily-status/v1",
        "read_only": True,
        "expected_session": "2026-10-16",
        "today_is_session": True,
        "calendar": ["2026-10-15", "2026-10-16", "2026-10-19"],
        "checked_at": now.isoformat(),
        "data": {"fresh": True, "snapshot_id": "s1"},
        "execution": {"available": False},
        "latest": None,
        "account": {"latest": None},
        "forward": {"state": "ready"},
    }
    state.update(
        config=config,
        profile=profile,
        native=native,
        scheduler={"verified": True, "enabled": True, "state": "Running"},
    )

    def worker(config, action, **kwargs):
        state["calls"].append(action)
        return deepcopy(state["native"] if action == "inspect" else state[action])

    monkeypatch.setattr(ops, "datetime", Clock)
    monkeypatch.setattr(ops, "load", lambda _: (config, profile, "config-sha"))
    monkeypatch.setattr(ops, "worker", worker)
    monkeypatch.setattr(ops, "task_status", lambda _: deepcopy(state["scheduler"]))
    monkeypatch.setattr(ops, "continuation", lambda _: {"allowed": True, "reason": "passed"})
    return state


def test_status_is_read_only_and_data_is_not_account_success(harness, tmp_path):
    before = set(tmp_path.rglob("*"))
    result = ops.status("config")
    assert result["operations"]["state"] == "waiting"
    assert set(tmp_path.rglob("*")) == before
    harness["scheduler"]["enabled"] = False
    result = ops.status("config")
    assert result["operations"]["next_attempt_at"] is None
    assert result["operations"]["state"] == "paused"


def test_disabled_continuation_cannot_write(harness):
    harness["config"]["execution_enabled"] = False
    assert ops.execute("config")["state"] == "paused"
    assert harness["calls"] == ["inspect"]


def test_windows_trigger_records_skipped_window_without_credit(harness, capsys):
    harness["now"] = harness["now"].replace(hour=8, minute=41)
    assert ops.main(["tick", "--config", "config", "--trigger", "windows_task"]) == 0
    assert json.loads(capsys.readouterr().out)["state"] == "waiting_for_slot"
    records = list(Path(harness["config"]["output_root"]).glob("triggers/*.json"))
    assert len(records) == 1 and not ops.read(records[0])["natural_credit"]
    assert ops.status("config")["schedule"]["missed_slots"] == ["16:30"]
    assert "collect" not in harness["calls"]


def test_cli_preview_does_not_execute_and_requires_revalidation(harness, tmp_path, capsys):
    harness["collect"] = {"state": "failed", "exceptions": [{"type": "TimeoutError"}]}
    original = ops.execute("config")
    harness["now"] += timedelta(minutes=30)
    assert ops.main(["recover-plan", "--config", "config", "--operation", original["id"]]) == 0
    preview = json.loads(capsys.readouterr().out)
    assert harness["calls"].count("collect") == 1
    plan_path = tmp_path / "preview.json"
    ops.save(plan_path, preview)
    harness["native"]["data"]["snapshot_id"] = "changed"
    assert ops.main(["recover-execute", "--config", "config", "--plan", str(plan_path)]) == 3
    assert "State changed" in capsys.readouterr().out
    assert harness["calls"].count("collect") == 1


@pytest.mark.parametrize(
    "hour,minute,trading,expected",
    [
        (8, 29, True, "not_due"),
        (8, 40, True, "waiting_for_slot"),
        (14, 10, True, "collection_exhausted"),
        (8, 30, False, "market_closed"),
    ],
)
def test_window_admission(harness, hour, minute, trading, expected):
    harness["now"] = harness["now"].replace(hour=hour, minute=minute)
    harness["native"]["today_is_session"] = trading
    assert ops.execute("config")["state"] == expected
    assert "collect" not in harness["calls"]


def test_natural_success_requires_task_and_native_ledger(harness):
    result = ops.execute("config", trigger="windows_task")
    assert result["state"] == "completed" and result["natural_credit"]
    harness["native"]["execution"] = harness["execute"]["execution"]
    assert ops.status("config")["operations"]["state"] == "completed"
    assert ops.execute("config")["state"] == "already_attempted"
    assert harness["calls"].count("execute") == 1


def test_task_claim_without_running_task_is_rejected(harness):
    harness["scheduler"]["state"] = "Ready"
    with pytest.raises(ValueError, match="task identity"):
        ops.execute("config", trigger="windows_task")
    assert "collect" not in harness["calls"]


def test_manual_success_never_gets_natural_credit(harness):
    result = ops.execute("config")
    assert result["state"] == "completed" and not result["natural_credit"]
    assert ops.status("config")["operations"]["acceptance"]["longest_consecutive"] == 0


def test_tls_retry_later_slot_only_and_final_exhaustion(harness):
    harness["collect"] = {
        "state": "failed",
        "stage": "master_refresh",
        "exceptions": [{"type": "SSLError", "message": "record layer failure"}],
    }
    for hour, minute in [(8, 30), (9, 0), (10, 0), (13, 0)]:
        harness["now"] = harness["now"].replace(hour=hour, minute=minute)
        assert ops.execute("config")["state"] == "collection_retryable"
        assert ops.execute("config")["state"] == "already_attempted"
    harness["now"] = harness["now"].replace(hour=14, minute=0)
    assert ops.execute("config")["state"] == "collection_exhausted"
    assert harness["calls"].count("collect") == 5


def test_preflight_block_and_missing_ledger_never_report_success(harness):
    harness["execute"] = {"state": "completed", "execution": {"available": False}}
    assert ops.execute("config")["state"] == "blocked"
    assert ops.status("config")["operations"]["state"] == "needs_attention"


def test_tampered_receipt_blocks_status(harness):
    result = ops.execute("config")
    path = Path(harness["config"]["output_root"]) / result["id"] / "operation.json"
    value = json.loads(path.read_text("utf-8"))
    value["state"] = "running"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="receipt changed"):
        ops.status("config")


def failed_attempt(harness):
    harness["execute"] = {"state": "failed", "error": "interrupted"}
    return ops.execute("config")


def test_recovery_expires_and_binds_data(harness):
    first = failed_attempt(harness)
    plan = ops.recover_plan("config", first["id"])
    assert plan["action"] == "resume"
    harness["native"]["data"]["snapshot_id"] = "changed"
    with pytest.raises(ValueError, match="State changed"):
        ops.execute("config", recovery=plan)
    harness["native"]["data"]["snapshot_id"] = "s1"
    harness["now"] += timedelta(minutes=16)
    with pytest.raises(ValueError, match="expired"):
        ops.execute("config", recovery=plan)


def test_reconcile_does_not_collect_and_no_natural_credit(harness):
    first = failed_attempt(harness)
    harness["assess"] = {"state": "completed"}
    harness["execute"] = {
        "state": "completed",
        "execution": {"available": True, "as_of": "2026-10-16"},
    }
    plan = ops.recover_plan("config", first["id"])
    assert plan["action"] == "reconcile"
    result = ops.execute("config", recovery=plan)
    assert result["state"] == "completed" and not result["natural_credit"]
    assert harness["calls"].count("collect") == 1
    with pytest.raises(ValueError, match="State changed"):
        ops.execute("config", recovery=plan)


def test_maintenance_and_concurrent_writer_block(harness):
    from quant_lab.research import study_lock

    with (
        study_lock(Path(harness["profile"]["output_root"])),
        pytest.raises((ValueError, RuntimeError, OSError)),
    ):
        ops.execute("config")
    marker = Path(harness["config"]["runs_root"]) / ".local-maintenance.json"
    marker.write_text("{}")
    with pytest.raises(ValueError, match="Maintenance"):
        ops.execute("config")


def test_acceptance_preserves_gaps_and_excludes_manual(harness):
    native = harness["native"]
    native["calendar"] = [f"2026-10-{day:02}" for day in range(15, 23)]
    native["expected_session"] = "2026-10-22"
    rows = [
        {
            "id": str(day),
            "as_of": f"2026-10-{day:02}",
            "state": "completed",
            "sha256": str(day),
            "trigger": "windows_task",
            "scheduler": {"verified": True},
            "verified_at": "now",
            "natural_credit": True,
        }
        for day in range(15, 23)
    ]
    rows[0]["trigger"] = "manual"
    rows[3]["state"] = "blocked"
    result = ops.acceptance(native, rows)
    assert result["state"] == "pending" and result["longest_consecutive"] == 4
    assert result["gaps"] == ["2026-10-15", "2026-10-18"]


def test_frozen_worker_rejects_clock_and_arbitrary_actions():
    with pytest.raises(ValueError, match="Unknown frozen worker fields"):
        frozen.main({"action": "execute", "config": {}, "now": "yesterday"})
    with pytest.raises(ValueError, match="Unknown frozen worker action"):
        frozen.main({"action": "promote", "config": {}})


def test_previous_day_completion_is_not_today_completion_before_close(harness):
    ops.execute("config", trigger="windows_task")
    harness["native"]["execution"] = harness["execute"]["execution"]
    harness["now"] = harness["now"].replace(day=19, hour=1)
    assert ops.status("config")["operations"]["state"] == "waiting"


def test_orphan_process_prevents_recovery_and_pid_reuse_is_not_liveness(harness, monkeypatch):
    first = failed_attempt(harness)
    directory = Path(harness["config"]["output_root"]) / first["id"]
    ops.save(
        directory / "worker-42.json", {"pid": 42, "process_token": "creation-1", "state": "running"}
    )
    monkeypatch.setattr(ops, "process_token", lambda _: "creation-1")
    assert ops.workers_running(harness["config"])
    assert ops.execute("config")["state"] == "running"
    assert ops.recover_plan("config", first["id"])["action"] == "none"
    monkeypatch.setattr(ops, "process_token", lambda _: "creation-2")
    assert not ops.workers_running(harness["config"])


def test_interrupted_collection_recovery_preserves_original_evidence(harness):
    first = failed_attempt(harness)
    directory = Path(harness["config"]["output_root"]) / first["id"]
    first.pop("forward_config", None)
    first.pop("forward_config_sha256", None)
    first.pop("sha256")
    first["state"] = "running"
    first["sha256"] = ops.digest(first)
    ops.save(directory / "operation.json", first)
    harness["now"] += timedelta(minutes=30)
    plan = ops.recover_plan("config", first["id"])
    assert plan["action"] == "retry"
    harness["execute"] = {
        "state": "completed",
        "execution": {"available": True, "as_of": "2026-10-16"},
    }
    result = ops.execute("config", recovery=plan)
    assert result["state"] == "completed" and not result["natural_credit"]
    assert ops.read(directory / "operation.json")["state"] == "interrupted"
    saved = list(directory.glob("interrupted-original-*.json"))
    assert len(saved) == 1 and ops.read(saved[0]) == first
