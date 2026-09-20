import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from quant_pipeline.daily import _input_request, run_daily


def test_failed_refresh_invalidates_old_pointer_and_still_renders(tmp_path, monkeypatch):
    config = tmp_path / "daily.yaml"
    config.write_text(
        yaml.safe_dump({"root": ".", "output": "runs", "decision_config": "decision.yaml"})
    )
    output = tmp_path / "runs"
    output.mkdir()
    (output / "latest.json").write_text(
        json.dumps({"status": "paper_ready", "decision": "old.json"})
    )
    called = []

    def fake(command, **kwargs):
        called.append(command)
        if "a_share_multifactor.decision_workflow" in command:
            raise subprocess.TimeoutExpired(command, 1)
        return subprocess.CompletedProcess(command, 0, "ok", "")

    monkeypatch.setattr(subprocess, "run", fake)
    result = run_daily(config)
    assert result["status"] == "failed"
    pointer = json.loads((output / "latest.json").read_text())
    assert pointer["status"] == "blocked"
    assert json.loads(Path(pointer["decision"]).read_text())["proposed_trades"] == []
    assert len(called) == 3
    assert not (output / ".pipeline.lock").exists()
    (output / ".pipeline.lock").touch()
    with pytest.raises(FileExistsError):
        run_daily(config)


def test_success_account_and_report_share_exact_run(tmp_path, monkeypatch):
    config = tmp_path / "daily.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "root": ".",
                "output": "runs",
                "decision_config": "decision.yaml",
                "account_config": "private/account.yaml",
                "report_command": ["-m", "report", "{output}", "{db}"],
            }
        )
    )
    calls = []

    def fake(command, **kwargs):
        calls.append(command)
        if "a_share_multifactor.decision_workflow" in command:
            (tmp_path / "runs/latest.json").write_text(
                json.dumps({"status": "observe", "decision": str(tmp_path / "decision.json")})
            )
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake)
    result = run_daily(config, inputs=tmp_path / "inputs", as_of="2026-09-18")
    assert result["status"] == "completed"
    assert [step["name"] for step in result["steps"]] == ["decision", "index", "account", "report"]
    assert str(tmp_path / "decision.json") in calls[2]


def test_data_policy_builds_inputs_before_research_without_leaking_provider_details(
    tmp_path, monkeypatch
):
    (tmp_path / "decision.yaml").write_text(
        yaml.safe_dump(
            {
                "watchlist": [{"symbol": "000001"}, {"symbol": "600000"}],
                "history_days": 10,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "data.yaml").write_text("schema_version: qdk.research-inputs/v1\n")
    config = tmp_path / "daily.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "root": ".",
                "output": "runs",
                "decision_config": "decision.yaml",
                "data_config": "data.yaml",
            }
        ),
        encoding="utf-8",
    )
    calls = []

    def fake(command, **kwargs):
        calls.append(command)
        if "a_share_multifactor.decision_workflow" in command:
            (tmp_path / "runs/latest.json").write_text(
                json.dumps({"status": "observe", "decision": str(tmp_path / "decision.json")})
            )
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake)
    result = run_daily(config, as_of="2026-09-18")
    assert result["status"] == "completed"
    assert [step["name"] for step in result["steps"]] == [
        "inputs",
        "decision",
        "index",
        "report",
    ]
    capture = calls[0]
    assert "quant_data_kit.research_inputs" in capture
    assert capture[capture.index("--symbols") + 1 : capture.index("--start")] == [
        "000001",
        "600000",
    ]
    decision = calls[1]
    assert "--inputs" in decision
    assert decision[decision.index("--inputs") + 1] == result["inputs"]


def test_input_request_uses_decision_watchlist_and_history(tmp_path):
    decision = tmp_path / "decision.yaml"
    decision.write_text(
        yaml.safe_dump({"watchlist": [{"symbol": "000001"}], "history_days": 5}),
        encoding="utf-8",
    )
    request = _input_request(
        decision,
        "2026-09-18",
        datetime(2026, 9, 20, tzinfo=timezone.utc),
    )
    assert request == {"symbols": ["000001"], "start": "2026-09-13", "end": "2026-09-18"}
