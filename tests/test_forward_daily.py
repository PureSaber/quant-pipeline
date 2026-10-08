import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
import yaml
from quant_lab.research import candidates, file_hash, load_recipe
from quant_lab.trials import TrialRegistry

from quant_pipeline import forward_daily
from quant_pipeline import research_paper as paper
from quant_pipeline.research_demo import create_demo
from quant_pipeline.research_validation import return_metrics

NOW = datetime(2023, 6, 1, 10, tzinfo=timezone.utc)


def ledger(recipe, candidate, output):
    dates = pd.bdate_range(recipe["interval"]["start"], recipe["interval"]["end"])
    returns = pd.Series([0.0] + [0.01] * (len(dates) - 1), index=dates)
    returns.to_csv(output / "returns.csv", header=["net_return"])
    return {"metrics": return_metrics(returns), "scope": "synthetic-test"}


def _files(root: Path) -> dict[str, tuple[str, int]]:
    return {
        str(path.relative_to(root)): (
            hashlib.sha256(path.read_bytes()).hexdigest(),
            path.stat().st_mtime_ns,
        )
        for path in root.rglob("*")
        if path.is_file()
    }


def _setup(tmp_path, monkeypatch, *, captured_at="2023-06-01T16:00:00+08:00"):
    recipe_path = create_demo(tmp_path / "data")
    manifest_path = tmp_path / "data" / "inputs" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if captured_at is not None:
        manifest["captured_at"] = captured_at
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    recipe = load_recipe(recipe_path)
    recipe["interval"] = {"start": "2023-04-03", "end": "2023-05-30"}
    recipe["diagnostics"] = {}
    recipe_path.write_text(yaml.safe_dump(recipe, sort_keys=False), encoding="utf-8")
    recipe = load_recipe(recipe_path)
    candidate = candidates(recipe)[0]
    summary = {
        "recipe": recipe,
        "definition_sha256": "study-hash",
        "results": [
            {
                "candidate": candidate,
                "status": "completed",
                "identity": {"code": "fixed"},
                "data_identity": paper.input_identity(recipe),
                "scope": "synthetic-software-demonstration",
                "metrics": {"total_return": 0.1},
            }
        ],
    }
    import quant_report_hub.research_workbench

    monkeypatch.setattr(quant_report_hub.research_workbench, "load_study", lambda _: summary)
    monkeypatch.setattr(paper, "equity_code_identity", lambda: {"code": "fixed"})
    source = tmp_path / "study.json"
    source.write_text("source study", encoding="utf-8")
    account_root = tmp_path / "paper"
    paper.promote(
        source,
        "base",
        account_root,
        account_id="forward-a",
        start="2023-06-01",
        end="2023-06-05",
        now=datetime(2023, 5, 31, tzinfo=timezone.utc),
    )
    account = paper.load_account(account_root)
    config_path = tmp_path / "forward.yaml"

    def write_config(approval="approved"):
        config = {
            "schema_version": "quant.forward-daily/v1",
            "approval": approval,
            "account": str(account_root),
            "account_id": account["account_id"],
            "definition_sha256": account["definition_sha256"],
            "input_recipe": str(recipe_path),
            "input_recipe_sha256": file_hash(recipe_path),
            "receipt_dir": str(tmp_path / "receipts"),
            "market": {
                "timezone": "Asia/Shanghai",
                "session_close": "15:00",
                "calendar_source": "input_bundle",
            },
        }
        config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
        return config_path

    return account_root, recipe_path, write_config


def test_status_and_plan_are_read_only_and_ready(tmp_path, monkeypatch):
    root, _, write_config = _setup(tmp_path, monkeypatch)
    config = write_config()
    before = _files(root)

    status = forward_daily.assess(
        config, "2023-06-01", now=NOW, schema="quant.forward-daily-status/v1"
    )
    plan = forward_daily.assess(config, "2023-06-01", now=NOW)

    assert status["state"] == plan["state"] == "ready", status["reason"]
    assert status["read_only"] and status["action"] == "run"
    assert status["identity"]["frozen_code_identity"] == {"code": "fixed"}
    assert status["identity"]["runtime_code_identity"] == {"code": "fixed"}
    assert status["identity"]["input_captured_at"].endswith("+08:00")
    assert _files(root) == before
    assert not (tmp_path / "receipts").exists()


def test_paused_run_is_blocked_with_external_receipt(tmp_path, monkeypatch):
    root, _, write_config = _setup(tmp_path, monkeypatch)
    before = _files(root)
    result = forward_daily.run(write_config("paused"), "2023-06-01", now=NOW, executor=ledger)

    assert result["outcome"] == "blocked" and result["state"] == "paused"
    assert Path(result["receipt"]).is_file()
    assert root not in Path(result["receipt"]).parents
    assert _files(root) == before
    assert TrialRegistry(root / "account.db", read_only=True).history("forward-a") == []


@pytest.mark.parametrize(
    ("as_of", "now", "expected"),
    [
        ("2023-06-01", datetime(2023, 6, 1, 6, tzinfo=timezone.utc), "not_due"),
        ("2023-06-03", datetime(2023, 6, 3, 10, tzinfo=timezone.utc), "market_closed"),
    ],
)
def test_time_and_exchange_calendar_states(tmp_path, monkeypatch, as_of, now, expected):
    capture = "2023-06-01T14:00:00+08:00" if expected == "not_due" else "2023-06-01T16:00:00+08:00"
    _, _, write_config = _setup(tmp_path, monkeypatch, captured_at=capture)
    result = forward_daily.assess(write_config(), as_of, now=now)
    assert result["state"] == expected


def test_missing_capture_evidence_blocks_instead_of_guessing(tmp_path, monkeypatch):
    _, _, write_config = _setup(tmp_path, monkeypatch, captured_at=None)
    result = forward_daily.assess(write_config(), "2023-06-01", now=NOW)
    assert result["state"] == "data_missing"
    assert "captured_at" in result["reason"]


def test_success_is_idempotent_and_ledger_is_verified(tmp_path, monkeypatch):
    root, _, write_config = _setup(tmp_path, monkeypatch)
    calls = []

    def counted(*args):
        calls.append(1)
        return ledger(*args)

    config = write_config()
    first = forward_daily.run(config, "2023-06-01", now=NOW, executor=counted)
    second = forward_daily.run(config, "2023-06-01", now=NOW, executor=counted)

    assert first["outcome"] == second["outcome"] == "success"
    assert first["native_result_sha256"]
    assert second["plan_state"] == "completed"
    assert calls == [1]
    history = TrialRegistry(root / "account.db", read_only=True).history("forward-a")
    assert [event["status"] for event in history] == ["running", "completed"]
    assert len(list((tmp_path / "receipts").rglob("*.json"))) == 2


def test_failed_attempt_and_receipt_are_preserved_then_explicitly_retried(tmp_path, monkeypatch):
    root, _, write_config = _setup(tmp_path, monkeypatch)
    config = write_config()

    def fail(*_):
        raise ValueError("missing market state")

    failed = forward_daily.run(config, "2023-06-01", now=NOW, executor=fail)
    plan = forward_daily.assess(config, "2023-06-01", now=NOW)
    completed = forward_daily.run(config, "2023-06-01", now=NOW, executor=ledger)

    assert failed["outcome"] == "failed" and "missing market state" in failed["reason"]
    assert plan["state"] == "retryable_failure" and plan["retry_allowed"]
    assert completed["outcome"] == "success"
    history = TrialRegistry(root / "account.db", read_only=True).history("forward-a")
    assert [event["status"] for event in history] == [
        "running",
        "failed",
        "running",
        "completed",
    ]


def test_interrupted_native_attempt_is_recovered_only_by_explicit_run(tmp_path, monkeypatch):
    root, _, write_config = _setup(tmp_path, monkeypatch)
    config = write_config()
    account = paper.load_account(root)
    orphan = TrialRegistry(root / "account.db").start(
        "forward-a", account["definition"]["parameters"][0], context={"as_of": "2023-06-01"}
    )

    plan = forward_daily.assess(config, "2023-06-01", now=NOW)
    result = forward_daily.run(config, "2023-06-01", now=NOW, executor=ledger)

    assert plan["state"] == "interrupted" and plan["retry_allowed"]
    assert result["outcome"] == "success"
    history = TrialRegistry(root / "account.db", read_only=True).history("forward-a")
    assert [(event["attempt_id"], event["status"]) for event in history[:2]] == [
        (orphan, "running"),
        (orphan, "interrupted"),
    ]


def test_same_account_concurrency_does_not_duplicate_observation(tmp_path, monkeypatch):
    root, _, write_config = _setup(tmp_path, monkeypatch)
    config = write_config()
    entered = threading.Event()
    release = threading.Event()

    def slow(*args):
        entered.set()
        assert release.wait(10)
        return ledger(*args)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(forward_daily.run, config, "2023-06-01", now=NOW, executor=slow)
        assert entered.wait(10)
        second = forward_daily.run(config, "2023-06-01", now=NOW, executor=ledger)
        release.set()
        first_result = first.result(timeout=10)

    assert first_result["outcome"] == "success"
    assert second["state"] in {"running", "execution_failed"}
    history = TrialRegistry(root / "account.db", read_only=True).history("forward-a")
    assert [event["status"] for event in history] == ["running", "completed"]


def test_historical_snapshot_cannot_be_introduced_as_new_forward_fact(tmp_path, monkeypatch):
    _, _, write_config = _setup(tmp_path, monkeypatch, captured_at="2023-06-02T16:00:00+08:00")
    result = forward_daily.assess(
        write_config(),
        "2023-06-01",
        now=datetime(2023, 6, 2, 10, tzinfo=timezone.utc),
    )
    assert result["state"] == "historical_backfill_blocked"


def test_recipe_and_native_code_changes_fail_closed(tmp_path, monkeypatch):
    _, recipe_path, write_config = _setup(tmp_path, monkeypatch)
    config = write_config()
    recipe_path.write_text(recipe_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    changed_recipe = forward_daily.assess(config, "2023-06-01", now=NOW)
    assert changed_recipe["state"] == "verification_failed"

    config = write_config()
    monkeypatch.setattr(paper, "equity_code_identity", lambda: {"code": "changed"})
    changed_code = forward_daily.assess(config, "2023-06-01", now=NOW)
    assert changed_code["state"] == "verification_failed"


def test_cli_has_no_clock_override_and_config_schema_is_strict(tmp_path, monkeypatch, capsys):
    _, _, write_config = _setup(tmp_path, monkeypatch)
    config = write_config("paused")
    assert forward_daily.main(["status", "--config", str(config), "--as-of", "2023-06-01"]) == 0
    assert json.loads(capsys.readouterr().out)["state"] == "paused"
    with pytest.raises(SystemExit):
        forward_daily.main(
            [
                "status",
                "--config",
                str(config),
                "--as-of",
                "2023-06-01",
                "--now",
                "2023-06-01T10:00:00Z",
            ]
        )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.pop("account_id"),
        lambda value: value.update(schema_version="unknown"),
        lambda value: value.update(approval="automatic"),
        lambda value: value.update(definition_sha256="not-a-hash"),
        lambda value: value["market"].pop("calendar_source"),
        lambda value: value["market"].update(session_close="16:00"),
        lambda value: value.update(receipt_dir=value["account"] + "/receipts"),
        lambda value: value.update(account_id=""),
    ],
)
def test_config_contract_rejects_ambiguous_or_unbound_values(tmp_path, monkeypatch, mutation):
    _, _, write_config = _setup(tmp_path, monkeypatch)
    path = write_config()
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutation(value)
    path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")
    with pytest.raises(forward_daily.ForwardDailyError):
        forward_daily.load_config(path)


def test_missing_config_invalid_date_and_naive_clock_are_explicit(tmp_path, monkeypatch):
    _, _, write_config = _setup(tmp_path, monkeypatch)
    config = write_config()
    with pytest.raises(forward_daily.ForwardDailyError, match="Cannot read config"):
        forward_daily.load_config(tmp_path / "absent.yaml")
    with pytest.raises(forward_daily.ForwardDailyError, match="ISO calendar date"):
        forward_daily.assess(config, "not-a-date", now=NOW)
    with pytest.raises(forward_daily.ForwardDailyError, match="timezone-aware"):
        forward_daily.assess(
            config,
            "2023-06-01",
            now=datetime(2023, 6, 1, 10),  # noqa: DTZ001 - deliberately invalid clock
        )


def test_missing_account_recipe_and_outside_window_are_distinct(tmp_path, monkeypatch):
    root, recipe, write_config = _setup(tmp_path, monkeypatch)
    config = write_config()
    outside = forward_daily.assess(
        config, "2023-06-06", now=datetime(2023, 6, 6, 10, tzinfo=timezone.utc)
    )
    assert outside["state"] == "outside_window"

    recipe.unlink()
    assert forward_daily.assess(config, "2023-06-01", now=NOW)["state"] == "data_missing"
    recipe.write_text("invalid", encoding="utf-8")
    root.rename(tmp_path / "moved-account")
    assert forward_daily.assess(config, "2023-06-01", now=NOW)["state"] == "data_missing"


def test_input_verification_error_and_running_writer_are_distinct(tmp_path, monkeypatch):
    root, _, write_config = _setup(tmp_path, monkeypatch)
    config = write_config()
    monkeypatch.setattr(paper, "input_lineage", lambda _: {"changed": True})
    assert forward_daily.assess(config, "2023-06-01", now=NOW)["state"] == ("verification_failed")

    monkeypatch.undo()
    # Restore the identity patch removed by undo and create a native running event.
    monkeypatch.setattr(paper, "equity_code_identity", lambda: {"code": "fixed"})
    account = paper.load_account(root)
    TrialRegistry(root / "account.db").start(
        "forward-a", account["definition"]["parameters"][0], context={"as_of": "2023-06-01"}
    )
    monkeypatch.setattr(forward_daily, "_lock_held", lambda _: True)
    assert forward_daily.assess(config, "2023-06-01", now=NOW)["state"] == "running"


def test_session_evidence_failures_are_data_missing(tmp_path, monkeypatch):
    root, recipe_path, write_config = _setup(tmp_path, monkeypatch)
    config = write_config()
    import a_share_multifactor.decision_workflow

    original = a_share_multifactor.decision_workflow.load_inputs
    spec = paper.load_account(root)["definition"]
    inputs = load_recipe(recipe_path)["inputs"]
    prefix = paper.input_prefix(inputs, spec["initial_input_cutoff"])
    lineage = paper.input_lineage(inputs)
    monkeypatch.setattr(paper, "input_prefix", lambda *_: prefix)
    monkeypatch.setattr(paper, "input_lineage", lambda _: lineage)

    def empty_calendar(path):
        manifest, frames = original(path)
        frames["calendar"] = frames["calendar"].iloc[0:0]
        return manifest, frames

    monkeypatch.setattr(a_share_multifactor.decision_workflow, "load_inputs", empty_calendar)
    result = forward_daily.assess(config, "2023-06-01", now=NOW)
    assert result["state"] == "data_missing" and "calendar is empty" in result["reason"]


def test_main_reports_config_errors_and_run_block_exit(tmp_path, monkeypatch, capsys):
    _, _, write_config = _setup(tmp_path, monkeypatch)
    config = write_config("paused")
    assert forward_daily.main(["run", "--config", str(config), "--as-of", "2023-06-01"]) == 3
    assert json.loads(capsys.readouterr().out)["outcome"] == "blocked"
    assert (
        forward_daily.main(
            ["status", "--config", str(tmp_path / "missing.yaml"), "--as-of", "2023-06-01"]
        )
        == 2
    )
    assert json.loads(capsys.readouterr().out)["code"] == "CONFIG_UNREADABLE"
    assert forward_daily._exit_code("run", {"outcome": "success"}) == 0
    assert forward_daily._exit_code("run", {"outcome": "failed"}) == 5
