import hashlib
import json
from datetime import datetime, timedelta, timezone

import pytest
from quant_lab.research import execute_study, file_hash, load_recipe

from quant_pipeline import research_paper as paper
from quant_pipeline.project_registration import SCHEMA, Registration
from quant_pipeline.research_demo import create_demo

NOW = datetime(2023, 5, 31, 10, tzinfo=timezone.utc)


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


@pytest.fixture
def registration(tmp_path_factory, monkeypatch):
    from a_share_multifactor.research_workbench import EquityResearchExecutor

    tmp_path = tmp_path_factory.mktemp("reg")
    recipe = load_recipe(create_demo(tmp_path / "data"))
    recipe["interval"] = {"start": "2023-04-03", "end": "2023-05-30"}
    recipe["diagnostics"] = {
        "single_factors": False,
        "ablations": False,
        "cost_multipliers": [],
        "signal_delays": [],
        "frequencies": [],
    }
    identity = {"code": "test-native-source"}
    monkeypatch.setattr(paper, "equity_code_identity", lambda: identity)
    runs = tmp_path / "runs"
    run = runs / "native-run"
    output = run / "strategy-output"
    study = execute_study(
        recipe,
        output,
        identity=identity,
        data_identity=paper.input_identity(recipe),
        executor=EquityResearchExecutor(),
    )
    assert study["completed"] >= 1 and study["failed"] == 0
    project = {
        "id": "a" * 32,
        "recipes": [{"id": "b" * 32, "revision": "c" * 64}],
        "research_card": {"hypothesis": "synthetic test"},
    }
    revision = hashlib.sha256(
        json.dumps(project, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()
    ).hexdigest()
    project["revision"] = revision
    project_dir = runs / ".projects" / project["id"]
    write(project_dir / (revision + ".json"), project)
    (project_dir / "HEAD").write_text(revision)
    request = {
        "template_id": "etf-weekly-research",
        "research_project": {"id": project["id"], "revision": revision},
        "recipe": project["recipes"][0],
    }
    write(run / "request.json", request)
    hashes = {p.relative_to(run).as_posix(): file_hash(p) for p in run.rglob("*") if p.is_file()}
    write(run / "result.json", {"status": "succeeded", "output_hashes": hashes})
    config = tmp_path / "registration.json"
    write(
        config,
        {
            "schema": SCHEMA,
            "runs_root": str(runs),
            "accounts_root": str(tmp_path / "accounts"),
            "state_root": str(tmp_path / "registration-state"),
            "preview_ttl_seconds": 600,
        },
    )
    payload = {
        "project_id": project["id"],
        "revision": revision,
        "run_id": run.name,
        "candidate": "base",
        "start": "2023-06-01",
        "end": "2023-06-05",
    }
    return Registration(config), payload


def test_real_native_preview_commit_verify_and_duplicate(registration):
    service, request = registration
    preview = service.preview(request, now=NOW)
    assert not service.accounts.exists()
    assert preview["plan"]["prerequisites"]["passed"] is True
    assert preview["plan"]["definition"]["recipe"]["costs"]["initial_capital"] == 100000
    second = service.preview(request, now=NOW)
    assert second["account_id"] == preview["account_id"]
    first = service.commit(request["project_id"], preview["id"], now=NOW)
    assert first["state"] == "completed" and first["scheduled"] is False
    assert first["inspection"]["observations"] == []
    assert first["inspection"]["state"] == "pending"
    assert (
        service.commit(request["project_id"], second["id"], now=NOW)["account_id"]
        == first["account_id"]
    )
    assert len(list(service.accounts.iterdir())) == 1
    assert service.get(request["project_id"], preview["id"])["inspection"]["read_only"]
    assert len(service.list(request["project_id"])["previews"]) == 2
    # Deduplication remains safe after the preview expires; no new promotion occurs.
    assert (
        service.commit(request["project_id"], preview["id"], now=NOW + timedelta(days=10))["state"]
        == "completed"
    )


@pytest.mark.parametrize(
    "change", ["expiry", "project", "artifact", "runtime", "config", "input", "preview"]
)
def test_stale_preview_never_creates_account(registration, monkeypatch, change):
    service, request = registration
    preview = service.preview(request, now=NOW)
    now = NOW
    if change == "expiry":
        now += timedelta(minutes=10)
    elif change == "project":
        (service.runs / ".projects" / request["project_id"] / "HEAD").write_text("d" * 64)
    elif change == "artifact":
        (service.runs / request["run_id"] / "strategy-output/study.json").write_text("changed")
    elif change == "runtime":
        monkeypatch.setattr(paper, "equity_code_identity", lambda: {"code": "changed"})
    elif change == "config":
        service.path.write_text(service.path.read_text() + "\n")
    elif change == "input":
        path = preview["plan"]["definition"]["recipe"]["inputs"]["catalog"]
        from pathlib import Path

        Path(path).write_text("changed")
    else:
        path = service.state / (preview["id"] + ".json")
        value = json.loads(path.read_text(encoding="utf-8"))
        value["account_id"] = "tampered"
        write(path, value)
    with pytest.raises((ValueError, OSError)):
        service.commit(request["project_id"], preview["id"], now=now)
    assert not service.accounts.exists()


@pytest.mark.parametrize("when", ["before_database", "after_database", "after_publish"])
def test_interruption_preserves_evidence_and_recovers_one_registration(
    registration, monkeypatch, when
):
    service, request = registration
    preview = service.preview(request, now=NOW)
    original_promote, original_publish = paper.promote, paper.publish_account

    def interrupted_promote(*args, **kwargs):
        args[2].mkdir(parents=True)
        raise KeyboardInterrupt("killed before database")

    def interrupted_publish(*args, **kwargs):
        if when == "after_publish":
            original_publish(*args, **kwargs)
        raise KeyboardInterrupt("killed after registration")

    if when == "before_database":
        monkeypatch.setattr(paper, "promote", interrupted_promote)
    else:
        monkeypatch.setattr(paper, "publish_account", interrupted_publish)
    with pytest.raises(KeyboardInterrupt):
        service.commit(request["project_id"], preview["id"], now=NOW)
    receipt = service.get(request["project_id"], preview["id"])["receipt"]
    assert receipt["state"] == "registering" and len(receipt["attempts"]) == 1
    monkeypatch.setattr(paper, "promote", original_promote)
    monkeypatch.setattr(paper, "publish_account", original_publish)
    # A committed definition may finish publication after expiry, but an empty stage
    # must still pass current preview validation before creating anything.
    now = NOW if when == "before_database" else NOW + timedelta(days=1)
    done = service.commit(request["project_id"], preview["id"], now=now)
    assert done["state"] == "completed"
    account = service.accounts / preview["account_id"]
    assert paper.inspect_account(account)["attempts"] == []
    assert paper.inspect_account(account)["observations"] == []
    if when == "before_database":
        assert list(service.accounts.glob(".failed-*"))


def test_native_failure_retained_and_explicit_retry(registration, monkeypatch):
    service, request = registration
    preview = service.preview(request, now=NOW)
    original = paper.promote

    def failed(*args, **kwargs):
        raise OSError("disk unavailable")

    monkeypatch.setattr(paper, "promote", failed)
    failed_receipt = service.commit(request["project_id"], preview["id"], now=NOW)
    assert failed_receipt["state"] == "recoverable_failure"
    assert failed_receipt["error"] == "disk unavailable"
    monkeypatch.setattr(paper, "promote", original)
    done = service.commit(request["project_id"], preview["id"], now=NOW)
    assert done["state"] == "completed"
    assert done["attempts"][0]["error"] == "disk unavailable"
    assert len(done["attempts"]) == 2


@pytest.mark.parametrize(
    "key,value",
    [
        ("run_id", "../escape"),
        ("candidate", "invented"),
        ("start", "2023-05-30"),
        ("end", "2023-06-01"),
    ],
)
def test_untrusted_selection_or_invalid_window_is_rejected(registration, key, value):
    service, request = registration
    with pytest.raises(ValueError):
        service.preview({**request, key: value}, now=NOW)
    assert not service.accounts.exists()


def test_prerequisite_failure_rejects_preview(registration, monkeypatch):
    import a_share_multifactor.research_workbench

    service, request = registration
    monkeypatch.setattr(
        a_share_multifactor.research_workbench,
        "preflight_recipe",
        lambda _: {"passed": False, "issues": ["missing history"]},
    )
    with pytest.raises(ValueError, match="prerequisites failed"):
        service.preview(request, now=NOW)
    assert not service.accounts.exists()


@pytest.mark.parametrize("change", ["schema", "ttl", "relative", "overlap"])
def test_operator_configuration_rejects_unsafe_roots(tmp_path, change):
    config = {
        "schema": SCHEMA,
        "runs_root": str(tmp_path / "runs"),
        "accounts_root": str(tmp_path / "accounts"),
        "state_root": str(tmp_path / "state"),
        "preview_ttl_seconds": 600,
    }
    if change == "schema":
        config["unexpected"] = True
    elif change == "ttl":
        config["preview_ttl_seconds"] = 0
    elif change == "relative":
        config["runs_root"] = "relative"
    else:
        config["state_root"] = str(tmp_path / "runs" / "nested")
    path = tmp_path / "config.json"
    write(path, config)
    with pytest.raises(ValueError):
        Registration(path)


def test_cli_round_trip_and_closed_request_schema(registration, monkeypatch, capsys):
    from quant_pipeline.project_registration import main

    service, request = registration
    original_now = paper._now
    monkeypatch.setattr(paper, "_now", lambda now: original_now(now or NOW))

    def call(command, value):
        status = main([command, "--config", str(service.path), "--request-json", json.dumps(value)])
        return status, json.loads(capsys.readouterr().out)

    status, preview = call("preview", request)
    assert status == 0
    ref = {"project_id": request["project_id"], "preview_id": preview["id"]}
    assert call("inspect", ref)[1]["state"] == "prepared"
    assert call("list", {"project_id": request["project_id"]})[1]["previews"]
    assert call("commit", ref)[1]["state"] == "completed"
    assert call("inspect", ref)[1]["inspection"]["observations"] == []
    assert call("commit", {**ref, "command": "injected"})[0] == 2
    assert call("list", ref)[0] == 2
    assert call("preview", {**request, "path": "untrusted"})[0] == 2
    from quant_pipeline.project_registration import _child

    with pytest.raises(ValueError, match="escapes"):
        _child(service.runs, "../outside")


def test_source_project_family_and_manifest_gates(registration):
    service, request = registration
    run = service.runs / request["run_id"]
    path = run / "request.json"
    saved = json.loads(path.read_text())
    for replacement, message in [
        ({**saved, "research_project": {"id": "other"}}, "explicitly bound"),
        ({**saved, "recipe": {}}, "no longer connected"),
        ({**saved, "template_id": "other"}, "completed native"),
    ]:
        write(path, replacement)
        with pytest.raises(ValueError, match=message):
            service.preview(request, now=NOW)
    write(path, saved)
    result_path = run / "result.json"
    result = json.loads(result_path.read_text())
    for replacement, message in [
        ({**result, "status": "failed"}, "completed native"),
        ({**result, "verification_status": "failed"}, "verification failed"),
        ({**result, "output_hashes": {}}, "artifact manifest"),
    ]:
        write(result_path, replacement)
        with pytest.raises(ValueError, match=message):
            service.preview(request, now=NOW)
    write(result_path, result)
    assert not service.accounts.exists()


def test_inspection_detects_changed_committed_account(registration):
    service, request = registration
    preview = service.preview(request, now=NOW)
    service.commit(request["project_id"], preview["id"], now=NOW)
    account_path = service.accounts / preview["account_id"] / "account.json"
    account = json.loads(account_path.read_text())
    account["definition_sha256"] = "bad"
    write(account_path, account)
    assert service.get(request["project_id"], preview["id"])["inspection_error"]
    with pytest.raises(ValueError):
        service.commit(request["project_id"], preview["id"], now=NOW)


def test_etf_native_inspection_exposes_verified_versions_and_errors(registration, capsys):
    from quant_pipeline import etf_research

    service, request = registration
    output = service.runs / request["run_id"] / "strategy-output"
    assert etf_research.main(["inspect", "--output", str(output)]) == 0
    value = json.loads(capsys.readouterr().out)
    assert value["family_complete"] and value["attempt_dates"]
    assert value["recipe"] and value["definition_sha256"]
    assert all(
        row["code_identity"] and row["input_identity"] and row["parameters"]
        for row in value["results"]
    )
    (output / "study.json").write_text("broken")
    assert etf_research.main(["inspect", "--output", str(output)]) == 2
    assert json.loads(capsys.readouterr().out)["state"] == "failed"


def test_etf_preflight_universe_gate_and_cli_contract(monkeypatch, tmp_path, capsys):
    import a_share_multifactor.decision_workflow as inputs
    import a_share_multifactor.research_workbench as research
    import pandas as pd

    from quant_pipeline import etf_research, research_workbench

    recipe = {
        "backend": "equity",
        "strategy": {"frequency": "weekly"},
        "inputs": {"bundle": str(tmp_path)},
    }
    monkeypatch.setattr(etf_research, "load_recipe", lambda _: recipe)
    frame = pd.DataFrame({"symbol": etf_research.SYMBOLS, "source": ["synthetic"] * 4})
    monkeypatch.setattr(inputs, "load_inputs", lambda _: ({}, {"raw": frame}))
    monkeypatch.setattr(research, "preflight_recipe", lambda _: {"passed": True})
    assert etf_research.main(["check", "--recipe", "fixture"]) == 0
    value = json.loads(capsys.readouterr().out)
    assert value["read_only"] and value["investable"] is False
    assert value["evidence_kind"] == "synthetic"
    called = []
    monkeypatch.setattr(research_workbench, "run_research", lambda *args: called.append(args))
    monkeypatch.setattr(etf_research, "inspect_study", lambda _: {"family_complete": True})
    assert etf_research.main(["run", "--recipe", "fixture", "--output", str(tmp_path)]) == 0
    capsys.readouterr()
    assert called and (tmp_path / "studio-view.json").is_file()
    monkeypatch.setattr(research, "preflight_recipe", lambda _: {"passed": False})
    assert etf_research.main(["run", "--recipe", "fixture", "--output", str(tmp_path)]) == 3
    capsys.readouterr()
    assert len(called) == 1
    frame.loc[0, "symbol"] = "unapproved"
    with pytest.raises(ValueError, match="fixed four-symbol"):
        etf_research.preflight("fixture")
    recipe["backend"] = "fixture"
    with pytest.raises(ValueError, match="weekly equity"):
        etf_research.preflight("fixture")


def test_read_only_promotion_rejects_invalid_protocol_and_missing_trading_history(
    registration, monkeypatch
):
    from copy import deepcopy

    import pandas as pd
    import quant_data_kit.research_coverage as coverage
    import quant_report_hub.research_workbench as report

    service, request = registration
    source = service.runs / request["run_id"] / "strategy-output/study.json"
    original = report.load_study(source)
    summary = deepcopy(original)
    monkeypatch.setattr(report, "load_study", lambda _: summary)

    def prepare(now=NOW):
        return paper.prepare_promotion(
            source, "base", account_id="test", start=request["start"], end=request["end"], now=now
        )

    summary["recipe"]["backend"] = "fixture"
    with pytest.raises(ValueError, match="equity backend"):
        prepare()
    summary = deepcopy(original)
    summary["results"][0]["status"] = "failed"
    with pytest.raises(ValueError, match="entire preregistered"):
        prepare()
    summary = deepcopy(original)
    summary["recipe"]["validation"] = {"direction_policy": "train_ic"}
    with pytest.raises(ValueError, match="explicit factor directions"):
        prepare()
    summary = deepcopy(original)
    with pytest.raises(ValueError, match="Timezone-aware"):
        prepare(NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="latest completed"):
        prepare(datetime(2022, 1, 1, tzinfo=timezone.utc))
    summary["recipe"]["execution"] = {
        "status_fields": {"listed": "listed", "delisted": "delisted", "tradable": "tradable"}
    }
    summary["recipe"]["inputs"]["history"] = "synthetic-test-history"
    monkeypatch.setattr(paper, "input_identity", lambda _: summary["results"][0]["data_identity"])
    monkeypatch.setattr(coverage, "load_history", lambda _: ({}, pd.DataFrame()))
    monkeypatch.setattr(
        coverage, "asof_history", lambda *args, **kwargs: pd.DataFrame({"symbol": [], "value": []})
    )
    with pytest.raises(ValueError, match="trading-state history is incomplete"):
        prepare()
    assert not service.accounts.exists()
