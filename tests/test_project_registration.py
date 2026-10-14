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
