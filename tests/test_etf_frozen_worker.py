import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_etf_daily import candidate

from quant_pipeline import etf_daily
from quant_pipeline import etf_frozen_worker as worker


@pytest.fixture
def native(tmp_path, monkeypatch):
    profile_path = candidate(tmp_path, monkeypatch)
    profile = etf_daily.load_profile(profile_path)
    original = etf_daily.forward_daily.load_config(profile["forward_config"])
    monkeypatch.setattr(worker, "verify", lambda _: (profile, original))
    manifest, _ = etf_daily._snapshot(profile)
    manifest.update(requested_end="2026-10-16", captured_at="2026-10-16T16:30:00+08:00")
    directory = tmp_path / "operations-new" / "a"
    directory.mkdir(parents=True)
    config = {"output_root": str(directory.parent)}

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 16, 9, tzinfo=timezone.utc)

    monkeypatch.setattr(worker, "datetime", Clock)
    return config, directory, profile, original, manifest


def call(native, action, **extra):
    config, directory, *_ = native
    return worker.main(
        {
            "action": action,
            "config": config,
            "directory": str(directory),
            "as_of": "2026-10-16",
            **extra,
        }
    )


def test_collect_preserves_native_nested_error(native, monkeypatch):
    from quant_pipeline import etf_collection

    def collect(profile, directory, session, stage):
        stage("master_refresh", "running")
        raise TimeoutError("source unavailable")

    monkeypatch.setattr(etf_collection, "collect", collect)
    result = call(native, "collect")
    assert result["state"] == "failed" and result["stage"] == "master_refresh"
    assert result["exceptions"][0]["type"] == "TimeoutError"
    assert (native[1] / "collection-progress.json").exists()


def test_collect_success_uses_native_collector(native):
    assert call(native, "collect")["state"] == "collected"


def test_worker_marks_actual_process_and_finishes_even_on_failure(native):
    config, directory, *_ = native
    result = worker.execute_request(
        {"action": "collect", "config": config, "directory": str(directory), "as_of": "2026-10-16"}
    )
    assert result["state"] == "collected"
    marker = next(iter(directory.glob("worker-*.json")))
    assert json.loads(marker.read_text())["state"] == "finished"
    with pytest.raises(ValueError, match="HISTORICAL_BACKFILL"):
        worker.execute_request(
            {
                "action": "execute",
                "config": config,
                "directory": str(directory),
                "as_of": "2026-10-15",
            }
        )
    assert json.loads(marker.read_text())["state"] == "finished"


def test_prepare_stages_inputs_without_altering_frozen_recipe(native):
    before = native[2]["recipe"].read_bytes()
    result = call(native, "prepare")
    assert result["state"] == "ready" and result["preflight"]["passed"]
    assert native[2]["recipe"].read_bytes() == before
    assert Path(result["forward_config"]).parent == native[1]


@pytest.mark.parametrize("case", ["revision", "missing", "stale", "preflight"])
def test_prepare_rejects_bad_data_before_account_write(native, monkeypatch, case):
    import a_share_multifactor.research_workbench as equity
    import quant_data_kit.research_dataset as qdk

    if case == "revision":
        monkeypatch.setattr(
            qdk, "inspect_dataset", lambda *_: {"update_evidence": {"revised_rows": {"raw": 1}}}
        )
        with pytest.raises(ValueError, match="DATA_REVISED"):
            call(native, "prepare")
    elif case == "stale":
        native[4]["captured_at"] = "2026-10-16T14:30:00+08:00"
        with pytest.raises(ValueError, match="DATA_STALE"):
            call(native, "prepare")
    else:
        if case == "missing":
            native[4]["requested_end"] = "2026-10-15"
        else:
            monkeypatch.setattr(equity, "preflight_recipe", lambda _: {"passed": False})
        result = call(native, "prepare")
        assert result["state"] == (
            "publication_delay" if case == "missing" else "preflight_blocked"
        )
    assert not (native[1] / "forward.yaml").exists()


def test_worker_rejects_arbitrary_paths_and_historical_dates(native):
    config, directory, *_ = native
    with pytest.raises(ValueError, match="direct operation"):
        worker.main(
            {
                "action": "execute",
                "config": config,
                "directory": str(directory.parent.parent),
                "as_of": "2026-10-16",
            }
        )
    with pytest.raises(ValueError, match="HISTORICAL_BACKFILL"):
        worker.main(
            {
                "action": "execute",
                "config": config,
                "directory": str(directory),
                "as_of": "2026-10-15",
            }
        )


def test_execute_requires_staged_binding_and_native_verify(native, monkeypatch):
    from quant_pipeline import forward_daily

    result = call(native, "prepare")
    original = native[3]
    original.account_id = "fixed"
    original.definition_sha256 = "frozen"
    native_call = []

    def execute(path, as_of, operation_gate):
        operation_gate()
        native_call.append(as_of)
        return {"state": "completed"}

    monkeypatch.setattr(forward_daily, "run", execute)
    assert (
        call(native, "assess", forward_sha256=result["forward_config_sha256"])["state"] == "ready"
    )
    assert call(native, "execute", forward_sha256=result["forward_config_sha256"])["execution"][
        "available"
    ]
    assert native_call == ["2026-10-16"]
    with pytest.raises(ValueError, match="staged forward"):
        call(native, "execute", forward_sha256="0" * 64)
    changed = SimpleNamespace(
        account=Path("wrong"),
        account_id="fixed",
        definition_sha256="frozen",
        receipt_dir=original.receipt_dir,
    )
    monkeypatch.setattr(forward_daily, "load_config", lambda _: changed)
    with pytest.raises(ValueError, match="operator binding"):
        call(native, "execute", forward_sha256=result["forward_config_sha256"])


def test_verify_wrong_interpreter_and_receipt(tmp_path, monkeypatch):
    config = {"frozen_root": str(tmp_path)}
    with pytest.raises(ValueError, match="wrong interpreter"):
        worker.verify(config)
    monkeypatch.setattr(worker.sys, "prefix", str(tmp_path / "env"))
    path = tmp_path / "installed-stack.json"
    path.write_text("{}")
    config["frozen_stack_sha256"] = "wrong"
    with pytest.raises(ValueError, match="receipt changed"):
        worker.verify(config)
    config.update(frozen_stack_sha256=worker.sha(path), protected_files={str(path): "wrong"})
    with pytest.raises(ValueError, match="protected file changed"):
        worker.verify(config)


def test_verify_actual_tree_and_code_binding(tmp_path, monkeypatch):
    from quant_pipeline import forward_daily, research_paper

    monkeypatch.setattr(worker.sys, "prefix", str(tmp_path / "env"))
    (tmp_path / "profile").mkdir()
    for path in [
        tmp_path / "installed-stack.json",
        tmp_path / "profile/stack.json",
        tmp_path / "daily.json",
    ]:
        path.write_text("{}")
    config = {
        "frozen_root": str(tmp_path),
        "frozen_stack_sha256": worker.sha(tmp_path / "installed-stack.json"),
        "protected_files": {},
        "daily_profile": str(tmp_path / "daily.json"),
        "daily_profile_sha256": worker.sha(tmp_path / "daily.json"),
    }
    monkeypatch.setattr(worker.runpy, "run_path", lambda _: {"installed_identity": lambda _: {}})
    monkeypatch.setattr(etf_daily, "load_profile", lambda _: {"forward_config": "original"})
    monkeypatch.setattr(
        forward_daily,
        "load_config",
        lambda _: SimpleNamespace(account=tmp_path, definition_sha256="sha"),
    )
    monkeypatch.setattr(
        research_paper,
        "load_account",
        lambda _: {"definition_sha256": "sha", "definition": {"code_identity": {"native": "old"}}},
    )
    monkeypatch.setattr(research_paper, "equity_code_identity", lambda: {"native": "old"})
    monkeypatch.setattr(research_paper, "inspect_account", lambda _: {"read_only": True})
    assert worker.verify(config)[0] == {"forward_config": "original"}
    monkeypatch.setattr(research_paper, "equity_code_identity", lambda: {"native": "new"})
    with pytest.raises(ValueError, match="native code"):
        worker.verify(config)
    (tmp_path / "daily.json").write_text(json.dumps({"changed": True}))
    with pytest.raises(ValueError, match="daily profile changed"):
        worker.verify(config)
