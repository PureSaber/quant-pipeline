import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from quant_pipeline import etf_operations as ops


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    (runtime / "profile").mkdir(parents=True)
    source = tmp_path / "config"
    source.mkdir()
    account = tmp_path / "account"
    account.mkdir()
    forward = source / "forward.yaml"
    forward.write_text(yaml.safe_dump({"account": str(account)}))
    profile = {
        "schema": "quant.etf-daily/v1",
        "dataset_root": str(tmp_path / "data"),
        "recipe": str(source / "recipe.yaml"),
        "master_refresh": str(source / "master.json"),
        "forward_config": str(forward),
        "output_root": str(tmp_path / "old"),
        "schedule": {
            "timezone": "Asia/Shanghai",
            "slots": ["16:30", "22:00"],
            "collection_timeout_seconds": 600,
        },
    }
    daily = source / "daily.json"
    daily.write_text(json.dumps(profile))
    pins = [
        runtime / "installed-stack.json",
        runtime / "profile/stack.json",
        runtime / "profile/bootstrap.py",
        account / "account.json",
        Path(profile["recipe"]),
        Path(profile["master_refresh"]),
    ]
    for path in pins:
        path.write_text("{}")
    pins += [daily, forward]
    value = {
        "schema": ops.SCHEMA,
        "daily_profile": str(daily),
        "daily_profile_sha256": ops.sha(daily),
        "protected_files": {str(p): ops.sha(p) for p in pins},
        "frozen_python": str(tmp_path / "frozen/env/Scripts/python.exe"),
        "frozen_root": str(tmp_path / "frozen"),
        "frozen_stack_sha256": "frozen",
        "output_root": str(tmp_path / "new"),
        "runs_root": str(tmp_path / "runs"),
        "execution_enabled": False,
        "continuation": None,
        "scheduler": {"execute": str(runtime / "env/Scripts/pythonw.exe")},
    }
    path = source / "operations.json"
    path.write_text(json.dumps(value))
    monkeypatch.setattr(ops.sys, "prefix", str(runtime / "env"))
    monkeypatch.setattr(ops.runpy, "run_path", lambda _: {"installed_identity": lambda _: {}})
    return path, value


def test_load_requires_protected_paths_and_unchanged_bytes(config_file):
    path, value = config_file
    assert ops.load(path)[0] == value
    changed = next(p for p in value["protected_files"] if p.endswith("account.json"))
    Path(changed).write_text('{"changed": true}')
    with pytest.raises(ValueError, match="protected file changed"):
        ops.load(path)


@pytest.mark.parametrize(
    "case", ["relative", "extra", "overlap", "boolean", "missing_pin", "runtime_tree"]
)
def test_load_fails_closed(config_file, monkeypatch, case):
    path, value = config_file
    if case == "relative":
        value["output_root"] = "relative"
    elif case == "extra":
        value["force"] = True
    elif case == "overlap":
        value["output_root"] = value["runs_root"]
    elif case == "boolean":
        value["execution_enabled"] = "yes"
    elif case == "missing_pin":
        value["protected_files"].pop(
            next(p for p in value["protected_files"] if p.endswith("account.json"))
        )
    else:
        monkeypatch.setattr(
            ops.runpy, "run_path", lambda _: {"installed_identity": lambda _: {"tampered": True}}
        )
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        ops.load(path)


def test_continuation_requires_bound_evidence(tmp_path):
    path = tmp_path / "proof.json"
    path.write_text(json.dumps({"continuation_permitted": False, "reason": "native gap"}))
    config = {"continuation": {"path": str(path), "sha256": ops.sha(path)}}
    assert ops.continuation(config)["reason"] == "native gap"
    path.write_text(json.dumps({"continuation_permitted": True}))
    assert not ops.continuation(config)["allowed"]


def test_worker_protocol_validates_output_and_retains_logs(tmp_path, monkeypatch):
    calls = []

    def run(argv, **kwargs):
        calls.append((argv, json.loads(kwargs["input"])))
        return SimpleNamespace(returncode=0, stdout='{"state":"collected"}', stderr="evidence")

    monkeypatch.setattr(ops, "invoke", run)
    config = {"frozen_python": "python-frozen"}
    for _ in range(2):
        assert (
            ops.worker(config, "collect", directory=tmp_path, as_of="2026-10-16")["state"]
            == "collected"
        )
    assert len(list(tmp_path.glob("collect-*-stderr.txt"))) == 2
    assert calls[0][0][1:3] == ["-I", "-B"]
    assert "now" not in calls[0][1]
    monkeypatch.setattr(
        ops,
        "invoke",
        lambda *a, **k: SimpleNamespace(returncode=1, stdout="bad", stderr="failure"),
    )
    with pytest.raises(ValueError, match="no valid result"):
        ops.worker(config, "inspect")


def test_timeout_does_not_turn_execution_into_auto_retry(monkeypatch):
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired("worker", 1)

    monkeypatch.setattr(ops, "invoke", timeout)
    result = ops.worker({"frozen_python": "python"}, "execute", timeout=1)
    assert ops.classify(result, "observe")[1] is False


def test_cli_exposes_no_clock_override(capsys, monkeypatch):
    with pytest.raises(SystemExit):
        ops.main(["tick", "--config", "x", "--now", "yesterday"])
    monkeypatch.setattr(ops, "status", lambda _: {"read_only": True})
    assert ops.main(["status", "--config", "x"]) == 0

    def fail(*args):
        raise ValueError("identity changed")

    monkeypatch.setattr(ops, "status", fail)
    assert ops.main(["status", "--config", "x"]) == 3
    assert "verification_failed" in capsys.readouterr().out


def test_real_child_is_bounded_and_creation_identity_is_observable(tmp_path):
    options = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
        "encoding": "utf-8",
        "creationflags": subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    }
    assert ops.process_token(os.getpid())
    result = ops.invoke(
        [sys.executable, "-I", "-c", "import sys; print(sys.stdin.read())"],
        input="probe",
        timeout=10,
        process_directory=tmp_path,
        **options,
    )
    assert result.returncode == 0 and result.stdout.strip() == "probe"
    markers = list(tmp_path.glob("worker-launcher-*.json"))
    assert len(markers) == 1 and ops.read(markers[0])["state"] == "finished"
    with pytest.raises(subprocess.TimeoutExpired):
        ops.invoke(
            [sys.executable, "-I", "-c", "import time; time.sleep(30)"],
            input="",
            timeout=0.3,
            process_directory=tmp_path,
            **options,
        )
    assert not ops.workers_running({"output_root": str(tmp_path.parent)})
