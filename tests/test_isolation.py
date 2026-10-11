import io
import json
import os
import tarfile
from pathlib import Path

import pytest

from quant_pipeline import isolation as iso
from quant_pipeline.runner import run_step


def policy(**kwargs):
    return {
        "schema": "quant.research-isolation/v1",
        "backend": "docker-linux",
        "image": "sha256:" + "1" * 64,
        **kwargs,
    }


@pytest.mark.parametrize(
    "change",
    [
        {"backend": "subprocess"},
        {"image": "python:latest"},
        {"network": "host"},
        {"pids": 0},
        {"cpus": True},
        {"output_bytes": -1},
        {"seed": -1},
    ],
)
def test_policy_rejects_weak_or_unbounded_modes(change):
    with pytest.raises(iso.IsolationError):
        iso.validate_policy(policy(**change))


def test_windows_refuses_and_records_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(iso.platform, "system", lambda: "Windows")
    monkeypatch.setattr(iso.subprocess, "Popen", lambda *a, **k: pytest.fail("host launched"))
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    result = iso.run_isolated(["python", "-c", "print(1)"], inputs, tmp_path / "out", policy())
    assert result["status"] == "blocked"
    assert result["evidence_kind"] == "not_executed"
    assert "Linux host" in result["error"]
    assert json.loads((tmp_path / "out/isolation.json").read_text())["status"] == "blocked"
    with pytest.raises(FileExistsError):
        iso.run_isolated(["python"], inputs, tmp_path / "out", policy())


@pytest.mark.parametrize(
    "name,kind",
    [("../escape", "file"), ("/absolute", "file"), ("link", "symlink"), ("pipe", "fifo")],
)
def test_malicious_tar_never_writes_outside_output(tmp_path, name, kind):
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        entry = tarfile.TarInfo(name)
        entry.type = {
            "file": tarfile.REGTYPE,
            "symlink": tarfile.SYMTYPE,
            "fifo": tarfile.FIFOTYPE,
        }[kind]
        entry.linkname = "/etc/passwd"
        archive.addfile(entry)
    with pytest.raises(iso.IsolationError):
        iso._extract(data.getvalue(), tmp_path, 1024)
    assert not (tmp_path.parent / "escape").exists()


def test_tar_byte_budget(tmp_path):
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        entry = tarfile.TarInfo("result")
        entry.size = 5
        archive.addfile(entry, io.BytesIO(b"hello"))
    with pytest.raises(iso.IsolationError, match="limit"):
        iso._extract(data.getvalue(), tmp_path, 4)


@pytest.mark.skipif(os.name != "posix", reason="Linux descriptor-based staging")
@pytest.mark.parametrize("kind", ["fifo", "file-link", "directory-link"])
def test_special_input_refused_before_open_can_block(tmp_path, kind):
    inputs, stage = tmp_path / "inputs", tmp_path / "stage"
    inputs.mkdir()
    stage.mkdir()
    if kind == "fifo":
        os.mkfifo(inputs / "bad")
    elif kind == "file-link":
        (inputs / "bad").symlink_to("/etc/passwd")
    else:
        (inputs / "bad").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(iso.IsolationError):
        iso._stage(inputs, stage, 100)


@pytest.mark.skipif(os.name != "posix", reason="Linux descriptor-based staging")
def test_directory_swapped_for_link_during_open_refused(tmp_path, monkeypatch):
    inputs, stage = tmp_path / "inputs", tmp_path / "stage"
    inputs.mkdir()
    stage.mkdir()
    (inputs / "swap").mkdir()
    original = os.open

    def racing_open(path, *args, **kwargs):
        if path == "swap":
            (inputs / "swap").rmdir()
            (inputs / "swap").symlink_to(tmp_path, target_is_directory=True)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(iso.os, "open", racing_open)
    with pytest.raises((OSError, iso.IsolationError)):
        iso._stage(inputs, stage, 100)


def test_pipeline_real_entry_refuses_on_windows(tmp_path, monkeypatch):
    monkeypatch.setattr(iso.platform, "system", lambda: "Windows")
    (tmp_path / "inputs").mkdir()
    step = {
        "cmd": ["python", "/inputs/research.py"],
        "inputs": "inputs",
        "output": "result",
        "isolation": policy(),
    }
    result = run_step("research", "ignored", cwd=tmp_path, raw_step=step)
    assert result.exit_code == 2
    assert "Linux host" in result.stderr
    with pytest.raises(iso.IsolationError, match="argument list"):
        run_step("research", "python script.py", raw_step={**step, "cmd": "python script.py"})


def test_docker_empty_client_config_ignores_home(tmp_path, monkeypatch):
    monkeypatch.setattr(iso.platform, "system", lambda: "Linux")
    monkeypatch.setattr(iso.shutil, "which", lambda _: "/usr/bin/docker")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("DOCKER_HOST", "tcp://untrusted.example:1234")
    docker = iso.Docker()
    try:
        config = Path(docker.base[docker.base.index("--config") + 1])
        assert config.is_dir() and not list(config.iterdir())
        assert str(tmp_path) not in docker.env.values()
        assert "DOCKER_HOST" not in docker.env
    finally:
        docker.close()
