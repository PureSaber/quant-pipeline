"""Actual containment tests. CI sets QUANT_ISOLATION_IMAGE; no daemon mocking."""

import json
import os
import subprocess
import sys
import threading
import time

import pytest

from quant_pipeline.isolation import run_isolated

pytestmark = pytest.mark.skipif(
    not os.environ.get("QUANT_ISOLATION_IMAGE"), reason="requires explicit real Docker test image"
)


def run(tmp_path, source, **changes):
    inputs = tmp_path / "inputs"
    inputs.mkdir(exist_ok=True)
    (inputs / "research.py").write_text(source)
    (inputs / "data.txt").write_text("read-only-sentinel")
    policy = {
        "schema": "quant.research-isolation/v1",
        "backend": "docker-linux",
        "image": os.environ["QUANT_ISOLATION_IMAGE"],
        "memory_mb": 128,
        "pids": 32,
        "work_mb": 2,
        "output_bytes": 1024**2,
        "timeout_seconds": 20,
        **changes,
    }
    result = run_isolated(
        ["/usr/local/bin/python", "-I", "/inputs/research.py"], inputs, tmp_path / "result", policy
    )
    assert result["cleanup_verified"] is True, result
    return result


def test_files_network_cpu_pid_and_proxy_boundaries(tmp_path, monkeypatch):
    secret = tmp_path / "host-private-sentinel.txt"
    secret.write_text("HOST-ONLY-NOT-MOUNTED")
    config = tmp_path / "operator/.docker"
    config.mkdir(parents=True)
    (config / "config.json").write_text(
        json.dumps(
            {
                "proxies": {
                    "default": {
                        "httpProxy": "http://sentinel-user:sentinel-password@proxy.invalid:8080"
                    }
                }
            }
        )
    )
    monkeypatch.setenv("HOME", str(config.parent))
    monkeypatch.setenv("DOCKER_CONFIG", str(config))
    monkeypatch.setenv("PRIVATE_API_KEY", "host-api-secret")
    result = run(
        tmp_path,
        f"""
import json, os, socket
from pathlib import Path
assert Path('/inputs/data.txt').read_text() == 'read-only-sentinel'
assert not Path({str(secret)!r}).exists()
assert not Path('/var/run/docker.sock').exists()
assert 'PRIVATE_API_KEY' not in os.environ
assert not any('sentinel-password' in v for v in os.environ.values())
for path in ['/inputs/data.txt', '/root/escape', '/etc/escape']:
    try:
        Path(path).write_text('bad')
    except OSError:
        pass
    else:
        raise AssertionError('write allowed: ' + path)
s = socket.socket()
s.settimeout(1)
try:
    s.connect(('1.1.1.1', 53))
except OSError:
    pass
else:
    raise AssertionError('external network allowed')
try:
    socket.getaddrinfo('example.com', 443)
except OSError:
    pass
else:
    raise AssertionError('external DNS allowed')
root = Path('/sys/fs/cgroup')
assert int((root/'memory.max').read_text()) == 128 * 1024**2
assert int((root/'pids.max').read_text()) == 32
quota, period = (root/'cpu.max').read_text().split()
assert int(quota) / int(period) == 1
status = Path('/proc/self/status').read_text()
assert 'NoNewPrivs:\\t1' in status and 'Seccomp:\\t2' in status
Path('/work/result.json').write_text(json.dumps({{'answer': 42, 'seed': os.environ['QUANT_RESEARCH_SEED']}}))
""",
    )
    assert result["status"] == "succeeded", result
    assert result["evidence_kind"] == "container-execution"
    assert (tmp_path / "inputs/data.txt").read_text() == "read-only-sentinel"
    assert json.loads((tmp_path / "result/artifacts/result.json").read_text())["answer"] == 42
    assert result["inputs"]["research.py"] and result["outputs"]["result.json"]


def test_real_memory_limit_kills_workload(tmp_path):
    result = run(tmp_path, "a = bytearray(300 * 1024**2)")
    assert result["status"] == "failed" and result["returncode"] != 0, result


def test_real_process_limit(tmp_path):
    result = run(
        tmp_path,
        """
import subprocess
children = []
try:
    for _ in range(80):
        children.append(subprocess.Popen(['sleep', '60']))
except OSError:
    print('pid-limit-reached', len(children), flush=True)
else:
    raise AssertionError('PID limit ineffective')
finally:
    for child in children:
        child.terminate()
    for child in children:
        child.wait()
""",
    )
    assert result["status"] == "succeeded", result
    assert "pid-limit-reached" in (tmp_path / "result/stdout.log").read_text()


def test_real_tmpfs_and_file_limit(tmp_path):
    result = run(
        tmp_path,
        """
from pathlib import Path
failed = False
try:
    for i in range(8):
        Path('/work/file-' + str(i)).write_bytes(b'x' * (600 * 1024))
except OSError:
    failed = True
assert failed, 'tmpfs boundary ineffective'
for p in Path('/work').iterdir():
    p.unlink()
try:
    Path('/work/large').write_bytes(b'x' * (2 * 1024**2))
except OSError:
    pass
else:
    raise AssertionError('fsize boundary ineffective')
Path('/work/large').unlink()
""",
    )
    assert result["status"] == "succeeded", result


def test_hostile_pid_exhaustion_cannot_claim_success_and_is_cleaned(tmp_path):
    result = run(
        tmp_path,
        """
import subprocess
children = []
try:
    for _ in range(80):
        children.append(subprocess.Popen(['sleep', '60']))
except OSError:
    print('hostile-pid-saturation', len(children), flush=True)
else:
    raise AssertionError('PID limit ineffective')
# Deliberately leave every child alive. Collector launch may fail because the
# runtime needs extra PIDs; that must produce a failed result, not a false pass.
""",
    )
    assert result["returncode"] == 0, result
    assert result["status"] == "failed", result
    assert "Output collection failed" in result["error"], result
    assert "hostile-pid-saturation" in (tmp_path / "result/stdout.log").read_text()
    assert result["cleanup_verified"] is True
    assert not subprocess.check_output(
        ["docker", "ps", "-aq", "--filter", "id=" + result["container_id"]], text=True
    ).strip()


def test_output_budget_and_untrusted_symlink(tmp_path):
    result = run(tmp_path, "from pathlib import Path; Path('/work/link').symlink_to('/etc/passwd')")
    assert result["status"] == "failed" and "forbidden" in result["error"], result


def test_total_output_budget_across_multiple_regular_files(tmp_path):
    result = run(
        tmp_path,
        "from pathlib import Path; "
        "Path('/work/a').write_bytes(b'a' * 700000); "
        "Path('/work/b').write_bytes(b'b' * 700000)",
    )
    assert result["status"] == "failed", result
    assert "Output byte limit exceeded" in result["error"], result


def test_log_flood_stopped(tmp_path):
    result = run(tmp_path, "while True: print('x' * 10000, flush=True)", log_bytes=4096)
    assert result["status"] == "stream_limit", result
    assert (tmp_path / "result/stdout.log").stat().st_size <= 4096


def test_timeout_removes_descendants(tmp_path):
    result = run(
        tmp_path,
        "import subprocess,time; subprocess.Popen(['sleep','60']); time.sleep(60)",
        timeout_seconds=1,
    )
    assert result["status"] == "timeout", result


def test_cancel_removes_container_and_all_children(tmp_path):
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    cancellation = threading.Event()
    policy = {
        "schema": "quant.research-isolation/v1",
        "backend": "docker-linux",
        "image": os.environ["QUANT_ISOLATION_IMAGE"],
    }
    outcome = {}

    # An external observer verifies the payload spawned a live child before cancelling.
    def worker():
        outcome.update(
            run_isolated(
                [
                    "python",
                    "-c",
                    (
                        "import subprocess,time; subprocess.Popen(['sleep','60']); "
                        "print('child-running',flush=True); time.sleep(60)"
                    ),
                ],
                inputs,
                tmp_path / "result",
                policy,
                cancellation.is_set,
            )
        )

    thread = threading.Thread(target=worker)
    thread.start()
    deadline = time.monotonic() + 20
    container_id = None
    while time.monotonic() < deadline:
        ids = subprocess.check_output(
            ["docker", "ps", "-q", "--filter", "label=quant.research-isolation=v1"], text=True
        ).split()
        if ids:
            top = subprocess.run(
                ["docker", "top", ids[0]], capture_output=True, text=True, check=False
            )
            if "sleep 60" in top.stdout:
                container_id = ids[0]
                break
        time.sleep(0.1)
    cancellation.set()
    thread.join(timeout=35)
    assert container_id is not None, outcome
    assert not thread.is_alive()
    assert outcome["status"] == "cancelled" and outcome["cleanup_verified"], outcome
    remaining = subprocess.check_output(
        ["docker", "ps", "-aq", "--filter", "id=" + container_id], text=True
    )
    assert not remaining.strip()


def test_real_isolation_and_pipeline_cli_entrypoints(tmp_path):
    from quant_pipeline.schema import validate_config

    inputs = tmp_path / "inputs"
    inputs.mkdir()
    (inputs / "research.py").write_text(
        "from pathlib import Path; Path('/work/answer').write_text('42')"
    )
    policy = {
        "schema": "quant.research-isolation/v1",
        "backend": "docker-linux",
        "image": os.environ["QUANT_ISOLATION_IMAGE"],
    }
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps(policy))
    command = ["/usr/local/bin/python", "-I", "/inputs/research.py"]
    standalone = subprocess.run(
        [
            sys.executable,
            "-m",
            "quant_pipeline.isolation",
            "--policy",
            str(policy_path),
            "--inputs",
            str(inputs),
            "--output",
            str(tmp_path / "standalone"),
            "--",
            *command,
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=40,
    )
    assert standalone.returncode == 0, standalone.stdout + standalone.stderr
    config = {
        "name": "contained-cli",
        "steps": [
            {
                "name": "research",
                "cmd": command,
                "inputs": str(inputs),
                "output": str(tmp_path / "pipeline"),
                "isolation": policy,
            }
        ],
    }
    assert validate_config(config, strict=True) == []
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(json.dumps(config))
    completed = subprocess.run(
        [sys.executable, "-m", "quant_pipeline.cli", "run", "--config", str(config_path), "--json"],
        capture_output=True,
        text=True,
        check=False,
        timeout=40,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert json.loads(completed.stdout)["ok"] is True
    for name in ("standalone", "pipeline"):
        assert (tmp_path / name / "artifacts/answer").read_text() == "42"
        result = json.loads((tmp_path / name / "isolation.json").read_text())
        assert result["cleanup_verified"] and result["evidence_kind"] == "container-execution"
