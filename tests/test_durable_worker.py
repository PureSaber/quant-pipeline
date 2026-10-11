import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from test_durable_jobs import request

from quant_pipeline.durable_jobs import JobStore
from quant_pipeline.durable_worker import Worker, main, resolve_handler


@pytest.fixture(autouse=True)
def child_pythonpath(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.setenv(
        "PYTHONPATH",
        os.pathsep.join([str(root / "src"), str(root / "tests"), os.environ.get("PYTHONPATH", "")]),
    )


def worker(tmp_path, handler="echo", **kwargs):
    return Worker(
        JobStore(tmp_path / "jobs.db"),
        tmp_path / "attempts",
        {"echo": f"durable_handlers:{handler}"},
        **kwargs,
    )


def wait_until(predicate, timeout=15):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if predicate():
            return
        time.sleep(0.03)
    raise AssertionError("condition timed out")


def test_subprocess_execution_and_logs(tmp_path):
    runner = worker(tmp_path, lease_seconds=1)
    job = runner.store.submit(request(payload={"value": 9, "sleep": 1.2}), "key")
    finished = runner.run_once()
    assert finished["status"] == "succeeded"
    assert finished["result"]["pid"] != os.getpid()
    assert finished["result"]["value"] == 9
    assert (Path(finished["result"]["workspace"]) / "stdout.log").exists()
    assert "process_started" in [event["event"] for event in runner.store.events(job["job_id"])]
    assert runner.run_once() is None


def test_real_subprocess_preserves_structured_log_fields(tmp_path):
    runner = worker(tmp_path, "structured_logs")
    job = runner.store.submit(request(), "logs")
    finished = runner.run_once()
    assert finished["status"] == "succeeded"
    log = tmp_path / "attempts" / finished["attempt_id"] / "stderr.log"
    events = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert [event["event"] for event in events] == ["component_event", "research_event", "log"]
    assert events[0]["input_rows"] == 7
    assert events[0]["context"]["run_id"] == "run"
    assert events[1]["value"] == 9
    assert "ValueError: example diagnostic" in events[2]["exception"]
    for event in events:
        assert event["job_id"] == job["job_id"]
        assert event["attempt_id"] == finished["attempt_id"]
        assert event["run_id"] == "run"
        assert event["worker_id"] == runner.worker_id
        assert event["timestamp"].endswith("+00:00")


def test_failure_retries_in_new_workspace(tmp_path):
    runner = worker(tmp_path, "fail_once")
    job = runner.store.submit(request(), "key")
    assert runner.run_once()["status"] == "queued"
    assert runner.run_once()["status"] == "succeeded"
    attempts = runner.store.attempts(job["job_id"])
    assert [attempt["status"] for attempt in attempts] == ["failed", "succeeded"]
    assert len(list((tmp_path / "attempts").iterdir())) == 3


def test_timeout_terminates_process_before_failure(tmp_path):
    runner = worker(tmp_path, lease_seconds=1)
    runner.store.submit(request(payload={"sleep": 20}, timeout_seconds=0.3, max_attempts=1), "key")
    started = time.monotonic()
    finished = runner.run_once()
    assert time.monotonic() - started < 10
    assert finished["status"] == "failed"
    assert finished["result"]["error_type"] == "TimeoutError"


def test_cancel_running_process(tmp_path):
    runner = worker(tmp_path, lease_seconds=0.8)
    job = runner.store.submit(request(payload={"sleep": 20}), "key")
    with ThreadPoolExecutor() as executor:
        future = executor.submit(runner.run_once)
        wait_until(
            lambda: any(e["event"] == "process_started" for e in runner.store.events(job["job_id"]))
        )
        runner.store.cancel(job["job_id"])
        assert future.result(timeout=10)["status"] == "cancelled"
    assert runner.store.attempts(job["job_id"])[0]["status"] == "cancelled"


@pytest.mark.parametrize("handler", ["malformed", "echo"])
def test_invalid_handler_result_or_failure(tmp_path, handler):
    runner = worker(tmp_path, handler)
    runner.store.submit(request(payload={"fail": True}, max_attempts=1), "key")
    assert runner.run_once()["status"] == "failed"


def test_artifacts_must_be_retained_before_success(tmp_path):
    ref = {
        "schema_version": "quant.artifact-ref/v1",
        "sha256": "a" * 64,
        "size_bytes": 2,
        "uri": "cas:sha256:" + "a" * 64,
    }
    runner = worker(tmp_path)
    runner.store.submit(request(payload={"outputs": [ref]}, max_attempts=1), "no-guard")
    assert runner.run_once()["status"] == "failed"
    seen = []
    runner.artifact_guard = lambda owner, refs: seen.append((owner, refs))
    job = runner.store.submit(request(inputs=[ref], payload={"outputs": [ref]}), "guard")
    assert runner.run_once()["outputs"] == [ref]
    assert len(seen) == 2
    assert seen[0][0] == f"job/{job['job_id']}/inputs"
    assert seen[1][0].endswith("/outputs")


def test_retention_failure_never_commits_success(tmp_path):
    def guard(owner, refs):
        raise ValueError("corrupt object")

    runner = worker(tmp_path, artifact_guard=guard)
    ref = {
        "schema_version": "quant.artifact-ref/v1",
        "sha256": "a" * 64,
        "size_bytes": 2,
        "uri": "cas:sha256:" + "a" * 64,
    }
    job = runner.store.submit(request(inputs=[ref], max_attempts=1), "key")
    result = runner.run_once()
    assert result["status"] == "failed"
    assert not any(e["event"] == "process_started" for e in runner.store.events(job["job_id"]))


def test_independent_cli_survives_submission_client_exit(tmp_path):
    path = tmp_path / "jobs.db"
    code = (
        "from quant_pipeline.durable_jobs import JobStore; "
        f"JobStore({str(path)!r}).submit({request()!r}, 'independent')"
    )
    submitted = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=30, check=False
    )
    assert submitted.returncode == 0, submitted.stderr
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "quant_pipeline.durable_worker",
            "--database",
            str(path),
            "--workspace",
            str(tmp_path / "worker"),
            "--handler",
            "echo=durable_handlers:echo",
            "--once",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout)["status"] == "succeeded"
    assert JobStore(path).list()[0]["status"] == "succeeded"


def test_real_worker_death_and_restart_recovery(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    job = store.submit(request(payload={"sleep": 1.5}), "key")
    command = [
        sys.executable,
        "-m",
        "quant_pipeline.durable_worker",
        "--database",
        str(store.path),
        "--workspace",
        str(tmp_path / "worker"),
        "--handler",
        "echo=durable_handlers:echo",
        "--lease-seconds",
        "0.8",
        "--once",
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        wait_until(
            lambda: any(e["event"] == "process_started" for e in store.events(job["job_id"]))
        )
        process.kill()
        process.communicate(timeout=10)
        time.sleep(1)
        recovered = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
        assert recovered.returncode == 0, recovered.stderr
        assert store.get(job["job_id"])["status"] == "succeeded"
        assert [row["status"] for row in store.attempts(job["job_id"])] == ["expired", "succeeded"]
        events = store.events(job["job_id"])
        assert len([e for e in events if e["event"] == "completed"]) == 1
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)


def test_invalid_configuration(tmp_path):
    with pytest.raises(ValueError):
        Worker(JobStore(tmp_path / "jobs.db"), tmp_path, {})
    with pytest.raises(ValueError):
        resolve_handler("shell-command")
    with pytest.raises(SystemExit):
        main(["--workspace", str(tmp_path), "--handler", "echo=durable_handlers:echo"])
