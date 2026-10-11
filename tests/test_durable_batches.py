import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path

import pytest

from quant_pipeline.durable_batches import BatchStore, shared_refs, validate_batch
from quant_pipeline.durable_client import main
from quant_pipeline.durable_jobs import JobError, JobStore
from quant_pipeline.durable_worker import Worker


def request(count=3):
    return {
        "schema_version": "quant.job-batch/v1",
        "project_id": "project",
        "run_id": "study",
        "shared_inputs": [],
        "candidates": [
            {
                "candidate_id": f"window-{i}",
                "kind": "echo",
                "payload": {"value": i},
                "inputs": [],
                "timeout_seconds": 15,
                "max_attempts": 2,
            }
            for i in range(count)
        ],
    }


@pytest.fixture(autouse=True)
def child_path(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.setenv(
        "PYTHONPATH",
        os.pathsep.join([str(root / "src"), str(root / "tests"), os.environ.get("PYTHONPATH", "")]),
    )


def test_atomic_idempotent_concurrent_submission_and_persistence(tmp_path):
    jobs = JobStore(tmp_path / "jobs.db")
    batches = BatchStore(jobs)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: batches.submit(request(), "same"), range(16)))
    assert len({item["batch_id"] for item in results}) == 1
    assert len(jobs.list()) == 3
    saved = BatchStore(JobStore(jobs.path)).get(results[0]["batch_id"])
    assert saved["status"] == "queued" and not saved["terminal"]
    assert saved["counts"]["queued"] == 3
    assert len(batches.list(project_id="project")) == 1
    assert batches.list(project_id="other") == []
    changed = request()
    changed["candidates"][0]["payload"] = {"changed": True}
    with pytest.raises(JobError, match="conflicts"):
        batches.submit(changed, "same")


def test_mid_transaction_failure_rolls_back_every_child(tmp_path, monkeypatch):
    jobs = JobStore(tmp_path / "jobs.db")
    batches = BatchStore(jobs)
    submit = jobs._submit
    calls = []

    def fail_second(*args):
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError("simulated interruption")
        return submit(*args)

    monkeypatch.setattr(jobs, "_submit", fail_second)
    with pytest.raises(RuntimeError):
        batches.submit(request(), "atomic")
    assert jobs.list() == [] and batches.list() == []
    monkeypatch.setattr(jobs, "_submit", submit)
    assert len(batches.submit(request(), "atomic")["members"]) == 3


@pytest.mark.parametrize(
    "mutate",
    [
        lambda x: x.update(schema_version="wrong"),
        lambda x: x.update(candidates=[]),
        lambda x: x.update(extra=True),
        lambda x: x["candidates"][0].update(candidate_id="../x"),
        lambda x: x["candidates"][0].update(candidate_id="window-1"),
        lambda x: x["candidates"][0].update(inputs={}),
        lambda x: x["candidates"][0].update(handler="shell"),
        lambda x: x["candidates"][0].update(max_attempts=0),
        lambda x: x.update(run_id="x" * 512),
        lambda x: x.update(shared_inputs=[{}]),
    ],
)
def test_invalid_batch_has_no_visible_jobs(tmp_path, mutate):
    value = request()
    mutate(value)
    jobs = JobStore(tmp_path / "jobs.db")
    with pytest.raises(ValueError):
        BatchStore(jobs).submit(value, "invalid")
    assert jobs.list() == []


def test_input_identity_deduplicates_and_rejects_conflicts():
    ref = {
        "schema_version": "quant.artifact-ref/v1",
        "sha256": "a" * 64,
        "size_bytes": 10,
        "uri": "qdk+sha256:" + "a" * 64,
    }
    value = request()
    value["shared_inputs"] = [ref]
    value["candidates"][0]["inputs"] = [deepcopy(ref)]
    assert all(child["inputs"] == [ref] for _, child in validate_batch(value))
    with pytest.raises(JobError, match="contradictory"):
        shared_refs([ref], [ref | {"size_bytes": 11}])


def test_worker_failure_explicit_retry_and_old_results_preserved(tmp_path):
    jobs = JobStore(tmp_path / "jobs.db")
    batches = BatchStore(jobs)
    value = request(2)
    value["candidates"][1]["payload"]["fail"] = True
    value["candidates"][1]["max_attempts"] = 1
    batch = batches.submit(value, "first")
    worker = Worker(jobs, tmp_path / "attempts", {"echo": "durable_handlers:echo"})
    worker.run_once()
    worker.run_once()
    finished = batches.get(batch["batch_id"])
    assert finished["terminal"] and finished["status"] == "failed"
    assert finished["counts"]["succeeded"] == finished["counts"]["failed"] == 1
    failed = finished["members"][1]["job"]
    before = jobs.attempts(failed["job_id"])
    retried = batches.retry(batch["batch_id"], "retry-1", ["window-1"])
    assert retried["retry_of"] == batch["batch_id"]
    assert retried["members"][0]["source_job_id"] == failed["job_id"]
    assert retried["members"][0]["job"]["job_id"] != failed["job_id"]
    assert jobs.attempts(failed["job_id"]) == before
    assert batches.retry(batch["batch_id"], "retry-1", ["window-1"]) == retried
    with pytest.raises(JobError, match="only failed"):
        batches.retry(batch["batch_id"], "bad", ["window-0"])
    worker.run_once()
    assert batches.get(retried["batch_id"])["status"] == "failed"
    assert (tmp_path / "attempts" / failed["attempt_id"] / "stderr.log").exists()


def wait_for(predicate, timeout=20):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if predicate():
            return
        time.sleep(0.03)
    raise AssertionError("condition timed out")


def test_cancel_running_and_queued_candidates_is_atomic(tmp_path):
    jobs = JobStore(tmp_path / "jobs.db")
    batches = BatchStore(jobs)
    value = request()
    for candidate in value["candidates"]:
        candidate["payload"]["sleep"] = 10
    batch = batches.submit(value, "cancel")
    worker = Worker(
        jobs, tmp_path / "attempts", {"echo": "durable_handlers:echo"}, lease_seconds=0.8
    )
    with ThreadPoolExecutor() as pool:
        running = pool.submit(worker.run_once)
        wait_for(lambda: any(job["status"] == "running" for job in jobs.list()))
        batches.cancel(batch["batch_id"])
        assert running.result(timeout=15)["status"] == "cancelled"
    cancelled = batches.get(batch["batch_id"])
    assert cancelled["counts"]["cancelled"] == 3 and cancelled["terminal"]
    assert worker.run_once() is None
    assert batches.cancel(batch["batch_id"])["counts"] == cancelled["counts"]


def test_real_worker_crash_batch_restart_preserves_expired_attempt(tmp_path):
    jobs = JobStore(tmp_path / "jobs.db")
    batches = BatchStore(jobs)
    value = request(1)
    value["candidates"][0]["payload"]["sleep"] = 1.5
    batch = batches.submit(value, "crash")
    job_id = batch["members"][0]["job"]["job_id"]
    command = [
        sys.executable,
        "-m",
        "quant_pipeline.durable_worker",
        "--database",
        str(jobs.path),
        "--workspace",
        str(tmp_path / "attempts"),
        "--handler",
        "echo=durable_handlers:echo",
        "--lease-seconds",
        ".8",
        "--once",
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        wait_for(lambda: any(e["event"] == "process_started" for e in jobs.events(job_id)))
        process.kill()
        process.communicate(timeout=10)
        time.sleep(1)
        restored = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
        assert restored.returncode == 0, restored.stderr
        assert batches.get(batch["batch_id"])["status"] == "succeeded"
        assert [item["status"] for item in jobs.attempts(job_id)] == ["expired", "succeeded"]
        assert len([item for item in jobs.events(job_id) if item["event"] == "completed"]) == 1
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)


def test_cli_submit_get_list_cancel_and_retry(tmp_path, capsys):
    path = tmp_path / "batch.json"
    path.write_text(json.dumps(request()), encoding="utf-8")
    base = ["--database", str(tmp_path / "jobs.db")]
    main([*base, "batch-submit", "--request", str(path), "--key", "cli"])
    batch = json.loads(capsys.readouterr().out)
    main([*base, "batch-get", batch["batch_id"]])
    assert json.loads(capsys.readouterr().out) == batch
    main([*base, "batch-list", "--project-id", "project"])
    assert len(json.loads(capsys.readouterr().out)) == 1
    main([*base, "batch-cancel", batch["batch_id"]])
    assert json.loads(capsys.readouterr().out)["counts"]["cancelled"] == 3
    main([*base, "batch-retry", batch["batch_id"], "--key", "again", "--candidate", "window-0"])
    assert json.loads(capsys.readouterr().out)["retry_of"] == batch["batch_id"]


def test_invalid_queries_and_retries(tmp_path):
    batches = BatchStore(JobStore(tmp_path / "jobs.db"))
    with pytest.raises(JobError, match="does not exist"):
        batches.get("missing")
    with pytest.raises(JobError, match="limit"):
        batches.list(limit=False)
    for selection in ([], ["x", "x"], [1], "x"):
        with pytest.raises(JobError, match="unique"):
            batches.retry("missing", "key", selection)


def test_status_reader_does_not_block_worker_heartbeat(tmp_path):
    jobs = JobStore(tmp_path / "jobs.db")
    batches = BatchStore(jobs)
    batch = batches.submit(request(1), "reader")
    claimed = jobs.claim("worker", 10)
    with batches._read() as db:
        assert batches._get(db, batch["batch_id"])["status"] == "running"
        with ThreadPoolExecutor() as pool:
            future = pool.submit(
                jobs.heartbeat, claimed["attempt_id"], claimed["fencing_token"], 20
            )
            assert future.result(timeout=2)["lease_expires_at"] > 0
