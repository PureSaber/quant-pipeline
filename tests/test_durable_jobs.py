import multiprocessing
from concurrent.futures import ThreadPoolExecutor

import pytest

from quant_pipeline.durable_jobs import CancellationRequested, JobError, JobStore, LeaseLost


def request(**updates):
    value = {
        "schema_version": "quant.job-request/v1",
        "kind": "echo",
        "project_id": "project",
        "run_id": "run",
        "payload": {"value": 42},
        "inputs": [],
        "timeout_seconds": 10,
        "max_attempts": 2,
    }
    value.update(updates)
    return value


def claim_process(path, start, results):
    store = JobStore(path)
    start.wait(20)
    claim = store.claim(str(multiprocessing.current_process().pid))
    results.put(claim["attempt_id"] if claim else None)


def test_idempotency_and_persistence(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    job = store.submit(request(), "same")
    assert JobStore(store.path).submit(request(), "same")["job_id"] == job["job_id"]
    with pytest.raises(JobError, match="conflicts"):
        store.submit(request(payload={"value": 43}), "same")
    assert len(store.list()) == 1
    assert store.list(status="running") == []
    assert store.events(job["job_id"])[0]["event"] == "submitted"
    with pytest.raises(JobError):
        store.get("missing")
    with pytest.raises(JobError):
        store.list(limit=0)


@pytest.mark.parametrize(
    "updates",
    [
        {"schema_version": "wrong"},
        {"kind": ""},
        {"run_id": 3},
        {"payload": []},
        {"timeout_seconds": 0},
        {"timeout_seconds": float("inf")},
        {"timeout_seconds": True},
        {"max_attempts": True},
        {"max_attempts": 0},
        {"max_attempts": 101},
        {"inputs": {}},
        {"inputs": [{}]},
        {"unexpected": 4},
        {"payload": {"value": float("nan")}},
    ],
)
def test_invalid_requests(tmp_path, updates):
    with pytest.raises(ValueError):
        JobStore(tmp_path / "jobs.db").submit(request(**updates), "key")


def test_real_process_competing_claims(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    job = store.submit(request(), "key")
    context = multiprocessing.get_context("spawn")
    start, results = context.Event(), context.Queue()
    processes = [
        context.Process(target=claim_process, args=(store.path, start, results)) for _ in range(4)
    ]
    for process in processes:
        process.start()
    start.set()
    claims = [results.get(timeout=30) for _ in processes]
    for process in processes:
        process.join(30)
        assert process.exitcode == 0
    assert sum(value is not None for value in claims) == 1
    assert len(store.attempts(job["job_id"])) == 1


def test_expiry_recovery_and_fencing(tmp_path):
    now = [100.0]
    store = JobStore(tmp_path / "jobs.db", clock=lambda: now[0])
    job = store.submit(request(), "key")
    first = store.claim("worker1", 5)
    assert store.claim("other", kinds=["other"]) is None
    now[0] = 104
    assert store.heartbeat(first["attempt_id"], 1, 5)["lease_expires_at"] == 109
    now[0] = 109
    second = store.claim("worker2", 5)
    assert second["fencing_token"] == 2
    assert second["attempt_id"] != first["attempt_id"]
    for action in [
        lambda: store.finish(first["attempt_id"], 1, {"stale": True}),
        lambda: store.heartbeat(first["attempt_id"], 1),
        lambda: store.finish(second["attempt_id"], True, {}),
    ]:
        with pytest.raises(LeaseLost):
            action()
    store.record_event(second["attempt_id"], 2, "verified", count=1)
    store.finish(second["attempt_id"], 2, {"ok": True})
    assert store.get(job["job_id"])["result"] == {"ok": True}
    assert [row["status"] for row in store.attempts(job["job_id"])] == ["expired", "succeeded"]


def test_expiry_exhausted_and_cancelled(tmp_path):
    now = [1.0]
    store = JobStore(tmp_path / "jobs.db", clock=lambda: now[0])
    job = store.submit(request(max_attempts=1), "one")
    store.claim("worker", 1)
    now[0] = 3
    assert store.claim("next") is None
    assert store.get(job["job_id"])["status"] == "failed"
    job = store.submit(request(), "two")
    store.claim("worker", 1)
    store.cancel(job["job_id"])
    now[0] = 5
    assert store.claim("next") is None
    assert store.get(job["job_id"])["status"] == "cancelled"
    assert store.events(job["job_id"])[-1]["details"]["process_termination_confirmed"] is False


def test_retry_and_cancel_completion_race(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    job = store.submit(request(), "key")
    first = store.claim("worker")
    finished = store.finish(
        first["attempt_id"], 1, {"error": "temporary"}, status="failed", retryable=True
    )
    assert finished["status"] == "queued"
    second = store.claim("worker")
    assert store.cancel(job["job_id"])["status"] == "running"
    with pytest.raises(CancellationRequested):
        store.finish(second["attempt_id"], 2, {"ok": True})
    assert store.finish(second["attempt_id"], 2, {}, status="cancelled")["status"] == "cancelled"
    assert store.cancel(job["job_id"])["status"] == "cancelled"
    other = store.submit(request(), "queued")
    assert store.cancel(other["job_id"])["status"] == "cancelled"
    assert store.claim("worker") is None


def test_concurrent_terminal_commits_have_one_winner(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    store.submit(request(), "key")
    attempt = store.claim("worker")["attempt_id"]

    def finish(value):
        try:
            return store.finish(attempt, 1, {"value": value})["status"]
        except LeaseLost:
            return "rejected"

    with ThreadPoolExecutor(max_workers=8) as executor:
        statuses = list(executor.map(finish, range(8)))
    assert statuses.count("succeeded") == 1
    assert statuses.count("rejected") == 7


@pytest.mark.parametrize(
    "kwargs",
    [
        {"status": "running"},
        {"retryable": True},
        {"status": "failed", "retryable": 1},
        {"outputs": [None]},
        {"outputs": [{"schema_version": "wrong"}]},
    ],
)
def test_invalid_completions(tmp_path, kwargs):
    store = JobStore(tmp_path / "jobs.db")
    store.submit(request(), "key")
    attempt = store.claim("worker")["attempt_id"]
    with pytest.raises(JobError):
        store.finish(attempt, 1, {}, **kwargs)
