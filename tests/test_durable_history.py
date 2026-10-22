import json

import pytest
from test_durable_batches import request as batch_request
from test_durable_jobs import request

from quant_pipeline.durable_batches import BatchStore
from quant_pipeline.durable_client import main
from quant_pipeline.durable_jobs import JobError, JobStore


@pytest.mark.parametrize("batch", [False, True])
def test_history_cursor_keeps_older_records_reachable_during_new_submissions(tmp_path, batch):
    clock = [1000.0]
    jobs = JobStore(tmp_path / "jobs.db", clock=lambda: clock[0])
    store = BatchStore(jobs) if batch else jobs
    payload = batch_request(1) if batch else request()
    identity = "batch_id" if batch else "job_id"
    original = {store.submit(payload, str(i))[identity] for i in range(35)}
    filters = {"project_id": "project"} if batch else {"status": "queued"}
    first = store.list(limit=30, **filters)
    clock[0] += 1
    store.submit(payload, "newer")
    rest = store.list(limit=30, before=first[-1][identity], **filters)
    found = [row[identity] for row in first + rest]
    assert len(found) == len(set(found)) == 35
    assert set(found) == original
    assert store.list(limit=30, before=rest[-1][identity], **filters) == []
    with pytest.raises(JobError, match="cursor"):
        store.list(before="missing")


def test_cli_advertises_and_accepts_history_cursors(tmp_path, capsys):
    database = tmp_path / "jobs.db"
    jobs = JobStore(database)
    jobs.submit(request(), "first")
    jobs.submit(request(), "second")
    batches = BatchStore(jobs)
    batches.submit(batch_request(1), "batch-first")
    batches.submit(batch_request(1), "batch-second")
    base = ["--database", str(database)]
    assert main([*base, "capabilities"]) == 0
    assert json.loads(capsys.readouterr().out)["history_pagination"] == "cursor/v1"
    for command, identity in (("list", "job_id"), ("batch-list", "batch_id")):
        assert main([*base, command, "--limit", "1"]) == 0
        first = json.loads(capsys.readouterr().out)
        assert main([*base, command, "--limit", "1", "--before", first[0][identity]]) == 0
        older = json.loads(capsys.readouterr().out)
        assert len(older) == 1 and older[0][identity] != first[0][identity]
