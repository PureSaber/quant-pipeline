"""Optional installed-stack checks; missing dependencies are explicitly skipped."""

import pytest

pytest.importorskip("quant_data_kit.input_bundles")

from quant_data_kit.artifacts import ArtifactRef, ArtifactStore
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.input_bundles import InputBundleStore
from test_durable_batches import request

from quant_pipeline.durable_batches import BatchStore
from quant_pipeline.durable_jobs import JobStore
from quant_pipeline.durable_research import (
    retain_artifacts,
    retry_research_batch,
    submit_research_batch,
)
from quant_pipeline.durable_worker import Worker


def test_batch_shared_registered_inputs_and_tamper_guard(tmp_path, monkeypatch):
    monkeypatch.setenv("QUANT_RESEARCH_STORE_ROOT", str(tmp_path / "cas"))
    source = tmp_path / "registered.csv"
    source.write_bytes(b"date,price\n2026-01-01,2\n")
    artifacts = ArtifactStore(tmp_path / "cas")
    bundle = InputBundleStore(artifacts).publish({"prices": source}, owner="project")
    jobs = JobStore(tmp_path / "jobs.db")
    value = request(3)
    value["shared_inputs"] = [bundle.artifact.to_dict()]
    batch = submit_research_batch(jobs, value, "shared")
    assert all(m["job"]["request"]["inputs"] == value["shared_inputs"] for m in batch["members"])
    artifacts.release("project")
    assert artifacts.collect(min_age_seconds=0) == []
    assert len(list((artifacts.root / "objects").glob("*/*"))) == 2
    BatchStore(jobs).cancel(batch["batch_id"])
    retry = retry_research_batch(jobs, batch["batch_id"], "retry", ["window-1"])
    assert retry["members"][0]["source_job_id"] == batch["members"][1]["job"]["job_id"]
    member = ArtifactRef.from_dict(bundle.document["members"][0]["artifact"])
    (artifacts.root / "objects" / member.sha256[:2] / member.sha256).write_bytes(b"tampered")
    with pytest.raises(ValidationError):
        submit_research_batch(jobs, value, "must-not-submit")
    assert len(BatchStore(jobs).list()) == 2
    worker = Worker(
        jobs,
        tmp_path / "attempts",
        {"echo": "durable_handlers:echo"},
        artifact_guard=retain_artifacts,
    )
    # Both attempts fail before the child process starts; no forged successful result.
    assert worker.run_once()["status"] == "queued"
    assert worker.run_once()["status"] == "failed"
    retried_job = retry["members"][0]["job"]["job_id"]
    assert len(jobs.attempts(retried_job)) == 2
    assert not any(e["event"] == "process_started" for e in jobs.events(retried_job))
    assert BatchStore(jobs).get(batch["batch_id"])["counts"]["cancelled"] == 3
