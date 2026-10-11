"""Optional real four-repository acceptance, enabled by source overlay/SDK installation."""

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
from copy import deepcopy
from pathlib import Path

import pytest

pytest.importorskip("quant_data_kit.shared_snapshots", reason="requires data-line candidate")
pytest.importorskip("quant_factors.cache", reason="requires factors-line candidate")
pytest.importorskip("quant_lab.rolling_research", reason="requires lab-line candidate")

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from quant_data_kit.artifacts import ArtifactRef, ArtifactStore
from quant_data_kit.shared_snapshots import SnapshotStore
from quant_factors.contracts_v2 import AsOfSpec, FactorDependency, FactorSpec, FrequencySpec

from quant_pipeline.durable_jobs import JobStore
from quant_pipeline.durable_research import submit_research


def test_real_shared_snapshot_multi_parameter_failure_and_restart(tmp_path, monkeypatch):
    source_root = Path(__file__).resolve().parents[1]
    monkeypatch.setenv(
        "PYTHONPATH",
        os.pathsep.join(
            [str(source_root / "src"), str(source_root / "tests"), os.environ.get("PYTHONPATH", "")]
        ),
    )
    monkeypatch.setenv("QUANT_RESEARCH_STORE_ROOT", str(tmp_path / "cas"))
    monkeypatch.setenv("QUANT_RESEARCH_CACHE_DB", str(tmp_path / "cache.db"))
    monkeypatch.delenv("QUANT_RESEARCH_REGISTRY_DB", raising=False)
    rows = []
    for symbol in ("A", "B"):
        for i in range(40):
            stamp = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(days=i)
            rows.append(
                {
                    "instrument_id": symbol,
                    "event_time": stamp,
                    "received_at": stamp,
                    "available_at": stamp,
                    "sequence": i,
                    "event_id": f"{symbol}-{i}",
                    "close": float(100 + i + i % 7),
                }
            )
    source = tmp_path / "prices.parquet"
    pq.write_table(pa.Table.from_pylist(rows), source)
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    artifacts = ArtifactStore(tmp_path / "cas", context={"run_id": "integration"})
    manifest = SnapshotStore(artifacts).publish(
        {"prices": source},
        dataset="synthetic",
        version="1",
        temporal={"calendar": "synthetic-daily", "timezone": "UTC"},
        owner="fixture",
        date_column="event_time",
        symbol_column="instrument_id",
        available_at_column="available_at",
    )
    snapshot = manifest.artifact.to_dict()
    freq = FrequencySpec(
        "daily",
        "session_bar",
        "365",
        "synthetic-daily",
        "v1",
        None,
        "trading_day",
        None,
        None,
        None,
    )
    store = JobStore(tmp_path / "jobs.db")
    requests, jobs = [], []
    for window in (3, 5, 7):
        factor = FactorSpec(
            "momentum",
            "1.0.0",
            "momentum",
            "bar",
            [FactorDependency("market", "close", "available_at")],
            window,
            "float64",
            False,
            "null",
        )
        request = {
            "schema_version": "quant.job-request/v1",
            "kind": "research",
            "project_id": "integration",
            "run_id": f"window-{window}",
            "inputs": [snapshot],
            "timeout_seconds": 30,
            "max_attempts": 2,
            "payload": {
                "schema_version": "quant.rolling-research/v1",
                "snapshot": snapshot,
                "frequency": freq.to_contract(),
                "factors": [factor.to_contract()],
                "as_of": AsOfSpec("source_available_at", None).to_contract(),
                "universe": ["A", "B"],
            },
        }
        requests.append(request)
        jobs.append(submit_research(store, request, f"window-{window}")["job_id"])
    assert submit_research(store, requests[0], "window-3")["job_id"] == jobs[0]
    assert len(store.list()) == 3

    def command(handler="quant_lab.rolling_research:worker_handler"):
        return [
            sys.executable,
            "-m",
            "quant_pipeline.durable_worker",
            "--database",
            str(store.path),
            "--workspace",
            str(tmp_path / "attempts"),
            "--handler",
            f"research={handler}",
            "--artifact-guard",
            "quant_pipeline.durable_research:retain_artifacts",
            "--lease-seconds",
            "2",
            "--once",
        ]

    def execute(handler="quant_lab.rolling_research:worker_handler"):
        completed = subprocess.run(
            command(handler), capture_output=True, text=True, timeout=60, check=False
        )
        assert completed.returncode == 0, completed.stderr
        result = json.loads(completed.stdout)
        if result["status"] == "failed":
            job = store.get(result["job_id"])
            stderr = tmp_path / "attempts" / job["attempt_id"] / "stderr.log"
            pytest.fail(f"{job['result']}\n{stderr.read_text(encoding='utf-8')}")
        return result

    processes = [
        subprocess.Popen(command(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for _ in range(2)
    ]
    for process in processes:
        stdout, stderr = process.communicate(timeout=60)
        assert process.returncode == 0, stderr
        assert json.loads(stdout)["status"] == "succeeded", stdout
    assert execute()["status"] == "succeeded"
    first_result = store.get(jobs[0])["result"]
    repeated = deepcopy(requests[0])
    repeated["run_id"] = "new-run-same-calculation"
    repeated_job = submit_research(store, repeated, "reuse")["job_id"]
    assert execute()["status"] == "succeeded"
    assert store.get(repeated_job)["result"]["cache"]["execution_mode"] == "cache_hit"
    assert store.get(repeated_job)["result"]["computation_id"] == first_result["computation_id"]

    failed_job = submit_research(store, repeated, "deliberate-retry")["job_id"]
    assert execute("durable_handlers:research_fail_once")["status"] == "queued"
    assert execute("durable_handlers:research_fail_once")["status"] == "succeeded"
    assert [a["status"] for a in store.attempts(failed_job)] == ["failed", "succeeded"]

    recovered_job = submit_research(store, repeated, "deliberate-restart")["job_id"]
    process = subprocess.Popen(
        command("durable_handlers:research_delay"), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    try:
        deadline = time.monotonic() + 20
        while not any(e["event"] == "process_started" for e in store.events(recovered_job)):
            assert time.monotonic() < deadline
            time.sleep(0.03)
        process.kill()
        process.communicate(timeout=10)
        time.sleep(2.2)
        assert execute("durable_handlers:research_delay")["status"] == "succeeded"
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)
    assert [a["status"] for a in store.attempts(recovered_job)] == ["expired", "succeeded"]
    assert len([e for e in store.events(recovered_job) if e["event"] == "completed"]) == 1
    assert hashlib.sha256(source.read_bytes()).hexdigest() == source_hash
    for job in store.list():
        assert job["status"] == "succeeded"
        assert job["request"]["inputs"] == [snapshot]
        for ref in job["outputs"]:
            artifacts.verify(ArtifactRef.from_dict(ref))
    assert artifacts.collect(min_age_seconds=0) == []
    with sqlite3.connect(tmp_path / "cache.db") as db:
        assert db.execute("SELECT COUNT(*) FROM factor_cache_inputs_v1").fetchone()[0] == 1
    report = {
        "schema_version": "quant.integration-evidence/v1",
        "source_sha256": source_hash,
        "snapshot": snapshot,
        "jobs": store.list(),
        "attempts": {job["job_id"]: store.attempts(job["job_id"]) for job in store.list()},
        "events": {job["job_id"]: store.events(job["job_id"]) for job in store.list()},
        "python": sys.executable,
        "modules": {
            name: getattr(module, "__file__", None)
            for name, module in sys.modules.copy().items()
            if name
            in (
                "quant_pipeline.durable_jobs",
                "quant_data_kit.artifacts",
                "quant_factors.cache",
                "quant_lab.rolling_research",
            )
        },
    }
    (tmp_path / "integration-evidence.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
