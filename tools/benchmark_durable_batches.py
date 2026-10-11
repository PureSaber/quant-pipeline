"""Reproducible synthetic capacity observation, never a performance acceptance target.

Requires the installed QDK/Lab/Factors durable profile plus psutil. Every run needs
a new work directory; inputs, outputs, logs and failures are preserved for audit.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.metadata
import json
import os
import platform
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
from quant_data_kit.artifacts import ArtifactStore
from quant_data_kit.input_bundles import InputBundleStore
from quant_data_kit.shared_snapshots import SnapshotStore
from quant_factors.contracts_v2 import AsOfSpec, FactorDependency, FactorSpec, FrequencySpec

from quant_pipeline.durable_batches import BatchStore
from quant_pipeline.durable_jobs import JobStore
from quant_pipeline.durable_research import retain_artifacts, submit_research_batch
from quant_pipeline.durable_worker import Worker


def bytes_under(root):
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def scenario(root: Path, rows: int, windows: list[int]) -> dict:
    root.mkdir()
    process, done = psutil.Process(), threading.Event()
    peak = [0]

    def sample():
        while not done.is_set():
            total = 0
            for item in [process, *process.children(recursive=True)]:
                try:
                    total += item.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            peak[0] = max(peak[0], total)
            done.wait(0.02)

    monitor = threading.Thread(target=sample, daemon=True)
    monitor.start()
    started = time.perf_counter()
    try:
        stamps = pd.date_range("2020-01-01", periods=rows, freq="min", tz="UTC")
        table = pa.Table.from_pydict(
            {
                "instrument_id": ["SYNTHETIC"] * rows,
                "event_time": stamps,
                "received_at": stamps,
                "available_at": stamps,
                "sequence": list(range(rows)),
                "event_id": [f"event-{i}" for i in range(rows)],
                "close": [100 + i * 0.001 + (i % 17) * 0.01 for i in range(rows)],
            }
        )
        source = root / "source.parquet"
        pq.write_table(table, source)
        artifacts = ArtifactStore(root / "cas")
        manifest = SnapshotStore(artifacts).publish(
            {"prices": source},
            dataset="synthetic-capacity",
            version="1",
            temporal={"calendar": "synthetic-minute", "timezone": "UTC"},
            owner="benchmark-input",
            date_column="event_time",
            symbol_column="instrument_id",
            available_at_column="available_at",
        )
        # This arbitrary registered input is shared by every job, in addition to the snapshot.
        notes = root / "research-plan.txt"
        notes.write_text("Synthetic capacity only; no alpha evidence.\n", encoding="utf-8")
        bundles = InputBundleStore(artifacts)
        bundle = bundles.publish({"plan.txt": notes}, owner="benchmark-plan")
        snapshot = manifest.artifact.to_dict()
        frequency = FrequencySpec(
            "minute",
            "session_bar",
            "365",
            "synthetic-minute",
            "v1",
            None,
            "trading_day",
            None,
            None,
            None,
        )
        candidates = []
        for window in windows:
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
            candidates.append(
                {
                    "candidate_id": f"window-{window}",
                    "kind": "rolling_research",
                    "inputs": [],
                    "timeout_seconds": 600,
                    "max_attempts": 1,
                    "payload": {
                        "schema_version": "quant.rolling-research/v1",
                        "snapshot": snapshot,
                        "frequency": frequency.to_contract(),
                        "factors": [factor.to_contract()],
                        "as_of": AsOfSpec("source_available_at", None).to_contract(),
                        "universe": ["SYNTHETIC"],
                    },
                }
            )
        request = {
            "schema_version": "quant.job-batch/v1",
            "project_id": "capacity",
            "run_id": "synthetic",
            "shared_inputs": [snapshot, bundle.artifact.to_dict()],
            "candidates": candidates,
        }
        os.environ["QUANT_RESEARCH_STORE_ROOT"] = str(artifacts.root)
        os.environ["QUANT_RESEARCH_CACHE_DB"] = str(root / "factor-cache.db")
        os.environ.pop("QUANT_RESEARCH_REGISTRY_DB", None)
        jobs = JobStore(root / "jobs.db")
        worker = Worker(
            jobs,
            root / "attempts",
            {"rolling_research": "quant_lab.rolling_research:worker_handler"},
            artifact_guard=retain_artifacts,
        )
        preparation_seconds = time.perf_counter() - started
        rounds = []
        input_bytes_before = bytes_under(artifacts.root / "objects")
        for index in range(2):
            round_start = time.perf_counter()
            batch = submit_research_batch(jobs, request, f"round-{index}")
            while worker.run_once() is not None:
                pass
            state = BatchStore(jobs).get(batch["batch_id"])
            rounds.append(
                {
                    "label": "cold" if index == 0 else "same-input-warm-cache",
                    "seconds": time.perf_counter() - round_start,
                    "batch_id": state["batch_id"],
                    "status": state["status"],
                    "counts": state["counts"],
                    "jobs": [
                        {
                            "job_id": m["job"]["job_id"],
                            "status": m["job"]["status"],
                            "result": m["job"]["result"],
                        }
                        for m in state["members"]
                    ],
                }
            )
        return {
            "rows": rows,
            "windows": windows,
            "preparation_seconds": preparation_seconds,
            "total_seconds": time.perf_counter() - started,
            "peak_tree_rss_bytes_sampled": peak[0],
            "source_bytes": source.stat().st_size,
            "input_cas_bytes": input_bytes_before,
            "copy_per_candidate_input_bytes_reference": source.stat().st_size * len(windows) * 2,
            "cas_total_bytes": bytes_under(artifacts.root),
            "cache_bytes": (root / "factor-cache.db").stat().st_size,
            "attempt_bytes": bytes_under(root / "attempts"),
            "workdir_bytes": bytes_under(root),
            "bundle": bundles.describe(bundle.artifact),
            "rounds": rounds,
        }
    finally:
        done.set()
        monitor.join(timeout=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", required=True, type=Path)
    parser.add_argument("--rows", nargs="+", type=int, default=[1000, 5000, 20000])
    parser.add_argument("--windows", nargs="+", type=int, default=[5, 20, 60])
    args = parser.parse_args()
    args.workdir = args.workdir.resolve()
    if (
        args.workdir.exists()
        or any(n < 100 for n in args.rows)
        or len(args.rows) != len(set(args.rows))
    ):
        parser.error("use a new workdir and distinct row counts >= 100")
    args.workdir.mkdir(parents=True)
    evidence = {
        "schema_version": "quant.batch-capacity-observation/v1",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "physical_memory_bytes": psutil.virtual_memory().total,
        "packages": {
            name: importlib.metadata.version(name)
            for name in (
                "quant-pipeline",
                "quant-data-kit",
                "quant-factors",
                "quant-lab",
                "pandas",
                "pyarrow",
                "psutil",
            )
        },
        "source_sha256": {
            name: hashlib.sha256(
                Path(importlib.import_module(name).__file__).read_bytes()
            ).hexdigest()
            for name in (
                "quant_pipeline.durable_batches",
                "quant_pipeline.durable_jobs",
                "quant_pipeline.durable_research",
                "quant_pipeline.durable_worker",
                "quant_data_kit.artifacts",
                "quant_data_kit.input_bundles",
                "quant_data_kit.shared_snapshots",
                "quant_factors.cache",
                "quant_lab.rolling_research",
            )
        },
        "measurement": {
            "rss_sampling_seconds": 0.02,
            "workers": 1,
            "target": None,
            "market_data": "synthetic",
            "end_to_end_streaming": False,
        },
        "scenarios": [],
    }
    result_path = args.workdir / "capacity.json"
    for rows in args.rows:
        evidence["scenarios"].append(scenario(args.workdir / f"rows-{rows}", rows, args.windows))
        result_path.write_text(json.dumps(evidence, indent=2, ensure_ascii=False), encoding="utf-8")
        print(
            json.dumps(
                {"rows": rows, "status": [r["status"] for r in evidence["scenarios"][-1]["rounds"]]}
            ),
            flush=True,
        )
    return (
        0
        if all(r["status"] == "succeeded" for s in evidence["scenarios"] for r in s["rounds"])
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
