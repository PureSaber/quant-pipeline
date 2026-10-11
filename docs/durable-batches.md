# Durable research batches

`BatchStore(JobStore(path))` groups existing durable jobs. Each candidate uses the
existing request, attempt, lease, log, timeout, cancellation, fencing and retry
protocol. Multiple existing workers can claim different candidates concurrently.
No new worker or executor is required.

## Request

Strict `quant.job-batch/v1` fields:

| Field | Meaning |
| --- | --- |
| `project_id`, `run_id` | Research identity; child run ID is `run_id/candidate_id` |
| `shared_inputs` | Immutable ArtifactRefs retained for every candidate |
| `candidates` | Ordered list of 1–1000 candidates; whole request <= 4 MiB |

Each candidate has exactly `candidate_id`, `kind`, `payload`, `inputs`,
`timeout_seconds`, `max_attempts`. Candidate IDs are unique simple identifiers
(1–80 characters). Payload and limits retain ordinary job semantics. Shared and
candidate inputs are deduplicated; contradictory references for one hash fail.
Handlers come only from administrator configuration, never payload import paths.

For rolling research, use the existing Lab `quant.rolling-research/v1` payload
and put its snapshot in `shared_inputs`. Lab already accepts multiple factor
specifications; batches do not expand the algorithms supported by its cache.

```powershell
python -m quant_pipeline.durable_client --database D:\research\jobs.db batch-submit --request batch.json --key project-study-v1 --research
python -m quant_pipeline.durable_client --database D:\research\jobs.db batch-list --project-id my-project
python -m quant_pipeline.durable_client --database D:\research\jobs.db batch-get BATCH_ID
python -m quant_pipeline.durable_client --database D:\research\jobs.db batch-cancel BATCH_ID
python -m quant_pipeline.durable_client --database D:\research\jobs.db batch-retry BATCH_ID --key study-retry-1 --candidate window-20 --research
```

Use `--research` for QDK-backed inputs: the input union is verified and pinned
before any child becomes visible. Failed submissions may leave conservative pins.
Workers reverify inputs before execution and outputs before success.

## Atomicity, state and history

Membership and all children are inserted in one SQLite transaction. Interruption
cannot expose half a batch. Concurrent identical idempotency keys return the same
batch; differing requests conflict. Order is part of identity. New tables are
additive to the v1 job database; ordinary clients/workers still execute children.

`batch-get` reads a consistent child snapshot and returns counts and `terminal`.
Status precedence is `running`, `queued`, `failed`, `cancelled`, `succeeded`.
Active mixed batches remain active; terminal mixed batches preserve every child
state. Read counts rather than interpreting `failed` as every candidate failing.
Lease expiry is reclaimed by worker claim logic, not by a status query.

Batch cancellation atomically requests cancellation of pending/running children.
Finished evidence remains. Running processes keep existing confirmation rules:
a cancellation request does not prove process termination.

Automatic retries obey each job's original attempt limit. Explicit `batch-retry`
accepts only named failed/cancelled candidates and creates new jobs and a new
batch, retaining `retry_of` and `source_job_id`. It never resets history, retries
successes or deletes logs. Parameters stay identical; changed parameters require
a new research request. Prior failed, cancelled and expired attempts remain.

## Shared arbitrary registered inputs

QDK `InputBundleStore` publishes named files into the same CAS as snapshots. Add
its ArtifactRef to `shared_inputs`; the retention guard verifies and protects the
manifest and members transitively. Trusted handlers can read `open_member` without
per-attempt copies. This does not capture undeclared Notebook filesystem/network
reads. See QDK `docs/input-bundles.md` for integrity and retention rules.

## Capacity observations

Install the six-repository durable profile plus `psutil`, then use a new folder:

```powershell
python tools/benchmark_durable_batches.py --workdir D:\research-capacity-new --rows 1000 5000 20000
```

Three window candidates run at each row count and repeat with a warm factor cache.
Evidence records wall time, sampled aggregate RSS of benchmark/child processes at
20 ms intervals, source/CAS/cache/attempt/total bytes, package versions and every
candidate result/cache mode. Inputs, outputs, logs and failures stay available.
Copy-per-candidate bytes are an arithmetic reference, not a measured alternative
implementation. Observations are machine-specific, not throughput/SLA targets.

CAS reuse saves duplicate stored bytes but integrity checks still read all bytes.
The v1 CAS serializes readers with a store lock. Factor input comparison and
materialization remain O(N); this is not end-to-end streaming. Service deployment
and live/full-market-data validation are outside this benchmark.

Tests cover atomic rollback, concurrent submission, real worker death/restart,
batch cancellation, immutable retry lineage, tampered input refusal and retention.
Optional-stack skips must be reported as skips, not passed acceptance evidence.
