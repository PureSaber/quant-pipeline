# quant-pipeline

Deterministic local orchestration for research, backtest, and paper-trading workflows across the
PureSaber quant stack. Version 0.3.0 adds the typed `schema_version: "2.0.0"` DAG while preserving
the existing v1 linear YAML behavior.

## Install

```bash
pip install -r requirements-dev.lock
```

## V1 compatibility

```bash
quant-pipe run --config configs/daily_paper.yaml --dry-run
quant-pipe run --config configs/daily_paper.yaml
```

For the research-integrity post-run gate, set the completed run ID and the point-in-time asset
return file before executing the standard-contract validation, attribution, and experiment indexing
steps:

```powershell
$env:QUANT_RUN_ID = "run_001"
$env:QUANT_ASSET_RETURNS = "D:/quant_data/asset_returns.csv"
quant-pipe run --config configs/research_integrity_postrun.yaml
```

Pipeline configs reference `quant-workspace` env vars such as `{QW_QUANT_LAB_REPO}`.

V1 string commands and opt-in `shell: true` remain available only through the legacy configuration
shape. Historical configs are not rewritten.

## Typed DAG v2

V2 requires stable step and artifact IDs, explicit dependencies, argv commands, retry and timeout
policies, and paths contained by `workspace_root`. Shell execution is rejected. The full structural
schema is at `configs/schema/pipeline-v2.schema.json`; semantic validation additionally rejects
duplicate or unknown IDs, self-dependencies, cycles, producer conflicts, undeclared artifact
dependencies, output path conflicts, and runtime path escape.

```yaml
schema_version: "2.0.0"
name: example
workspace_root: "."
checkpoint_path: ".state/checkpoint.json"
log_dir: ".state/logs"
fail_fast: false
artifacts:
  - artifact_id: result
    path: artifacts/result.txt
    producer: build
    required: true
    immutable: true
steps:
  - id: build
    kind: research
    needs: []
    command: [python, -m, research_job, --out, artifacts/result.txt]
    inputs: []
    outputs: [result]
    retry:
      max_attempts: 2
      retry_exit_codes: [75]
      retry_exceptions: [TimeoutExpired]
      backoff_seconds: 1
    timeout: 300
```

Run and strictly resume with an immutable `StackManifest 1.0.0`:

```bash
quant-pipe run --config pipeline-v2.yaml --stack-manifest stack-manifest.json --run-id run-001 --seed 7
quant-pipe run --config pipeline-v2.yaml --stack-manifest stack-manifest.json --run-id run-001 --seed 7 --resume
```

The runner uses lexicographically deterministic topology ordering. Failed descendants become
`blocked`; independent branches continue unless `fail_fast` is enabled. Retries match only the
configured process exit codes or executor exception names. Contract, schema, path, and artifact hash
failures never retry.

Each attempt has separate immutable stdout/stderr logs and SHA-256 values. Checkpoints are canonical
JSON, self-hashed, fsynced, and atomically replaced after state transitions. Resume verifies the run
ID, config hash, stack manifest hash, seed, event sequence, attempt logs, idempotency keys, and every
immutable output hash. Missing or modified inputs, outputs, or logs fail closed.

The v2 implementation consumes a stack manifest as JSON or a mapping and does not import the pending
`quant-workspace` M6 API. The legacy optional dependency is pinned to full commit
`0deb1a30afc8b77c4d8d4b92bb86e44b6acbfefb`; the project lead must replace it with the final published
M6 tag before release.

This package does not provide distributed scheduling, network execution, credentials, or live order
submission.

## Related

- [quant-workspace](../quant-workspace)
- [quant-lab](../quant-lab)
