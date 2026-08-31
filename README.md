# quant-pipeline

Deterministic local orchestration for research, backtest, and paper-trading workflows across the
PureSaber quant stack. Version 0.3.3 retains the typed `schema_version: "2.0.0"` DAG introduced in
0.3.0, the published workspace dependency from0.3.2, and pins the CI runtime actions to reviewed
full SHAs. Generated full/core coverage evidence is explicitly ignored locally. The existing v1
linear YAML behavior remains available.

## Install

```bash
pip install --no-deps -r requirements.lock
pip check
pip install -e . --no-deps --no-build-isolation
pip check
```

Rebuild the complete runtime, development, and editable-build lock with Python 3.10 so the oldest
supported interpreter's conditional dependency closure remains explicit:

```bash
python -m piptools compile --extra dev --build-deps-for editable \
  --allow-unsafe --strip-extras --resolver backtracking \
  --index-url https://pypi.org/simple --output-file requirements.lock pyproject.toml
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
configured process exit codes or executor exception names. Steps whose `kind` is `data_quality`,
`schema_validation`, `sequence_validation`, `hash_validation`, or `pit_validation` are fail-closed
gates: their failures never retry even if a retry matcher is configured. Contract, path, and
artifact hash failures also never retry.

Each attempt has separate immutable stdout/stderr logs and SHA-256 values. Checkpoints are canonical
JSON, self-hashed, fsynced, and atomically replaced after state transitions. Resume verifies the run
ID, config hash, stack manifest hash, seed, event sequence, attempt logs, idempotency keys, and every
immutable output hash. This validation also applies to a pending retry checkpoint before any resume
event or replacement checkpoint is written. Missing or modified inputs, outputs, or logs fail
closed.

The v2 implementation accepts only a canonical, self-hashed, release-ready `StackManifest 1.0.0`
produced by the published `quant-workspace v0.3.1` contract. The dependency uses the immutable
annotated tag `v0.3.1`, which peels to commit
`537388a4d9548b612fa1e4b306c482c04b45c433`. File inputs must use canonical JSON;
both files and mappings are validated by `quant-workspace`. The integration is a required runtime
dependency and is pinned in `requirements.lock`.

The0.3.2 migration replaced the workspace dependency tag and lock/install governance. The0.3.3
change only removes CI-runtime drift and generated coverage-file noise; neither release changes DAG,
checkpoint, retry, integrity, or v1 compatibility semantics. To roll back the candidate, use
`git revert <0.3.3-ci-hygiene-commit>` and rebuild from the reverted`pyproject.toml`. Never move or
recreate any historical tag.

This package does not provide distributed scheduling, network execution, credentials, or live order
submission.

## Related

- [quant-workspace](../quant-workspace)
- [quant-lab](../quant-lab)
