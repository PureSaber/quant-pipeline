# M6 typed DAG local validation

- Date: 2026-08-29
- Branch: `codex/cross-asset-v2-m6`
- Baseline: `bbb1bb7cef8b14258b8100fa02dfd19f60a0787b`
- Platform: Windows, Python 3.12.5
- Isolated environment: `.venv-m6` installed only from `requirements-dev.lock`
- Lock SHA-256: `589b04612ff97e01a89d5f009906b046c539ea53a155124587f4c48e8e5fd995`
- Internal dependency: `quant-workspace@v0.2.0`，解引用commit
  `1a9134ac329704060a3ae96cc81e31db481a938f`；安装日志确认不再读取浮动分支。
- `pip check`: no broken requirements

## Commands and results

```text
ruff check src tests
All checks passed.

ruff format --check src tests
22 files already formatted.

pytest -q --junitxml=validation/m6/20260829/pytest-full.xml \
  --cov=quant_pipeline --cov-branch --cov-report=term-missing \
  --cov-report=json:validation/m6/20260829/coverage-full.json --cov-fail-under=80
75 passed; total branch coverage 93.63%.

pytest -q tests/test_v2_schema.py tests/test_dag_runner.py tests/test_integrity_checkpoint.py \
  --junitxml=validation/m6/20260829/pytest-core.xml \
  --cov=quant_pipeline.dag_schema --cov=quant_pipeline.dag_runner \
  --cov=quant_pipeline.checkpoint --cov=quant_pipeline.integrity \
  --cov-branch --cov-report=term-missing \
  --cov-report=json:validation/m6/20260829/coverage-core.json
52 passed; combined report is informational; pure branch gates are checked per file from JSON.
```

Core pure branch coverage from `coverage-core.json`:

- `dag_runner.py`: 96/102 = 94.1176%
- `dag_schema.py`: 107/118 = 90.6780%
- `checkpoint.py`: 29/30 = 96.6667%
- `integrity.py`: 27/28 = 96.4286%

Added regression coverage:

- `data_quality_gate` exits with an unconfigured code and has `max_attempts=3`; it executes once,
  its `curated_builder` and `strategy_research` descendants are `blocked`, and the independent
  branch succeeds when `fail_fast=false`.
- Added fail-closed checks for exception stdout/stderr preservation, optional input hashing,
  declared artifact hash, attempt-log collision, existing run-log directory, and exhausted retry
  checkpoint handling.
- CI now reads `coverage-core.json` and independently enforces `covered_branches/num_branches >= 0.90`
  for each of the four core files; no combined coverage percentage substitutes for a file gate.

## Evidence hashes

- `pytest-full.xml`: `d7abc5834c17206568f57eeb840f949f26cb3f81dbf8239065f3af221690a8a1`
- `coverage-full.json`: `9b6c486c1e0130b1c76c271ba679c97768f501fd61007cc70c879049f59833a2`
- `pytest-core.xml`: `731a21921c380b0cb39aa705d1f46a2079476cbdbfbcc3b2f3b906ad2d4427c7`
- `coverage-core.json`: `0cc0ec8734cf224a6ebb324c323afedcf62427c27b22bdffc1c2171c3b7a22c6`

Python 3.10 and 3.11 were not installed on this workstation. The GitHub Actions matrix is configured
for Python 3.10, 3.11, and 3.12; those two versions require project-lead push/PR validation.

The L2 storage quota stop is owned by `quant-data-kit`; this pipeline change keeps the frozen DAG
artifact/failure-isolation contract and does not invent a new resource artifact field.
