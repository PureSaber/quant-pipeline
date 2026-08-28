# M6 typed DAG local validation

- Date: 2026-08-29
- Branch: `codex/cross-asset-v2-m6`
- Platform: Windows, Python 3.12.5
- Isolated environment: `.venv-m6` installed only from `requirements-dev.lock`
- Lock SHA-256: `16a09b093a94bcdc86fb4d0fb349148222862d8d780b582671f228676361571a`
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
68 passed; total branch coverage 91.73%.

pytest -q tests/test_v2_schema.py tests/test_dag_runner.py tests/test_integrity_checkpoint.py \
  --junitxml=validation/m6/20260829/pytest-core.xml \
  --cov=quant_pipeline.dag_schema --cov=quant_pipeline.dag_runner \
  --cov=quant_pipeline.checkpoint --cov=quant_pipeline.integrity \
  --cov-branch --cov-report=term-missing \
  --cov-report=json:validation/m6/20260829/coverage-core.json --cov-fail-under=90
45 passed; combined v2 core branch coverage 92.42%.
```

Core file coverage from the final run:

- `checkpoint.py`: 95%
- `dag_runner.py`: 91%
- `dag_schema.py`: 92%
- `integrity.py`: 98%

## Evidence hashes

- `pytest-full.xml`: `da05cdab8d78d28f5b13b70a7f244258b217ae49163dc3e54b91ebfe8f2c425c`
- `coverage-full.json`: `7e16eb06d7fe839c6f904a4e0328a2b736e3449420cd9597aaeddc9a0debebbb`
- `pytest-core.xml`: `9dab13fc3372fd7bde967667509156869eb86e3dad5bc75a41573d7f35d7fd1d`
- `coverage-core.json`: `a29cbf42054707ed3e91f8f3eba0b3c8401fb23d903865b6e97603b72df16698`

Python 3.10 and 3.11 were not installed on this workstation. The GitHub Actions matrix is configured
for Python 3.10, 3.11, and 3.12; those two versions require project-lead push/PR validation.
