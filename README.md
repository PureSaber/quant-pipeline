# quant-pipeline

YAML-driven orchestration for post-run workflows across the PureSaber quant stack.

## Install

```bash
pip install -e ../quant-workspace
pip install -e ".[dev]"
```

## Usage

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

## Related

- [quant-workspace](../quant-workspace)
- [quant-lab](../quant-lab)
