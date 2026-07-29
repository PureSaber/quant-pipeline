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

Pipeline configs reference `quant-workspace` env vars such as `{QW_QUANT_LAB_REPO}`.

## Related

- [quant-workspace](../quant-workspace)
- [quant-lab](../quant-lab)
