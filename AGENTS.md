# quant-pipeline

YAML-driven post-run orchestration for the PureSaber quant stack.

## Commands

```bash
pip install -e ../quant-workspace
pip install -e ".[dev]"
quant-pipe run --config configs/daily_paper.yaml --dry-run
pytest -q
ruff check src tests
```

## Related

- [quant-workspace](../quant-workspace)
- [quant-lab](../quant-lab)
