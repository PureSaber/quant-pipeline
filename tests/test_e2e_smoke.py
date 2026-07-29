"""End-to-end smoke tests using local fixtures."""

from __future__ import annotations

import sys
from pathlib import Path

import yaml


def test_pipeline_with_workspace_env(tmp_path: Path) -> None:
    root = tmp_path / "stack"
    (root / "alpha").mkdir(parents=True)

    ws_cfg = tmp_path / "workspace.yaml"
    ws_cfg.write_text(
        yaml.safe_dump({"root": str(root), "projects": {"alpha": {"repo": "alpha", "outputs": "outputs"}}}),
        encoding="utf-8",
    )

    pipe_cfg = tmp_path / "pipe.yaml"
    pipe_cfg.write_text(
        yaml.safe_dump(
            {
                "name": "smoke",
                "workspace": str(ws_cfg),
                "steps": [{"name": "show", "cmd": f"{sys.executable} -c \"print('ok')\""}],
            }
        ),
        encoding="utf-8",
    )

    from quant_pipeline.runner import run_pipeline

    result = run_pipeline(pipe_cfg, dry_run=False, stop_on_error=True)
    assert result.ok
