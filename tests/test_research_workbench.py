import json
import subprocess
import sys
from pathlib import Path

import pytest

from quant_pipeline.research_demo import create_demo
from quant_pipeline.research_workbench import FixtureExecutor, input_identity


def test_demo_data_is_explicitly_synthetic_and_hashes_verified(tmp_path):
    from quant_lab.research import load_recipe

    path = create_demo(tmp_path / "demo", asset="etf")
    recipe = load_recipe(path)
    evidence = input_identity(recipe)
    assert evidence["bundle"]["raw"]
    manifest = json.loads((tmp_path / "demo/inputs/manifest.json").read_text())
    assert manifest["origin"] == "synthetic_fixture"
    (tmp_path / "demo/inputs/raw.parquet").write_bytes(b"changed")
    with pytest.raises(ValueError, match="integrity"):
        input_identity(recipe)
    with pytest.raises(FileExistsError):
        create_demo(tmp_path / "demo")
    with pytest.raises(ValueError):
        create_demo(tmp_path / "invalid", asset="unknown")


def test_frozen_backend_errors_preserve_log_and_reject_unsupported_variants(tmp_path, monkeypatch):
    executor = FixtureExecutor(Path(sys.executable))
    candidate = {"cost_multiplier": 2, "signal_delay": 0}
    with pytest.raises(ValueError, match="parameters only"):
        executor({}, candidate, tmp_path)
    candidate.update(cost_multiplier=1, factors={}, strategy={}, backend_parameters={})
    recipe = {"factors": {}, "strategy": {}}
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 1, "", "fixture error")
    )
    with pytest.raises(RuntimeError, match="worker"):
        executor(recipe, candidate, tmp_path)
    assert "fixture error" in (tmp_path / "worker.log").read_text()
