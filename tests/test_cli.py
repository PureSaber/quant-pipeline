from __future__ import annotations

import json
from pathlib import Path

import yaml
from v2_helpers import STACK_MANIFEST, base_config, write_config

from quant_pipeline.cli import main
from quant_pipeline.integrity import canonical_json_bytes


def test_v1_cli_json_dry_run(tmp_path: Path, capsys) -> None:
    path = tmp_path / "v1.yaml"
    path.write_text(
        yaml.safe_dump({"name": "legacy", "steps": [{"name": "a", "cmd": "echo ok"}]}),
        encoding="utf-8",
    )
    assert main(["run", "--config", str(path), "--dry-run", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["name"] == "legacy"


def test_v2_cli_requires_manifest_and_prints_json(tmp_path: Path, capsys) -> None:
    config_path = write_config(tmp_path, base_config())
    assert main(["run", "--config", str(config_path), "--dry-run"]) == 2
    assert "--stack-manifest" in capsys.readouterr().err

    manifest_path = tmp_path / "stack.json"
    manifest_path.write_bytes(canonical_json_bytes(STACK_MANIFEST) + b"\n")
    exit_code = main(
        [
            "run",
            "--config",
            str(config_path),
            "--stack-manifest",
            str(manifest_path),
            "--run-id",
            "cli-run",
            "--seed",
            "7",
            "--dry-run",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["step_status"] == {"build": "dry_run"}


def test_cli_rejects_v1_resume_and_invalid_v2(tmp_path: Path, capsys) -> None:
    v1 = tmp_path / "v1.yaml"
    v1.write_text(yaml.safe_dump({"steps": [{"cmd": "echo ok"}]}), encoding="utf-8")
    assert main(["run", "--config", str(v1), "--resume"]) == 2
    assert "only available" in capsys.readouterr().err

    invalid = base_config()
    invalid["steps"][0]["command"] = "shell command"
    invalid_path = write_config(tmp_path, invalid, "invalid.yaml")
    manifest = tmp_path / "stack.json"
    manifest.write_bytes(canonical_json_bytes(STACK_MANIFEST) + b"\n")
    assert (
        main(
            [
                "run",
                "--config",
                str(invalid_path),
                "--stack-manifest",
                str(manifest),
            ]
        )
        == 2
    )
    assert "v2 pipeline rejected" in capsys.readouterr().err
