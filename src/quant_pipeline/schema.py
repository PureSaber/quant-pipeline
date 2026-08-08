"""Validate pipeline YAML configs against a JSON schema."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "configs" / "schema" / "pipeline.schema.json"


def load_schema() -> dict[str, Any]:
    return json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))


def validate_config(cfg: dict[str, Any], *, strict: bool = False) -> list[str]:
    """Return validation messages. Empty list means OK."""
    issues: list[str] = []
    allowed_top = {"name", "workspace", "cwd", "env", "steps", "run_id", "error_log_dir"}
    for key in cfg:
        if key not in allowed_top:
            msg = f"Unknown top-level key: {key!r}"
            issues.append(msg if strict else f"WARN: {msg}")

    steps = cfg.get("steps")
    if not isinstance(steps, list) or not steps:
        issues.append("steps must be a non-empty list")
        return issues

    for idx, step in enumerate(steps):
        if not isinstance(step, dict):
            issues.append(f"steps[{idx}] must be a mapping")
            continue
        if "cmd" not in step:
            issues.append(f"steps[{idx}] missing required key 'cmd'")
        if "name" not in step:
            issues.append(f"WARN: steps[{idx}] missing 'name'")
        for key in step:
            if key not in {"name", "cmd", "cwd", "shell"}:
                msg = f"steps[{idx}] unknown key {key!r}"
                issues.append(msg if strict else f"WARN: {msg}")
    return issues


def validate_config_file(path: Path, *, strict: bool = False) -> list[str]:
    cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return validate_config(cfg, strict=strict)
