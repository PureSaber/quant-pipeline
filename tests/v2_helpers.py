from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


def release_manifest(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": "1.0.0",
        "mode": "release",
        "created_at": "2026-08-29T00:00:00Z",
        "workspace_config_sha256": "1" * 64,
        "repositories": [],
        "dependency_dag": [],
        "allowed_schemas": [{"schema_id": "standard/v2", "version": "2.0.0"}],
        "release_ready": True,
        **overrides,
    }
    return {
        **payload,
        "manifest_hash": hashlib.sha256(
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
    }


STACK_MANIFEST = release_manifest()


def fixed_clock() -> datetime:
    return datetime(2026, 8, 29, tzinfo=timezone.utc)


def python_write_command(path: str, content: str = "ok") -> list[str]:
    code = (
        "from pathlib import Path; "
        f"p=Path({path!r}); p.parent.mkdir(parents=True, exist_ok=True); "
        f"p.write_text({content!r}, encoding='utf-8')"
    )
    return [sys.executable, "-c", code]


def base_config(*, output: str = "artifacts/output.txt") -> dict[str, Any]:
    return {
        "schema_version": "2.0.0",
        "name": "fixture",
        "workspace_root": ".",
        "checkpoint_path": ".state/checkpoint.json",
        "log_dir": ".state/logs",
        "fail_fast": False,
        "artifacts": [
            {
                "artifact_id": "output",
                "path": output,
                "producer": "build",
                "required": True,
                "immutable": True,
            }
        ],
        "steps": [
            {
                "id": "build",
                "kind": "fixture",
                "needs": [],
                "command": python_write_command(output),
                "inputs": [],
                "outputs": ["output"],
                "retry": {
                    "max_attempts": 1,
                    "retry_exit_codes": [],
                    "retry_exceptions": [],
                    "backoff_seconds": 0,
                },
                "timeout": 10,
            }
        ],
    }


def write_config(root: Path, config: dict[str, Any], name: str = "pipeline.yaml") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / name
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path
