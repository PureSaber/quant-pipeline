"""Canonical hashing primitives used by v2 DAGs and checkpoints."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from quant_pipeline.v2_models import ArtifactIntegrityError, CheckpointError


def canonical_json_bytes(value: Any) -> bytes:
    if is_dataclass(value):
        value = asdict(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def hash_artifact_path(path: Path) -> str:
    """Hash a file or directory without following symlinks."""
    if not path.exists():
        raise ArtifactIntegrityError(f"Artifact does not exist: {path}")
    if path.is_symlink():
        raise ArtifactIntegrityError(f"Artifact symlinks are not allowed: {path}")
    if path.is_file():
        return sha256_file(path)
    if not path.is_dir():
        raise ArtifactIntegrityError(f"Unsupported artifact type: {path}")

    entries: list[dict[str, str]] = []
    for child in sorted(path.rglob("*"), key=lambda item: item.relative_to(path).as_posix()):
        relative = child.relative_to(path).as_posix()
        if child.is_symlink():
            raise ArtifactIntegrityError(f"Artifact directory contains symlink: {child}")
        if child.is_file():
            entries.append({"path": relative, "type": "file", "sha256": sha256_file(child)})
        elif child.is_dir():
            entries.append({"path": relative, "type": "directory"})
        else:
            raise ArtifactIntegrityError(f"Unsupported artifact entry: {child}")
    return sha256_bytes(canonical_json_bytes({"type": "directory", "entries": entries}))


def load_stack_manifest(value: Mapping[str, Any] | Path | str) -> dict[str, Any]:
    if isinstance(value, (str, Path)):
        path = Path(value)
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CheckpointError(f"Cannot load stack manifest {path}: {exc}") from exc
    else:
        loaded = dict(value)
    if not isinstance(loaded, dict):
        raise CheckpointError("Stack manifest root must be an object")
    return loaded


def stack_manifest_hash(value: Mapping[str, Any] | Path | str) -> tuple[dict[str, Any], str]:
    manifest = load_stack_manifest(value)
    if manifest.get("schema_version") != "1.0.0":
        raise CheckpointError("Stack manifest schema_version must be '1.0.0'")
    payload = dict(manifest)
    embedded = payload.pop("manifest_sha256", None)
    calculated = sha256_bytes(canonical_json_bytes(payload))
    if embedded is not None and embedded != calculated:
        raise CheckpointError(
            f"Stack manifest hash mismatch: expected {embedded}, calculated {calculated}"
        )
    return manifest, calculated
