"""Canonical hashing primitives used by v2 DAGs and checkpoints."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from quant_workspace import (
    StackManifest,
    validate_stack_manifest,
)
from quant_workspace import (
    load_stack_manifest as load_workspace_stack_manifest,
)

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
    try:
        if isinstance(value, (str, Path)):
            manifest = load_workspace_stack_manifest(Path(value))
        else:
            manifest = StackManifest.from_dict(dict(value))
    except (OSError, TypeError, ValueError) as exc:
        raise CheckpointError(f"Cannot load strict StackManifest: {exc}") from exc
    result = validate_stack_manifest(manifest)
    if not result.valid or not result.release_ready:
        codes = ", ".join(issue.code for issue in result.issues) or "NOT_RELEASE_READY"
        raise CheckpointError(f"StackManifest is not release-ready: {codes}")
    return manifest.to_dict()


def stack_manifest_hash(value: Mapping[str, Any] | Path | str) -> tuple[dict[str, Any], str]:
    manifest = load_stack_manifest(value)
    return manifest, str(manifest["manifest_hash"])
