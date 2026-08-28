from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path

import pytest
from v2_helpers import STACK_MANIFEST

from quant_pipeline.checkpoint import (
    checkpoint_from_payload,
    checkpoint_hash,
    checkpoint_payload,
    load_checkpoint,
    write_checkpoint_atomic,
)
from quant_pipeline.integrity import (
    canonical_json_bytes,
    hash_artifact_path,
    load_stack_manifest,
    sha256_bytes,
    sha256_file,
    stack_manifest_hash,
)
from quant_pipeline.v2_models import (
    ArtifactIntegrityError,
    CheckpointError,
    PipelineCheckpoint,
    PipelineEvent,
    RetryPolicy,
    StepAttempt,
    StepStatus,
)


def _checkpoint() -> PipelineCheckpoint:
    attempt = StepAttempt(
        attempt=1,
        started_at="2026-08-29T00:00:00Z",
        ended_at="2026-08-29T00:00:00Z",
        exit_code=None,
        stdout_log="logs/out.log",
        stderr_log="logs/err.log",
        stdout_sha256="a" * 64,
        stderr_sha256="b" * 64,
        idempotency_key="c" * 64,
        error_type="OSError",
        error_message="transient",
    )
    event = PipelineEvent(
        sequence=1,
        event_time="2026-08-29T00:00:00Z",
        event_type="step_failed",
        step_id="a",
        status=StepStatus.FAILED,
        details={"retryable": False},
    )
    return PipelineCheckpoint(
        schema_version="2.0.0",
        config_hash="1" * 64,
        stack_manifest_hash="2" * 64,
        run_id="run",
        seed=7,
        topology=("a",),
        step_status={"a": StepStatus.FAILED},
        idempotency_keys={"a": "c" * 64},
        input_hashes={"a": {"input": "d" * 64}},
        output_hashes={"a": {}},
        attempts={"a": [attempt]},
        events=[event],
    )


def _signed_payload(checkpoint: PipelineCheckpoint) -> dict:
    payload = checkpoint_payload(checkpoint)
    payload["checkpoint_hash"] = sha256_bytes(canonical_json_bytes(payload))
    return payload


def _resign(payload: dict) -> dict:
    payload.pop("checkpoint_hash", None)
    payload["checkpoint_hash"] = sha256_bytes(canonical_json_bytes(payload))
    return payload


def test_canonical_hashing_file_directory_and_dataclass(tmp_path: Path) -> None:
    file_path = tmp_path / "file.bin"
    file_path.write_bytes(b"abc")
    assert hash_artifact_path(file_path) == sha256_file(file_path)
    assert len(sha256_bytes(b"abc")) == 64
    assert b"max_attempts" in canonical_json_bytes(RetryPolicy(max_attempts=1))

    directory = tmp_path / "tree"
    (directory / "nested").mkdir(parents=True)
    (directory / "z.txt").write_text("z", encoding="utf-8")
    (directory / "nested/a.txt").write_text("a", encoding="utf-8")
    first = hash_artifact_path(directory)
    (directory / "nested/a.txt").write_text("changed", encoding="utf-8")
    assert hash_artifact_path(directory) != first


def test_artifact_hash_rejects_missing_symlink_and_unsupported_types(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    missing = tmp_path / "missing"
    with pytest.raises(ArtifactIntegrityError, match="does not exist"):
        hash_artifact_path(missing)

    target = tmp_path / "target.txt"
    target.write_text("x", encoding="utf-8")
    original_symlink = Path.is_symlink
    monkeypatch.setattr(Path, "is_symlink", lambda self: self == target or original_symlink(self))
    with pytest.raises(ArtifactIntegrityError, match="symlinks"):
        hash_artifact_path(target)
    monkeypatch.undo()

    original_file = Path.is_file
    original_dir = Path.is_dir
    monkeypatch.setattr(
        Path, "is_file", lambda self: False if self == target else original_file(self)
    )
    monkeypatch.setattr(
        Path, "is_dir", lambda self: False if self == target else original_dir(self)
    )
    with pytest.raises(ArtifactIntegrityError, match="Unsupported artifact type"):
        hash_artifact_path(target)


def test_directory_hash_rejects_symlink_entries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "tree"
    directory.mkdir()
    child = directory / "child.txt"
    child.write_text("x", encoding="utf-8")
    original = Path.is_symlink
    monkeypatch.setattr(Path, "is_symlink", lambda self: self == child or original(self))
    with pytest.raises(ArtifactIntegrityError, match="contains symlink"):
        hash_artifact_path(directory)


def test_stack_manifest_mapping_and_file_loading(tmp_path: Path) -> None:
    path = tmp_path / "stack.json"
    path.write_text(json.dumps(STACK_MANIFEST), encoding="utf-8")
    assert load_stack_manifest(path) == STACK_MANIFEST
    assert stack_manifest_hash(path)[1] == stack_manifest_hash(STACK_MANIFEST)[1]

    malformed = tmp_path / "malformed.json"
    malformed.write_text("not-json", encoding="utf-8")
    with pytest.raises(CheckpointError, match="Cannot load"):
        load_stack_manifest(malformed)
    non_object = tmp_path / "array.json"
    non_object.write_text("[]", encoding="utf-8")
    with pytest.raises(CheckpointError, match="root must be an object"):
        load_stack_manifest(non_object)


def test_checkpoint_round_trip_and_stable_hash(tmp_path: Path) -> None:
    checkpoint = _checkpoint()
    path = tmp_path / "checkpoint.json"
    file_hash = write_checkpoint_atomic(path, checkpoint)
    assert file_hash == sha256_file(path)
    assert load_checkpoint(path) == checkpoint
    assert checkpoint_hash(checkpoint) == checkpoint_hash(checkpoint)


def test_checkpoint_parser_rejects_keys_hash_schema_attempt_event_and_types() -> None:
    base = _signed_payload(_checkpoint())
    with pytest.raises(CheckpointError, match="must be an object"):
        checkpoint_from_payload([])

    missing_key = dict(base)
    missing_key.pop("events")
    with pytest.raises(CheckpointError, match="keys mismatch"):
        checkpoint_from_payload(missing_key)

    bad_hash = dict(base)
    bad_hash["run_id"] = "changed"
    with pytest.raises(CheckpointError, match="hash mismatch"):
        checkpoint_from_payload(bad_hash)

    bad_schema = _resign({**base, "schema_version": "1.0.0"})
    with pytest.raises(CheckpointError, match="schema_version"):
        checkpoint_from_payload(bad_schema)

    bad_attempt = json.loads(json.dumps(base))
    bad_attempt["attempts"]["a"][0].pop("attempt")
    with pytest.raises(CheckpointError, match="Invalid step attempt"):
        checkpoint_from_payload(_resign(bad_attempt))

    bad_event = json.loads(json.dumps(base))
    bad_event["events"][0]["status"] = "unknown"
    with pytest.raises(CheckpointError, match="Invalid pipeline event"):
        checkpoint_from_payload(_resign(bad_event))

    bad_seed = _resign({**base, "seed": "not-an-integer"})
    with pytest.raises(CheckpointError, match="Invalid checkpoint payload"):
        checkpoint_from_payload(bad_seed)

    bad_status = json.loads(json.dumps(base))
    bad_status["step_status"]["a"] = "unknown"
    with pytest.raises(CheckpointError, match="Invalid checkpoint payload"):
        checkpoint_from_payload(_resign(bad_status))

    bad_topology = _resign({**base, "topology": "not-a-list"})
    with pytest.raises(CheckpointError, match="must be a list"):
        checkpoint_from_payload(bad_topology)

    bad_digest = _resign({**base, "config_hash": "short"})
    with pytest.raises(CheckpointError, match="lowercase SHA-256"):
        checkpoint_from_payload(bad_digest)

    bad_digest_type = _resign({**base, "config_hash": 7})
    with pytest.raises(CheckpointError, match="must be a string"):
        checkpoint_from_payload(bad_digest_type)

    bad_event_keys = json.loads(json.dumps(base))
    bad_event_keys["events"][0].pop("details")
    with pytest.raises(CheckpointError, match="Invalid pipeline event"):
        checkpoint_from_payload(_resign(bad_event_keys))


def test_load_checkpoint_rejects_missing_and_invalid_json(tmp_path: Path) -> None:
    with pytest.raises(CheckpointError, match="Cannot load checkpoint"):
        load_checkpoint(tmp_path / "missing.json")
    invalid = tmp_path / "invalid.json"
    invalid.write_text("{", encoding="utf-8")
    with pytest.raises(CheckpointError, match="Cannot load checkpoint"):
        load_checkpoint(invalid)


def test_atomic_checkpoint_rejects_symlink_and_wraps_replace_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkpoint = _checkpoint()
    path = tmp_path / "checkpoint.json"
    original = Path.is_symlink
    monkeypatch.setattr(Path, "is_symlink", lambda self: self == path or original(self))
    with pytest.raises(CheckpointError, match="symlink"):
        write_checkpoint_atomic(path, checkpoint)
    monkeypatch.undo()

    def fail_replace(source, destination) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(CheckpointError, match="atomically write"):
        write_checkpoint_atomic(path, checkpoint)
    assert not list(tmp_path.glob("*.tmp"))


def test_checkpoint_event_without_status_and_attempt_without_error_round_trip() -> None:
    checkpoint = _checkpoint()
    checkpoint.events[0] = replace(checkpoint.events[0], status=None, step_id=None)
    checkpoint.attempts["a"][0] = replace(
        checkpoint.attempts["a"][0], exit_code=0, error_type=None, error_message=None
    )
    assert checkpoint_from_payload(_signed_payload(checkpoint)) == checkpoint
