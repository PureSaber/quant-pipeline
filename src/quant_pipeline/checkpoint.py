"""Deterministic, self-verifying and atomically updated DAG checkpoints."""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any

from quant_pipeline.integrity import canonical_json_bytes, sha256_bytes, sha256_file
from quant_pipeline.v2_models import (
    CheckpointError,
    PipelineCheckpoint,
    PipelineEvent,
    StepAttempt,
    StepStatus,
)

_CHECKPOINT_KEYS = {
    "schema_version",
    "config_hash",
    "stack_manifest_hash",
    "run_id",
    "seed",
    "topology",
    "step_status",
    "idempotency_keys",
    "input_hashes",
    "output_hashes",
    "attempts",
    "events",
    "checkpoint_hash",
}
_ATTEMPT_KEYS = {
    "attempt",
    "started_at",
    "ended_at",
    "exit_code",
    "stdout_log",
    "stderr_log",
    "stdout_sha256",
    "stderr_sha256",
    "idempotency_key",
    "error_type",
    "error_message",
}
_EVENT_KEYS = {"sequence", "event_time", "event_type", "step_id", "status", "details"}
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _attempt_payload(attempt: StepAttempt) -> dict[str, Any]:
    return {
        "attempt": attempt.attempt,
        "started_at": attempt.started_at,
        "ended_at": attempt.ended_at,
        "exit_code": attempt.exit_code,
        "stdout_log": attempt.stdout_log,
        "stderr_log": attempt.stderr_log,
        "stdout_sha256": attempt.stdout_sha256,
        "stderr_sha256": attempt.stderr_sha256,
        "idempotency_key": attempt.idempotency_key,
        "error_type": attempt.error_type,
        "error_message": attempt.error_message,
    }


def _event_payload(event: PipelineEvent) -> dict[str, Any]:
    return {
        "sequence": event.sequence,
        "event_time": event.event_time,
        "event_type": event.event_type,
        "step_id": event.step_id,
        "status": event.status.value if event.status is not None else None,
        "details": event.details,
    }


def checkpoint_payload(checkpoint: PipelineCheckpoint) -> dict[str, Any]:
    return {
        "schema_version": checkpoint.schema_version,
        "config_hash": checkpoint.config_hash,
        "stack_manifest_hash": checkpoint.stack_manifest_hash,
        "run_id": checkpoint.run_id,
        "seed": checkpoint.seed,
        "topology": list(checkpoint.topology),
        "step_status": {
            step_id: status.value for step_id, status in sorted(checkpoint.step_status.items())
        },
        "idempotency_keys": dict(sorted(checkpoint.idempotency_keys.items())),
        "input_hashes": {
            step_id: dict(sorted(values.items()))
            for step_id, values in sorted(checkpoint.input_hashes.items())
        },
        "output_hashes": {
            step_id: dict(sorted(values.items()))
            for step_id, values in sorted(checkpoint.output_hashes.items())
        },
        "attempts": {
            step_id: [_attempt_payload(attempt) for attempt in values]
            for step_id, values in sorted(checkpoint.attempts.items())
        },
        "events": [_event_payload(event) for event in checkpoint.events],
    }


def checkpoint_hash(checkpoint: PipelineCheckpoint) -> str:
    return sha256_bytes(canonical_json_bytes(checkpoint_payload(checkpoint)))


def _write_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise CheckpointError(f"Checkpoint path cannot be a symlink: {path}")
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
        except OSError:
            directory_fd = None
        if directory_fd is not None:
            try:
                os.fsync(directory_fd)
            except OSError:
                pass
            finally:
                os.close(directory_fd)
    except OSError as exc:
        raise CheckpointError(f"Cannot atomically write checkpoint {path}: {exc}") from exc
    finally:
        if temporary is not None:
            try:
                temporary.unlink()
            except OSError:
                pass


def write_checkpoint_atomic(path: Path, checkpoint: PipelineCheckpoint) -> str:
    payload = checkpoint_payload(checkpoint)
    payload["checkpoint_hash"] = sha256_bytes(canonical_json_bytes(payload))
    _write_atomic(path, canonical_json_bytes(payload) + b"\n")
    return sha256_file(path)


def _expect_mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CheckpointError(f"Checkpoint field {field!r} must be an object")
    return value


def _expect_list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise CheckpointError(f"Checkpoint field {field!r} must be a list")
    return value


def _expect_string(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise CheckpointError(f"Checkpoint field {field!r} must be a string")
    return value


def _expect_optional_string(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _expect_string(value, field)


def _expect_integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CheckpointError(f"Checkpoint field {field!r} must be an integer")
    return value


def _expect_sha256(value: Any, field: str) -> str:
    digest = _expect_string(value, field)
    if not _SHA256_PATTERN.fullmatch(digest):
        raise CheckpointError(f"Checkpoint field {field!r} must be a lowercase SHA-256")
    return digest


def _expect_string_map(value: Any, field: str, *, hashes: bool = False) -> dict[str, str]:
    mapping = _expect_mapping(value, field)
    parsed: dict[str, str] = {}
    for key, item in mapping.items():
        name = _expect_string(key, f"{field}.key")
        parsed[name] = (
            _expect_sha256(item, f"{field}.{name}")
            if hashes and item != "MISSING"
            else _expect_string(item, f"{field}.{name}")
        )
    return parsed


def _expect_hash_maps(value: Any, field: str) -> dict[str, dict[str, str]]:
    mapping = _expect_mapping(value, field)
    return {
        _expect_string(key, f"{field}.key"): _expect_string_map(item, f"{field}.{key}", hashes=True)
        for key, item in mapping.items()
    }


def _parse_attempt(value: Any) -> StepAttempt:
    item = _expect_mapping(value, "attempt")
    try:
        if set(item) != _ATTEMPT_KEYS:
            raise CheckpointError("Step attempt keys do not match the frozen contract")
        return StepAttempt(
            attempt=_expect_integer(item["attempt"], "attempt.attempt"),
            started_at=_expect_string(item["started_at"], "attempt.started_at"),
            ended_at=_expect_string(item["ended_at"], "attempt.ended_at"),
            exit_code=(
                None
                if item["exit_code"] is None
                else _expect_integer(item["exit_code"], "attempt.exit_code")
            ),
            stdout_log=_expect_string(item["stdout_log"], "attempt.stdout_log"),
            stderr_log=_expect_string(item["stderr_log"], "attempt.stderr_log"),
            stdout_sha256=_expect_sha256(item["stdout_sha256"], "attempt.stdout_sha256"),
            stderr_sha256=_expect_sha256(item["stderr_sha256"], "attempt.stderr_sha256"),
            idempotency_key=_expect_sha256(item["idempotency_key"], "attempt.idempotency_key"),
            error_type=_expect_optional_string(item.get("error_type"), "attempt.error_type"),
            error_message=_expect_optional_string(
                item.get("error_message"), "attempt.error_message"
            ),
        )
    except (KeyError, TypeError, ValueError, CheckpointError) as exc:
        raise CheckpointError(f"Invalid step attempt: {exc}") from exc


def _parse_event(value: Any) -> PipelineEvent:
    item = _expect_mapping(value, "event")
    try:
        if set(item) != _EVENT_KEYS:
            raise CheckpointError("Pipeline event keys do not match the frozen contract")
        status = item.get("status")
        return PipelineEvent(
            sequence=_expect_integer(item["sequence"], "event.sequence"),
            event_time=_expect_string(item["event_time"], "event.event_time"),
            event_type=_expect_string(item["event_type"], "event.event_type"),
            step_id=_expect_optional_string(item.get("step_id"), "event.step_id"),
            status=None if status is None else StepStatus(status),
            details=dict(_expect_mapping(item.get("details", {}), "event.details")),
        )
    except (KeyError, TypeError, ValueError, CheckpointError) as exc:
        raise CheckpointError(f"Invalid pipeline event: {exc}") from exc


def checkpoint_from_payload(value: Any) -> PipelineCheckpoint:
    item = _expect_mapping(value, "root")
    if set(item) != _CHECKPOINT_KEYS:
        missing = sorted(_CHECKPOINT_KEYS - set(item))
        unknown = sorted(set(item) - _CHECKPOINT_KEYS)
        raise CheckpointError(f"Checkpoint keys mismatch; missing={missing}, unknown={unknown}")
    embedded = item.get("checkpoint_hash")
    unsigned = dict(item)
    unsigned.pop("checkpoint_hash", None)
    calculated = sha256_bytes(canonical_json_bytes(unsigned))
    if embedded != calculated:
        raise CheckpointError(
            f"Checkpoint hash mismatch: expected {embedded}, calculated {calculated}"
        )
    try:
        status_raw = _expect_mapping(item["step_status"], "step_status")
        attempts_raw = _expect_mapping(item["attempts"], "attempts")
        checkpoint = PipelineCheckpoint(
            schema_version=_expect_string(item["schema_version"], "schema_version"),
            config_hash=_expect_sha256(item["config_hash"], "config_hash"),
            stack_manifest_hash=_expect_sha256(item["stack_manifest_hash"], "stack_manifest_hash"),
            run_id=_expect_string(item["run_id"], "run_id"),
            seed=_expect_integer(item["seed"], "seed"),
            topology=tuple(
                _expect_string(value, f"topology[{index}]")
                for index, value in enumerate(_expect_list(item["topology"], "topology"))
            ),
            step_status={
                _expect_string(key, "step_status.key"): StepStatus(value)
                for key, value in status_raw.items()
            },
            idempotency_keys=_expect_string_map(
                item["idempotency_keys"], "idempotency_keys", hashes=True
            ),
            input_hashes=_expect_hash_maps(item["input_hashes"], "input_hashes"),
            output_hashes=_expect_hash_maps(item["output_hashes"], "output_hashes"),
            attempts={
                _expect_string(key, "attempts.key"): [
                    _parse_attempt(attempt) for attempt in _expect_list(values, f"attempts.{key}")
                ]
                for key, values in attempts_raw.items()
            },
            events=[_parse_event(event) for event in _expect_list(item["events"], "events")],
        )
    except (KeyError, TypeError, ValueError, CheckpointError) as exc:
        raise CheckpointError(f"Invalid checkpoint payload: {exc}") from exc
    if checkpoint.schema_version != "2.0.0":
        raise CheckpointError("Checkpoint schema_version must be '2.0.0'")
    return checkpoint


def load_checkpoint(path: Path | str) -> PipelineCheckpoint:
    source = Path(path)
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CheckpointError(f"Cannot load checkpoint {source}: {exc}") from exc
    return checkpoint_from_payload(value)
