"""Deterministic local DAG runner for schema_version 2.0.0."""

from __future__ import annotations

import os
import re
import subprocess
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from quant_pipeline.checkpoint import load_checkpoint, write_checkpoint_atomic
from quant_pipeline.dag_schema import (
    deterministic_topology,
    pipeline_config_hash,
    step_definition_hash,
    validate_pipeline_spec,
)
from quant_pipeline.integrity import (
    canonical_json_bytes,
    hash_artifact_path,
    sha256_bytes,
    sha256_file,
    stack_manifest_hash,
)
from quant_pipeline.v2_models import (
    ArtifactIntegrityError,
    ArtifactSpec,
    CheckpointError,
    DagRunResult,
    PipelineCheckpoint,
    PipelineEvent,
    PipelineSpec,
    PipelineV2Error,
    StepAttempt,
    StepSpec,
    StepStatus,
)

_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_MISSING_HASH = "MISSING"


@dataclass(frozen=True)
class ExecutionOutcome:
    exit_code: int
    stdout: str = ""
    stderr: str = ""


Executor = Callable[[StepSpec, Path, Mapping[str, str]], ExecutionOutcome]


def _default_executor(step: StepSpec, cwd: Path, env: Mapping[str, str]) -> ExecutionOutcome:
    completed = subprocess.run(
        list(step.command),
        shell=False,
        cwd=str(cwd),
        env=dict(env),
        capture_output=True,
        text=True,
        check=False,
        timeout=step.timeout,
    )
    return ExecutionOutcome(
        exit_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def _exception_stream(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


class DagRunner:
    """Run or strictly resume one local, typed and artifact-driven DAG."""

    def __init__(
        self,
        *,
        stack_manifest: Mapping[str, Any] | Path | str,
        seed: int,
        spec: PipelineSpec | None = None,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        executor: Executor | None = None,
        environment: Mapping[str, str] | None = None,
        dry_run: bool = False,
    ) -> None:
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise TypeError("seed must be an integer")
        self._stack_manifest, self.stack_manifest_hash = stack_manifest_hash(stack_manifest)
        self.seed = seed
        self._spec = spec
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._sleeper = sleeper
        self._executor = executor or _default_executor
        self._environment = dict(os.environ if environment is None else environment)
        self._dry_run = dry_run

    def _timestamp(self) -> str:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise CheckpointError("DAG clock must return a timezone-aware datetime")
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _runtime_path(spec: PipelineSpec, path: Path, label: str) -> Path:
        resolved = path.resolve()
        try:
            resolved.relative_to(spec.workspace_root.resolve())
        except ValueError as exc:
            raise CheckpointError(f"{label} escapes workspace_root at runtime: {path}") from exc
        return resolved

    def _event(
        self,
        checkpoint: PipelineCheckpoint,
        event_type: str,
        *,
        step_id: str | None = None,
        status: StepStatus | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        checkpoint.events.append(
            PipelineEvent(
                sequence=len(checkpoint.events) + 1,
                event_time=self._timestamp(),
                event_type=event_type,
                step_id=step_id,
                status=status,
                details=details or {},
            )
        )

    @staticmethod
    def _artifact_path(spec: PipelineSpec, artifact: ArtifactSpec) -> Path:
        path = (spec.workspace_root / artifact.path).resolve()
        try:
            path.relative_to(spec.workspace_root.resolve())
        except ValueError as exc:
            raise ArtifactIntegrityError(
                f"Artifact {artifact.artifact_id!r} escapes workspace_root"
            ) from exc
        return path

    @staticmethod
    def _check_hash(artifact: ArtifactSpec, actual: str) -> None:
        if artifact.expected_sha256 is not None and actual != artifact.expected_sha256:
            raise ArtifactIntegrityError(
                f"Artifact {artifact.artifact_id!r} expected hash "
                f"{artifact.expected_sha256}, got {actual}"
            )
        if artifact.actual_sha256 is not None and actual != artifact.actual_sha256:
            raise ArtifactIntegrityError(
                f"Artifact {artifact.artifact_id!r} declared actual hash "
                f"{artifact.actual_sha256}, got {actual}"
            )

    def _hash_artifact(self, spec: PipelineSpec, artifact: ArtifactSpec) -> str:
        path = self._artifact_path(spec, artifact)
        if not path.exists():
            if artifact.required:
                raise ArtifactIntegrityError(
                    f"Required artifact {artifact.artifact_id!r} is missing: {path}"
                )
            return _MISSING_HASH
        actual = hash_artifact_path(path)
        self._check_hash(artifact, actual)
        return actual

    def _collect_input_hashes(self, spec: PipelineSpec, step: StepSpec) -> dict[str, str]:
        artifacts = spec.artifact_map
        return {
            artifact_id: self._hash_artifact(spec, artifacts[artifact_id])
            for artifact_id in step.inputs
        }

    def _collect_output_hashes(self, spec: PipelineSpec, step: StepSpec) -> dict[str, str]:
        artifacts = spec.artifact_map
        return {
            artifact_id: self._hash_artifact(spec, artifacts[artifact_id])
            for artifact_id in step.outputs
        }

    def _idempotency_key(
        self,
        spec: PipelineSpec,
        step: StepSpec,
        run_id: str,
        input_hashes: Mapping[str, str],
    ) -> str:
        artifacts = spec.artifact_map
        payload = {
            "run_id": run_id,
            "step_id": step.id,
            "step_definition_hash": step_definition_hash(step, artifacts),
            "input_hashes": [[item, input_hashes[item]] for item in step.inputs],
            "stack_manifest_hash": self.stack_manifest_hash,
            "seed": self.seed,
        }
        return sha256_bytes(canonical_json_bytes(payload))

    @staticmethod
    def _ensure_fresh_output_paths(spec: PipelineSpec, step: StepSpec) -> None:
        artifacts = spec.artifact_map
        for artifact_id in step.outputs:
            path = DagRunner._artifact_path(spec, artifacts[artifact_id])
            if path.exists() or path.is_symlink():
                raise ArtifactIntegrityError(
                    f"Output artifact {artifact_id!r} already exists before execution: {path}"
                )

    @staticmethod
    def _write_log(path: Path, text: str) -> str:
        path.parent.mkdir(parents=True, exist_ok=True)
        data = text.encode("utf-8")
        try:
            with path.open("xb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError as exc:
            raise CheckpointError(f"Attempt log already exists: {path}") from exc
        return sha256_bytes(data)

    def _attempt(
        self,
        spec: PipelineSpec,
        checkpoint: PipelineCheckpoint,
        step: StepSpec,
        attempt_number: int,
        idempotency_key: str,
    ) -> tuple[StepAttempt, bool]:
        cwd = self._runtime_path(spec, spec.workspace_root / step.cwd, f"Step {step.id!r} cwd")
        log_root = spec.log_dir / checkpoint.run_id / step.id
        stdout_path = self._runtime_path(
            spec, log_root / f"attempt-{attempt_number:04d}.stdout.log", "stdout log"
        )
        stderr_path = self._runtime_path(
            spec, log_root / f"attempt-{attempt_number:04d}.stderr.log", "stderr log"
        )
        for log_path in (stdout_path, stderr_path):
            if log_path.exists() or log_path.is_symlink():
                raise CheckpointError(f"Attempt log already exists: {log_path}")
        started_at = self._timestamp()
        environment = dict(self._environment)
        environment.update(
            {
                "QUANT_PIPELINE_RUN_ID": checkpoint.run_id,
                "QUANT_PIPELINE_STEP_ID": step.id,
                "QUANT_PIPELINE_SEED": str(self.seed),
                "QUANT_PIPELINE_IDEMPOTENCY_KEY": idempotency_key,
            }
        )
        exit_code: int | None = None
        stdout = ""
        stderr = ""
        error_type: str | None = None
        error_message: str | None = None
        retryable = False
        try:
            outcome = self._executor(step, cwd, environment)
            exit_code = outcome.exit_code
            stdout = outcome.stdout
            stderr = outcome.stderr
            if exit_code != 0:
                error_type = "ProcessExit"
                error_message = f"process exited with code {exit_code}"
                retryable = exit_code in step.retry.retry_exit_codes
        except Exception as exc:  # noqa: BLE001 - arbitrary executors are an explicit boundary
            error_type = type(exc).__name__
            error_message = str(exc)
            stdout = _exception_stream(getattr(exc, "stdout", getattr(exc, "output", None)))
            stderr = _exception_stream(getattr(exc, "stderr", None))
            retryable = not isinstance(exc, PipelineV2Error) and (
                error_type in step.retry.retry_exceptions
            )
        ended_at = self._timestamp()
        stdout_hash = self._write_log(stdout_path, stdout)
        stderr_hash = self._write_log(stderr_path, stderr)
        attempt = StepAttempt(
            attempt=attempt_number,
            started_at=started_at,
            ended_at=ended_at,
            exit_code=exit_code,
            stdout_log=stdout_path.relative_to(spec.workspace_root).as_posix(),
            stderr_log=stderr_path.relative_to(spec.workspace_root).as_posix(),
            stdout_sha256=stdout_hash,
            stderr_sha256=stderr_hash,
            idempotency_key=idempotency_key,
            error_type=error_type,
            error_message=error_message,
        )
        return attempt, retryable

    def _write_checkpoint(self, spec: PipelineSpec, checkpoint: PipelineCheckpoint) -> str:
        path = self._runtime_path(spec, spec.checkpoint_path, "checkpoint")
        return write_checkpoint_atomic(path, checkpoint)

    def _mark_contract_failure(
        self,
        spec: PipelineSpec,
        checkpoint: PipelineCheckpoint,
        step: StepSpec,
        error: Exception,
    ) -> None:
        checkpoint.step_status[step.id] = StepStatus.FAILED
        self._event(
            checkpoint,
            "step_contract_failed",
            step_id=step.id,
            status=StepStatus.FAILED,
            details={"error_type": type(error).__name__, "message": str(error), "retryable": False},
        )
        self._write_checkpoint(spec, checkpoint)

    def _execute_step(
        self,
        spec: PipelineSpec,
        checkpoint: PipelineCheckpoint,
        step: StepSpec,
    ) -> None:
        try:
            inputs = self._collect_input_hashes(spec, step)
            self._ensure_fresh_output_paths(spec, step)
        except ArtifactIntegrityError as exc:
            self._mark_contract_failure(spec, checkpoint, step, exc)
            return

        idempotency_key = self._idempotency_key(spec, step, checkpoint.run_id, inputs)
        checkpoint.input_hashes[step.id] = inputs
        checkpoint.idempotency_keys[step.id] = idempotency_key
        first_attempt = len(checkpoint.attempts[step.id]) + 1
        if first_attempt > step.retry.max_attempts:
            self._mark_contract_failure(
                spec,
                checkpoint,
                step,
                CheckpointError(f"Step {step.id!r} has exhausted max_attempts"),
            )
            return

        for attempt_number in range(first_attempt, step.retry.max_attempts + 1):
            checkpoint.step_status[step.id] = StepStatus.RUNNING
            self._event(
                checkpoint,
                "step_attempt_started",
                step_id=step.id,
                status=StepStatus.RUNNING,
                details={"attempt": attempt_number, "idempotency_key": idempotency_key},
            )
            self._write_checkpoint(spec, checkpoint)
            try:
                attempt, retryable = self._attempt(
                    spec, checkpoint, step, attempt_number, idempotency_key
                )
            except PipelineV2Error as exc:
                self._mark_contract_failure(spec, checkpoint, step, exc)
                return
            checkpoint.attempts[step.id].append(attempt)

            if attempt.exit_code == 0 and attempt.error_type is None:
                try:
                    outputs = self._collect_output_hashes(spec, step)
                except ArtifactIntegrityError as exc:
                    self._mark_contract_failure(spec, checkpoint, step, exc)
                    return
                checkpoint.output_hashes[step.id] = outputs
                checkpoint.step_status[step.id] = StepStatus.SUCCEEDED
                self._event(
                    checkpoint,
                    "step_succeeded",
                    step_id=step.id,
                    status=StepStatus.SUCCEEDED,
                    details={"attempt": attempt_number, "output_hashes": outputs},
                )
                self._write_checkpoint(spec, checkpoint)
                return

            has_attempt = attempt_number < step.retry.max_attempts
            if retryable and has_attempt:
                try:
                    self._ensure_fresh_output_paths(spec, step)
                except ArtifactIntegrityError as exc:
                    self._mark_contract_failure(spec, checkpoint, step, exc)
                    return
                checkpoint.step_status[step.id] = StepStatus.PENDING
                self._event(
                    checkpoint,
                    "step_retry_scheduled",
                    step_id=step.id,
                    status=StepStatus.PENDING,
                    details={
                        "attempt": attempt_number,
                        "backoff_seconds": step.retry.backoff_seconds,
                        "error_type": attempt.error_type,
                    },
                )
                self._write_checkpoint(spec, checkpoint)
                self._sleeper(step.retry.backoff_seconds)
                continue

            checkpoint.step_status[step.id] = StepStatus.FAILED
            self._event(
                checkpoint,
                "step_failed",
                step_id=step.id,
                status=StepStatus.FAILED,
                details={
                    "attempt": attempt_number,
                    "error_type": attempt.error_type,
                    "retryable": retryable,
                },
            )
            self._write_checkpoint(spec, checkpoint)
            return

    def _result(self, spec: PipelineSpec, checkpoint: PipelineCheckpoint) -> DagRunResult:
        artifact_index: dict[str, str] = {}
        for values in checkpoint.input_hashes.values():
            artifact_index.update(values)
        for values in checkpoint.output_hashes.values():
            artifact_index.update(values)
        return DagRunResult(
            run_id=checkpoint.run_id,
            topology=checkpoint.topology,
            step_status=dict(checkpoint.step_status),
            attempts={key: tuple(value) for key, value in checkpoint.attempts.items()},
            events=tuple(checkpoint.events),
            artifact_index=artifact_index,
            checkpoint_path=spec.checkpoint_path,
            checkpoint_hash=sha256_file(
                self._runtime_path(spec, spec.checkpoint_path, "checkpoint")
            ),
        )

    def _execute(self, spec: PipelineSpec, checkpoint: PipelineCheckpoint) -> DagRunResult:
        steps = spec.step_map
        fail_fast_triggered = (
            any(status == StepStatus.FAILED for status in checkpoint.step_status.values())
            and spec.fail_fast
        )
        successful = {StepStatus.SUCCEEDED, StepStatus.CACHED, StepStatus.DRY_RUN}
        for step_id in checkpoint.topology:
            status = checkpoint.step_status[step_id]
            if status != StepStatus.PENDING:
                continue
            step = steps[step_id]
            dependency_states = {
                dependency: checkpoint.step_status[dependency] for dependency in step.needs
            }
            if fail_fast_triggered or any(
                state not in successful for state in dependency_states.values()
            ):
                checkpoint.step_status[step_id] = StepStatus.BLOCKED
                reason = "fail_fast" if fail_fast_triggered else "dependency_failed"
                self._event(
                    checkpoint,
                    "step_blocked",
                    step_id=step_id,
                    status=StepStatus.BLOCKED,
                    details={"reason": reason, "dependencies": dependency_states},
                )
                self._write_checkpoint(spec, checkpoint)
                continue
            if self._dry_run:
                checkpoint.step_status[step_id] = StepStatus.DRY_RUN
                self._event(
                    checkpoint,
                    "step_dry_run",
                    step_id=step_id,
                    status=StepStatus.DRY_RUN,
                )
                self._write_checkpoint(spec, checkpoint)
                continue
            self._execute_step(spec, checkpoint, step)
            if checkpoint.step_status[step_id] == StepStatus.FAILED and spec.fail_fast:
                fail_fast_triggered = True

        self._event(
            checkpoint,
            "pipeline_completed",
            details={"ok": all(state in successful for state in checkpoint.step_status.values())},
        )
        self._write_checkpoint(spec, checkpoint)
        return self._result(spec, checkpoint)

    @staticmethod
    def _validate_run_id(run_id: str) -> None:
        if not _RUN_ID_PATTERN.fullmatch(run_id):
            raise ValueError("run_id must match [A-Za-z0-9][A-Za-z0-9._-]*")

    def run(self, spec: PipelineSpec, run_id: str, resume: bool = False) -> DagRunResult:
        self._validate_run_id(run_id)
        validate_pipeline_spec(spec, self._stack_manifest).raise_for_error()
        self._spec = spec
        if resume:
            return self._resume(spec.checkpoint_path, expected_run_id=run_id)
        if spec.checkpoint_path.exists() or spec.checkpoint_path.is_symlink():
            raise CheckpointError(
                f"Checkpoint already exists; use strict resume instead: {spec.checkpoint_path}"
            )
        run_log_dir = spec.log_dir / run_id
        if run_log_dir.exists() or run_log_dir.is_symlink():
            raise CheckpointError(f"Run log directory already exists: {run_log_dir}")
        topology = deterministic_topology(spec)
        checkpoint = PipelineCheckpoint(
            schema_version="2.0.0",
            config_hash=pipeline_config_hash(spec),
            stack_manifest_hash=self.stack_manifest_hash,
            run_id=run_id,
            seed=self.seed,
            topology=topology,
            step_status={step_id: StepStatus.PENDING for step_id in topology},
            idempotency_keys={},
            input_hashes={},
            output_hashes={},
            attempts={step_id: [] for step_id in topology},
            events=[],
        )
        self._event(checkpoint, "pipeline_started", details={"topology": list(topology)})
        self._write_checkpoint(spec, checkpoint)
        return self._execute(spec, checkpoint)

    def _verify_attempt_logs(self, spec: PipelineSpec, checkpoint: PipelineCheckpoint) -> None:
        for step_id, attempts in checkpoint.attempts.items():
            expected_numbers = list(range(1, len(attempts) + 1))
            if [attempt.attempt for attempt in attempts] != expected_numbers:
                raise CheckpointError(f"Attempt sequence is invalid for step {step_id!r}")
            for attempt in attempts:
                for relative, expected in (
                    (attempt.stdout_log, attempt.stdout_sha256),
                    (attempt.stderr_log, attempt.stderr_sha256),
                ):
                    path = (spec.workspace_root / relative).resolve()
                    try:
                        path.relative_to(spec.workspace_root.resolve())
                    except ValueError as exc:
                        raise CheckpointError(f"Attempt log escapes workspace: {relative}") from exc
                    if not path.is_file() or sha256_file(path) != expected:
                        raise CheckpointError(f"Attempt log is missing or has changed: {relative}")

    def _validate_checkpoint(
        self,
        spec: PipelineSpec,
        checkpoint: PipelineCheckpoint,
        *,
        expected_run_id: str | None,
    ) -> None:
        topology = deterministic_topology(spec)
        expected_ids = set(topology)
        checks = {
            "config_hash": (checkpoint.config_hash, pipeline_config_hash(spec)),
            "stack_manifest_hash": (checkpoint.stack_manifest_hash, self.stack_manifest_hash),
            "seed": (checkpoint.seed, self.seed),
            "topology": (checkpoint.topology, topology),
            "step_status ids": (set(checkpoint.step_status), expected_ids),
            "attempt ids": (set(checkpoint.attempts), expected_ids),
        }
        if expected_run_id is not None:
            checks["run_id"] = (checkpoint.run_id, expected_run_id)
        mismatches = [name for name, (actual, expected) in checks.items() if actual != expected]
        if mismatches:
            raise CheckpointError(f"Checkpoint does not match current run: {', '.join(mismatches)}")
        try:
            self._validate_run_id(checkpoint.run_id)
        except ValueError as exc:
            raise CheckpointError(f"Checkpoint run_id is invalid: {checkpoint.run_id!r}") from exc
        if any(status == StepStatus.RUNNING for status in checkpoint.step_status.values()):
            raise CheckpointError("Checkpoint contains interrupted running state")
        keyed_fields = {
            "idempotency_keys": set(checkpoint.idempotency_keys),
            "input_hashes": set(checkpoint.input_hashes),
            "output_hashes": set(checkpoint.output_hashes),
        }
        unexpected = {
            name: sorted(keys - expected_ids)
            for name, keys in keyed_fields.items()
            if keys - expected_ids
        }
        if unexpected:
            raise CheckpointError(f"Checkpoint contains unknown step keys: {unexpected}")
        steps = spec.step_map
        for step_id, attempts in checkpoint.attempts.items():
            if len(attempts) > steps[step_id].retry.max_attempts:
                raise CheckpointError(f"Step {step_id!r} exceeds configured max_attempts")
            expected_key = checkpoint.idempotency_keys.get(step_id)
            if expected_key and any(
                attempt.idempotency_key != expected_key for attempt in attempts
            ):
                raise CheckpointError(f"Attempt idempotency key mismatch for step {step_id!r}")
        sequences = [event.sequence for event in checkpoint.events]
        if sequences != list(range(1, len(sequences) + 1)):
            raise CheckpointError("Checkpoint event sequence is not contiguous")
        if any(
            event.step_id is not None and event.step_id not in expected_ids
            for event in checkpoint.events
        ):
            raise CheckpointError("Checkpoint event references an unknown step")
        self._verify_attempt_logs(spec, checkpoint)

    def _cache_completed_steps(self, spec: PipelineSpec, checkpoint: PipelineCheckpoint) -> None:
        steps = spec.step_map
        artifacts = spec.artifact_map
        for step_id in checkpoint.topology:
            if checkpoint.step_status[step_id] not in {StepStatus.SUCCEEDED, StepStatus.CACHED}:
                continue
            step = steps[step_id]
            inputs = self._collect_input_hashes(spec, step)
            expected_key = self._idempotency_key(spec, step, checkpoint.run_id, inputs)
            if checkpoint.input_hashes.get(step_id) != inputs:
                raise ArtifactIntegrityError(f"Input artifact hashes changed for step {step_id!r}")
            if checkpoint.idempotency_keys.get(step_id) != expected_key:
                raise CheckpointError(f"Idempotency key changed for completed step {step_id!r}")
            outputs: dict[str, str] = {}
            for artifact_id in step.outputs:
                artifact = artifacts[artifact_id]
                if not artifact.immutable:
                    raise ArtifactIntegrityError(
                        f"Completed step {step_id!r} has non-immutable output {artifact_id!r}"
                    )
                path = self._artifact_path(spec, artifact)
                if not path.exists():
                    raise ArtifactIntegrityError(
                        f"Completed output {artifact_id!r} is missing during resume"
                    )
                outputs[artifact_id] = self._hash_artifact(spec, artifact)
            if checkpoint.output_hashes.get(step_id) != outputs:
                raise ArtifactIntegrityError(f"Output artifact hashes changed for step {step_id!r}")
            checkpoint.step_status[step_id] = StepStatus.CACHED
            self._event(
                checkpoint,
                "step_cached",
                step_id=step_id,
                status=StepStatus.CACHED,
                details={"idempotency_key": expected_key, "output_hashes": outputs},
            )
            self._write_checkpoint(spec, checkpoint)

    def _resume(
        self, checkpoint: PipelineCheckpoint | Path | str, *, expected_run_id: str | None = None
    ) -> DagRunResult:
        if self._spec is None:
            raise CheckpointError("DagRunner.resume requires a PipelineSpec in the runner")
        spec = self._spec
        validate_pipeline_spec(spec, self._stack_manifest).raise_for_error()
        loaded = (
            checkpoint
            if isinstance(checkpoint, PipelineCheckpoint)
            else load_checkpoint(Path(checkpoint))
        )
        self._validate_checkpoint(spec, loaded, expected_run_id=expected_run_id)
        self._cache_completed_steps(spec, loaded)
        self._event(loaded, "pipeline_resumed")
        self._write_checkpoint(spec, loaded)
        return self._execute(spec, loaded)

    def resume(self, checkpoint: PipelineCheckpoint | Path | str) -> DagRunResult:
        return self._resume(checkpoint)
