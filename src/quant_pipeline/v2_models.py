"""Public typed contracts for schema_version 2.0.0 pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class PipelineV2Error(RuntimeError):
    """Base class for fail-closed v2 pipeline errors."""


class PipelineSpecError(PipelineV2Error, ValueError):
    """Raised when a v2 document cannot be loaded into the frozen contract."""


class PipelineValidationError(PipelineV2Error, ValueError):
    """Raised when a typed pipeline fails semantic validation."""


class ArtifactIntegrityError(PipelineV2Error):
    """Raised when an artifact is missing, mutable, or has the wrong hash."""


class CheckpointError(PipelineV2Error):
    """Raised when a checkpoint is corrupt or incompatible with the run."""


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED = "blocked"
    CACHED = "cached"
    DRY_RUN = "dry_run"


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    retry_exit_codes: tuple[int, ...] = ()
    retry_exceptions: tuple[str, ...] = ()
    backoff_seconds: float = 0.0


@dataclass(frozen=True)
class ArtifactSpec:
    artifact_id: str
    path: str
    producer: str | None
    schema_id: str | None = None
    schema_version: str | None = None
    required: bool = True
    immutable: bool = True
    expected_sha256: str | None = None
    actual_sha256: str | None = None


@dataclass(frozen=True)
class StepSpec:
    id: str
    kind: str
    needs: tuple[str, ...]
    command: tuple[str, ...]
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    retry: RetryPolicy
    timeout: float
    cwd: str = "."


@dataclass(frozen=True)
class PipelineSpec:
    schema_version: str
    name: str
    workspace_root: Path
    checkpoint_path: Path
    log_dir: Path
    fail_fast: bool
    artifacts: tuple[ArtifactSpec, ...]
    steps: tuple[StepSpec, ...]
    source_path: Path | None = field(default=None, compare=False, repr=False)

    @property
    def artifact_map(self) -> dict[str, ArtifactSpec]:
        return {artifact.artifact_id: artifact for artifact in self.artifacts}

    @property
    def step_map(self) -> dict[str, StepSpec]:
        return {step.id: step for step in self.steps}


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    message: str
    location: str = ""


@dataclass(frozen=True)
class ValidationResult:
    issues: tuple[ValidationIssue, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.issues

    def raise_for_error(self) -> None:
        if self.issues:
            details = "; ".join(
                f"{issue.code}@{issue.location}: {issue.message}"
                if issue.location
                else f"{issue.code}: {issue.message}"
                for issue in self.issues
            )
            raise PipelineValidationError(details)


@dataclass(frozen=True)
class StepAttempt:
    attempt: int
    started_at: str
    ended_at: str
    exit_code: int | None
    stdout_log: str
    stderr_log: str
    stdout_sha256: str
    stderr_sha256: str
    idempotency_key: str
    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class PipelineEvent:
    sequence: int
    event_time: str
    event_type: str
    step_id: str | None
    status: StepStatus | None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineCheckpoint:
    schema_version: str
    config_hash: str
    stack_manifest_hash: str
    run_id: str
    seed: int
    topology: tuple[str, ...]
    step_status: dict[str, StepStatus]
    idempotency_keys: dict[str, str]
    input_hashes: dict[str, dict[str, str]]
    output_hashes: dict[str, dict[str, str]]
    attempts: dict[str, list[StepAttempt]]
    events: list[PipelineEvent]


@dataclass(frozen=True)
class DagRunResult:
    run_id: str
    topology: tuple[str, ...]
    step_status: dict[str, StepStatus]
    attempts: dict[str, tuple[StepAttempt, ...]]
    events: tuple[PipelineEvent, ...]
    artifact_index: dict[str, str]
    checkpoint_path: Path
    checkpoint_hash: str

    @property
    def ok(self) -> bool:
        return all(
            status in {StepStatus.SUCCEEDED, StepStatus.CACHED, StepStatus.DRY_RUN}
            for status in self.step_status.values()
        )
