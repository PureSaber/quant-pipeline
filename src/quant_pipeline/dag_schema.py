"""Loading and semantic validation for typed v2 DAG specifications."""

from __future__ import annotations

import heapq
import math
import re
from collections import Counter, defaultdict
from collections.abc import Hashable, Mapping, Sequence
from pathlib import Path
from typing import Any, TypeVar

import yaml

from quant_pipeline.integrity import canonical_json_bytes, sha256_bytes, stack_manifest_hash
from quant_pipeline.v2_models import (
    ArtifactSpec,
    CheckpointError,
    PipelineSpec,
    PipelineSpecError,
    RetryPolicy,
    StepSpec,
    ValidationIssue,
    ValidationResult,
)

_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_TOP_LEVEL_KEYS = {
    "schema_version",
    "name",
    "workspace_root",
    "checkpoint_path",
    "log_dir",
    "fail_fast",
    "artifacts",
    "steps",
}
_ARTIFACT_KEYS = {
    "artifact_id",
    "path",
    "producer",
    "schema_id",
    "schema_version",
    "required",
    "immutable",
    "expected_sha256",
    "actual_sha256",
}
_STEP_KEYS = {
    "id",
    "kind",
    "needs",
    "command",
    "inputs",
    "outputs",
    "retry",
    "timeout",
    "cwd",
}
_RETRY_KEYS = {
    "max_attempts",
    "retry_exit_codes",
    "retry_exceptions",
    "backoff_seconds",
}
_T = TypeVar("_T", bound=Hashable)


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PipelineSpecError(f"{location} must be a mapping")
    return value


def _list(value: Any, location: str) -> list[Any]:
    if not isinstance(value, list):
        raise PipelineSpecError(f"{location} must be a list")
    return value


def _string(value: Any, location: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise PipelineSpecError(f"{location} must be a non-empty string")
    return value


def _bool(value: Any, location: str) -> bool:
    if not isinstance(value, bool):
        raise PipelineSpecError(f"{location} must be a boolean")
    return value


def _number(value: Any, location: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PipelineSpecError(f"{location} must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise PipelineSpecError(f"{location} must be finite")
    return result


def _integer(value: Any, location: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PipelineSpecError(f"{location} must be an integer")
    return value


def _optional_string(value: Any, location: str) -> str | None:
    if value is None:
        return None
    return _string(value, location)


def _string_tuple(value: Any, location: str) -> tuple[str, ...]:
    return tuple(
        _string(item, f"{location}[{index}]") for index, item in enumerate(_list(value, location))
    )


def _reject_unknown_keys(value: Mapping[str, Any], allowed: set[str], location: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise PipelineSpecError(f"{location} has unknown keys: {', '.join(unknown)}")


def _required(value: Mapping[str, Any], keys: set[str], location: str) -> None:
    missing = sorted(keys - set(value))
    if missing:
        raise PipelineSpecError(f"{location} is missing required keys: {', '.join(missing)}")


def _normalize_path(root: Path, raw: str, location: str, *, allow_root: bool = False) -> str:
    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = root / candidate
    resolved = candidate.resolve()
    try:
        relative = resolved.relative_to(root)
    except ValueError as exc:
        raise PipelineSpecError(f"{location} escapes workspace_root: {raw}") from exc
    if not allow_root and relative == Path("."):
        raise PipelineSpecError(f"{location} cannot be workspace_root itself")
    return relative.as_posix() or "."


def _parse_retry(raw: Any, location: str) -> RetryPolicy:
    value = _mapping(raw, location)
    _reject_unknown_keys(value, _RETRY_KEYS, location)
    _required(value, {"max_attempts"}, location)
    exit_codes = tuple(
        _integer(item, f"{location}.retry_exit_codes[{index}]")
        for index, item in enumerate(
            _list(value.get("retry_exit_codes", []), f"{location}.retry_exit_codes")
        )
    )
    exceptions = _string_tuple(value.get("retry_exceptions", []), f"{location}.retry_exceptions")
    return RetryPolicy(
        max_attempts=_integer(value["max_attempts"], f"{location}.max_attempts"),
        retry_exit_codes=exit_codes,
        retry_exceptions=exceptions,
        backoff_seconds=_number(value.get("backoff_seconds", 0.0), f"{location}.backoff_seconds"),
    )


def _parse_artifact(raw: Any, index: int, root: Path) -> ArtifactSpec:
    location = f"artifacts[{index}]"
    value = _mapping(raw, location)
    _reject_unknown_keys(value, _ARTIFACT_KEYS, location)
    _required(value, {"artifact_id", "path", "producer"}, location)
    return ArtifactSpec(
        artifact_id=_string(value["artifact_id"], f"{location}.artifact_id"),
        path=_normalize_path(root, _string(value["path"], f"{location}.path"), f"{location}.path"),
        producer=_optional_string(value["producer"], f"{location}.producer"),
        schema_id=_optional_string(value.get("schema_id"), f"{location}.schema_id"),
        schema_version=_optional_string(value.get("schema_version"), f"{location}.schema_version"),
        required=_bool(value.get("required", True), f"{location}.required"),
        immutable=_bool(value.get("immutable", True), f"{location}.immutable"),
        expected_sha256=_optional_string(
            value.get("expected_sha256"), f"{location}.expected_sha256"
        ),
        actual_sha256=_optional_string(value.get("actual_sha256"), f"{location}.actual_sha256"),
    )


def _parse_step(raw: Any, index: int, root: Path) -> StepSpec:
    location = f"steps[{index}]"
    value = _mapping(raw, location)
    if "shell" in value:
        raise PipelineSpecError(f"{location}.shell is forbidden for schema_version 2.0.0")
    _reject_unknown_keys(value, _STEP_KEYS, location)
    _required(
        value,
        {"id", "kind", "needs", "command", "inputs", "outputs", "retry", "timeout"},
        location,
    )
    command = _string_tuple(value["command"], f"{location}.command")
    cwd = _normalize_path(
        root,
        _string(value.get("cwd", "."), f"{location}.cwd"),
        f"{location}.cwd",
        allow_root=True,
    )
    return StepSpec(
        id=_string(value["id"], f"{location}.id"),
        kind=_string(value["kind"], f"{location}.kind"),
        needs=_string_tuple(value["needs"], f"{location}.needs"),
        command=command,
        inputs=_string_tuple(value["inputs"], f"{location}.inputs"),
        outputs=_string_tuple(value["outputs"], f"{location}.outputs"),
        retry=_parse_retry(value["retry"], f"{location}.retry"),
        timeout=_number(value["timeout"], f"{location}.timeout"),
        cwd=cwd,
    )


def load_pipeline_spec(path: Path | str) -> PipelineSpec:
    """Load a strict v2 YAML document without executing or expanding shell text."""
    source = Path(path).resolve()
    try:
        raw = yaml.safe_load(source.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise PipelineSpecError(f"Cannot load pipeline spec {source}: {exc}") from exc
    value = _mapping(raw, "pipeline")
    _reject_unknown_keys(value, _TOP_LEVEL_KEYS, "pipeline")
    _required(value, {"schema_version", "name", "artifacts", "steps"}, "pipeline")
    schema_version = _string(value["schema_version"], "pipeline.schema_version")
    if schema_version != "2.0.0":
        raise PipelineSpecError("pipeline.schema_version must be '2.0.0'")

    workspace_raw = _string(value.get("workspace_root", "."), "pipeline.workspace_root")
    workspace_root = Path(workspace_raw)
    if not workspace_root.is_absolute():
        workspace_root = source.parent / workspace_root
    workspace_root = workspace_root.resolve()
    checkpoint_relative = _normalize_path(
        workspace_root,
        _string(
            value.get("checkpoint_path", ".quant-pipeline/checkpoint.json"),
            "pipeline.checkpoint_path",
        ),
        "pipeline.checkpoint_path",
    )
    log_relative = _normalize_path(
        workspace_root,
        _string(value.get("log_dir", ".quant-pipeline/logs"), "pipeline.log_dir"),
        "pipeline.log_dir",
    )
    artifacts = tuple(
        _parse_artifact(raw_artifact, index, workspace_root)
        for index, raw_artifact in enumerate(_list(value["artifacts"], "pipeline.artifacts"))
    )
    steps = tuple(
        _parse_step(raw_step, index, workspace_root)
        for index, raw_step in enumerate(_list(value["steps"], "pipeline.steps"))
    )
    return PipelineSpec(
        schema_version=schema_version,
        name=_string(value["name"], "pipeline.name"),
        workspace_root=workspace_root,
        checkpoint_path=workspace_root / checkpoint_relative,
        log_dir=workspace_root / log_relative,
        fail_fast=_bool(value.get("fail_fast", False), "pipeline.fail_fast"),
        artifacts=artifacts,
        steps=steps,
        source_path=source,
    )


def _duplicates(values: Sequence[_T]) -> list[_T]:
    return sorted(value for value, count in Counter(values).items() if count > 1)


def deterministic_topology(spec: PipelineSpec) -> tuple[str, ...]:
    """Return a lexicographically stable Kahn ordering or an empty tuple for a cycle."""
    step_ids = {step.id for step in spec.steps}
    if len(step_ids) != len(spec.steps):
        return ()
    indegree = {step.id: 0 for step in spec.steps}
    children: dict[str, list[str]] = defaultdict(list)
    for step in spec.steps:
        for dependency in step.needs:
            if dependency not in step_ids:
                continue
            indegree[step.id] += 1
            children[dependency].append(step.id)
    ready = [step_id for step_id, count in indegree.items() if count == 0]
    heapq.heapify(ready)
    ordered: list[str] = []
    while ready:
        step_id = heapq.heappop(ready)
        ordered.append(step_id)
        for child in sorted(children[step_id]):
            indegree[child] -= 1
            if indegree[child] == 0:
                heapq.heappush(ready, child)
    return tuple(ordered) if len(ordered) == len(spec.steps) else ()


def validate_pipeline_spec(
    spec: PipelineSpec,
    stack_manifest: Mapping[str, Any] | Path | str,
) -> ValidationResult:
    issues: list[ValidationIssue] = []

    def issue(code: str, message: str, location: str = "") -> None:
        issues.append(ValidationIssue(code=code, message=message, location=location))

    if spec.schema_version != "2.0.0":
        issue("SCHEMA_VERSION", "schema_version must be 2.0.0", "schema_version")
    try:
        stack_manifest_hash(stack_manifest)
    except CheckpointError as exc:
        issue("STACK_MANIFEST_INVALID", str(exc), "stack_manifest")

    step_ids = [step.id for step in spec.steps]
    artifact_ids = [artifact.artifact_id for artifact in spec.artifacts]
    if not spec.steps:
        issue("EMPTY_DAG", "steps must contain at least one typed step", "steps")
    for duplicate in _duplicates(step_ids):
        issue("DUPLICATE_STEP", f"duplicate step id {duplicate!r}", "steps")
    for duplicate in _duplicates(artifact_ids):
        issue("DUPLICATE_ARTIFACT", f"duplicate artifact id {duplicate!r}", "artifacts")
    step_id_set = set(step_ids)
    artifact_id_set = set(artifact_ids)
    unique_steps = {step.id: step for step in spec.steps}
    unique_artifacts = {artifact.artifact_id: artifact for artifact in spec.artifacts}

    for step in spec.steps:
        location = f"steps.{step.id}"
        if not _ID_PATTERN.fullmatch(step.id):
            issue("INVALID_STEP_ID", f"invalid step id {step.id!r}", location)
        if not step.command or any(not part for part in step.command):
            issue("INVALID_COMMAND", "v2 command must be a non-empty argv list", location)
        if step.timeout <= 0:
            issue("INVALID_TIMEOUT", "timeout must be greater than zero", location)
        if step.retry.max_attempts < 1:
            issue("INVALID_RETRY", "max_attempts must include the first attempt", location)
        if step.retry.backoff_seconds < 0:
            issue("INVALID_RETRY", "backoff_seconds cannot be negative", location)
        for field_name, values in (
            ("retry_exit_codes", step.retry.retry_exit_codes),
            ("retry_exceptions", step.retry.retry_exceptions),
        ):
            for duplicate in _duplicates(values):
                issue(
                    "DUPLICATE_RETRY_MATCH",
                    f"duplicate {field_name} value {duplicate!r}",
                    location,
                )
        for field_name, values in (
            ("needs", step.needs),
            ("inputs", step.inputs),
            ("outputs", step.outputs),
        ):
            for duplicate in _duplicates(values):
                issue(
                    "DUPLICATE_REFERENCE",
                    f"duplicate {field_name} reference {duplicate!r}",
                    location,
                )
        if step.id in step.needs:
            issue("SELF_DEPENDENCY", "step cannot depend on itself", location)
        for dependency in step.needs:
            if dependency not in step_id_set:
                issue("UNKNOWN_DEPENDENCY", f"unknown dependency {dependency!r}", location)
        for artifact_id in step.inputs + step.outputs:
            if artifact_id not in artifact_id_set:
                issue("UNKNOWN_ARTIFACT", f"unknown artifact {artifact_id!r}", location)
        for artifact_id in set(step.inputs) & set(step.outputs):
            issue(
                "INPUT_OUTPUT_CONFLICT",
                f"artifact {artifact_id!r} is both input and output",
                location,
            )

    output_owners: dict[str, list[str]] = defaultdict(list)
    output_paths: dict[str, list[str]] = defaultdict(list)
    for step in spec.steps:
        for artifact_id in step.outputs:
            output_owners[artifact_id].append(step.id)
            artifact = unique_artifacts.get(artifact_id)
            if artifact is not None:
                output_paths[artifact.path].append(artifact_id)
                if artifact.producer != step.id:
                    issue(
                        "PRODUCER_MISMATCH",
                        f"artifact {artifact_id!r} producer is {artifact.producer!r}, not {step.id!r}",
                        f"steps.{step.id}",
                    )
        for artifact_id in step.inputs:
            artifact = unique_artifacts.get(artifact_id)
            if (
                artifact
                and artifact.producer
                and artifact.producer != step.id
                and artifact.producer not in step.needs
            ):
                issue(
                    "UNDECLARED_ARTIFACT_DEPENDENCY",
                    f"input {artifact_id!r} requires needs {artifact.producer!r}",
                    f"steps.{step.id}",
                )

    for artifact in spec.artifacts:
        location = f"artifacts.{artifact.artifact_id}"
        if not _ID_PATTERN.fullmatch(artifact.artifact_id):
            issue("INVALID_ARTIFACT_ID", f"invalid artifact id {artifact.artifact_id!r}", location)
        if artifact.producer is not None and artifact.producer not in step_id_set:
            issue("UNKNOWN_PRODUCER", f"unknown producer {artifact.producer!r}", location)
        owners = output_owners.get(artifact.artifact_id, [])
        if len(owners) > 1:
            issue("PRODUCER_CONFLICT", f"artifact is output by steps {sorted(owners)!r}", location)
        if artifact.producer is not None and owners != [artifact.producer]:
            issue("PRODUCER_OUTPUT_MISSING", "producer must declare artifact in outputs", location)
        for hash_name, hash_value in (
            ("expected_sha256", artifact.expected_sha256),
            ("actual_sha256", artifact.actual_sha256),
        ):
            if hash_value is not None and not _SHA256_PATTERN.fullmatch(hash_value):
                issue(
                    "INVALID_SHA256", f"{hash_name} must be 64 lowercase hex characters", location
                )
        resolved = (spec.workspace_root / artifact.path).resolve()
        try:
            resolved.relative_to(spec.workspace_root.resolve())
        except ValueError:
            issue("PATH_ESCAPE", f"artifact path escapes workspace: {artifact.path}", location)
        if resolved == spec.checkpoint_path.resolve():
            issue("RESERVED_PATH", "artifact path conflicts with checkpoint", location)
        try:
            resolved.relative_to(spec.log_dir.resolve())
        except ValueError:
            pass
        else:
            issue("RESERVED_PATH", "artifact path is inside the log directory", location)

    for path, ids in output_paths.items():
        if len(ids) > 1:
            issue(
                "OUTPUT_PATH_CONFLICT",
                f"output path {path!r} is shared by {sorted(ids)!r}",
                "artifacts",
            )

    if spec.steps and len(unique_steps) == len(spec.steps) and not deterministic_topology(spec):
        issue("DAG_CYCLE", "step dependency graph contains a cycle", "steps")
    return ValidationResult(tuple(issues))


def artifact_definition(artifact: ArtifactSpec) -> dict[str, Any]:
    return {
        "artifact_id": artifact.artifact_id,
        "path": artifact.path,
        "producer": artifact.producer,
        "schema_id": artifact.schema_id,
        "schema_version": artifact.schema_version,
        "required": artifact.required,
        "immutable": artifact.immutable,
        "expected_sha256": artifact.expected_sha256,
        "actual_sha256": artifact.actual_sha256,
    }


def step_definition(step: StepSpec, artifacts: Mapping[str, ArtifactSpec]) -> dict[str, Any]:
    referenced = sorted(set(step.inputs + step.outputs))
    return {
        "id": step.id,
        "kind": step.kind,
        "needs": list(step.needs),
        "command": list(step.command),
        "inputs": list(step.inputs),
        "outputs": list(step.outputs),
        "retry": {
            "max_attempts": step.retry.max_attempts,
            "retry_exit_codes": list(step.retry.retry_exit_codes),
            "retry_exceptions": list(step.retry.retry_exceptions),
            "backoff_seconds": step.retry.backoff_seconds,
        },
        "timeout": step.timeout,
        "cwd": step.cwd,
        "artifacts": [
            artifact_definition(artifacts[item]) for item in referenced if item in artifacts
        ],
    }


def pipeline_definition(spec: PipelineSpec) -> dict[str, Any]:
    artifacts = spec.artifact_map
    return {
        "schema_version": spec.schema_version,
        "name": spec.name,
        "checkpoint_path": spec.checkpoint_path.relative_to(spec.workspace_root).as_posix(),
        "log_dir": spec.log_dir.relative_to(spec.workspace_root).as_posix(),
        "fail_fast": spec.fail_fast,
        "artifacts": [
            artifact_definition(item)
            for item in sorted(spec.artifacts, key=lambda item: item.artifact_id)
        ],
        "steps": [
            step_definition(item, artifacts)
            for item in sorted(spec.steps, key=lambda item: item.id)
        ],
    }


def pipeline_config_hash(spec: PipelineSpec) -> str:
    return sha256_bytes(canonical_json_bytes(pipeline_definition(spec)))


def step_definition_hash(step: StepSpec, artifacts: Mapping[str, ArtifactSpec]) -> str:
    return sha256_bytes(canonical_json_bytes(step_definition(step, artifacts)))
