from __future__ import annotations

import json
import sys
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest
from v2_helpers import (
    STACK_MANIFEST,
    base_config,
    fixed_clock,
    python_write_command,
    write_config,
)

from quant_pipeline.checkpoint import load_checkpoint, write_checkpoint_atomic
from quant_pipeline.dag_runner import DagRunner, ExecutionOutcome
from quant_pipeline.dag_schema import load_pipeline_spec
from quant_pipeline.integrity import sha256_file
from quant_pipeline.v2_models import (
    ArtifactIntegrityError,
    CheckpointError,
    StepStatus,
)


def _step(
    step_id: str,
    output: str,
    *,
    needs: list[str] | None = None,
    inputs: list[str] | None = None,
    command: list[str] | None = None,
    retry: dict | None = None,
) -> dict:
    return {
        "id": step_id,
        "kind": "fixture",
        "needs": needs or [],
        "command": command or python_write_command(output),
        "inputs": inputs or [],
        "outputs": [f"{step_id}_output"],
        "retry": retry or {"max_attempts": 1},
        "timeout": 10,
    }


def _artifact(step_id: str, path: str) -> dict:
    return {
        "artifact_id": f"{step_id}_output",
        "path": path,
        "producer": step_id,
        "required": True,
        "immutable": True,
    }


def _runner(**kwargs) -> DagRunner:
    kwargs.setdefault("clock", fixed_clock)
    return DagRunner(stack_manifest=STACK_MANIFEST, seed=7, **kwargs)


def test_success_logs_hashes_and_strict_cached_resume(tmp_path: Path) -> None:
    spec = load_pipeline_spec(write_config(tmp_path, base_config()))
    result = _runner().run(spec, "run-1")
    assert result.ok
    assert result.topology == ("build",)
    assert result.step_status == {"build": StepStatus.SUCCEEDED}
    assert result.artifact_index["output"] == sha256_file(tmp_path / "artifacts/output.txt")
    attempt = result.attempts["build"][0]
    assert sha256_file(tmp_path / attempt.stdout_log) == attempt.stdout_sha256
    assert sha256_file(tmp_path / attempt.stderr_log) == attempt.stderr_sha256

    resumed = _runner(spec=spec).resume(spec.checkpoint_path)
    assert resumed.ok
    assert resumed.step_status["build"] == StepStatus.CACHED
    assert any(event.event_type == "step_cached" for event in resumed.events)


@pytest.mark.parametrize("tamper", ["missing", "changed"])
def test_resume_fails_closed_for_missing_or_tampered_output(tmp_path: Path, tamper: str) -> None:
    spec = load_pipeline_spec(write_config(tmp_path, base_config()))
    _runner().run(spec, "run-1")
    output = tmp_path / "artifacts/output.txt"
    if tamper == "missing":
        output.unlink()
    else:
        output.write_text("tampered", encoding="utf-8")
    with pytest.raises(ArtifactIntegrityError):
        _runner(spec=spec).resume(spec.checkpoint_path)


def test_three_fixed_clock_runs_are_byte_deterministic(tmp_path: Path) -> None:
    snapshots = []
    for index in range(3):
        root = tmp_path / str(index)
        spec = load_pipeline_spec(write_config(root, base_config()))
        result = _runner(environment={}).run(spec, "stable-run")
        snapshots.append(
            (
                result.topology,
                result.step_status,
                result.events,
                result.artifact_index,
                result.checkpoint_hash,
                spec.checkpoint_path.read_bytes(),
            )
        )
    assert snapshots[0] == snapshots[1] == snapshots[2]


def test_retry_only_configured_exit_code_and_preserves_attempt_logs(tmp_path: Path) -> None:
    config = base_config()
    code = """from pathlib import Path
import sys
p = Path('.attempt-counter')
n = int(p.read_text()) + 1 if p.exists() else 1
p.write_text(str(n))
if n == 1:
    raise SystemExit(75)
out = Path('artifacts/output.txt')
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text('ok')
"""
    config["steps"][0]["command"] = [sys.executable, "-c", code]
    config["steps"][0]["retry"] = {
        "max_attempts": 3,
        "retry_exit_codes": [75],
        "backoff_seconds": 0.25,
    }
    sleeps: list[float] = []
    spec = load_pipeline_spec(write_config(tmp_path, config))
    result = _runner(sleeper=sleeps.append).run(spec, "retry")
    assert result.ok
    assert len(result.attempts["build"]) == 2
    assert result.attempts["build"][0].exit_code == 75
    assert result.attempts["build"][1].exit_code == 0
    assert result.attempts["build"][0].stdout_log != result.attempts["build"][1].stdout_log
    assert sleeps == [0.25]


def test_retry_only_explicit_exception(tmp_path: Path) -> None:
    config = base_config()
    config["steps"][0]["retry"] = {
        "max_attempts": 2,
        "retry_exceptions": ["OSError"],
    }
    calls = 0

    def executor(step, cwd: Path, env) -> ExecutionOutcome:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("transient")
        output = cwd / "artifacts/output.txt"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text("ok", encoding="utf-8")
        return ExecutionOutcome(0, stdout=env["QUANT_PIPELINE_IDEMPOTENCY_KEY"])

    spec = load_pipeline_spec(write_config(tmp_path, config))
    result = _runner(executor=executor).run(spec, "exception")
    assert result.ok
    assert calls == 2
    assert result.attempts["build"][0].error_type == "OSError"


def test_unconfigured_failure_does_not_retry_and_isolates_independent_branch(
    tmp_path: Path,
) -> None:
    config = base_config()
    config["artifacts"] = [
        _artifact("bad", "artifacts/bad.txt"),
        _artifact("child", "artifacts/child.txt"),
        _artifact("independent", "artifacts/independent.txt"),
    ]
    config["steps"] = [
        _step(
            "bad",
            "artifacts/bad.txt",
            command=[sys.executable, "-c", "raise SystemExit(2)"],
            retry={"max_attempts": 3, "retry_exit_codes": [75]},
        ),
        _step("child", "artifacts/child.txt", needs=["bad"], inputs=["bad_output"]),
        _step("independent", "artifacts/independent.txt"),
    ]
    spec = load_pipeline_spec(write_config(tmp_path, config))
    result = _runner().run(spec, "isolated")
    assert result.step_status == {
        "bad": StepStatus.FAILED,
        "child": StepStatus.BLOCKED,
        "independent": StepStatus.SUCCEEDED,
    }
    assert len(result.attempts["bad"]) == 1


def test_data_quality_failure_blocks_curated_and_strategy_descendants_but_keeps_independent_branch(
    tmp_path: Path,
) -> None:
    config = base_config()
    config["artifacts"] = [
        _artifact("data_quality_gate", "artifacts/quality.json"),
        _artifact("curated_builder", "artifacts/curated.parquet"),
        _artifact("strategy_research", "artifacts/strategy.json"),
        _artifact("independent_report", "artifacts/independent.json"),
    ]
    config["steps"] = [
        _step(
            "data_quality_gate",
            "artifacts/quality.json",
            command=[sys.executable, "-c", "raise SystemExit(23)"],
            retry={"max_attempts": 3, "retry_exit_codes": [75]},
        ),
        _step(
            "curated_builder",
            "artifacts/curated.parquet",
            needs=["data_quality_gate"],
            inputs=["data_quality_gate_output"],
        ),
        _step(
            "strategy_research",
            "artifacts/strategy.json",
            needs=["curated_builder"],
            inputs=["curated_builder_output"],
        ),
        _step("independent_report", "artifacts/independent.json"),
    ]

    result = _runner().run(load_pipeline_spec(write_config(tmp_path, config)), "quality-isolation")

    assert result.step_status == {
        "data_quality_gate": StepStatus.FAILED,
        "curated_builder": StepStatus.BLOCKED,
        "strategy_research": StepStatus.BLOCKED,
        "independent_report": StepStatus.SUCCEEDED,
    }
    assert len(result.attempts["data_quality_gate"]) == 1
    assert result.attempts["curated_builder"] == ()
    assert result.attempts["strategy_research"] == ()
    blocked = {
        event.step_id: event.details["reason"]
        for event in result.events
        if event.event_type == "step_blocked"
    }
    assert blocked == {
        "curated_builder": "dependency_failed",
        "strategy_research": "dependency_failed",
    }


def test_executor_exception_streams_and_optional_missing_input_are_preserved(
    tmp_path: Path,
) -> None:
    config = base_config()
    config["artifacts"].insert(
        0,
        {
            "artifact_id": "optional_input",
            "path": "inputs/optional.txt",
            "producer": None,
            "required": False,
            "immutable": True,
        },
    )
    config["steps"][0]["inputs"] = ["optional_input"]

    class FixtureError(Exception):
        stdout = b"binary stdout"
        stderr = "text stderr"

    def executor(step, cwd: Path, env) -> ExecutionOutcome:
        raise FixtureError("fixture failure")

    spec = load_pipeline_spec(write_config(tmp_path, config))
    result = _runner(executor=executor).run(spec, "exception-streams")

    assert result.step_status["build"] == StepStatus.FAILED
    attempt = result.attempts["build"][0]
    assert attempt.exit_code is None
    assert (tmp_path / attempt.stdout_log).read_text(encoding="utf-8") == "binary stdout"
    assert (tmp_path / attempt.stderr_log).read_text(encoding="utf-8") == "text stderr"
    assert result.artifact_index["optional_input"] == "MISSING"


def test_declared_actual_hash_and_existing_run_log_fail_closed(tmp_path: Path) -> None:
    spec = load_pipeline_spec(write_config(tmp_path, base_config()))
    output = tmp_path / "artifacts/output.txt"
    output.parent.mkdir(parents=True)
    output.write_text("output", encoding="utf-8")
    artifact = replace(spec.artifacts[0], actual_sha256="0" * 64)
    with pytest.raises(ArtifactIntegrityError, match="declared actual hash"):
        _runner()._hash_artifact(spec, artifact)

    log_dir = spec.log_dir / "existing-log"
    log_dir.mkdir(parents=True)
    with pytest.raises(CheckpointError, match="Run log directory already exists"):
        _runner().run(spec, "existing-log")


def test_attempt_log_collision_and_exhausted_retry_checkpoint_are_not_silent(
    tmp_path: Path,
) -> None:
    spec = load_pipeline_spec(write_config(tmp_path / "collision", base_config()))
    created = False

    def clock() -> datetime:
        nonlocal created
        if not created:
            log_dir = spec.log_dir / "log-collision" / "build"
            log_dir.mkdir(parents=True, exist_ok=True)
            (log_dir / "attempt-0001.stdout.log").write_text("collision", encoding="utf-8")
            created = True
        return fixed_clock()

    result = _runner(clock=clock).run(spec, "log-collision")
    assert result.step_status["build"] == StepStatus.FAILED
    assert result.attempts["build"] == ()
    assert any(event.event_type == "step_contract_failed" for event in result.events)

    failed_root = tmp_path / "exhausted"
    failed_config = base_config()
    failed_config["steps"][0]["command"] = [sys.executable, "-c", "raise SystemExit(2)"]
    failed_spec = load_pipeline_spec(write_config(failed_root, failed_config))
    _runner().run(failed_spec, "exhausted")
    checkpoint = load_checkpoint(failed_spec.checkpoint_path)
    checkpoint.step_status["build"] = StepStatus.PENDING
    write_checkpoint_atomic(failed_spec.checkpoint_path, checkpoint)
    resumed = _runner(spec=failed_spec).resume(failed_spec.checkpoint_path)
    assert resumed.step_status["build"] == StepStatus.FAILED
    assert any(event.event_type == "step_contract_failed" for event in resumed.events)


def test_fail_fast_blocks_independent_branch(tmp_path: Path) -> None:
    config = base_config()
    config["fail_fast"] = True
    config["artifacts"] = [
        _artifact("a_bad", "artifacts/bad.txt"),
        _artifact("z_independent", "artifacts/independent.txt"),
    ]
    config["steps"] = [
        _step(
            "a_bad",
            "artifacts/bad.txt",
            command=[sys.executable, "-c", "raise SystemExit(2)"],
        ),
        _step("z_independent", "artifacts/independent.txt"),
    ]
    result = _runner().run(load_pipeline_spec(write_config(tmp_path, config)), "fail-fast")
    assert result.step_status["a_bad"] == StepStatus.FAILED
    assert result.step_status["z_independent"] == StepStatus.BLOCKED


def test_contract_hash_error_is_not_retried(tmp_path: Path) -> None:
    config = base_config()
    config["artifacts"][0]["expected_sha256"] = "0" * 64
    config["steps"][0]["retry"] = {
        "max_attempts": 3,
        "retry_exit_codes": [0],
        "retry_exceptions": ["ArtifactIntegrityError"],
    }
    result = _runner().run(load_pipeline_spec(write_config(tmp_path, config)), "hash")
    assert result.step_status["build"] == StepStatus.FAILED
    assert len(result.attempts["build"]) == 1
    assert any(event.event_type == "step_contract_failed" for event in result.events)


@pytest.mark.parametrize("mode", ["missing_input", "preexisting_output"])
def test_pre_execution_artifact_failure_has_no_attempt(tmp_path: Path, mode: str) -> None:
    config = base_config()
    if mode == "missing_input":
        config["artifacts"].insert(
            0,
            {
                "artifact_id": "input",
                "path": "inputs/missing.txt",
                "producer": None,
                "required": True,
                "immutable": True,
            },
        )
        config["steps"][0]["inputs"] = ["input"]
    else:
        output = tmp_path / "artifacts/output.txt"
        output.parent.mkdir(parents=True)
        output.write_text("stale", encoding="utf-8")
    spec = load_pipeline_spec(write_config(tmp_path, config))
    result = _runner().run(spec, "precheck")
    assert result.step_status["build"] == StepStatus.FAILED
    assert result.attempts["build"] == ()


def test_resume_rejects_changed_input_definition_stack_seed_and_logs(tmp_path: Path) -> None:
    cases = ["input", "definition", "stack", "seed", "log"]
    for case in cases:
        root = tmp_path / case
        config = base_config()
        input_path = root / "inputs/source.txt"
        input_path.parent.mkdir(parents=True)
        input_path.write_text("source", encoding="utf-8")
        config["artifacts"].insert(
            0,
            {
                "artifact_id": "source",
                "path": "inputs/source.txt",
                "producer": None,
                "required": True,
                "immutable": True,
            },
        )
        config["steps"][0]["inputs"] = ["source"]
        config_path = write_config(root, config)
        spec = load_pipeline_spec(config_path)
        result = _runner().run(spec, "strict")
        runner = _runner(spec=spec)
        expected_error = CheckpointError
        if case == "input":
            input_path.write_text("changed", encoding="utf-8")
            expected_error = ArtifactIntegrityError
        elif case == "definition":
            config["steps"][0]["kind"] = "changed"
            spec = load_pipeline_spec(write_config(root, config))
            runner = _runner(spec=spec)
        elif case == "stack":
            runner = DagRunner(
                stack_manifest={**STACK_MANIFEST, "repositories": ["changed"]},
                seed=7,
                spec=spec,
                clock=fixed_clock,
            )
        elif case == "seed":
            runner = DagRunner(stack_manifest=STACK_MANIFEST, seed=8, spec=spec, clock=fixed_clock)
        else:
            (root / result.attempts["build"][0].stdout_log).write_text("changed", encoding="utf-8")
        with pytest.raises(expected_error):
            runner.resume(spec.checkpoint_path)


def test_resume_rejects_running_state_and_corrupt_checkpoint(tmp_path: Path) -> None:
    spec = load_pipeline_spec(write_config(tmp_path, base_config()))
    _runner().run(spec, "strict")
    checkpoint = load_checkpoint(spec.checkpoint_path)
    checkpoint.step_status["build"] = StepStatus.RUNNING
    write_checkpoint_atomic(spec.checkpoint_path, checkpoint)
    with pytest.raises(CheckpointError, match="running"):
        _runner(spec=spec).resume(spec.checkpoint_path)

    payload = json.loads(spec.checkpoint_path.read_text(encoding="utf-8"))
    payload["run_id"] = "tampered"
    spec.checkpoint_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(CheckpointError, match="hash mismatch"):
        load_checkpoint(spec.checkpoint_path)


def test_run_resume_argument_validates_run_id_and_checkpoint_identity(tmp_path: Path) -> None:
    spec = load_pipeline_spec(write_config(tmp_path, base_config()))
    with pytest.raises(ValueError, match="run_id"):
        _runner().run(spec, "../bad")
    _runner().run(spec, "original")
    with pytest.raises(CheckpointError, match="run_id"):
        _runner().run(spec, "different", resume=True)
    with pytest.raises(CheckpointError, match="already exists"):
        _runner().run(spec, "original")


def test_dry_run_is_deterministic_and_does_not_create_artifacts(tmp_path: Path) -> None:
    spec = load_pipeline_spec(write_config(tmp_path, base_config()))
    result = _runner(dry_run=True).run(spec, "dry")
    assert result.ok
    assert result.step_status == {"build": StepStatus.DRY_RUN}
    assert result.attempts["build"] == ()
    assert not (tmp_path / "artifacts/output.txt").exists()


def test_naive_clock_and_invalid_seed_fail_closed(tmp_path: Path) -> None:
    spec = load_pipeline_spec(write_config(tmp_path, base_config()))
    with pytest.raises(TypeError, match="seed"):
        DagRunner(stack_manifest=STACK_MANIFEST, seed=True)
    runner = DagRunner(
        stack_manifest=STACK_MANIFEST,
        seed=1,
        clock=lambda: datetime(2026, 8, 29),  # noqa: DTZ001 - intentionally invalid fixture
    )
    with pytest.raises(CheckpointError, match="timezone-aware"):
        runner.run(spec, "clock")


def test_runtime_path_rechecks_workspace_boundary(tmp_path: Path) -> None:
    spec = load_pipeline_spec(write_config(tmp_path / "workspace", base_config()))
    with pytest.raises(CheckpointError, match="escapes workspace_root"):
        DagRunner._runtime_path(spec, tmp_path / "outside.log", "test path")


def test_resume_rejects_unknown_keys_attempt_overflow_key_mismatch_and_unknown_event(
    tmp_path: Path,
) -> None:
    for case, message in (
        ("unknown_key", "unknown step keys"),
        ("attempt_overflow", "max_attempts"),
        ("attempt_key", "idempotency key mismatch"),
        ("unknown_event", "unknown step"),
        ("invalid_run", "run_id is invalid"),
    ):
        root = tmp_path / case
        spec = load_pipeline_spec(write_config(root, base_config()))
        _runner().run(spec, "strict")
        checkpoint = load_checkpoint(spec.checkpoint_path)
        if case == "unknown_key":
            checkpoint.idempotency_keys["unknown"] = "a" * 64
        elif case == "attempt_overflow":
            checkpoint.attempts["build"].append(replace(checkpoint.attempts["build"][0], attempt=2))
        elif case == "attempt_key":
            checkpoint.attempts["build"][0] = replace(
                checkpoint.attempts["build"][0], idempotency_key="a" * 64
            )
        elif case == "unknown_event":
            checkpoint.events[-1] = replace(checkpoint.events[-1], step_id="unknown")
        else:
            checkpoint.run_id = "../invalid"
        write_checkpoint_atomic(spec.checkpoint_path, checkpoint)
        with pytest.raises(CheckpointError, match=message):
            _runner(spec=spec).resume(spec.checkpoint_path)
