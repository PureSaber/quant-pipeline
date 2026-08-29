from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest
from v2_helpers import STACK_MANIFEST, base_config, python_write_command, write_config

from quant_pipeline.dag_schema import (
    deterministic_topology,
    load_pipeline_spec,
    pipeline_config_hash,
    validate_pipeline_spec,
)
from quant_pipeline.integrity import stack_manifest_hash
from quant_pipeline.v2_models import CheckpointError, PipelineSpecError


def _codes(path: Path) -> set[str]:
    result = validate_pipeline_spec(load_pipeline_spec(path), STACK_MANIFEST)
    return {issue.code for issue in result.issues}


def _add_second_step(config: dict, *, needs: list[str] | None = None) -> None:
    config["artifacts"].append(
        {
            "artifact_id": "second",
            "path": "artifacts/second.txt",
            "producer": "consume",
            "required": True,
            "immutable": True,
        }
    )
    config["steps"].append(
        {
            "id": "consume",
            "kind": "fixture",
            "needs": ["build"] if needs is None else needs,
            "command": python_write_command("artifacts/second.txt"),
            "inputs": ["output"],
            "outputs": ["second"],
            "retry": {"max_attempts": 1},
            "timeout": 10,
        }
    )


def test_load_and_validate_valid_v2(tmp_path: Path) -> None:
    spec = load_pipeline_spec(write_config(tmp_path, base_config()))
    result = validate_pipeline_spec(spec, STACK_MANIFEST)
    assert result.ok
    assert spec.schema_version == "2.0.0"
    assert spec.steps[0].command[0]
    assert spec.artifacts[0].path == "artifacts/output.txt"


def test_repository_v2_smoke_config_validates() -> None:
    path = Path(__file__).resolve().parents[1] / "configs" / "e2e_v2.yaml"
    assert validate_pipeline_spec(load_pipeline_spec(path), STACK_MANIFEST).ok


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda cfg: cfg.update(schema_version="1.0.0"), "schema_version"),
        (lambda cfg: cfg.update(unexpected=True), "unknown keys"),
        (lambda cfg: cfg["steps"][0].update(shell=True), "forbidden"),
        (lambda cfg: cfg["steps"][0].update(command="echo unsafe"), "must be a list"),
        (lambda cfg: cfg.update(name=0), "must be a non-empty string"),
        (lambda cfg: cfg.update(fail_fast="false"), "must be a boolean"),
        (lambda cfg: cfg["artifacts"][0].update(path="../escape.txt"), "escapes"),
        (lambda cfg: cfg["steps"][0].update(cwd="../escape"), "escapes"),
        (lambda cfg: cfg["steps"][0].update(timeout=float("inf")), "must be finite"),
    ],
)
def test_loader_rejects_contract_and_path_errors(tmp_path: Path, mutation, message: str) -> None:
    config = base_config()
    mutation(config)
    with pytest.raises(PipelineSpecError, match=message):
        load_pipeline_spec(write_config(tmp_path, config))


def test_duplicate_step_and_artifact_ids(tmp_path: Path) -> None:
    config = base_config()
    config["steps"].append(deepcopy(config["steps"][0]))
    config["artifacts"].append(deepcopy(config["artifacts"][0]))
    assert {"DUPLICATE_STEP", "DUPLICATE_ARTIFACT"} <= _codes(write_config(tmp_path, config))


def test_deterministic_topology_rejects_duplicate_step_ids(tmp_path: Path) -> None:
    spec = load_pipeline_spec(write_config(tmp_path, base_config()))
    duplicate = replace(spec, steps=(spec.steps[0], spec.steps[0]))
    assert deterministic_topology(duplicate) == ()


def test_empty_dag_and_duplicate_retry_matchers(tmp_path: Path) -> None:
    empty = base_config()
    empty["steps"] = []
    assert "EMPTY_DAG" in _codes(write_config(tmp_path / "empty", empty))

    duplicate = base_config()
    duplicate["steps"][0]["retry"] = {
        "max_attempts": 2,
        "retry_exit_codes": [75, 75],
        "retry_exceptions": ["OSError", "OSError"],
    }
    assert "DUPLICATE_RETRY_MATCH" in _codes(write_config(tmp_path / "retry", duplicate))


def test_unknown_self_and_cycle_dependencies(tmp_path: Path) -> None:
    unknown = base_config()
    unknown["steps"][0]["needs"] = ["missing"]
    assert "UNKNOWN_DEPENDENCY" in _codes(write_config(tmp_path / "unknown", unknown))

    self_dependency = base_config()
    self_dependency["steps"][0]["needs"] = ["build"]
    assert {"SELF_DEPENDENCY", "DAG_CYCLE"} <= _codes(
        write_config(tmp_path / "self", self_dependency)
    )

    cycle = base_config()
    _add_second_step(cycle)
    cycle["steps"][0]["needs"] = ["consume"]
    assert "DAG_CYCLE" in _codes(write_config(tmp_path / "cycle", cycle))


def test_producer_and_artifact_dependency_conflicts(tmp_path: Path) -> None:
    undeclared = base_config()
    _add_second_step(undeclared, needs=[])
    assert "UNDECLARED_ARTIFACT_DEPENDENCY" in _codes(
        write_config(tmp_path / "undeclared", undeclared)
    )

    conflict = base_config()
    _add_second_step(conflict)
    conflict["steps"][1]["outputs"] = ["output", "second"]
    codes = _codes(write_config(tmp_path / "producer", conflict))
    assert {"PRODUCER_CONFLICT", "PRODUCER_MISMATCH"} <= codes

    missing = base_config()
    missing["steps"][0]["outputs"] = []
    assert "PRODUCER_OUTPUT_MISSING" in _codes(write_config(tmp_path / "missing", missing))


def test_unknown_artifact_output_path_and_reserved_path_conflicts(tmp_path: Path) -> None:
    unknown = base_config()
    unknown["steps"][0]["inputs"] = ["unknown"]
    assert "UNKNOWN_ARTIFACT" in _codes(write_config(tmp_path / "unknown", unknown))

    output_path = base_config()
    _add_second_step(output_path)
    output_path["artifacts"][1]["path"] = "artifacts/output.txt"
    assert "OUTPUT_PATH_CONFLICT" in _codes(write_config(tmp_path / "path", output_path))

    reserved = base_config(output=".state/checkpoint.json")
    assert "RESERVED_PATH" in _codes(write_config(tmp_path / "reserved", reserved))


def test_invalid_retry_timeout_ids_hashes_and_duplicate_references(tmp_path: Path) -> None:
    config = base_config()
    step = config["steps"][0]
    step["id"] = "bad/id"
    config["artifacts"][0]["producer"] = "bad/id"
    config["artifacts"][0]["artifact_id"] = "bad artifact"
    step["outputs"] = ["bad artifact", "bad artifact"]
    step["timeout"] = 0
    step["retry"] = {"max_attempts": 0, "backoff_seconds": -1}
    config["artifacts"][0]["expected_sha256"] = "ABC"
    codes = _codes(write_config(tmp_path, config))
    assert {
        "INVALID_STEP_ID",
        "INVALID_ARTIFACT_ID",
        "INVALID_TIMEOUT",
        "INVALID_RETRY",
        "INVALID_SHA256",
        "DUPLICATE_REFERENCE",
    } <= codes


def test_stack_manifest_requires_release_ready_embedded_hash() -> None:
    payload = dict(STACK_MANIFEST)
    loaded, actual = stack_manifest_hash(payload)
    assert loaded["manifest_hash"] == actual == STACK_MANIFEST["manifest_hash"]

    payload["manifest_hash"] = "0" * 64
    with pytest.raises(CheckpointError, match="MANIFEST_HASH_MISMATCH"):
        stack_manifest_hash(payload)

    missing_hash = dict(STACK_MANIFEST)
    missing_hash.pop("manifest_hash")
    with pytest.raises(CheckpointError, match="Invalid StackManifest payload"):
        stack_manifest_hash(missing_hash)

    with pytest.raises(CheckpointError, match="Invalid StackManifest payload"):
        stack_manifest_hash(
            {
                "schema_version": "1.0.0",
                "created_at": "2026-08-29T00:00:00Z",
                "repositories": [],
            }
        )


def test_topology_and_config_hash_are_order_and_location_stable(tmp_path: Path) -> None:
    first = base_config()
    _add_second_step(first)
    first["steps"] = list(reversed(first["steps"]))
    first["artifacts"] = list(reversed(first["artifacts"]))
    spec_a = load_pipeline_spec(write_config(tmp_path / "a", first))
    spec_b = load_pipeline_spec(write_config(tmp_path / "b", first))
    assert deterministic_topology(spec_a) == ("build", "consume")
    assert pipeline_config_hash(spec_a) == pipeline_config_hash(spec_b)


def test_validation_reports_invalid_stack_manifest(tmp_path: Path) -> None:
    result = validate_pipeline_spec(
        load_pipeline_spec(write_config(tmp_path, base_config())), {"schema_version": "0"}
    )
    assert {issue.code for issue in result.issues} == {"STACK_MANIFEST_INVALID"}
