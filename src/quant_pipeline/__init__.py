"""YAML-driven post-run orchestration."""

from importlib import import_module


def __getattr__(name):
    """Keep standalone isolation usable without the optional research stack."""
    modules = {
        "DagRunner": "dag_runner",
        "load_pipeline_spec": "dag_schema",
        "validate_pipeline_spec": "dag_schema",
        "PipelineResult": "runner",
        "run_pipeline": "runner",
    }
    if name not in __all__:
        raise AttributeError(name)
    value = getattr(import_module("quant_pipeline." + modules.get(name, "v2_models")), name)
    globals()[name] = value
    return value

__all__ = [
    "NON_RETRYABLE_GATE_KINDS",
    "ArtifactSpec",
    "DagRunResult",
    "DagRunner",
    "PipelineCheckpoint",
    "PipelineResult",
    "PipelineSpec",
    "RetryPolicy",
    "StepAttempt",
    "StepSpec",
    "StepStatus",
    "load_pipeline_spec",
    "run_pipeline",
    "validate_pipeline_spec",
]
