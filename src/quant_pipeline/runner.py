from __future__ import annotations

import logging
import os
import shlex
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from quant_pipeline.logging_util import write_step_failure_log

logger = logging.getLogger(__name__)

_LOG_TAIL = 4000


@dataclass
class StepResult:
    name: str
    command: str
    exit_code: int
    duration_s: float
    stdout: str = ""
    stderr: str = ""


@dataclass
class PipelineResult:
    name: str
    steps: list[StepResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(s.exit_code == 0 for s in self.steps)


class PipelineExpandError(KeyError):
    """Raised when a template key is missing during step expansion."""


def _expand(text: str, env: dict[str, str], *, step_name: str = "step") -> str:
    try:
        return os.path.expandvars(text).format(**env)
    except KeyError as exc:
        missing = exc.args[0]
        raise PipelineExpandError(
            f"Step {step_name!r}: missing template key {missing!r} in {text!r}"
        ) from exc


def _build_env(workspace_config: Path | None, extra: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(os.environ)
    if extra:
        env.update(extra)
    if workspace_config and Path(workspace_config).is_file():
        try:
            from quant_workspace.loader import load_workspace
        except ImportError as exc:
            raise ImportError(
                "quant-workspace is required when pipeline config sets `workspace`. "
                "Install with: pip install 'quant-pipeline[workspace]'"
            ) from exc

        ws = load_workspace(workspace_config, root_override=env.get("QUANT_WORKSPACE_ROOT"))
        env["QUANT_WORKSPACE_ROOT"] = str(ws.root)
        for name, proj in ws.projects.items():
            env[f"QW_{name.replace('-', '_').upper()}_REPO"] = str(proj.repo)
            for key in ("outputs", "state", "data", "notes", "reports"):
                val = getattr(proj, key, None)
                if val is not None:
                    suffix = key.upper()
                    env[f"QW_{name.replace('-', '_').upper()}_{suffix}"] = str(val)
    return env


def _resolve_argv(raw_step: dict[str, Any], command: str) -> tuple[list[str] | str, bool]:
    """Return subprocess argv. Shell mode is opt-in per step via shell: true."""
    cmd_raw = raw_step.get("cmd", command)
    use_shell = bool(raw_step.get("shell", False))

    if isinstance(cmd_raw, list):
        return [str(part) for part in cmd_raw], False

    if use_shell:
        return command, True

    return shlex.split(command, posix=(os.name != "nt")), False


def run_step(
    name: str,
    command: str,
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    raw_step: dict[str, Any] | None = None,
) -> StepResult:
    raw_step = raw_step or {"cmd": command}
    argv, use_shell = _resolve_argv(raw_step, command)
    start = time.perf_counter()
    proc = subprocess.run(
        argv,
        shell=use_shell,
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return StepResult(
        name=name,
        command=command if isinstance(argv, str) else " ".join(argv),
        exit_code=proc.returncode,
        duration_s=round(time.perf_counter() - start, 3),
        stdout=proc.stdout[-_LOG_TAIL:],
        stderr=proc.stderr[-_LOG_TAIL:],
    )


def run_pipeline(config_path: Path, *, dry_run: bool = False, stop_on_error: bool = True) -> PipelineResult:
    cfg = yaml.safe_load(Path(config_path).read_text(encoding="utf-8")) or {}
    name = str(cfg.get("name", config_path.stem))
    workspace_cfg = cfg.get("workspace")
    ws_path = Path(workspace_cfg) if workspace_cfg else None
    if ws_path and not ws_path.is_absolute():
        ws_path = (config_path.parent / ws_path).resolve()

    env = _build_env(ws_path, cfg.get("env"))
    run_id = _expand(str(cfg.get("run_id") or name), env, step_name="pipeline.run_id")
    env["QUANT_PIPELINE_RUN_ID"] = run_id
    cwd_raw = cfg.get("cwd")
    cwd = Path(_expand(str(cwd_raw), env, step_name="pipeline.cwd")).resolve() if cwd_raw else None

    error_log_dir = cfg.get("error_log_dir")
    result = PipelineResult(name=name)
    for raw_step in cfg.get("steps") or []:
        step_name = str(raw_step.get("name", "step"))
        command = _expand(str(raw_step["cmd"]), env, step_name=step_name)
        if dry_run:
            result.steps.append(StepResult(name=step_name, command=command, exit_code=0, duration_s=0.0))
            continue
        step_cwd = cwd
        if raw_step.get("cwd"):
            step_cwd = Path(_expand(str(raw_step["cwd"]), env, step_name=step_name)).resolve()
        step_result = run_step(
            step_name,
            command,
            cwd=step_cwd,
            env=env,
            raw_step=raw_step,
        )
        result.steps.append(step_result)
        if stop_on_error and step_result.exit_code != 0:
            if error_log_dir:
                log_root = Path(_expand(str(error_log_dir), env, step_name=step_name))
                write_step_failure_log(
                    log_root,
                    run_id=run_id,
                    step_id=step_name,
                    result=step_result,
                )
            break
    return result


def load_pipeline_config(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
