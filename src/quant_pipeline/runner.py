from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


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


def _expand(text: str, env: dict[str, str]) -> str:
    return os.path.expandvars(text.format(**env))


def _build_env(workspace_config: Path | None, extra: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(os.environ)
    if extra:
        env.update(extra)
    if workspace_config and Path(workspace_config).is_file():
        from quant_workspace.loader import load_workspace

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


def run_step(name: str, command: str, *, cwd: Path | None = None, env: dict[str, str] | None = None) -> StepResult:
    start = time.perf_counter()
    proc = subprocess.run(
        command,
        shell=True,
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=True,
        text=True,
    )
    return StepResult(
        name=name,
        command=command,
        exit_code=proc.returncode,
        duration_s=round(time.perf_counter() - start, 3),
        stdout=proc.stdout[-4000:],
        stderr=proc.stderr[-4000:],
    )


def run_pipeline(config_path: Path, *, dry_run: bool = False, stop_on_error: bool = True) -> PipelineResult:
    cfg = yaml.safe_load(Path(config_path).read_text(encoding="utf-8")) or {}
    name = str(cfg.get("name", config_path.stem))
    workspace_cfg = cfg.get("workspace")
    ws_path = Path(workspace_cfg) if workspace_cfg else None
    if ws_path and not ws_path.is_absolute():
        ws_path = (config_path.parent / ws_path).resolve()

    env = _build_env(ws_path, cfg.get("env"))
    cwd_raw = cfg.get("cwd")
    cwd = Path(_expand(str(cwd_raw), env)).resolve() if cwd_raw else None

    result = PipelineResult(name=name)
    for raw_step in cfg.get("steps") or []:
        step_name = str(raw_step.get("name", "step"))
        command = _expand(str(raw_step["cmd"]), env)
        if dry_run:
            result.steps.append(StepResult(name=step_name, command=command, exit_code=0, duration_s=0.0))
            continue
        step_cwd = cwd
        if raw_step.get("cwd"):
            step_cwd = Path(_expand(str(raw_step["cwd"]), env)).resolve()
        step_result = run_step(step_name, command, cwd=step_cwd, env=env)
        result.steps.append(step_result)
        if stop_on_error and step_result.exit_code != 0:
            break
    return result


def load_pipeline_config(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
