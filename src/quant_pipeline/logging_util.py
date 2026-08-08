"""Pipeline step failure logging."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from quant_pipeline.runner import StepResult


def write_step_failure_log(
    log_root: Path,
    *,
    run_id: str,
    step_id: str,
    result: StepResult,
) -> Path:
    """Write stdout/stderr for a failed pipeline step."""
    dest_dir = log_root / run_id
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{step_id}.log"
    body = (
        f"command: {result.command}\n"
        f"exit_code: {result.exit_code}\n"
        f"duration_s: {result.duration_s}\n\n"
        f"--- stdout ---\n{result.stdout}\n\n"
        f"--- stderr ---\n{result.stderr}\n"
    )
    dest.write_text(body, encoding="utf-8")
    return dest
