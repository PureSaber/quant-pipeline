from pathlib import Path

import yaml

from quant_pipeline.logging_util import write_step_failure_log
from quant_pipeline.runner import StepResult, run_pipeline


def test_error_log_written_on_failure(tmp_path: Path) -> None:
    cfg = tmp_path / "pipe.yaml"
    log_dir = tmp_path / "logs"
    cfg.write_text(
        yaml.safe_dump(
            {
                "name": "fail",
                "run_id": "run42",
                "error_log_dir": str(log_dir),
                "steps": [{"name": "bad", "cmd": "exit 1", "shell": True}],
            }
        ),
        encoding="utf-8",
    )
    result = run_pipeline(cfg, stop_on_error=True)
    assert not result.ok
    log_file = log_dir / "run42" / "bad.log"
    assert log_file.is_file()
    assert "exit_code: 1" in log_file.read_text(encoding="utf-8")


def test_write_step_failure_log_helper(tmp_path: Path) -> None:
    step = StepResult(name="x", command="cmd", exit_code=2, duration_s=0.1, stderr="boom")
    path = write_step_failure_log(tmp_path, run_id="r1", step_id="x", result=step)
    assert path.is_file()
    assert "boom" in path.read_text(encoding="utf-8")
