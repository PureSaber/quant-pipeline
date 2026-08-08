from pathlib import Path

import pytest
import yaml

from quant_pipeline.runner import PipelineExpandError, _expand, run_pipeline, run_step


def test_run_step_argv_without_shell(tmp_path: Path) -> None:
    marker = tmp_path / "done.txt"
    result = run_step(
        "write",
        f'python -c "open(r\'{marker}\', \'w\').close()"',
        cwd=tmp_path,
        raw_step={"cmd": ["python", "-c", f"open(r'{marker}', 'w').close()"]},
    )
    assert result.exit_code == 0
    assert marker.is_file()


def test_run_step_shell_opt_in() -> None:
    result = run_step("echo", "echo shell_ok", raw_step={"cmd": "echo shell_ok", "shell": True})
    assert result.exit_code == 0
    assert "shell_ok" in result.stdout


def test_argv_list_without_shell() -> None:
    """Explicit argv list never enables shell."""
    result = run_step(
        "py",
        "python -c pass",
        raw_step={"cmd": ["python", "-c", "print('argv_ok')"]},
    )
    assert result.exit_code == 0
    assert "argv_ok" in result.stdout


def test_expand_missing_key_raises() -> None:
    with pytest.raises(PipelineExpandError, match="missing_key"):
        _expand("{missing_key}", {"present": "x"}, step_name="demo")


def test_run_pipeline_dry_run(tmp_path: Path) -> None:
    cfg = tmp_path / "pipe.yaml"
    cfg.write_text(
        yaml.safe_dump({"name": "demo", "steps": [{"name": "a", "cmd": "echo hello"}]}),
        encoding="utf-8",
    )
    result = run_pipeline(cfg, dry_run=True)
    assert result.ok
    assert len(result.steps) == 1
    assert result.steps[0].command == "echo hello"


def test_run_pipeline_stops_on_error(tmp_path: Path) -> None:
    cfg = tmp_path / "pipe.yaml"
    cfg.write_text(
        yaml.safe_dump(
            {
                "name": "fail",
                "steps": [
                    {"name": "bad", "cmd": "exit 1", "shell": True},
                    {"name": "skip", "cmd": "echo never", "shell": True},
                ],
            }
        ),
        encoding="utf-8",
    )
    result = run_pipeline(cfg, stop_on_error=True)
    assert not result.ok
    assert len(result.steps) == 1
