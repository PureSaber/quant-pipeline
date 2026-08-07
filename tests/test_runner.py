from pathlib import Path

import yaml

from quant_pipeline.runner import run_pipeline


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
                    {"name": "bad", "cmd": "python -c \"import sys; sys.exit(1)\""},
                    {"name": "skip", "cmd": "echo never"},
                ],
            }
        ),
        encoding="utf-8",
    )
    result = run_pipeline(cfg, stop_on_error=True)
    assert not result.ok
    assert len(result.steps) == 1


def test_run_step_shell_mode() -> None:
    from quant_pipeline.runner import run_step

    result = run_step("sh", "echo hi", shell=True)
    assert result.exit_code == 0
    assert "hi" in result.stdout


def test_expand_missing_key_raises() -> None:
    from quant_pipeline.runner import _expand

    try:
        _expand("{missing}", {})
        raise AssertionError("expected KeyError")
    except KeyError:
        pass
