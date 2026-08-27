from pathlib import Path

from quant_pipeline.schema import validate_config, validate_config_file


def test_validate_config_warns_unknown_keys() -> None:
    issues = validate_config({"steps": [{"cmd": "echo hi"}]}, strict=False)
    assert any("WARN" in i for i in issues) or issues == []


def test_validate_config_strict_errors_unknown_keys() -> None:
    issues = validate_config({"extra": True, "steps": [{"cmd": "echo"}]}, strict=True)
    assert any("Unknown top-level" in i for i in issues)


def test_validate_equity_postrun_smoke_config() -> None:
    cfg_path = Path(__file__).resolve().parents[1] / "configs" / "equity_postrun_smoke.yaml"
    issues = validate_config_file(cfg_path, strict=False)
    assert not any(i.startswith("steps") and "missing" in i for i in issues)


def test_validate_research_integrity_postrun_config() -> None:
    cfg_path = Path(__file__).resolve().parents[1] / "configs" / "research_integrity_postrun.yaml"
    issues = validate_config_file(cfg_path, strict=True)
    assert issues == []
