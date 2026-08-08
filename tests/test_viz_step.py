"""Tests for pipeline viz step template."""

from pathlib import Path


def test_render_report_step_config_exists() -> None:
    path = Path(__file__).resolve().parents[1] / "configs" / "steps" / "render_report.yaml"
    assert path.is_file()
    text = path.read_text(encoding="utf-8")
    assert "report_hub" in text or "quant-report-hub" in text.lower() or "spread_parity" in text
