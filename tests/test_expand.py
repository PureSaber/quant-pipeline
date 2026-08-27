import pytest

from quant_pipeline.runner import PipelineExpandError, _expand


def test_expand_substitutes_env() -> None:
    assert _expand("hello {name}", {"name": "world"}, step_name="t1") == "hello world"


def test_expand_environment_syntax_before_format_placeholders(monkeypatch) -> None:
    monkeypatch.setenv("QUANT_RUN_ID", "research_42")
    assert (
        _expand("${QUANT_RUN_ID}/{root}", {"root": "outputs"}, step_name="t1")
        == "research_42/outputs"
    )


def test_expand_missing_key_includes_step_name() -> None:
    with pytest.raises(PipelineExpandError) as exc:
        _expand("{foo}", {}, step_name="my_step")
    assert "my_step" in str(exc.value)
    assert "foo" in str(exc.value)
