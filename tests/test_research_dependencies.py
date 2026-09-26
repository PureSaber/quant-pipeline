from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


def test_research_ci_lock_contains_every_declared_application_revision():
    root = Path(__file__).resolve().parents[1]
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    base_lock = (root / "requirements.lock").read_text(encoding="utf-8")
    research_lock = (root / "requirements-research.lock").read_text(encoding="utf-8")
    for requirement in project["dependencies"]:
        if "git+" in requirement:
            assert requirement in base_lock
            assert requirement in research_lock
    for requirement in project["optional-dependencies"]["research"]:
        assert requirement in research_lock
