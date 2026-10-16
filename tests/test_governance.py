from __future__ import annotations

import re
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_COMMIT = "2cb59b1c433a154335f13d5fd9f65d3aee974596"


def test_release_and_workspace_governance_are_declared() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = data["project"]
    governance = data["tool"]["quant-workspace"]

    assert project["version"] == "0.4.0"
    assert governance == {
        "layer": "orchestration",
        "schemas": [{"id": "puresaber.pipeline", "version": "2.0.0"}],
        "lock-files": ["requirements.lock"],
    }
    dependencies = "\n".join(project["dependencies"])
    assert "quant-workspace.git@" + WORKSPACE_COMMIT in dependencies
    assert not re.search(r"git\+[^\s]+@(main|master|latest)(?:\b|$)", dependencies)


def test_lock_closes_python310_and_editable_build_dependencies() -> None:
    lock = (ROOT / "requirements.lock").read_text(encoding="utf-8")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert "pip-compile with Python 3.10" in lock
    assert "quant-workspace.git@" + WORKSPACE_COMMIT in lock
    assert WORKSPACE_COMMIT in readme
    assert re.search(r'tomli==[^\s]+ ; python_version < "3\.11"', lock)
    assert "exceptiongroup==" in lock
    assert "typing-extensions==" in lock
    assert "setuptools==" in lock
    assert not re.search(r"git\+[^\s]+@(main|master|latest)(?:\b|$)", lock)
