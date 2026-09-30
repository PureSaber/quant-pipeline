import base64
import hashlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from quant_pipeline.code_identity import package_revision


def installed(tmp_path, monkeypatch, revision="a" * 40):
    package = tmp_path / "site-packages" / "sample"
    package.mkdir(parents=True)
    module = package / "__init__.py"
    module.write_text("VALUE = 1\n")
    hash_value = base64.urlsafe_b64encode(hashlib.sha256(module.read_bytes()).digest())
    entry = type("Entry", (str,), {})("sample/__init__.py")
    entry.hash = SimpleNamespace(mode="sha256", value=hash_value.decode().rstrip("="))
    distribution = SimpleNamespace(
        files=[entry],
        locate_file=lambda item: package.parent / item,
        read_text=lambda _: json.dumps({"vcs_info": {"vcs": "git", "commit_id": revision}}),
    )
    monkeypatch.setattr("importlib.metadata.distribution", lambda _: distribution)
    monkeypatch.setattr("importlib.import_module", lambda _: SimpleNamespace(__file__=module))
    return package, distribution


def test_installed_vcs_wheel_requires_no_git_checkout(tmp_path, monkeypatch):
    installed(tmp_path, monkeypatch)
    assert package_revision("sample") == "a" * 40


@pytest.mark.parametrize("revision", ["", "main", "v0.1.0", "a" * 39])
def test_installed_package_without_immutable_provenance_is_actionable(
    tmp_path, monkeypatch, revision
):
    installed(tmp_path, monkeypatch, revision)
    with pytest.raises(ValueError, match="bootstrap.py"):
        package_revision("sample")


@pytest.mark.parametrize("change", ["modified", "extra", "missing_record", "no_hash"])
def test_installed_package_mutation_is_rejected(tmp_path, monkeypatch, change):
    package, distribution = installed(tmp_path, monkeypatch)
    if change == "modified":
        (package / "__init__.py").write_text("VALUE = 2\n")
    elif change == "extra":
        (package / "injected.py").write_text("pass\n")
    elif change == "missing_record":
        distribution.files = []
    else:
        distribution.files[0].hash = None
    with pytest.raises(ValueError, match="RECORD|Unverifiable"):
        package_revision("sample")


def test_source_requires_clean_tracked_checkout(tmp_path, monkeypatch):
    package = tmp_path / "src" / "sample"
    package.mkdir(parents=True)
    module = package / "__init__.py"
    module.write_text("VALUE = 1\n")
    monkeypatch.setattr("importlib.import_module", lambda _: SimpleNamespace(__file__=module))
    for args in (
        ["init"],
        ["add", "."],
        [
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-m",
            "fixture",
        ],
    ):
        subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True)
    expected = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True
    ).strip()
    assert package_revision("sample") == expected
    Path(module).write_text("VALUE = 2\n")
    with pytest.raises(ValueError, match="clean"):
        package_revision("sample")
