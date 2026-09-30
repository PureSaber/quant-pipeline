"""Immutable provenance for clean source checkouts and verified VCS installations."""

from __future__ import annotations

import base64
import hashlib
import importlib
import importlib.metadata
import json
import re
import subprocess
from pathlib import Path


def package_revision(name: str) -> str:
    module = importlib.import_module(name)
    package = Path(module.__file__).resolve().parent
    root = package.parent.parent
    if (root / ".git").exists() and package == root / "src" / name:
        status = subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=all"], cwd=root, text=True
        )
        if status.strip():
            raise ValueError(f"Research execution requires a clean {name} checkout")
        subprocess.run(
            ["git", "ls-files", "--error-unmatch", f"src/{name}/__init__.py"],
            cwd=root,
            check=True,
            capture_output=True,
        )
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()

    distribution = importlib.metadata.distribution(name.replace("_", "-"))
    direct = json.loads(distribution.read_text("direct_url.json") or "{}")
    vcs = direct.get("vcs_info", {})
    revision = vcs.get("commit_id", "")
    if vcs.get("vcs") != "git" or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError(
            f"{name} requires immutable Git provenance; install from an exact Git commit "
            "or use quant-workspace/profiles/research-workbench/bootstrap.py"
        )
    verified = set()
    for entry in distribution.files or ():
        if not str(entry).replace("\\", "/").startswith(name + "/"):
            continue
        path = Path(distribution.locate_file(entry)).resolve()
        if path.suffix == ".pyc":
            continue
        if not path.is_relative_to(package) or not entry.hash or entry.hash.mode != "sha256":
            raise ValueError(f"Unverifiable installed file: {name}/{entry}")
        actual = base64.urlsafe_b64encode(hashlib.sha256(path.read_bytes()).digest()).decode()
        if actual.rstrip("=") != entry.hash.value:
            raise ValueError(f"Installed file differs from RECORD: {name}/{entry}")
        verified.add(path)
    actual_files = {
        path.resolve()
        for path in package.rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    }
    if not verified or actual_files != verified:
        raise ValueError(f"Installed package contains files missing from RECORD: {name}")
    return revision
