"""Check that research constraint updates reach the environment exercised by CI."""

from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name


def test_research_lock_satisfies_declared_constraints():
    root = Path(__file__).resolve().parents[1]
    locked = {}
    for line in (root / "requirements-research.lock").read_text(encoding="utf-8").splitlines():
        if not line or line.startswith(("#", " ", "-")):
            continue
        requirement = Requirement(line)
        locked[canonicalize_name(requirement.name)] = requirement

    for line in (root / "requirements-research.in").read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        constraint = Requirement(line)
        requirement = locked[canonicalize_name(constraint.name)]
        if constraint.url:
            assert requirement.url == constraint.url, constraint.name
        else:
            pins = [pin.version for pin in requirement.specifier if pin.operator == "=="]
            assert len(pins) == 1, f"{constraint.name} needs one exact locked version"
            assert constraint.specifier.contains(pins[0]), (
                f"{constraint.name}: lock has {pins[0]}, constraints require {constraint.specifier}"
            )
