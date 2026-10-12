"""Reproducibility (CLAUDE.md): every tool version is pinned, and the copies agree.

Regression (compliance audit 2026-10-07): the dev group used ``>=`` ranges, so a new ruff, mypy
or pytest release could change lint and test results with no change in the repository, and the
ruff of pre-commit (pinned by tag) could drift from the ruff of the dev group.
"""

import re
import tomllib
from pathlib import Path

import yaml  # type: ignore[import-untyped]

PINNED = re.compile(r"^[A-Za-z0-9_.\[\]-]+==[0-9][0-9A-Za-z.+-]*$")


def _dev_group(repo_root: Path) -> list[str]:
    config = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    group: list[str] = config["dependency-groups"]["dev"]
    return group


def test_every_dev_dependency_is_pinned_exactly(repo_root: Path) -> None:
    unpinned = [spec for spec in _dev_group(repo_root) if not PINNED.match(spec)]
    assert unpinned == []


def test_ci_installs_pinned_tools_only(repo_root: Path) -> None:
    for path in (repo_root / ".github").rglob("*.y*ml"):
        for spec in re.findall(r"pip install ([^\n]+)", path.read_text(encoding="utf-8")):
            for package in re.findall(r"\"([^\"]+)\"", spec):
                if package.startswith("pip"):
                    continue  # the installer itself: a floor is enough (``--group`` needs 25.1)
                assert PINNED.match(package), f"{path.name}: {package}"


def test_pre_commit_ruff_matches_the_dev_group(repo_root: Path) -> None:
    config = yaml.safe_load((repo_root / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    [ruff_repo] = [r for r in config["repos"] if r["repo"].endswith("ruff-pre-commit")]
    [ruff_spec] = [s for s in _dev_group(repo_root) if s.startswith("ruff==")]
    assert ruff_repo["rev"] == "v" + ruff_spec.removeprefix("ruff==")
