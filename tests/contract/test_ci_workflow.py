"""CI governance: no credentials, read-only token, the documented stage order, and that every CI
step is a real Makefile target (the commands developers run locally).
"""

import re
import tomllib
from itertools import pairwise
from pathlib import Path
from typing import Any

import pytest
import yaml  # type: ignore[import-untyped]  # no stubs needed for safe_load

GITHUB = Path(".github")
REQUESTED_TARGETS = {
    "install",
    "lint",
    "test",
    "test-unit",
    "test-quality",
    "test-integration",
    "coverage",
}
CHAIN = ("lint", "unit", "transformation", "data-quality", "coverage")


@pytest.fixture(scope="module")
def ci(repo_root: Path) -> dict[str, Any]:
    document: dict[str, Any] = yaml.safe_load(
        (repo_root / GITHUB / "workflows" / "ci.yml").read_text(encoding="utf-8")
    )
    return document


@pytest.fixture(scope="module")
def makefile(repo_root: Path) -> str:
    return (repo_root / "Makefile").read_text(encoding="utf-8")


def _targets(makefile: str) -> set[str]:
    return set(re.findall(r"^([a-z][a-z-]*):(?!=)", makefile, flags=re.MULTILINE))


def _recipe(makefile: str, target: str) -> str:
    match = re.search(rf"^{target}:[^\n]*\n((?:\t[^\n]*\n)+)", makefile, flags=re.MULTILINE)
    assert match, f"no recipe for {target}"
    return match.group(1)


def _runs(job: dict[str, Any]) -> str:
    return " ".join(step.get("run", "") for step in job["steps"])


def _needs(job: dict[str, Any]) -> set[str]:
    needs = job.get("needs", [])
    return {needs} if isinstance(needs, str) else set(needs)


def test_github_files_use_no_secrets_or_credentials(repo_root: Path) -> None:
    for path in (repo_root / GITHUB).rglob("*.y*ml"):
        text = path.read_text(encoding="utf-8")
        assert "secrets." not in text, path
        assert not re.search(r"(?i)(token|password|api[_-]?key)\s*:\s*\S", text), path


def test_workflow_token_is_read_only(ci: dict[str, Any]) -> None:
    assert ci["permissions"] == {"contents": "read"}


def test_stages_run_in_the_documented_order(ci: dict[str, Any]) -> None:
    jobs = ci["jobs"]
    assert set(jobs) == {*CHAIN, "integration"}
    for previous, current in pairwise(CHAIN):
        assert previous in _needs(jobs[current]), (current, "must wait for", previous)
    assert _needs(jobs["integration"]) == {"lint"}  # separate from the main chain


def test_every_ci_step_is_a_makefile_target(
    ci: dict[str, Any], makefile: str, repo_root: Path
) -> None:
    targets = _targets(makefile)
    used = set()
    for job in ci["jobs"].values():
        used |= set(re.findall(r"\bmake ([a-z][a-z-]*)", _runs(job)))
    assert used, "CI must call the Makefile"
    assert used <= targets
    install = (repo_root / GITHUB / "actions" / "setup" / "action.yml").read_text(encoding="utf-8")
    assert "make install" in install


def test_requested_make_commands_exist(makefile: str) -> None:
    assert _targets(makefile) >= REQUESTED_TARGETS


def test_unit_tests_need_no_jvm(ci: dict[str, Any], makefile: str) -> None:
    unit = ci["jobs"]["unit"]
    assert "make test-unit" in _runs(unit)
    assert "java" not in str(unit)
    assert "-m unit" in _recipe(makefile, "test-unit")


def test_make_test_targets_use_declared_markers(makefile: str, repo_root: Path) -> None:
    config = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    declared = {m.split(":")[0] for m in config["tool"]["pytest"]["ini_options"]["markers"]}
    used: set[str] = set()
    for target in ("test", "test-unit", "test-transformation", "test-quality", "test-integration"):
        [expression] = re.findall(r'-m ("[^"]+"|\S+)', _recipe(makefile, target))
        used |= set(re.findall(r"[a-z_]+", expression)) - {"or", "and", "not"}
    assert used == declared  # every category runs somewhere, nothing undeclared
