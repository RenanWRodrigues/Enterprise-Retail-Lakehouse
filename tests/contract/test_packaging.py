"""One installable distribution with exactly the runtime packages (ADR 0005)."""

import re
import tomllib
from pathlib import Path

TEST_ONLY = {"lakehouse_testing"}  # test infrastructure: never in the wheel


def _pyproject(repo_root: Path) -> dict[str, object]:
    return tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))


def test_the_build_backend_is_pinned(repo_root: Path) -> None:
    build = _pyproject(repo_root)["build-system"]

    assert build["build-backend"] == "hatchling.build"  # type: ignore[index]
    assert all(re.fullmatch(r"[\w.-]+==[\w.]+", r) for r in build["requires"])  # type: ignore[index]


def test_the_wheel_ships_every_runtime_package_and_nothing_else(repo_root: Path) -> None:
    listed = _pyproject(repo_root)["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]  # type: ignore[index]
    runtime = {
        path.relative_to(repo_root).as_posix()
        for path in [*repo_root.glob("shared/src/*"), *repo_root.glob("projects/*/src/*")]
        if (path / "__init__.py").exists() and path.name not in TEST_ONLY
    }

    assert set(listed) == runtime


def test_the_runtime_needs_nothing_from_the_dev_group(repo_root: Path) -> None:
    """Databricks installs the wheel with --no-deps: PySpark and Delta come from the runtime."""
    assert _pyproject(repo_root)["project"]["dependencies"] == []  # type: ignore[index]
