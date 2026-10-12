"""Contract for the monorepo layout defined in docs/ARCHITECTURE.md §3.

Guards the directory skeleton, the package import configuration, the project
README template and the "no premature shared abstractions" rule.
"""

import importlib
from pathlib import Path

import pytest

PROJECTS: dict[str, str] = {
    "enterprise-retail-lakehouse": "retail_lakehouse",
    "industrial-iot-lakehouse": "iot_lakehouse",
    "brazilian-financial-lakehouse": "financial_lakehouse",
}
PROJECT_DIRS = (
    "config",
    "notebooks/bronze",
    "notebooks/silver",
    "notebooks/gold",
    "sql",
    "tests/unit",
    "tests/transformation",
    "tests/data_quality",
    "tests/integration",
    "docs",
)
PACKAGE_SUBPACKAGES = ("ingestion", "bronze", "silver", "gold", "quality", "utils")
SHARED_PACKAGE = "lakehouse_common"
SHARED_SUBPACKAGES = ("logging", "quality", "spark", "observability")
PROJECT_README_SECTIONS = (
    "## Problem",
    "## Architecture",
    "## Data",
    "## Medallion Layers",
    "## Testing",
    "## Status",
    "## Roadmap",
)
SOURCE_ROOTS = ("shared/src", *(f"projects/{project}/src" for project in PROJECTS))


@pytest.mark.parametrize("project", PROJECTS)
@pytest.mark.parametrize("relative_dir", PROJECT_DIRS)
def test_project_directory_exists(repo_root: Path, project: str, relative_dir: str) -> None:
    assert (repo_root / "projects" / project / relative_dir).is_dir()


@pytest.mark.parametrize(("project", "package"), PROJECTS.items())
@pytest.mark.parametrize("subpackage", PACKAGE_SUBPACKAGES)
def test_project_package_layout(
    repo_root: Path, project: str, package: str, subpackage: str
) -> None:
    assert (repo_root / "projects" / project / "src" / package / subpackage).is_dir()


@pytest.mark.parametrize("subpackage", SHARED_SUBPACKAGES)
def test_shared_package_layout(repo_root: Path, subpackage: str) -> None:
    assert (repo_root / "shared" / "src" / SHARED_PACKAGE / subpackage).is_dir()


def test_shared_tests_directory_exists(repo_root: Path) -> None:
    assert (repo_root / "shared" / "tests").is_dir()


@pytest.mark.parametrize("package", [*PROJECTS.values(), SHARED_PACKAGE])
def test_package_is_importable(package: str) -> None:
    """Imports are configured once (pytest pythonpath / mypy_path), not per test."""
    module = importlib.import_module(package)
    assert module.__doc__, f"{package}/__init__.py must document the package"


@pytest.mark.parametrize("project", PROJECTS)
@pytest.mark.parametrize("section", PROJECT_README_SECTIONS)
def test_project_readme_has_section(repo_root: Path, project: str, section: str) -> None:
    readme = (repo_root / "projects" / project / "README.md").read_text(encoding="utf-8")
    assert section in readme.splitlines()


@pytest.mark.parametrize("source_root", SOURCE_ROOTS)
def test_no_empty_python_files(repo_root: Path, source_root: str) -> None:
    assert (repo_root / source_root).is_dir()
    empty = [
        str(path.relative_to(repo_root))
        for path in (repo_root / source_root).rglob("*.py")
        if not path.read_text(encoding="utf-8").strip()
    ]
    assert not empty, f"empty Python files are not allowed: {empty}"


PRODUCTION_ROOTS = (
    "shared/src/lakehouse_common",
    *(f"projects/{project}/src" for project in PROJECTS),
)


@pytest.mark.parametrize("production_root", PRODUCTION_ROOTS)
def test_production_code_never_imports_test_infrastructure(
    repo_root: Path, production_root: str
) -> None:
    offenders = [
        str(path.relative_to(repo_root))
        for path in (repo_root / production_root).rglob("*.py")
        if "lakehouse_testing" in path.read_text(encoding="utf-8")
    ]
    assert not offenders, f"lakehouse_testing is test-only: {offenders}"


def test_architecture_doc_contains_mermaid_overview(repo_root: Path) -> None:
    architecture = (repo_root / "docs" / "ARCHITECTURE.md").read_text(encoding="utf-8")
    assert "```mermaid" in architecture
    for node in ("Sources", "Ingestion", "Retail", "IoT", "Financial", "Analytics"):
        assert node in architecture
