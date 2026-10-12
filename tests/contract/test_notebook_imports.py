"""Notebooks put every source root their packages need on sys.path.

Regression: project packages import ``lakehouse_common`` (naming, config, quality,
observability), but some notebooks only added ``projects/<p>/src`` and failed with
ModuleNotFoundError on Databricks. Same for the documented ``PYTHONPATH`` commands.
"""

import re
from pathlib import Path

import pytest

NOTEBOOKS = sorted(Path("projects").glob("*/notebooks/**/*.py"))
SHARED_IMPORT = re.compile(r"^\s*(from|import) lakehouse_common", re.MULTILINE)


def _uses_shared(project: Path, repo_root: Path) -> bool:
    sources = (repo_root / project / "src").rglob("*.py")
    return any(SHARED_IMPORT.search(path.read_text(encoding="utf-8")) for path in sources)


@pytest.mark.parametrize("notebook", NOTEBOOKS, ids=lambda p: p.as_posix())
def test_notebooks_add_shared_sources_when_the_project_needs_them(
    repo_root: Path, notebook: Path
) -> None:
    project = Path(*notebook.parts[:2])
    text = (repo_root / notebook).read_text(encoding="utf-8")
    if "sys.path" not in text or not _uses_shared(project, repo_root):
        pytest.skip("notebook does not set sys.path or project does not use shared code")
    assert '"shared"' in text and '"src"' in text, f"{notebook} must add shared/src to sys.path"


def test_documented_pythonpath_includes_shared_sources(repo_root: Path) -> None:
    offenders = []
    documented = [
        *repo_root.glob("projects/*/docs/*.md"),
        *repo_root.glob("projects/*/src/**/*.py"),
    ]
    for path in documented:
        for line in path.read_text(encoding="utf-8").splitlines():
            match = re.search(r"PYTHONPATH=(\S+)", line)
            if not match:
                continue
            project = re.search(r"projects/([\w-]+)/src", match.group(1))
            needs_shared = project and _uses_shared(Path("projects", project.group(1)), repo_root)
            if needs_shared and "shared/src" not in match.group(1):
                offenders.append(f"{path.relative_to(repo_root)}: {line.strip()}")
    assert offenders == []
