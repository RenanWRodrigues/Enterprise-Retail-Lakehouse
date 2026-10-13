"""Notebooks import the installed packages: a ``%pip install`` of the wheel, never ``sys.path``.

History: notebooks used to put ``projects/<p>/src`` and ``shared/src`` on ``sys.path`` (a
notebook that forgot ``shared/src`` failed with ModuleNotFoundError on Databricks). Since the
packages are installable (docs/ADR/0005-installable-packages.md), every notebook installs the
project wheel from the libs Volume, restarts Python, and only then imports.
"""

import re
import tomllib
from pathlib import Path

import pytest

NOTEBOOKS = sorted(
    [*Path("projects").glob("*/notebooks/**/*.py"), *Path("shared").glob("notebooks/*.py")]
)
OWN_IMPORT = re.compile(
    r"^(?:from|import) (lakehouse_common|retail_lakehouse|iot_lakehouse|financial_lakehouse)\b",
    re.MULTILINE,
)
WHEEL_VOLUME = "/Volumes/workspace/libs/wheels"


def _wheel(repo_root: Path) -> str:
    project = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    distribution = re.sub(r"[-_.]+", "_", project["name"]).lower()
    return f"{WHEEL_VOLUME}/{distribution}-{project['version']}-py3-none-any.whl"


@pytest.mark.parametrize("notebook", NOTEBOOKS, ids=lambda p: p.as_posix())
def test_notebooks_never_touch_sys_path(repo_root: Path, notebook: Path) -> None:
    assert "sys.path" not in (repo_root / notebook).read_text(encoding="utf-8")


@pytest.mark.parametrize("notebook", NOTEBOOKS, ids=lambda p: p.as_posix())
def test_notebooks_install_the_current_wheel_before_importing(
    repo_root: Path, notebook: Path
) -> None:
    text = (repo_root / notebook).read_text(encoding="utf-8")
    first_import = OWN_IMPORT.search(text)
    if first_import is None:
        pytest.skip("the notebook imports no project package (a runner of other notebooks)")
    install = f"# MAGIC %pip install --quiet --no-deps {_wheel(repo_root)}"

    assert install in text, f"{notebook}: install the wheel of the current version"
    restart = text.index("dbutils.library.restartPython()")
    assert text.index(install) < restart < first_import.start()


def test_documented_pythonpath_includes_shared_sources(repo_root: Path) -> None:
    """Commands documented with PYTHONPATH (local runs without install) keep shared/src."""
    offenders = []
    documented = [
        *repo_root.glob("projects/*/docs/*.md"),
        *repo_root.glob("projects/*/src/**/*.py"),
    ]
    for path in documented:
        for line in path.read_text(encoding="utf-8").splitlines():
            match = re.search(r"PYTHONPATH=(\S+)", line)
            project_only = (
                match is not None
                and re.search(r"projects/[\w-]+/src", match.group(1)) is not None
                and "shared/src" not in match.group(1)
            )
            if project_only:
                offenders.append(f"{path.relative_to(repo_root)}: {line.strip()}")
    assert offenders == []
