"""Every run id comes from ``lakehouse_common.observability.new_run_id``.

Regression: pipelines built ``run_id = f"{started:%Y%m%dT%H%M%SZ}"``, so two runs started in the
same second shared an id, and a run that rebuilt its id in two places was safe only by accident.
"""

import re
from pathlib import Path

SECONDS_ONLY_ID = re.compile(r"%Y%m%dT%H%M%SZ")
ALLOWED = {"shared/src/lakehouse_common/observability/runs.py"}  # new_run_id itself


def _production_sources(repo_root: Path) -> list[Path]:
    roots = [repo_root / "shared" / "src", repo_root / "shared" / "notebooks"]
    for project in (repo_root / "projects").iterdir():
        roots += [project / "src", project / "notebooks"]
    return [path for root in roots if root.exists() for path in root.rglob("*.py")]


def test_no_run_id_is_built_from_the_clock_alone(repo_root: Path) -> None:
    offenders = [
        f"{path.relative_to(repo_root).as_posix()}:{number}"
        for path in _production_sources(repo_root)
        if path.relative_to(repo_root).as_posix() not in ALLOWED
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if SECONDS_ONLY_ID.search(line)
    ]

    assert offenders == []
