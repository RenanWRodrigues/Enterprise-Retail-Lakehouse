"""Text sources are read with an explicit schema (CLAUDE.md, PySpark rules).

Without one, Spark infers it with a full extra pass over the data (``samplingRatio`` defaults to
1.0). Regression: the IoT generator re-read ~32 million gzip JSON readings twice just to count
them (docs/ADR/0001-spark-without-cache.md).
"""

import re
from pathlib import Path

TEXT_READ = re.compile(r"spark\.read\.(json|csv|text)\(")


def test_production_text_reads_declare_a_schema(repo_root: Path) -> None:
    offenders = [
        f"{path.relative_to(repo_root).as_posix()}:{number}"
        for pattern in ("projects/*/src/**/*.py", "shared/src/lakehouse_common/**/*.py")
        for path in sorted(repo_root.glob(pattern))
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if TEXT_READ.search(line)  # `spark.read.schema(...).json(...)` does not match
    ]
    assert offenders == []
