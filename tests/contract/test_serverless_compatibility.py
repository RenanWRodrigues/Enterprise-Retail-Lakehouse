"""Production code must run on Databricks Free Edition, which is serverless only.

Serverless rejects the DataFrame and SQL cache APIs with an exception ("Dataframe and SQL cache
APIs are not supported on serverless compute", Databricks docs, serverless limitations). Local
Spark accepts them, so a call would pass every test and only fail on the platform: the Financial
Silver run called ``.cache()`` three times (regression, docs/ADR/0001).

Tests may cache (local Spark); production code (``src/``, notebooks and
``shared/src/lakehouse_common``) may not.
"""

import re
from pathlib import Path

PRODUCTION = (
    "projects/*/src/**/*.py",
    "projects/*/notebooks/**/*.py",
    "shared/src/lakehouse_common/**/*.py",
    "shared/notebooks/*.py",
)
FORBIDDEN = re.compile(
    r"\.(cache|persist|unpersist|checkpoint|localCheckpoint)\(\)"
    r"|\.(cacheTable|uncacheTable|clearCache)\("
    r"|\b(CACHE|UNCACHE)\s+TABLE\b|\bCLEAR\s+CACHE\b"
)


def test_production_code_uses_no_cache_api(repo_root: Path) -> None:
    offenders = [
        f"{path.relative_to(repo_root).as_posix()}:{number}: {line.strip()}"
        for pattern in PRODUCTION
        for path in sorted(repo_root.glob(pattern))
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if FORBIDDEN.search(line.split("#", 1)[0])
    ]
    assert offenders == []


def test_the_pattern_catches_every_rejected_api() -> None:
    rejected = [
        "df.cache()",
        "df.persist()",
        "df.unpersist()",
        "df.checkpoint()",
        "df.localCheckpoint()",
        "spark.catalog.cacheTable('t')",
        'spark.sql("CACHE TABLE t")',
        'spark.sql("CLEAR CACHE")',
    ]
    assert all(FORBIDDEN.search(code) for code in rejected)
    assert not FORBIDDEN.search("F.coalesce(F.col('a'), F.lit(0))")
