"""The data_quality_results schema is a contract documented in docs/DATA_QUALITY.md §6."""

import re
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.testing import assertSchemaEqual

from lakehouse_common.quality import (
    DQ_RESULTS_SCHEMA,
    CheckResult,
    Dimension,
    Severity,
    Status,
    results_to_dataframe,
)

EXECUTED_AT = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)


def _documented_columns(repo_root: Path) -> list[str]:
    doc = (repo_root / "docs" / "DATA_QUALITY.md").read_text(encoding="utf-8")
    match = re.search(r"^`(run_id,[^`]+)`$", doc, flags=re.MULTILINE)
    assert match, "data_quality_results column list not found in docs/DATA_QUALITY.md"
    return [column.strip() for column in match.group(1).split(",")]


def test_schema_matches_documented_columns(repo_root: Path) -> None:
    assert DQ_RESULTS_SCHEMA.fieldNames() == _documented_columns(repo_root)


def test_results_to_dataframe_follows_contract(spark: SparkSession) -> None:
    # Arrange
    results = [
        CheckResult(
            check_name="unique__order_id",
            check_type="unique",
            table_name="silver.orders",
            column_name="order_id",
            dimension=Dimension.UNIQUENESS,
            status=Status.ERROR,
            severity=Severity.ERROR,
            rows_checked=4,
            failed_rows=1,
            failure_rate=0.25,
            execution_timestamp=EXECUTED_AT,
        )
    ]

    # Act
    frame = results_to_dataframe(spark, results, run_id="run-1")

    # Assert
    assertSchemaEqual(frame.schema, DQ_RESULTS_SCHEMA)
    [row] = frame.withColumn(  # timestamp rendered by Spark in UTC
        "execution_timestamp", F.date_format("execution_timestamp", "yyyy-MM-dd HH:mm:ss")
    ).collect()
    assert row.asDict() == {
        "run_id": "run-1",
        "table_name": "silver.orders",
        "check_name": "unique__order_id",
        "check_type": "unique",
        "column_name": "order_id",
        "dimension": "uniqueness",
        "severity": "ERROR",
        "status": "ERROR",
        "rows_checked": 4,
        "failed_rows": 1,
        "failure_rate": 0.25,
        "execution_timestamp": "2026-01-15 12:00:00",
        "details": None,
    }
