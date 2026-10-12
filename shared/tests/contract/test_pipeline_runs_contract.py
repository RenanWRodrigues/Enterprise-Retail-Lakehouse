"""The pipeline_runs schema is a contract documented in docs/OBSERVABILITY.md."""

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.testing import assertSchemaEqual

from lakehouse_common.observability import (
    PIPELINE_RUNS_SCHEMA,
    Layer,
    PipelineRun,
    RunStatus,
    runs_to_dataframe,
)
from lakehouse_common.quality import CheckResult, Dimension, Severity, Status

START = datetime(2026, 10, 6, 12, tzinfo=UTC)


def _documented_columns(repo_root: Path) -> list[str]:
    doc = (repo_root / "docs" / "OBSERVABILITY.md").read_text(encoding="utf-8")
    match = re.search(r"^`(pipeline_name,[^`]+)`$", doc, flags=re.MULTILINE)
    assert match, "pipeline_runs column list not found in docs/OBSERVABILITY.md"
    return [column.strip() for column in match.group(1).split(",")]


def test_schema_matches_documented_columns(repo_root: Path) -> None:
    assert PIPELINE_RUNS_SCHEMA.fieldNames() == _documented_columns(repo_root)


def test_requested_run_metrics_are_all_present() -> None:
    requested = {
        "pipeline_name",
        "run_id",
        "project",
        "layer",
        "table",
        "started_at",
        "finished_at",
        "duration_seconds",
        "input_rows",
        "output_rows",
        "rejected_rows",
        "duplicate_rows",
        "status",
        "error_message",
        "completeness",
        "uniqueness",
        "validity",
        "referential_integrity",
    }
    assert requested <= set(PIPELINE_RUNS_SCHEMA.fieldNames())


def test_runs_to_dataframe_follows_contract(spark: SparkSession) -> None:
    result = CheckResult(
        check_name="unique__order_id",
        check_type="unique",
        table_name="silver_orders",
        column_name="order_id",
        dimension=Dimension.UNIQUENESS,
        status=Status.WARNING,
        severity=Severity.WARNING,
        rows_checked=4,
        failed_rows=1,
        failure_rate=0.25,
        execution_timestamp=START,
    )
    run = PipelineRun(
        "retail_silver",
        "R1",
        "retail",
        Layer.SILVER,
        "silver_orders",
        START,
        finished_at=START + timedelta(seconds=2),
        duration_seconds=2.0,
        input_rows=4,
        output_rows=4,
        rejected_rows=0,
        duplicate_rows=0,
        status=RunStatus.SUCCEEDED_WITH_ALERTS,
        quality=[result],
    )

    frame = runs_to_dataframe(spark, [run])

    assertSchemaEqual(frame.schema, PIPELINE_RUNS_SCHEMA)
    [row] = frame.collect()
    assert (row.layer, row.status, row.uniqueness, row.validity, row.checks_failed) == (
        "silver",
        "SUCCEEDED_WITH_ALERTS",
        0.75,
        None,
        1,
    )
