"""One query over the three projects: ``<ops>.pipeline_runs`` plus the per-layer history.

Legacy per-layer ``pipeline_runs`` tables are read through the standard columns; a run present
in both (written by the fan-out sink) appears once, from the ops record.
"""

from datetime import UTC, datetime

from pyspark.sql import SparkSession

from lakehouse_common.observability import Layer, PipelineRun, RunStatus, runs_to_dataframe
from lakehouse_common.observability.ops import (
    OPS_RUNS_SCHEMA,
    consolidated_runs,
    ops_runs_to_dataframe,
)

AT = datetime(2026, 10, 12, 12, tzinfo=UTC)


def _run(
    project: str, run_id: str, table: str, status: RunStatus, error: str | None = None
) -> PipelineRun:
    return PipelineRun(
        f"{project}_silver", run_id, project, Layer.SILVER, table, AT, AT, 1.25,
        input_rows=5, output_rows=4, rejected_rows=1, duplicate_rows=0,
        status=status, error_message=error,
    )  # fmt: skip


def test_one_consolidated_view_of_the_three_projects(spark: SparkSession) -> None:
    both = _run("retail", "r-new", "retail.silver.silver_orders", RunStatus.SUCCEEDED)
    ops = ops_runs_to_dataframe(spark, [both], environment="databricks")
    legacy = [
        (
            runs_to_dataframe(
                spark,
                [both, _run("retail", "r-old", "retail.silver.silver_orders", RunStatus.SUCCEEDED)],
            ),
            "databricks",
        ),
        (
            runs_to_dataframe(
                spark,
                [
                    _run(
                        "financial",
                        "f-1",
                        "financial.silver.obs",
                        RunStatus.FAILED,
                        "ValueError: bad",
                    )
                ],
            ),
            "databricks",
        ),
        (
            runs_to_dataframe(
                spark, [_run("iot", "i-1", "iot.silver.readings", RunStatus.SUCCEEDED_WITH_ALERTS)]
            ),
            "databricks",
        ),
    ]

    result = consolidated_runs(ops, legacy)
    rows = {(r["run_id"], r["source_name"]): r for r in result.collect()}

    assert result.schema == OPS_RUNS_SCHEMA
    assert sorted(rows) == [
        ("f-1", "financial.silver.obs"), ("i-1", "iot.silver.readings"),
        ("r-new", "retail.silver.silver_orders"), ("r-old", "retail.silver.silver_orders"),
    ]  # fmt: skip  # r-new once, although it is in both
    assert {r["project"] for r in rows.values()} == {"retail", "financial", "iot"}
    failed = rows[("f-1", "financial.silver.obs")]
    assert (failed["status"], failed["error_type"], failed["records_valid"]) == (
        "FAILED",
        "ValueError",
        4,
    )
    assert rows[("r-old", "retail.silver.silver_orders")]["duration_ms"] == 1250.0


def test_history_without_an_ops_table(spark: SparkSession) -> None:
    legacy = [
        (runs_to_dataframe(spark, [_run("iot", "i-1", "iot.gold.x", RunStatus.SUCCEEDED)]), "local")
    ]

    [row] = consolidated_runs(None, legacy).collect()

    assert (row["run_id"], row["environment"], row["layer"]) == ("i-1", "local", "silver")
