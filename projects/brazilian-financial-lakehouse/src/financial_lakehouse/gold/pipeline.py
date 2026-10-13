"""Gold run (I/O), observed: one step for the whole set of tables.

Contract and grain of *every* table are checked (CRITICAL) before *any* table is replaced; the
run and its check results go to ``pipeline_runs`` / ``data_quality_results``.
"""

from datetime import UTC, datetime
from typing import Final

from pyspark.sql import SparkSession

from financial_lakehouse import PROJECT
from financial_lakehouse.gold.build import build_gold, write_gold
from financial_lakehouse.quality.suites import gold_suite
from lakehouse_common.naming import validate_table_name
from lakehouse_common.observability import (
    DeltaObservabilitySink,
    Layer,
    ObservabilitySink,
    PipelineRun,
    new_run_id,
    track_run,
)
from lakehouse_common.quality import run_suite

PIPELINE_NAME: Final = "financial_gold"


def run_gold(
    spark: SparkSession,
    *,
    silver_table: str,
    gold_namespace: str,
    now: datetime | None = None,
    sink: ObservabilitySink | None = None,
) -> PipelineRun:
    """Build, check (CRITICAL stops before writing) and replace every Gold table."""
    started = (now or datetime.now(UTC)).astimezone(UTC)
    observer = sink or DeltaObservabilitySink(spark, gold_namespace)
    with track_run(
        pipeline_name=PIPELINE_NAME,
        run_id=new_run_id(started),
        project=PROJECT,
        layer=Layer.GOLD,
        table=f"{validate_table_name(gold_namespace)}.*",
        sink=observer,
    ) as step:
        silver = spark.table(validate_table_name(silver_table))
        tables = build_gold(silver)
        results = run_suite(gold_suite(tables), execution_timestamp=started)
        step.record_quality(results)  # CRITICAL raises here, before anything is replaced
        # The suite already counted every table in its single pass (rows_checked): counting them
        # again would recompute each Gold plan once more (docs/ADR/0001-spark-without-cache.md).
        rows_by_table = {result.table_name: result.rows_checked for result in results}
        step.record_counts(input_rows=silver.count(), output_rows=sum(rows_by_table.values()))
        write_gold(tables, gold_namespace)
    return step.run
