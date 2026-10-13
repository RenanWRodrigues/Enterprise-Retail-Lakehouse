"""Gold publication: stage every table, gate them all, only then replace the published tables.

1. **Stage**: each candidate table is written to ``<gold>_staging.<table>`` (Delta). The gate and
   the publication read these materialized tables, so what is published is exactly what was
   validated (no cache on serverless: re-running the build plan could differ only by timing).
2. **Gate**: ``retail_gold_suite`` on all staged tables, one ``pipeline_runs`` row for the step.
   CRITICAL raises before anything is published: the Gold tables keep their previous version,
   the run is ``FAILED`` with the checks in ``data_quality_results`` and a CRITICAL alert, and
   the staged tables stay for inspection (replaced by the next run).
3. **Publish**: each table is replaced (one Delta transaction per table), dimensions before
   facts, then commented with its grain; one ``pipeline_runs`` row per table.

Delta has no transaction across tables: an infrastructure failure *during* step 3 leaves some
tables already replaced. Dimensions go first so a published fact never references a key that
is missing from the published dimensions; re-running republishes everything (idempotent).
docs/DIMENSIONAL_MODEL.md §8.
"""

import logging
from collections.abc import Mapping
from datetime import UTC, datetime

from pyspark.sql import DataFrame

from lakehouse_common.observability import Layer, ObservabilitySink, RunTracker, track_run
from lakehouse_common.quality import CheckResult, CriticalQualityError, run_suite
from retail_lakehouse import PROJECT
from retail_lakehouse.gold.build import GOLD_PIPELINE_NAME, GoldTarget
from retail_lakehouse.gold.schemas import GOLD_SCHEMAS, TABLE_COMMENTS
from retail_lakehouse.quality.suites import retail_gold_suite

logger = logging.getLogger(__name__)


def stage_gold(tables: Mapping[str, DataFrame], target: GoldTarget) -> dict[str, DataFrame]:
    """Write every candidate table to the staging schema; return them read back from Delta."""
    missing = sorted(set(GOLD_SCHEMAS) - set(tables))
    if missing:
        raise ValueError(f"missing Gold tables: {missing}")
    spark = next(iter(tables.values())).sparkSession
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target.staging_namespace}")
    for name in GOLD_SCHEMAS:
        tables[name].writeTo(target.staging_table(name)).using("delta").createOrReplace()
    return {name: spark.table(target.staging_table(name)) for name in GOLD_SCHEMAS}


def _record_gate(step: RunTracker, results: list[CheckResult]) -> None:
    """Counts first (kept on failure), then the policy: CRITICAL raises here."""
    rows_by_table = {  # Gold tables only: the R3 reconciliation is a gate input, not output
        result.table_name: result.rows_checked
        for result in results
        if result.table_name in GOLD_SCHEMAS
    }
    step.record_counts(input_rows=sum(rows_by_table.values()))
    step.record_quality(results)
    step.record_counts(output_rows=sum(rows_by_table.values()), rejected_rows=0)


def validate_gold(  # noqa: PLR0913 - keyword-only after `target`
    tables: Mapping[str, DataFrame],
    target: GoldTarget,
    *,
    run_id: str,
    sink: ObservabilitySink,
    execution_timestamp: datetime,
    inventory_reconciliation: DataFrame | None = None,
) -> dict[str, DataFrame]:
    """Stage and gate every table (one observed step); return the staged tables if they pass.

    ``inventory_reconciliation`` (``gold.inventory``) adds R3 to the gate. Raises
    ``CriticalQualityError`` (run ``FAILED``) when any CRITICAL check fails.
    """
    with track_run(
        pipeline_name=GOLD_PIPELINE_NAME,
        run_id=run_id,
        project=PROJECT,
        layer=Layer.GOLD,
        table=f"{target.staging_namespace}.*",
        sink=sink,
    ) as step:
        staged = stage_gold(tables, target)
        suite = retail_gold_suite(staged, inventory_reconciliation=inventory_reconciliation)
        results = run_suite(suite, execution_timestamp=execution_timestamp)
        try:
            _record_gate(step, results)
        except CriticalQualityError:
            logger.error(
                "gold run %s blocked by the quality gate: nothing published to %s; "
                "candidate tables kept in %s",
                run_id,
                target.namespace,
                target.staging_namespace,
            )
            raise
    return staged


def _publish_table(
    frame: DataFrame, name: str, target: GoldTarget, *, run_id: str, sink: ObservabilitySink
) -> None:
    spark, table = frame.sparkSession, target.table(name)
    with track_run(
        pipeline_name=GOLD_PIPELINE_NAME,
        run_id=run_id,
        project=PROJECT,
        layer=Layer.GOLD,
        table=table,
        sink=sink,
    ) as step:
        frame.writeTo(table).using("delta").createOrReplace()
        spark.sql(f"COMMENT ON TABLE {table} IS '{TABLE_COMMENTS[name]}'")
        step.record_counts(output_rows=spark.table(table).count(), rejected_rows=0)
    logger.info("gold %s published", table)


def write_gold(  # noqa: PLR0913 - keyword-only after `target`
    tables: Mapping[str, DataFrame],
    target: GoldTarget,
    *,
    run_id: str,
    sink: ObservabilitySink,
    now: datetime | None = None,
    inventory_reconciliation: DataFrame | None = None,
) -> None:
    """Publish the Gold model only if every table passes the gate (see the module docstring).

    ``inventory_reconciliation`` (R3, ``gold.inventory.inventory_reconciliation`` over Silver)
    is gated with the tables; production passes it (notebooks/gold/01_build_gold.py).

    Effects: replaces the staging tables always; replaces the Gold tables only on success.
    Raises ``CriticalQualityError`` (nothing published) or the original write error.
    """
    execution_timestamp = (now or datetime.now(UTC)).astimezone(UTC)
    staged = validate_gold(
        tables,
        target,
        run_id=run_id,
        sink=sink,
        execution_timestamp=execution_timestamp,
        inventory_reconciliation=inventory_reconciliation,
    )
    spark = next(iter(staged.values())).sparkSession
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target.namespace}")
    for name in GOLD_SCHEMAS:  # dimensions first
        _publish_table(staged[name], name, target, run_id=run_id, sink=sink)
