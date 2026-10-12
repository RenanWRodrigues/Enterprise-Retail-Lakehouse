"""Silver run (I/O): consume new Bronze commits, standardize, quality gate, MERGE — observed.

- progress (``silver.progress``): a durable checkpoint holds the last Bronze version that was
  published and reconciled; the run captures the latest version once and consumes only the
  commits ``(checkpoint, latest]`` through the Change Data Feed. The checkpoint advances after
  the MERGEs and the reconciliation, so a failure anywhere before it repeats the same commits;
- input: every Bronze version of the keys those commits touch (``versions_of_touched_keys``),
  read ``VERSION AS OF`` the captured version: commits written meanwhile wait for the next run;
- reconciliation: ``bronze_consumed = valid + quarantine + duplicate + non_closing_bulletin +
  superseded_revision + redundant_latest_version`` (CRITICAL); the breakdown, the Bronze
  version range and the Bronze run ids consumed go to ``silver_reconciliation`` before the gate;
- quality: Bronze schema and the Silver contract are CRITICAL and stop the run before anything
  is written. Alerts are raised once per problem: row rules on Bronze rows not yet quarantined,
  plausibility on observations not yet in Silver, gaps next to keys this run inserts;
- output: ``MERGE`` on (indicator_id, reference_date) (same version: no-op) and insert-only
  quarantine by (record_key, reasons): repeating a commit range changes nothing;
- observability: one ``pipeline_runs`` row per run (also on failure) and every check result in
  ``data_quality_results`` (``lakehouse_common.observability``).

Guarantees and limits: docs/INCREMENTAL_STRATEGY.md, "Silver: progresso por versão Delta".
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from financial_lakehouse import PROJECT
from financial_lakehouse.quality.suites import (
    bronze_suite,
    silver_suite,
    silver_table_suite,
    standardization_suite,
)
from financial_lakehouse.silver.progress import (
    CHECKPOINT_TABLE,
    Checkpoint,
    CommitRange,
    ProgressError,
    changed_rows,
    commit_checkpoint,
    latest_version,
    plan_range,
    read_checkpoint,
    snapshot,
)
from financial_lakehouse.silver.schemas import BRONZE_SCHEMA, QUARANTINE_SCHEMA, SILVER_SCHEMA
from financial_lakehouse.silver.standardize import (
    DESTINATIONS,
    DUPLICATE,
    KEY,
    QUARANTINE,
    VALID,
    SilverResult,
    build_silver,
    catalog_frame,
    destination_counts,
    not_in,
    reconcile_silver,
    select_official_observations,
    versions_of_touched_keys,
    with_parsed_value,
)
from lakehouse_common.naming import validate_table_name
from lakehouse_common.observability import (
    DeltaObservabilitySink,
    Layer,
    ObservabilitySink,
    RunStatus,
    RunTracker,
    track_run,
)
from lakehouse_common.observability.runs import alert_logger
from lakehouse_common.quality import CheckResult, Reconciliation, run_suite

logger = logging.getLogger(__name__)
PIPELINE_NAME: Final = "financial_silver"
SILVER_TABLE: Final = "silver_indicator_observations"
QUARANTINE_TABLE: Final = "silver_indicator_observations_quarantine"
RECONCILIATION_TABLE: Final = "silver_reconciliation"
RECONCILIATION_SCHEMA: Final = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("execution_timestamp", TimestampType(), False),
        StructField("bronze_table", StringType(), False),
        StructField("from_version", LongType(), True),  # exclusive; NULL = whole table
        StructField("bronze_version", LongType(), False),  # inclusive end of the range
        StructField("bronze_run_ids", ArrayType(StringType()), False),
        StructField("bronze_rows", LongType(), False),
        StructField("destination", StringType(), False),
        StructField("rows", LongType(), False),
        StructField("balanced", BooleanType(), False),
    ]
)
"""One row per (run, destination): the reconciliation of the run and what it consumed."""


@dataclass(frozen=True, slots=True)
class SilverRunReport:
    run_id: str
    after_version: int | None  # checkpoint at the start (exclusive); None = first run
    bronze_version: int  # last Bronze version consumed (inclusive)
    bronze_rows_read: int
    silver_rows: int
    quarantined_rows: int
    duplicate_rows: int
    status: RunStatus
    quality: tuple[CheckResult, ...]
    destinations: dict[str, int]


@dataclass(frozen=True, slots=True)
class SilverTables:
    """Fully qualified names of everything a Silver run reads and writes."""

    bronze: str
    silver: str
    quarantine: str
    reconciliation: str
    checkpoint: str

    @classmethod
    def of(cls, bronze_table: str, namespace: str) -> "SilverTables":
        return cls(
            bronze=validate_table_name(bronze_table),
            silver=validate_table_name(f"{namespace}.{SILVER_TABLE}"),
            quarantine=validate_table_name(f"{namespace}.{QUARANTINE_TABLE}"),
            reconciliation=validate_table_name(f"{namespace}.{RECONCILIATION_TABLE}"),
            checkpoint=validate_table_name(f"{namespace}.{CHECKPOINT_TABLE}"),
        )


def read_bronze_json(spark: SparkSession, root: str | Path) -> DataFrame:
    """Local Bronze (``LocalBronzeSink`` JSON Lines) with the explicit Bronze schema.

    Files are listed in Python and passed explicitly: a Hadoop glob on Windows calls native
    code (winutils) and kills the JVM without HADOOP_HOME. ``FileNotFoundError`` if empty.
    """
    files = sorted(str(path) for path in Path(root).rglob("part-*.jsonl"))
    if not files:
        raise FileNotFoundError(f"no Bronze files (part-*.jsonl) under {root}")
    return spark.read.schema(BRONZE_SCHEMA).json(files)


@dataclass(frozen=True, slots=True)
class ConsumedBronze:
    """What a run read: the Bronze commit range, its rows and the Bronze runs they came from."""

    table: str
    commits: CommitRange
    rows: int
    run_ids: tuple[str, ...]


def consumed_bronze(bronze: DataFrame, table: str, commits: CommitRange) -> ConsumedBronze:
    """Row count and Bronze run ids of the consumed rows, in one aggregation."""
    totals = bronze.agg(
        F.count(F.lit(1)).alias("rows"), F.sort_array(F.collect_set("run_id")).alias("run_ids")
    ).first()
    rows, run_ids = (int(totals["rows"]), tuple(totals["run_ids"])) if totals else (0, ())
    return ConsumedBronze(table, commits, rows, run_ids)


@dataclass(frozen=True, slots=True)
class ReconciliationRecord:
    """The evidence of one run: what it consumed and where every row went."""

    run_id: str
    executed_at: datetime
    consumed: ConsumedBronze
    balance: Reconciliation

    def rows(self) -> list[tuple[object, ...]]:
        """``RECONCILIATION_SCHEMA`` rows, one per destination."""
        c = self.consumed
        return [
            (self.run_id, PIPELINE_NAME, self.executed_at, c.table, c.commits.after,
             c.commits.end, list(c.run_ids), c.rows, destination, count, self.balance.balanced)
            for destination, count in self.balance.destinations.items()
        ]  # fmt: skip


def _ddl(schema: StructType) -> str:
    return ", ".join(
        f"{f.name} {f.dataType.simpleString()}{'' if f.nullable else ' NOT NULL'}"
        for f in schema.fields
    )


def write_reconciliation(spark: SparkSession, table: str, record: ReconciliationRecord) -> None:
    """Append the run's breakdown (one row per destination); a log, like ``pipeline_runs``."""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({_ddl(RECONCILIATION_SCHEMA)}) USING DELTA")
    frame = spark.createDataFrame(record.rows(), RECONCILIATION_SCHEMA)
    frame.write.format("delta").mode("append").saveAsTable(table)


def merge_silver(silver: DataFrame, table: str) -> None:
    """Upsert by (indicator_id, reference_date); a row changes only if the winning version does."""
    spark = silver.sparkSession
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({_ddl(SILVER_SCHEMA)}) USING DELTA")
    silver.createOrReplaceTempView("_silver_incoming")
    spark.sql(
        f"MERGE INTO {table} AS t USING _silver_incoming AS s "
        "ON t.indicator_id = s.indicator_id AND t.reference_date = s.reference_date "
        "WHEN MATCHED AND t.record_key <> s.record_key THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def merge_quarantine(quarantine: DataFrame, table: str) -> None:
    """Insert-only by (record_key, reasons): reprocessing the same rejected rows adds nothing."""
    spark = quarantine.sparkSession
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({_ddl(QUARANTINE_SCHEMA)}) USING DELTA")
    quarantine.createOrReplaceTempView("_quarantine_incoming")
    spark.sql(
        f"MERGE INTO {table} AS t USING _quarantine_incoming AS s "
        "ON t.record_key = s.record_key "
        "AND array_join(t._dq_failed_checks, ',') = array_join(s._dq_failed_checks, ',') "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def _pinned(spark: SparkSession, table: str) -> DataFrame | None:
    """``table`` at its current version (``None`` if it does not exist yet)."""
    if not spark.catalog.tableExists(table):
        return None
    return snapshot(spark, table, latest_version(spark, table))


@dataclass(frozen=True)
class _Batch:
    """One consumed commit range, standardized and counted (nothing written yet)."""

    commits: CommitRange
    changed: DataFrame  # Bronze rows of the commits (first seen by this pipeline)
    bronze: DataFrame  # every version of the touched keys: the reconciled input
    result: SilverResult
    counts: dict[str, int]
    consumed: ConsumedBronze


def _standardize(
    spark: SparkSession, tables: SilverTables, commits: CommitRange, started: datetime
) -> _Batch:
    changed = changed_rows(spark, tables.bronze, commits, checkpoint_table=tables.checkpoint)
    bronze = versions_of_touched_keys(snapshot(spark, tables.bronze, commits.end), changed)
    result = build_silver(bronze, catalog_frame(spark), execution_timestamp=started)
    consumed = consumed_bronze(bronze, tables.bronze, commits)
    return _Batch(commits, changed, bronze, result, destination_counts(result), consumed)


def _gate(
    spark: SparkSession, tables: SilverTables, batch: _Batch, started: datetime
) -> list[CheckResult]:
    """CRITICAL checks on the whole batch; WARNING/ERROR only on problems seen for the first time.

    Event identity: (record_key, rule) for row rules and plausibility; a Bronze row already in
    quarantine, or an observation already in Silver, was reported when it arrived.
    """
    quarantined, published = _pinned(spark, tables.quarantine), _pinned(spark, tables.silver)
    fresh = not_in(batch.changed, quarantined, ["record_key"])
    candidates = with_parsed_value(select_official_observations(fresh), catalog_frame(spark))
    new_rows = not_in(batch.result.silver, published, ["record_key"])
    suites = [*standardization_suite(candidates), *silver_suite(batch.result.silver, new_rows)]
    balance = reconcile_silver(batch.consumed.rows, batch.counts)
    return [
        *run_suite(suites, execution_timestamp=started),
        balance.to_check_result(table_name=tables.silver, execution_timestamp=started),
    ]


def _publish(spark: SparkSession, tables: SilverTables, batch: _Batch) -> DataFrame:
    """MERGE Silver and quarantine; return the keys this run inserted (for the gap check)."""
    before = _pinned(spark, tables.silver)  # pinned: still the pre-MERGE state afterwards
    inserted = not_in(batch.result.silver.select(*KEY), before, list(KEY))
    merge_silver(batch.result.silver, tables.silver)
    merge_quarantine(batch.result.quarantine, tables.quarantine)
    return inserted


def _consume(
    spark: SparkSession,
    step: RunTracker,
    tables: SilverTables,
    commits: CommitRange,
    started: datetime,
) -> dict[str, int]:  # fmt: skip
    """Standardize, reconcile, gate, publish, then advance the checkpoint (in this order)."""
    batch = _standardize(spark, tables, commits, started)
    step.record_counts(
        input_rows=batch.consumed.rows,
        output_rows=batch.counts[VALID],
        rejected_rows=batch.counts[QUARANTINE],
        duplicate_rows=batch.counts[DUPLICATE],
    )
    balance = reconcile_silver(batch.consumed.rows, batch.counts)
    run_id = run_id_of(started)
    record = ReconciliationRecord(run_id, started, batch.consumed, balance)
    write_reconciliation(spark, tables.reconciliation, record)  # kept even if the gate fails
    step.record_quality(_gate(spark, tables, batch, started))  # CRITICAL raises: nothing written
    inserted = _publish(spark, tables, batch)
    table_suite = silver_table_suite(spark.table(tables.silver), new_keys=inserted)
    step.record_quality(run_suite(table_suite, execution_timestamp=started))
    commit_checkpoint(
        spark,
        tables.checkpoint,
        Checkpoint(PIPELINE_NAME, tables.bronze, commits.end, run_id, started),
    )
    return batch.counts


def run_id_of(started: datetime) -> str:
    """The run id: the UTC start, to the second (one run per pipeline at a time)."""
    return f"{started:%Y%m%dT%H%M%SZ}"


def _plan(spark: SparkSession, tables: SilverTables) -> CommitRange:
    checkpoint = read_checkpoint(
        spark, tables.checkpoint, pipeline=PIPELINE_NAME, source=tables.bronze
    )
    latest = latest_version(spark, tables.bronze)  # captured once: the end of this run
    return plan_range(checkpoint, latest, source=tables.bronze, checkpoint_table=tables.checkpoint)


def run_silver(
    spark: SparkSession,
    *,
    bronze_table: str,
    namespace: str,
    now: datetime | None = None,
    sink: ObservabilitySink | None = None,
) -> SilverRunReport:
    """One incremental, observed Silver run over the Bronze commits not yet consumed.

    Raises ``CriticalQualityError`` (nothing written) or ``ProgressError`` (the commit range
    cannot be read; the message carries the recovery procedure).
    """
    started = (now or datetime.now(UTC)).astimezone(UTC)
    run_id = run_id_of(started)
    tables = SilverTables.of(bronze_table, namespace)
    observer = sink or DeltaObservabilitySink(spark, namespace)
    counts = dict.fromkeys(DESTINATIONS, 0)
    with track_run(
        pipeline_name=PIPELINE_NAME,
        run_id=run_id,
        project=PROJECT,
        layer=Layer.SILVER,
        table=tables.silver,
        sink=observer,
    ) as step:
        # Schema first: a changed source stops the run before anything else.
        latest = spark.table(tables.bronze)
        step.record_quality(run_suite(bronze_suite(latest), execution_timestamp=started))
        try:
            commits = _plan(spark, tables)
            if commits.is_empty:  # nothing committed since the checkpoint: nothing to do
                step.record_counts(input_rows=0, output_rows=0, rejected_rows=0, duplicate_rows=0)
            else:
                counts = _consume(spark, step, tables, commits, started)
        except ProgressError as error:
            alert_logger.critical("[CRITICAL] %s/%s: %s", PIPELINE_NAME, tables.bronze, error)
            raise

    run = step.run
    return SilverRunReport(
        run_id=run_id,
        after_version=commits.after,
        bronze_version=commits.end,
        bronze_rows_read=run.input_rows or 0,
        silver_rows=run.output_rows or 0,
        quarantined_rows=run.rejected_rows or 0,
        duplicate_rows=run.duplicate_rows or 0,
        status=run.status or RunStatus.FAILED,
        quality=tuple(run.quality),
        destinations=counts,
    )
