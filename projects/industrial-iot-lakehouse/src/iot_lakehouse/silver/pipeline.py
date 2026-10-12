"""IoT Silver run (I/O): pending Bronze files -> classified readings -> Delta, observed.

1. pending = Bronze ``_file_id``s not in ``silver_processed_files`` (a file is the unit, as in
   the Bronze; a late file in an old partition is just another pending file);
2. the Silver as it is now is pinned (``VERSION AS OF``): it is "what arrived first" for
   deduplication and out-of-order detection, and stays so after this run's own MERGE;
3. ``build_silver`` classifies every Bronze row: valid, quarantine or duplicate;
4. reconciliation ``bronze rows = valid + quarantine + duplicate`` (CRITICAL) and the run's
   metrics (``silver_run_metrics``) are recorded before anything is written;
5. gate (CRITICAL stops before writing): unique ``reading_id``, systemic unknown sensors;
6. MERGE insert-only into the Silver (by ``reading_id``: a published reading is never replaced)
   and the quarantine; then the files are marked processed.

A failure before step 6 writes nothing; a failure inside it is repeated harmlessly by the next
run (insert-only MERGEs, files not yet marked).
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from iot_lakehouse import PROJECT
from iot_lakehouse.bronze.ingest import BronzeTarget, utc
from iot_lakehouse.quality.suites import SYSTEMIC_ORPHAN_RATE
from iot_lakehouse.silver.standardize import (
    CONTENT_HASH,
    SilverBatch,
    SilverHistory,
    build_silver,
    sensor_reference,
)
from lakehouse_common.config import load_environment
from lakehouse_common.naming import validate_namespace
from lakehouse_common.observability import Layer, ObservabilitySink, RunTracker, track_run
from lakehouse_common.quality import (
    CheckResult,
    CheckSet,
    Dimension,
    Reconciliation,
    ReferentialIntegrity,
    Severity,
    Status,
    Unique,
    failure_rate_of,
    run_suite,
)
from lakehouse_common.quality.runner import FAILED_CHECKS_COLUMN

PIPELINE_NAME: Final = "iot_silver"
REASONS: Final = FAILED_CHECKS_COLUMN
ERROR, WARNING = Severity.ERROR, Severity.WARNING
METRICS_SCHEMA: Final = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("metric", StringType(), False),
        StructField("value", LongType(), True),
        StructField("recorded_at", TimestampType(), False),
    ]
)
PROCESSED_SCHEMA: Final = "file_id string, run_id string, processed_at timestamp"


@dataclass(frozen=True, slots=True)
class SilverTarget:
    namespace: str

    def __post_init__(self) -> None:
        validate_namespace(self.namespace)

    def _name(self, table: str) -> str:
        return f"{self.namespace}.{table}"

    @property
    def readings(self) -> str:
        return self._name("silver_sensor_readings")

    @property
    def quarantine(self) -> str:
        return self._name("silver_sensor_readings_quarantine")

    @property
    def processed(self) -> str:
        return self._name("silver_processed_files")

    @property
    def metrics(self) -> str:
        return self._name("silver_run_metrics")


@dataclass(frozen=True, slots=True)
class SilverSettings:
    bronze: BronzeTarget
    target: SilverTarget
    late_after_seconds: int


def load_silver_settings(path: Path, environment: str) -> SilverSettings:
    """Bronze and Silver namespaces and the lateness threshold (``config/silver.toml``)."""
    section = load_environment(path, environment)
    return SilverSettings(
        BronzeTarget(str(section["bronze_namespace"])),
        SilverTarget(str(section["silver_namespace"])),
        int(section["late_after_minutes"]) * 60,
    )


def pending_files(spark: SparkSession, bronze_table: str, processed_table: str) -> list[str]:
    """Bronze files not yet processed (one id per file reaches the driver)."""
    if not spark.catalog.tableExists(bronze_table):
        return []
    files = spark.table(bronze_table).select("_file_id").distinct()
    if spark.catalog.tableExists(processed_table):
        files = files.join(
            spark.table(processed_table).select(F.col("file_id").alias("_file_id")),
            "_file_id",
            "left_anti",
        )
    return sorted(r["_file_id"] for r in files.collect())


def _pinned(spark: SparkSession, table: str) -> DataFrame | None:
    """``table`` at its current Delta version, so later writes do not change what it returns."""
    if not spark.catalog.tableExists(table):
        return None
    latest = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").select("version").first()
    version = int(latest["version"]) if latest else 0
    return spark.sql(f"SELECT * FROM {table} VERSION AS OF {version}")  # noqa: S608 - validated


def silver_history(spark: SparkSession, target: SilverTarget) -> SilverHistory:
    published = _pinned(spark, target.readings)
    if published is None:
        return SilverHistory()
    latest = published.groupBy("sensor_id").agg(F.max("event_timestamp").alias("_stored_event"))
    return SilverHistory(published.select("reading_id", CONTENT_HASH), latest)


def _ddl(frame: DataFrame) -> str:
    return ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in frame.schema.fields)


def _merge_insert_only(frame: DataFrame, table: str, condition: str) -> None:
    spark = frame.sparkSession
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({_ddl(frame)}) USING DELTA")
    frame.createOrReplaceTempView("_iot_silver_incoming")
    spark.sql(
        f"MERGE INTO {table} AS t USING _iot_silver_incoming AS s ON {condition} "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def publish(
    batch: SilverBatch, target: SilverTarget, files: list[str], run_id: str, at: datetime
) -> None:
    """Insert-only writes: Silver by reading_id, quarantine by row and reasons, then the files."""
    _merge_insert_only(batch.valid, target.readings, "t.reading_id = s.reading_id")
    _merge_insert_only(
        batch.quarantine,
        target.quarantine,
        "t._file_id = s._file_id AND t._content_hash = s._content_hash AND "
        f"array_join(t.{REASONS}, ',') = array_join(s.{REASONS}, ',')",
    )
    spark = batch.valid.sparkSession
    processed = spark.createDataFrame([(f, run_id, at) for f in files], PROCESSED_SCHEMA)
    _merge_insert_only(processed, target.processed, "t.file_id = s.file_id")


@dataclass(frozen=True)
class BatchMetrics:
    bronze_rows: int
    destinations: dict[str, int]  # valid, quarantine, duplicate
    flags: dict[str, int]  # late, out_of_order, unit_converted, unknown_sensor, max_lateness
    quarantine_reasons: dict[str, int]
    new_quarantine: int  # quarantined rows not in the quarantine table yet (alerted once)


def _new_quarantine(batch: SilverBatch, recorded: DataFrame | None) -> int:
    rows = batch.quarantine
    if recorded is not None:
        keys = recorded.select("_file_id", CONTENT_HASH).distinct()
        rows = rows.join(keys, ["_file_id", CONTENT_HASH], "left_anti")
    return rows.count()


def measure(bronze: DataFrame, batch: SilverBatch, recorded: DataFrame | None) -> BatchMetrics:
    """Counts of every destination and flag, each from its own DataFrame (4 small actions).

    ``recorded``: the quarantine table before this run (pinned), to tell new problems apart.
    """
    labels = (
        batch.valid.select(F.lit("valid").alias("d"))
        .unionByName(batch.quarantine.select(F.lit("quarantine").alias("d")))
        .unionByName(batch.duplicates.select(F.lit("duplicate").alias("d")))
        .unionByName(bronze.select(F.lit("bronze").alias("d")))
    )
    counted = {r["d"]: int(r["count"]) for r in labels.groupBy("d").count().collect()}
    flags = batch.valid.agg(
        F.sum(F.col("is_late").cast("long")).alias("late"),
        F.sum(F.col("arrived_out_of_order").cast("long")).alias("out_of_order"),
        F.sum(F.col("unit_converted").cast("long")).alias("unit_converted"),
        F.sum(F.col("unknown_sensor").cast("long")).alias("unknown_sensor"),
        F.max("lateness_seconds").alias("max_lateness_seconds"),
    ).first()
    reasons = (
        batch.quarantine.select(F.explode(FAILED_CHECKS_COLUMN).alias("r")).groupBy("r").count()
    )
    return BatchMetrics(
        counted.get("bronze", 0),
        {d: counted.get(d, 0) for d in ("valid", "quarantine", "duplicate")},
        {k: int(v or 0) for k, v in (flags.asDict() if flags else {}).items()},
        {r["r"]: int(r["count"]) for r in reasons.collect()},
        _new_quarantine(batch, recorded),
    )


def write_metrics(
    spark: SparkSession, table: str, metrics: BatchMetrics, run_id: str, at: datetime
) -> None:
    """Append the run's metrics (a log: one row per metric; kept even if the gate then fails)."""
    values = {"bronze_rows": metrics.bronze_rows, **metrics.destinations, **metrics.flags}
    values["new_quarantine"] = metrics.new_quarantine
    values |= {f"quarantine__{reason}": n for reason, n in metrics.quarantine_reasons.items()}
    frame = spark.createDataFrame(
        [(run_id, k, v, at) for k, v in sorted(values.items())], METRICS_SCHEMA
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({_ddl(frame)}) USING DELTA")
    frame.write.format("delta").mode("append").saveAsTable(table)


def _gate(
    batch: SilverBatch, sensors: DataFrame, metrics: BatchMetrics, table: str, at: datetime
) -> list[CheckResult]:
    checks = [
        Unique(("reading_id",), severity=Severity.CRITICAL, name="duplicate__reading_id"),
        Unique(("sensor_id", "event_timestamp"), severity=ERROR, name="conflicting_readings"),
        ReferentialIntegrity(
            "sensor_id", sensors, "sensor_id", severity=WARNING, name="unknown_sensor"
        ),
        ReferentialIntegrity(
            "sensor_id", sensors, "sensor_id", severity=Severity.CRITICAL,
            threshold=SYSTEMIC_ORPHAN_RATE, name="unknown_sensor_systemic",
        ),
    ]  # fmt: skip
    balance = Reconciliation(metrics.bronze_rows, metrics.destinations)
    return [
        *run_suite([CheckSet(table, batch.valid, checks)], execution_timestamp=at),
        quarantine_check(metrics, table, at),
        balance.to_check_result(table_name=table, execution_timestamp=at),
    ]


def quarantine_check(metrics: BatchMetrics, table: str, at: datetime) -> CheckResult:
    """ERROR for rows quarantined for the first time; ``details`` = rows per reason in the batch.

    Identity of a quarantined row: (Bronze file, content). A row already in the quarantine (e.g.
    a retry after a failure) is counted in the reconciliation but not alerted again.
    """
    failed, checked = metrics.new_quarantine, metrics.bronze_rows
    reasons = ", ".join(f"{r}={n}" for r, n in sorted(metrics.quarantine_reasons.items()))
    return CheckResult(
        check_name="quarantined_rows", check_type="quarantine", table_name=table,
        column_name=None, dimension=Dimension.VALIDITY,
        status=Status.ERROR if failed else Status.PASS, severity=Severity.ERROR,
        rows_checked=checked, failed_rows=failed,
        failure_rate=failure_rate_of(failed_rows=failed, rows_checked=checked),
        execution_timestamp=at, details=reasons or None,
    )  # fmt: skip


def _consume(
    spark: SparkSession, step: RunTracker, settings: SilverSettings, files: list[str], at: datetime
) -> None:
    run_id, target = f"{at:%Y%m%dT%H%M%SZ}", settings.target
    bronze = spark.table(settings.bronze.table("sensor_readings")).where(
        F.col("_file_id").isin(files)
    )
    sensors = sensor_reference(spark.table(settings.bronze.table("sensors")))
    batch = build_silver(
        bronze, sensors, silver_history(spark, target),
        late_after_seconds=settings.late_after_seconds, execution_timestamp=at,
    )  # fmt: skip
    metrics = measure(bronze, batch, _pinned(spark, target.quarantine))
    step.record_counts(
        input_rows=metrics.bronze_rows,
        output_rows=metrics.destinations["valid"],
        rejected_rows=metrics.destinations["quarantine"],
        duplicate_rows=metrics.destinations["duplicate"],
    )
    write_metrics(spark, target.metrics, metrics, run_id, at)
    step.record_quality(_gate(batch, sensors, metrics, target.readings, at))  # CRITICAL raises
    publish(batch, target, files, run_id, at)


def run_silver(
    spark: SparkSession,
    *,
    settings: SilverSettings,
    ingestion_timestamp: datetime,
    sink: ObservabilitySink,
) -> list[str]:
    """One observed Silver run over the pending Bronze files; returns the files processed."""
    at = utc(ingestion_timestamp)
    target = settings.target
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target.namespace}")
    with track_run(
        pipeline_name=PIPELINE_NAME, run_id=f"{at:%Y%m%dT%H%M%SZ}", project=PROJECT,
        layer=Layer.SILVER, table=target.readings, sink=sink,
    ) as step:  # fmt: skip
        files = pending_files(spark, settings.bronze.table("sensor_readings"), target.processed)
        if not files:
            step.record_counts(input_rows=0, output_rows=0, rejected_rows=0, duplicate_rows=0)
        else:
            _consume(spark, step, settings, files, at)
    return files
