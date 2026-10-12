"""Retail Silver run (I/O): pending Bronze batches -> standardize -> gate -> MERGE, observed.

Per entity, in dependency order (references first, so foreign keys are checked against the
current Silver):

1. pending batches: Bronze batches not yet in ``<silver>.silver_processed_batches`` (the
   watermark is a Delta control table, one row per entity and batch);
2. each batch, in ingestion order: ``standardize``; counts reconciled
   (``input = valid + quarantine + duplicates``, CRITICAL); quality gate
   (``retail_silver_suite``: schema, key, uniqueness CRITICAL; orphans WARNING); then MERGE:
   - customers: insert-only new versions (``customer_versions``) on (customer_id, effective_from);
   - other entities: SCD1 on the business key, a newer batch with different content updates;
   - quarantine: insert-only on (row fingerprint, reasons);
   and the batch is marked processed — only after its writes succeeded;
3. one ``pipeline_runs`` row per entity (also on failure), counts summed over its batches.

Re-running is idempotent: processed batches are skipped; a batch interrupted before it was
marked is processed again, and the MERGEs ignore what they already hold. No cache: serverless
rejects the cache APIs (docs/ADR/0001-spark-without-cache.md); batches are small.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from lakehouse_common.config import load_environment
from lakehouse_common.naming import validate_namespace
from lakehouse_common.observability import Layer, ObservabilitySink, PipelineRun, track_run
from lakehouse_common.quality import CheckSet, run_suite
from retail_lakehouse import PROJECT
from retail_lakehouse.bronze.ingest import BronzeTarget
from retail_lakehouse.quality.suites import BUSINESS_KEYS, REFERENCES, retail_entity_suite
from retail_lakehouse.silver.schemas import (
    QUARANTINE_TABLES,
    SILVER_SCHEMAS,
    SILVER_TABLES,
    silver_table_schema,
)
from retail_lakehouse.silver.standardize import (
    BatchCounts,
    StandardizedBatch,
    customer_versions,
    quarantine_table_schema,
    standardize,
)

logger = logging.getLogger(__name__)
PIPELINE_NAME: Final = "retail_silver"
PROCESSED_BATCHES_TABLE: Final = "silver_processed_batches"
ENTITY_ORDER: Final = (
    "categories",
    "suppliers",
    "stores",
    "products",
    "customers",
    "orders",
    "order_items",
    "payments",
    "shipments",
    "inventory",
    "inventory_movements",
)
CONTROL_DDL: Final = (
    "entity STRING NOT NULL, batch_id STRING NOT NULL, "
    "bronze_ingestion_timestamp TIMESTAMP NOT NULL, run_id STRING NOT NULL, "
    "processed_at TIMESTAMP NOT NULL"
)


@dataclass(frozen=True, slots=True)
class SilverTarget:
    """Where Silver tables live: ``schema`` (local) or ``catalog.schema`` (Unity Catalog)."""

    namespace: str

    def __post_init__(self) -> None:
        validate_namespace(self.namespace)

    def table(self, entity: str) -> str:
        return f"{self.namespace}.{SILVER_TABLES[entity]}"

    def quarantine(self, entity: str) -> str:
        return f"{self.namespace}.{QUARANTINE_TABLES[entity]}"

    @property
    def control(self) -> str:
        return f"{self.namespace}.{PROCESSED_BATCHES_TABLE}"


@dataclass(frozen=True, slots=True)
class SilverSettings:
    bronze: BronzeTarget
    target: SilverTarget


def load_silver_settings(path: Path, environment: str) -> SilverSettings:
    """Bronze and Silver namespaces of ``environment`` (``config/silver.toml``)."""
    section = load_environment(path, environment)
    return SilverSettings(
        bronze=BronzeTarget(str(section["bronze_namespace"])),
        target=SilverTarget(str(section["silver_namespace"])),
    )


@dataclass
class _Totals:
    input_rows: int = 0
    output_rows: int = 0
    rejected_rows: int = 0
    duplicate_rows: int = 0
    batches: list[str] = field(default_factory=list)

    def add(self, batch_id: str, counts: BatchCounts) -> None:
        self.input_rows += counts.input_rows
        self.output_rows += counts.output_rows
        self.rejected_rows += counts.rejected_rows
        self.duplicate_rows += counts.duplicate_rows
        self.batches.append(batch_id)


def _ddl(schema: StructType) -> str:
    return ", ".join(
        f"`{f.name}` {f.dataType.simpleString()}{'' if f.nullable else ' NOT NULL'}"
        for f in schema.fields
    )


def ensure_tables(spark: SparkSession, target: SilverTarget) -> None:
    """Every Silver, quarantine and control table exists (empty on the first run).

    Created up front so foreign-key checks always have a reference table: an empty reference
    makes every key an orphan, which the suite reports as a systemic (CRITICAL) break.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target.namespace}")
    for entity in ENTITY_ORDER:
        for table, schema in (
            (target.table(entity), silver_table_schema(entity)),
            (target.quarantine(entity), quarantine_table_schema(entity)),
        ):
            spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({_ddl(schema)}) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target.control} ({CONTROL_DDL}) USING DELTA")


def pending_batches(
    spark: SparkSession, bronze_table: str, control_table: str, entity: str
) -> list[tuple[str, datetime]]:
    """Bronze batches of ``entity`` not processed yet, oldest first.

    ``collect`` returns one row per batch (a handful), never data rows.
    """
    batches = spark.table(bronze_table).select("_batch_id", "_ingestion_timestamp").distinct()
    processed = (
        spark.table(control_table)
        .where(F.col("entity") == entity)
        .select(F.col("batch_id").alias("_batch_id"))
    )
    pending = batches.join(processed, "_batch_id", "left_anti").collect()
    return sorted(
        ((row["_batch_id"], row["_ingestion_timestamp"]) for row in pending),
        key=lambda item: (item[1], item[0]),
    )


def _contract(frame: DataFrame, entity: str) -> DataFrame:
    return frame.select(*SILVER_SCHEMAS[entity].fieldNames())


def _merge(spark: SparkSession, frame: DataFrame, table: str, statement: str) -> None:
    view = "_silver_incoming"  # entities and batches are processed one at a time
    frame.createOrReplaceTempView(view)
    try:
        spark.sql(statement.format(table=table, view=view))
    finally:
        spark.catalog.dropTempView(view)


def merge_entity(spark: SparkSession, frame: DataFrame, table: str, entity: str) -> None:
    """SCD1 upsert on the business key: a newer batch with different content replaces the row."""
    keys = BUSINESS_KEYS[entity]
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    same = " AND ".join(
        f"t.`{c}` <=> s.`{c}`" for c in SILVER_SCHEMAS[entity].fieldNames() if c not in keys
    )
    _merge(
        spark,
        frame,
        table,
        f"MERGE INTO {{table}} AS t USING {{view}} AS s ON {on} "
        f"WHEN MATCHED AND s._ingestion_timestamp > t._ingestion_timestamp AND NOT ({same}) "
        "THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
    )


def merge_versions(spark: SparkSession, frame: DataFrame, table: str) -> None:
    """Customer versions: insert-only on (customer_id, effective_from)."""
    _merge(
        spark,
        frame,
        table,
        "MERGE INTO {table} AS t USING {view} AS s "
        "ON t.customer_id = s.customer_id AND t.effective_from = s.effective_from "
        "WHEN NOT MATCHED THEN INSERT *",
    )


def merge_quarantine(spark: SparkSession, frame: DataFrame, table: str) -> None:
    """Insert-only on (row fingerprint, reasons): re-processing a batch adds nothing."""
    _merge(
        spark,
        frame,
        table,
        "MERGE INTO {table} AS t USING {view} AS s ON t._row_hash = s._row_hash "
        "AND array_join(t._dq_failed_checks, ',') = array_join(s._dq_failed_checks, ',') "
        "WHEN NOT MATCHED THEN INSERT *",
    )


def mark_processed(  # noqa: PLR0913 - keyword-only description of one control row
    spark: SparkSession,
    *,
    control_table: str,
    entity: str,
    batch_id: str,
    ingested_at: datetime,
    run_id: str,
    processed_at: datetime,
) -> None:
    row = spark.createDataFrame(
        [(entity, batch_id, ingested_at, run_id, processed_at)], CONTROL_DDL
    )
    _merge(
        spark,
        row,
        control_table,
        "MERGE INTO {table} AS t USING {view} AS s "
        "ON t.entity = s.entity AND t.batch_id = s.batch_id WHEN NOT MATCHED THEN INSERT *",
    )


def _gate(
    spark: SparkSession, target: SilverTarget, entity: str, rows: DataFrame
) -> list[CheckSet]:
    """The entity's checks on the batch, foreign keys against the current Silver tables."""
    references = {
        reference: _contract(spark.table(target.table(reference)), reference)
        for reference in REFERENCES.get(entity, ())
    }
    return retail_entity_suite(entity, _contract(rows, entity), references)


@dataclass(frozen=True)
class _PreparedBatch:
    batch: StandardizedBatch
    counts: BatchCounts
    gate: list[CheckSet]


def _prepare_batch(
    spark: SparkSession, settings: SilverSettings, entity: str, batch_id: str, started: datetime
) -> _PreparedBatch:
    bronze = spark.table(settings.bronze.table(entity)).where(F.col("_batch_id") == batch_id)
    batch = standardize(bronze, entity, execution_timestamp=started)
    counts = BatchCounts(
        input_rows=bronze.count(),
        output_rows=batch.valid.count(),
        rejected_rows=batch.quarantine.count(),
        duplicate_rows=batch.duplicates.count(),
    )
    counts.reconcile()  # CRITICAL: a lost row stops the run before anything is written
    return _PreparedBatch(batch, counts, _gate(spark, settings.target, entity, batch.valid))


def run_entity(  # noqa: PLR0913 - keyword-only, injectable clock and sink
    spark: SparkSession,
    entity: str,
    *,
    settings: SilverSettings,
    run_id: str,
    started: datetime,
    sink: ObservabilitySink,
) -> PipelineRun:
    """Process every pending batch of ``entity``; one observed run (CRITICAL stops it)."""
    target = settings.target
    with track_run(
        pipeline_name=PIPELINE_NAME,
        run_id=run_id,
        project=PROJECT,
        layer=Layer.SILVER,
        table=target.table(entity),
        sink=sink,
    ) as step:
        totals = _Totals()
        pending = pending_batches(spark, settings.bronze.table(entity), target.control, entity)
        for batch_id, ingested_at in pending:
            prepared = _prepare_batch(spark, settings, entity, batch_id, started)
            step.record_quality(run_suite(prepared.gate, execution_timestamp=started))
            valid = prepared.batch.valid  # CRITICAL findings raised above: nothing written
            if entity == "customers":
                stored = spark.table(target.table(entity)).join(
                    valid.select("customer_id"), "customer_id", "left_semi"
                )
                merge_versions(spark, customer_versions(valid, stored), target.table(entity))
            else:
                merge_entity(spark, valid, target.table(entity), entity)
            merge_quarantine(spark, prepared.batch.quarantine, target.quarantine(entity))
            mark_processed(
                spark,
                control_table=target.control,
                entity=entity,
                batch_id=batch_id,
                ingested_at=ingested_at,
                run_id=run_id,
                processed_at=started,
            )
            totals.add(batch_id, prepared.counts)
            logger.info("silver %s: batch %s %s", entity, batch_id, prepared.counts)
        step.record_counts(
            input_rows=totals.input_rows,
            output_rows=totals.output_rows,
            rejected_rows=totals.rejected_rows,
            duplicate_rows=totals.duplicate_rows,
        )
    return step.run


def run_silver(
    spark: SparkSession,
    *,
    settings: SilverSettings,
    sink: ObservabilitySink,
    now: datetime | None = None,
    entities: tuple[str, ...] = ENTITY_ORDER,
) -> list[PipelineRun]:
    """Every entity in dependency order; stops at the first failed entity (its run is recorded)."""
    started = (now or datetime.now(UTC)).astimezone(UTC)
    run_id = f"{started:%Y%m%dT%H%M%SZ}"
    ensure_tables(spark, settings.target)
    order = [entity for entity in ENTITY_ORDER if entity in entities]
    return [
        run_entity(spark, entity, settings=settings, run_id=run_id, started=started, sink=sink)
        for entity in order
    ]
