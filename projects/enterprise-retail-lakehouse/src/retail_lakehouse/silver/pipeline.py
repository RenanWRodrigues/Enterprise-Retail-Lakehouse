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
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Final, cast

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from lakehouse_common.config import load_environment
from lakehouse_common.incremental import (
    Checkpoint,
    DeltaCheckpointStore,
    Discovery,
    SparkDeltaLog,
    discover,
    pending_units,
)
from lakehouse_common.naming import validate_namespace
from lakehouse_common.observability import (
    Layer,
    ObservabilitySink,
    PipelineRun,
    RunTracker,
    new_run_id,
    track_run,
)
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
CHECKPOINT_TABLE: Final = "silver_discovery_checkpoints"  # last Bronze version consumed
BATCH_COLUMNS: Final = ("_batch_id", "_ingestion_timestamp")  # one discovered unit
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

    @property
    def checkpoints(self) -> str:
        return f"{self.namespace}.{CHECKPOINT_TABLE}"


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

    def record(self, step: RunTracker) -> None:
        step.record_counts(
            input_rows=self.input_rows,
            output_rows=self.output_rows,
            rejected_rows=self.rejected_rows,
            duplicate_rows=self.duplicate_rows,
        )


class SilverStage(StrEnum):
    """Stages of one entity run, in order; the batch stages repeat for every pending batch."""

    PENDING_BATCHES = "pending_batches"
    STANDARDIZE = "standardize"  # standardize + counts reconciliation
    QUALITY_GATE = "quality_gate"
    MERGE = "merge"  # Silver (or customer versions) and quarantine
    MARK_PROCESSED = "mark_processed"


@dataclass
class EntityProgress:
    """Where an entity run is, and what it has confirmed.

    A batch is *confirmed* once its MERGEs succeeded and it was marked processed: only then do
    its counts enter the run. A batch that fails before that is re-processed by the next run
    (the MERGEs ignore what they already hold), so counting it here would count it twice.
    """

    entity: str
    stage: SilverStage = SilverStage.PENDING_BATCHES
    batch_id: str | None = None
    confirmed: _Totals = field(default_factory=_Totals)

    def start_batch(self, batch_id: str) -> None:
        self.batch_id = batch_id

    def enter(self, stage: SilverStage) -> None:
        self.stage = stage

    def confirm(self, batch_id: str, counts: BatchCounts) -> None:
        self.confirmed.add(batch_id, counts)
        self.batch_id = None

    def describe(self) -> str:
        """Failure context for ``pipeline_runs.error_message`` (added as an exception note)."""
        where = f"{self.stage} of batch {self.batch_id}" if self.batch_id else str(self.stage)
        confirmed = ", ".join(self.confirmed.batches) or "none"
        return f"silver {self.entity}: failed at {where}; confirmed batches: {confirmed}"


BatchWork = Callable[[str, datetime, EntityProgress], BatchCounts]


def process_pending(
    step: RunTracker,
    progress: EntityProgress,
    pending: Callable[[], Sequence[tuple[str, datetime]]],
    process_batch: BatchWork,
) -> None:
    """Process every pending batch; the run counts are the confirmed batches, kept up to date.

    Counts start at zero (nothing confirmed is a known state) and are recorded after every
    confirmed batch, so a failure leaves the run with exactly the persisted progress. The
    failure context is added to the error as a note; the original exception propagates.
    """
    progress.confirmed.record(step)
    try:
        progress.enter(SilverStage.PENDING_BATCHES)
        for batch_id, ingested_at in pending():
            progress.start_batch(batch_id)
            counts = process_batch(batch_id, ingested_at, progress)
            progress.confirm(batch_id, counts)
            progress.confirmed.record(step)
            logger.info("silver %s: batch %s %s", progress.entity, batch_id, counts)
    except BaseException as error:
        error.add_note(progress.describe())
        raise


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


def processed_batches(spark: SparkSession, control_table: str, entity: str) -> dict[str, datetime]:
    """Batches of ``entity`` already processed, with their Bronze ingestion time.

    ``collect`` returns one row per batch of the control table (metadata, never data rows).
    """
    rows = (
        spark.table(control_table)
        .where(F.col("entity") == entity)
        .select("batch_id", "bronze_ingestion_timestamp")
        .collect()
    )
    return {row["batch_id"]: row["bronze_ingestion_timestamp"] for row in rows}


def plan_batches(
    discovery: Discovery, *, processed: Mapping[str, datetime], reprocess: Sequence[str] = ()
) -> list[tuple[str, datetime]]:
    """Discovered batches not processed yet, plus the requested reprocessing; oldest first.

    A batch to reprocess that this entity never had (an incremental landing without it) is
    ignored here; ``validate_reprocess`` refuses a batch no entity has.
    """
    discovered = {str(unit[0]): cast(datetime, unit[1]) for unit in discovery.units}
    known = {**processed, **discovered}
    ids = pending_units(
        list(discovered),
        processed=processed.keys(),
        reprocess=[batch for batch in reprocess if batch in known],
        known=known.keys(),
    )
    return sorted(((batch, known[batch]) for batch in ids), key=lambda item: (item[1], item[0]))


def validate_reprocess(reprocess: Sequence[str], *, known: Collection[str]) -> None:
    """Every batch asked to be reprocessed must exist (``known``: processed by some entity)."""
    unknown = sorted(set(reprocess) - set(known))
    if unknown:
        raise ValueError(f"cannot reprocess batches that were never processed: {unknown}")


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


def batch_counts(bronze: DataFrame, batch: StandardizedBatch) -> BatchCounts:
    """The four counts of a batch in **one** Spark action.

    Each count is still taken from its own DataFrame (four independent plans), so the
    reconciliation stays a real check; running them as one query lets Spark share the common
    shuffles. Measured locally: 16 -> 10 jobs, 26-44% less time than four ``count()`` calls
    (docs/performance/spark-count-benchmark.md). No cache: serverless rejects it (ADR 0001).
    """
    parts = (
        ("input", bronze),
        ("valid", batch.valid),
        ("quarantine", batch.quarantine),
        ("duplicates", batch.duplicates),
    )
    counted = [
        frame.agg(F.count(F.lit(1)).alias("rows")).select(F.lit(name).alias("part"), "rows")
        for name, frame in parts
    ]
    union = counted[0]
    for frame in counted[1:]:
        union = union.unionByName(frame)
    rows = {row["part"]: int(row["rows"]) for row in union.collect()}  # four rows
    return BatchCounts(
        input_rows=rows["input"],
        output_rows=rows["valid"],
        rejected_rows=rows["quarantine"],
        duplicate_rows=rows["duplicates"],
    )


def _prepare_batch(
    spark: SparkSession, settings: SilverSettings, entity: str, batch_id: str, started: datetime
) -> _PreparedBatch:
    bronze = spark.table(settings.bronze.table(entity)).where(F.col("_batch_id") == batch_id)
    batch = standardize(bronze, entity, execution_timestamp=started)
    counts = batch_counts(bronze, batch)
    counts.reconcile()  # CRITICAL: a lost row stops the run before anything is written
    return _PreparedBatch(batch, counts, _gate(spark, settings.target, entity, batch.valid))


@dataclass(frozen=True)
class _EntityRun:
    """One entity's observed run: what ``process_batch`` needs, without closures."""

    spark: SparkSession
    settings: SilverSettings
    entity: str
    run_id: str
    started: datetime
    step: RunTracker

    def process_batch(
        self, batch_id: str, ingested_at: datetime, progress: EntityProgress
    ) -> BatchCounts:
        spark, entity, target = self.spark, self.entity, self.settings.target
        progress.enter(SilverStage.STANDARDIZE)
        prepared = _prepare_batch(spark, self.settings, entity, batch_id, self.started)
        progress.enter(SilverStage.QUALITY_GATE)
        self.step.record_quality(run_suite(prepared.gate, execution_timestamp=self.started))
        progress.enter(SilverStage.MERGE)  # CRITICAL findings raised above: nothing written
        valid = prepared.batch.valid
        if entity == "customers":
            stored = spark.table(target.table(entity)).join(
                valid.select("customer_id"), "customer_id", "left_semi"
            )
            merge_versions(spark, customer_versions(valid, stored), target.table(entity))
        else:
            merge_entity(spark, valid, target.table(entity), entity)
        merge_quarantine(spark, prepared.batch.quarantine, target.quarantine(entity))
        progress.enter(SilverStage.MARK_PROCESSED)
        mark_processed(
            spark,
            control_table=target.control,
            entity=entity,
            batch_id=batch_id,
            ingested_at=ingested_at,
            run_id=self.run_id,
            processed_at=self.started,
        )
        return prepared.counts


def run_entity(  # noqa: PLR0913 - keyword-only, injectable clock and sink
    spark: SparkSession,
    entity: str,
    *,
    settings: SilverSettings,
    run_id: str,
    started: datetime,
    sink: ObservabilitySink,
    rediscover: bool = False,
    reprocess: Sequence[str] = (),
) -> PipelineRun:
    """Process every pending batch of ``entity``; one observed run (CRITICAL stops it).

    Pending = batches of the Bronze commits after the entity's checkpoint (Change Data Feed;
    full scan on first run, ``rediscover`` or unusable change data) minus the processed ones,
    plus ``reprocess``. The checkpoint moves to the discovered version only after every batch
    was written and marked. Counts are the confirmed batches', also when the run fails.
    """
    target = settings.target
    bronze_table = settings.bronze.table(entity)
    checkpoints = DeltaCheckpointStore(spark, target.checkpoints)
    found: list[Discovery] = []

    def pending() -> list[tuple[str, datetime]]:
        discovery = discover(
            SparkDeltaLog(spark),
            bronze_table,
            columns=BATCH_COLUMNS,
            checkpoint=checkpoints.read(PIPELINE_NAME, bronze_table),
            rediscover=rediscover,
        )
        found.append(discovery)
        logger.info("silver %s: discovery %s %s", entity, discovery.mode, discovery.reason)
        processed = processed_batches(spark, target.control, entity)
        return plan_batches(discovery, processed=processed, reprocess=reprocess)

    with track_run(
        pipeline_name=PIPELINE_NAME,
        run_id=run_id,
        project=PROJECT,
        layer=Layer.SILVER,
        table=target.table(entity),
        sink=sink,
    ) as step:
        work = _EntityRun(spark, settings, entity, run_id, started, step)
        process_pending(step, EntityProgress(entity), pending, work.process_batch)
        discovery = found[0] if found else None
        if discovery is not None and discovery.end is not None:  # all confirmed: consume them
            checkpoints.commit(
                Checkpoint(
                    PIPELINE_NAME, bronze_table, discovery.end, discovery.mode, run_id, started
                )
            )
    return step.run


def run_silver(  # noqa: PLR0913 - keyword-only
    spark: SparkSession,
    *,
    settings: SilverSettings,
    sink: ObservabilitySink,
    now: datetime | None = None,
    entities: tuple[str, ...] = ENTITY_ORDER,
    rediscover: bool = False,
    reprocess: Sequence[str] = (),
) -> list[PipelineRun]:
    """Every entity in dependency order; stops at the first failed entity (its run is recorded).

    ``rediscover``: scan the whole Bronze instead of the commits after the checkpoint (recovery).
    ``reprocess``: batch ids to process again (idempotent MERGEs); each must be processed already.
    """
    started = (now or datetime.now(UTC)).astimezone(UTC)
    run_id = new_run_id(started)
    ensure_tables(spark, settings.target)
    if reprocess:
        control = spark.table(settings.target.control).select("batch_id").distinct()
        validate_reprocess(reprocess, known={row["batch_id"] for row in control.collect()})
    order = [entity for entity in ENTITY_ORDER if entity in entities]
    return [
        run_entity(
            spark,
            entity,
            settings=settings,
            run_id=run_id,
            started=started,
            sink=sink,
            rediscover=rediscover,
            reprocess=reprocess,
        )
        for entity in order
    ]
