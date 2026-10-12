"""Land source files into append-only Delta tables, one batch at a time.

Idempotency and duplicate protection: a batch whose ``_batch_id`` is already present in
the target table is skipped. Delta commits are atomic, so a batch is either fully in the
table or absent — a failed run can simply be repeated. Assumes one writer per table
(one job per dataset), which holds for this project's jobs.
"""

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Final

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse_common.naming import validate_namespace
from lakehouse_common.observability import Layer, ObservabilitySink, track_run
from retail_lakehouse import PROJECT
from retail_lakehouse.bronze.transformations import BATCH_ID_COLUMN, to_bronze
from retail_lakehouse.ingestion.readers import read_source
from retail_lakehouse.ingestion.source_contracts import RETAIL_SOURCE_CONTRACTS

logger = logging.getLogger(__name__)

PIPELINE_NAME: Final = "retail_bronze"
BRONZE_TABLES: Final = {dataset: f"bronze_{dataset}" for dataset in RETAIL_SOURCE_CONTRACTS}
APPEND_ONLY_PROPERTY: Final = "delta.appendOnly"
_BATCH_ID: Final = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True, slots=True)
class BronzeTarget:
    """Where Bronze tables live: ``schema`` (local) or ``catalog.schema`` (Unity Catalog)."""

    namespace: str

    def __post_init__(self) -> None:
        validate_namespace(self.namespace)

    def table(self, dataset: str) -> str:
        return f"{self.namespace}.{BRONZE_TABLES[dataset]}"


def landing_path(landing_root: str, dataset: str, batch_id: str) -> str:
    """``<root>/<system>/<dataset>/<dataset>_<batch_id>.<format>`` (SOURCE_SYSTEMS.md)."""
    if not _BATCH_ID.match(batch_id):
        raise ValueError(f"invalid batch_id: {batch_id!r}")
    contract = RETAIL_SOURCE_CONTRACTS[dataset]
    root = landing_root.rstrip("/")
    return f"{root}/{contract.source_system}/{dataset}/{dataset}_{batch_id}.{contract.file_format}"


class IngestionStatus(StrEnum):
    INGESTED = "ingested"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class IngestionResult:
    dataset: str
    table_name: str
    batch_id: str
    source_path: str
    status: IngestionStatus
    rows_written: int


def batch_already_ingested(spark: SparkSession, table: str, batch_id: str) -> bool:
    """Whether ``batch_id`` is already in ``table`` (re-running a batch is then skipped)."""
    if not spark.catalog.tableExists(table):
        return False
    return not spark.table(table).where(F.col(BATCH_ID_COLUMN) == batch_id).limit(1).isEmpty()


def _rows_written(spark: SparkSession, table: str) -> int:
    """Rows of the latest commit, from Delta's operation metrics (no extra scan)."""
    latest = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").select("operationMetrics").first()
    metrics = latest["operationMetrics"] if latest else None
    return int((metrics or {}).get("numOutputRows", 0))


def ingest_dataset(  # noqa: PLR0913 - keyword-only after `dataset`
    spark: SparkSession,
    dataset: str,
    *,
    landing_root: str,
    batch_id: str,
    target: BronzeTarget,
    ingestion_timestamp: datetime,
) -> IngestionResult:
    """Append one landing batch of ``dataset`` to its Bronze table (or skip it if present)."""
    contract = RETAIL_SOURCE_CONTRACTS[dataset]
    table = target.table(dataset)
    path = landing_path(landing_root, dataset, batch_id)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target.namespace}")

    if batch_already_ingested(spark, table, batch_id):
        logger.info("bronze %s: batch %s already ingested, skipped", table, batch_id)
        return IngestionResult(dataset, table, batch_id, path, IngestionStatus.SKIPPED, 0)

    bronze = to_bronze(
        read_source(spark, contract, path),
        contract,
        batch_id=batch_id,
        ingestion_timestamp=ingestion_timestamp,
    )
    writer = bronze.writeTo(table).using("delta")
    if spark.catalog.tableExists(table):
        writer.append()
    else:  # atomic CTAS: a failed read leaves no table behind
        writer.tableProperty(APPEND_ONLY_PROPERTY, "true").create()
    rows = _rows_written(spark, table)
    logger.info("bronze %s: batch %s ingested, %d rows", table, batch_id, rows)
    return IngestionResult(dataset, table, batch_id, path, IngestionStatus.INGESTED, rows)


def ingest_all(  # noqa: PLR0913 - keyword-only after `spark`
    spark: SparkSession,
    *,
    landing_root: str,
    batch_id: str,
    target: BronzeTarget,
    ingestion_timestamp: datetime,
    sink: ObservabilitySink,
    datasets: Sequence[str] | None = None,
) -> list[IngestionResult]:
    """Ingest one batch for every dataset (or the given subset), in contract order.

    Each dataset is one ``pipeline_runs`` row (table = its Bronze table). A skipped batch is a
    zero-records run; a failing dataset is recorded as FAILED and stops the run.
    """
    run_id = f"{ingestion_timestamp:%Y%m%dT%H%M%SZ}"
    results: list[IngestionResult] = []
    for dataset in datasets or list(RETAIL_SOURCE_CONTRACTS):
        with track_run(
            pipeline_name=PIPELINE_NAME,
            run_id=run_id,
            project=PROJECT,
            layer=Layer.BRONZE,
            table=target.table(dataset),
            sink=sink,
        ) as step:
            result = ingest_dataset(
                spark,
                dataset,
                landing_root=landing_root,
                batch_id=batch_id,
                target=target,
                ingestion_timestamp=ingestion_timestamp,
            )
            # Bronze appends every row it reads (no filtering): input = output, nothing rejected.
            rows = result.rows_written
            step.record_counts(input_rows=rows, output_rows=rows, rejected_rows=0)
        results.append(result)
    return results
