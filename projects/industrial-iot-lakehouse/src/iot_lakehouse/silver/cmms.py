"""CMMS Silver: equipment, sensors, failures and maintenance orders, typed by their contracts.

Master data is small, so each run **rebuilds** every CMMS Silver table from the whole Bronze
(deterministic, idempotent; the documented exception to incremental processing). Per dataset:

1. typed columns from the contract's logical types (``try_cast``; text trimmed, blank = NULL);
   the delivered text stays in the quarantine;
2. rules from the contract (ERROR, quarantined with every reason): a required column missing,
   a value that does not parse, and a maintenance order whose times go backwards;
3. one row per business key: the latest delivery (Bronze arrival, then file) wins; earlier
   deliveries are counted as superseded;
4. ``bronze rows = valid + quarantine + superseded`` (CRITICAL).
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Final

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, TimestampType

from iot_lakehouse import PROJECT
from iot_lakehouse.bronze.ingest import BronzeTarget
from iot_lakehouse.ingestion.source_contracts import IOT_SOURCE_CONTRACTS, DatasetContract
from iot_lakehouse.quality.suites import LOGICAL_TYPES
from lakehouse_common.observability import Layer, ObservabilitySink, new_run_id, track_run
from lakehouse_common.quality import (
    CustomCondition,
    NotNull,
    QualityCheck,
    Reconciliation,
    Severity,
    split_by_quality,
)
from lakehouse_common.quality.runner import EXECUTION_TIMESTAMP_COLUMN, FAILED_CHECKS_COLUMN

PIPELINE_NAME: Final = "iot_silver_cmms"
CMMS_DATASETS: Final = ("equipment", "sensors", "failure_events", "maintenance_orders")
LINEAGE: Final = ("_file_id", "_batch_id", "_source_file")
_SRC: Final = "_src_"
ERROR = Severity.ERROR
CHRONOLOGY: Final = CustomCondition(
    name="invalid_chronology",  # an order cannot start before it opens nor end before it starts
    condition="(opened_at IS NULL OR started_at IS NULL OR started_at >= opened_at) AND "
    "(started_at IS NULL OR completed_at IS NULL OR completed_at >= started_at)",
    severity=ERROR,
)


def _blank_to_null(column: Column) -> Column:
    trimmed = F.trim(column)
    return F.when(trimmed == "", None).otherwise(trimmed)


def typed(contract: DatasetContract, bronze: DataFrame) -> DataFrame:
    """Contract columns typed, the delivered text in ``_src_*``, and the lineage."""
    typed_columns = []
    for column in contract.columns:
        text = _blank_to_null(F.col(column.name))
        kind = LOGICAL_TYPES[column.logical_type]
        value = text if column.logical_type == "string" else text.try_cast(kind)
        typed_columns.append(value.alias(column.name))
    return bronze.select(
        *(F.col(c).alias(f"{_SRC}{c}") for c in contract.column_names),
        *typed_columns,
        *LINEAGE,
        F.col("_ingestion_timestamp").alias("bronze_ingestion_timestamp"),
    )


def _parsed(column: str) -> CustomCondition:
    """Delivered text present but not parseable as the contract type (absent: NotNull)."""
    raw = f"{_SRC}{column}"
    return CustomCondition(
        name=f"unparseable__{column}",
        condition=f"{raw} IS NULL OR trim({raw}) = '' OR {column} IS NOT NULL",
        severity=ERROR,
    )


def cmms_rules(contract: DatasetContract) -> list[QualityCheck]:
    """Required columns present, typed values parsed, maintenance times in order (ERROR)."""
    rules: list[QualityCheck] = [
        NotNull(c.name, severity=ERROR, name=f"missing__{c.name}")
        for c in contract.columns
        if not c.nullable
    ]
    rules += [_parsed(c.name) for c in contract.columns if c.logical_type != "string"]
    return [*rules, CHRONOLOGY] if contract.name == "maintenance_orders" else rules


@dataclass(frozen=True)
class CmmsBatch:
    valid: DataFrame  # one row per business key: the latest delivery
    quarantine: DataFrame
    superseded: DataFrame  # earlier deliveries of a key


def standardize_cmms(dataset: str, bronze: DataFrame, at: datetime) -> CmmsBatch:
    contract = IOT_SOURCE_CONTRACTS[dataset]
    valid, rejected = split_by_quality(
        typed(contract, bronze), cmms_rules(contract), execution_timestamp=at
    )
    latest_first = Window.partitionBy(*contract.business_key).orderBy(
        F.col("bronze_ingestion_timestamp").desc(), F.col("_file_id").desc()
    )
    ranked = valid.withColumn("_rank", F.row_number().over(latest_first))
    keep = [*contract.column_names, *LINEAGE, "bronze_ingestion_timestamp"]
    quarantine = rejected.select(
        *(F.col(f"{_SRC}{c}").alias(c) for c in contract.column_names),
        *LINEAGE,
        "bronze_ingestion_timestamp",
        F.col(FAILED_CHECKS_COLUMN).cast(ArrayType(StringType())),
        F.col(EXECUTION_TIMESTAMP_COLUMN).cast(TimestampType()),
    )
    return CmmsBatch(
        ranked.where(F.col("_rank") == 1).select(*keep),
        quarantine,
        ranked.where(F.col("_rank") > 1).select(*keep),
    )


def silver_cmms_table(namespace: str, dataset: str) -> str:
    return f"{namespace}.silver_{dataset}"


def run_cmms_silver(
    spark: SparkSession, *, bronze: BronzeTarget, silver_namespace: str, at: datetime,
    sink: ObservabilitySink,
) -> dict[str, int]:  # fmt: skip
    """Rebuild every CMMS Silver table (and its quarantine); rows written per dataset."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_namespace}")
    written = {}
    for dataset in CMMS_DATASETS:
        table = silver_cmms_table(silver_namespace, dataset)
        with track_run(
            pipeline_name=PIPELINE_NAME, run_id=new_run_id(at), project=PROJECT,
            layer=Layer.SILVER, table=table, sink=sink,
        ) as step:  # fmt: skip
            source = spark.table(bronze.table(dataset))
            batch = standardize_cmms(dataset, source, at)
            counts = {"valid": batch.valid.count(), "quarantine": batch.quarantine.count(),
                      "superseded": batch.superseded.count()}  # fmt: skip
            balance = Reconciliation(source.count(), counts)
            step.record_counts(
                input_rows=balance.input_rows, output_rows=counts["valid"],
                rejected_rows=counts["quarantine"], duplicate_rows=counts["superseded"],
            )  # fmt: skip
            step.record_quality([balance.to_check_result(table_name=table, execution_timestamp=at)])
            batch.valid.writeTo(table).using("delta").createOrReplace()
            batch.quarantine.writeTo(f"{table}_quarantine").using("delta").createOrReplace()
            written[dataset] = counts["valid"]
    return written
