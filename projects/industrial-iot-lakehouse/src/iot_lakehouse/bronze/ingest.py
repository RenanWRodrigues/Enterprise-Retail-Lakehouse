"""IoT Bronze: land every new source file into append-only Delta tables, file by file.

Same contract as the Retail Bronze (docs/ARCHITECTURE.md): the data as delivered, all text, plus
ingestion metadata, in append-only Delta tables. The unit of progress is the **file** (the
historian writes many part files per batch and day, and a late file may arrive in an old
partition), identified by its content hash (``bronze.landing``):

1. discover the dataset's files; skip the ``file_id``s already decided (control table
   ``bronze_files``) or already present in the data table (a run that crashed after its append
   and before recording it: the decision is repaired, the rows are not written again);
2. file problems quarantine the whole file, recorded with its reason (``bronze_files``,
   status ``quarantined``): empty, corrupt gzip, not UTF-8, schema drift, malformed lines,
   or a path already ingested whose bytes changed. The other files continue;
3. the readable files are appended in **one** Delta commit per dataset;
4. then their decisions are recorded (insert-only MERGE by ``dataset, file_id``).

Record-level quality (bad values in well-formed records) is not Bronze's business: those rows are
stored as delivered and judged by the Silver.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from iot_lakehouse.bronze.landing import (
    CHANGED_AFTER_INGESTION,
    MALFORMED_RECORDS,
    SCHEMA_DRIFT,
    FileProblem,
    LandingFile,
    discover,
    inspect,
)
from iot_lakehouse.bronze.readers import (
    RELATIVE_PATH,
    SOURCE_FILE,
    file_stats,
    json_key_problems,
    read_files,
)
from iot_lakehouse.ingestion.source_contracts import (
    IOT_SOURCE_CONTRACTS,
    DatasetContract,
    FileFormat,
)
from lakehouse_common.naming import validate_namespace

logger = logging.getLogger(__name__)

INGESTED, QUARANTINED = "ingested", "quarantined"
FILES_TABLE: Final = "bronze_files"
APPEND_ONLY_PROPERTY: Final = "delta.appendOnly"
METADATA_FIELDS: Final = (
    StructField("_ingestion_timestamp", TimestampType(), False),
    StructField(SOURCE_FILE, StringType(), False),
    StructField("_batch_id", StringType(), False),
    StructField("_ingestion_date", DateType(), False),
    StructField("_file_id", StringType(), False),
    StructField("_source_system", StringType(), False),
)
FILES_SCHEMA: Final = StructType(
    [
        StructField("dataset", StringType(), False),
        StructField("relative_path", StringType(), False),
        StructField("file_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("size_bytes", LongType(), False),
        StructField("status", StringType(), False),
        StructField("reason", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("rows", LongType(), True),
        StructField("run_id", StringType(), False),
        StructField("processed_at", TimestampType(), False),
    ]
)
"""One row per file decision: ingested (with its rows) or quarantined (with the reason)."""


def bronze_schema(contract: DatasetContract) -> StructType:
    """A Bronze table: the source columns as text, then the ingestion metadata."""
    columns = [StructField(name, StringType(), True) for name in contract.column_names]
    return StructType([*columns, *METADATA_FIELDS])


@dataclass(frozen=True, slots=True)
class BronzeTarget:
    """Where the IoT Bronze lives: ``schema`` (local) or ``catalog.schema`` (Unity Catalog)."""

    namespace: str

    def __post_init__(self) -> None:
        validate_namespace(self.namespace)

    def table(self, dataset: str) -> str:
        return f"{self.namespace}.bronze_{dataset}"

    @property
    def files(self) -> str:
        return f"{self.namespace}.{FILES_TABLE}"


@dataclass(frozen=True, slots=True)
class Decision:
    """The decision about one file, as recorded in ``bronze_files``."""

    file: LandingFile
    status: str
    rows: int | None = None
    reason: str | None = None
    detail: str | None = None


@dataclass
class DatasetOutcome:
    dataset: str
    table: str
    files_found: int = 0
    files_skipped: int = 0
    decisions: list[Decision] = field(default_factory=list)
    rows_read: int = 0  # rows of the files that were read, quarantined ones included
    rows_expected: int = 0  # rows of the files appended
    rows_written: int = 0  # rows of the Delta commit

    @property
    def ingested(self) -> list[Decision]:
        return [d for d in self.decisions if d.status == INGESTED]

    @property
    def quarantined(self) -> list[Decision]:
        return [d for d in self.decisions if d.status == QUARANTINED]


def _decided(spark: SparkSession, target: BronzeTarget, dataset: str) -> dict[str, tuple[str, str]]:
    """``file_id -> (relative_path, status)`` already recorded (bounded by the file count)."""
    if not spark.catalog.tableExists(target.files):
        return {}
    rows = (
        spark.table(target.files)
        .where(F.col("dataset") == dataset)
        .select("file_id", "relative_path", "status")
        .distinct()
        .collect()
    )
    return {r["file_id"]: (r["relative_path"], r["status"]) for r in rows}


def _rows_in_table(spark: SparkSession, table: str, file_ids: list[str]) -> dict[str, int]:
    """Rows per ``_file_id`` of ``file_ids`` already in the data table (crash repair).

    Only files with no recorded decision are looked up, so the scan is filtered to them and at
    most one row per file reaches the driver.
    """
    if not file_ids or not spark.catalog.tableExists(table):
        return {}
    landed = spark.table(table).where(F.col("_file_id").isin(file_ids))
    return {r["_file_id"]: int(r["count"]) for r in landed.groupBy("_file_id").count().collect()}


def _plan(
    files: list[LandingFile], decided: dict[str, tuple[str, str]], landed: dict[str, int]
) -> tuple[list[Decision], list[LandingFile], list[FileProblem], int]:
    """Repairs (landed but unrecorded), files to read, file problems, and files skipped."""
    ingested_paths = {path for path, status in decided.values() if status == INGESTED}
    ingested_paths |= {f.relative_path for f in files if f.file_id in landed}
    repairs, readable, problems, skipped = [], [], [], 0
    for file in files:
        if file.file_id in decided:
            skipped += 1
        elif file.file_id in landed:
            repairs.append(Decision(file, INGESTED, rows=landed[file.file_id]))
        elif file.relative_path in ingested_paths:
            problems.append(
                FileProblem(file, CHANGED_AFTER_INGESTION, "bytes differ from the ingested file")
            )
        elif problem := inspect(file):
            problems.append(problem)
        else:
            readable.append(file)
    return repairs, readable, problems, skipped


def _to_bronze(
    records: DataFrame, files: list[LandingFile], contract: DatasetContract, at: datetime
) -> DataFrame:
    """Records of ``files`` with the metadata (inner join N:1 on the file: other files drop)."""
    spark = records.sparkSession
    meta = spark.createDataFrame(
        [(f.relative_path, f.file_id, f.batch_id) for f in files],
        "_meta_path string, _file_id string, _batch_id string",
    )
    joined = records.join(F.broadcast(meta), F.col(RELATIVE_PATH) == F.col("_meta_path"))
    return joined.select(
        *contract.column_names,
        F.lit(at).cast(TimestampType()).alias("_ingestion_timestamp"),
        SOURCE_FILE,
        "_batch_id",
        F.lit(at.date()).alias("_ingestion_date"),
        "_file_id",
        F.lit(str(contract.source_system)).alias("_source_system"),
    )


def _append(frame: DataFrame, table: str) -> int:
    """One append commit (create on the first one); rows written, from the commit metrics."""
    spark = frame.sparkSession
    writer = frame.writeTo(table).using("delta")
    if spark.catalog.tableExists(table):
        writer.append()
    else:
        writer.tableProperty(APPEND_ONLY_PROPERTY, "true").create()
    latest = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").select("operationMetrics").first()
    return int(((latest["operationMetrics"] if latest else None) or {}).get("numOutputRows", 0))


def _read_and_check(
    spark: SparkSession, contract: DatasetContract, readable: list[LandingFile]
) -> tuple[DataFrame, dict[str, int], list[FileProblem]]:
    """Records of the readable files, rows per file, and the problems only Spark sees."""
    paths = [f.path.as_posix() for f in readable]
    by_path = {f.relative_path: f for f in readable}
    problems = []
    if contract.file_format is FileFormat.JSONL_GZIP:
        drift = json_key_problems(spark, contract, paths)
        problems += [FileProblem(by_path[p], SCHEMA_DRIFT, d) for p, d in drift.items()]
    records = read_files(spark, contract, paths)
    stats = file_stats(records, contract)
    unknown = sorted(set(stats) - set(by_path))
    if unknown:  # a Spark path that does not match discovery would drop rows in the join
        raise RuntimeError(f"files read by Spark but not discovered: {unknown[:3]}")
    drifted = {p.file.relative_path for p in problems}
    problems += [
        FileProblem(
            by_path[path], MALFORMED_RECORDS, f"{s.corrupt_rows} of {s.rows} lines unparseable"
        )
        for path, s in stats.items()
        if s.corrupt_rows and path not in drifted
    ]
    return records, {p: s.rows for p, s in stats.items()}, problems


def record_decisions(
    spark: SparkSession,
    target: BronzeTarget,
    decisions: list[Decision],
    *,
    run_id: str,
    at: datetime,
) -> None:
    """Insert-only by (dataset, file_id): recording the same decision twice adds nothing."""
    if not decisions:
        return
    rows = [
        (d.file.dataset, d.file.relative_path, d.file.file_id, d.file.batch_id, d.file.size_bytes,
         d.status, d.reason, d.detail, d.rows, run_id, at)
        for d in decisions
    ]  # fmt: skip
    spark.createDataFrame(rows, FILES_SCHEMA).createOrReplaceTempView("_bronze_files_incoming")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target.files} ({_ddl(FILES_SCHEMA)}) USING DELTA")
    spark.sql(
        f"MERGE INTO {target.files} AS t USING _bronze_files_incoming AS s "
        "ON t.dataset = s.dataset AND t.file_id = s.file_id WHEN NOT MATCHED THEN INSERT *"
    )


def _ddl(schema: StructType) -> str:
    return ", ".join(
        f"{f.name} {f.dataType.simpleString()}{'' if f.nullable else ' NOT NULL'}"
        for f in schema.fields
    )


def ingest_dataset(
    spark: SparkSession, dataset: str, *, landing_root: Path, target: BronzeTarget, at: datetime
) -> DatasetOutcome:
    """Plan, read, append and return the decisions (recorded by the caller, after the append)."""
    contract, table = IOT_SOURCE_CONTRACTS[dataset], target.table(dataset)
    files = discover(landing_root, dataset)
    decided = _decided(spark, target, dataset)
    undecided = [f.file_id for f in files if f.file_id not in decided]
    repairs, readable, problems, skipped = _plan(
        files, decided, _rows_in_table(spark, table, undecided)
    )
    outcome = DatasetOutcome(dataset, table, len(files), skipped, decisions=list(repairs))
    if readable:
        records, rows_per_file, spark_problems = _read_and_check(spark, contract, readable)
        problems += spark_problems
        bad = {p.file.relative_path for p in spark_problems}
        good = [f for f in readable if f.relative_path not in bad]
        outcome.rows_read = sum(rows_per_file.values())
        outcome.rows_expected = sum(rows_per_file.get(f.relative_path, 0) for f in good)
        if good:
            outcome.rows_written = _append(_to_bronze(records, good, contract, at), table)
        outcome.decisions += [
            Decision(f, INGESTED, rows=rows_per_file.get(f.relative_path, 0)) for f in good
        ]
    outcome.decisions += [
        Decision(p.file, QUARANTINED, reason=p.reason, detail=p.detail[:500]) for p in problems
    ]
    logger.info(
        "iot_bronze dataset=%s files=%d skipped=%d ingested=%d quarantined=%d rows=%d",
        dataset, len(files), skipped, len(outcome.ingested), len(outcome.quarantined),
        outcome.rows_written,
    )  # fmt: skip
    return outcome


def utc(at: datetime) -> datetime:
    if at.tzinfo is None:
        raise ValueError("ingestion timestamp must be timezone-aware (use UTC)")
    return at.astimezone(UTC)
