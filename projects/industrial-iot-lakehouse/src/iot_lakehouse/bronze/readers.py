"""Read IoT landing files exactly as delivered, and the per-file checks only Spark can do.

Every contract column is read as text (no inference, no casting, no trimming). Reading is
PERMISSIVE so that a broken file can be named instead of failing every file of the run: an
unparseable line goes to ``_corrupt_record``, and ``file_stats`` counts rows and corrupt lines
*per file*. JSON key drift is checked per file too (``json_key_problems``): with an explicit
schema the JSON reader silently drops unknown keys, so a new source field would vanish.

Only per-file aggregates reach the driver (one row per file, or one per file and key name),
never the records.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from iot_lakehouse.ingestion.source_contracts import DatasetContract, FileFormat

SOURCE_FILE: Final = "_source_file"
RELATIVE_PATH: Final = "_relative_path"
CORRUPT_RECORD: Final = "_corrupt_record"
# The landing-relative path starts at the source-system directory (bronze.landing layout).
_RELATIVE: Final = r"((?:cmms|historian)/.+)$"
CSV_OPTIONS: Final = {
    "header": "true",
    "enforceSchema": "true",  # the header was compared with the contract before (landing.py)
    "quote": '"',
    "escape": '"',
    "encoding": "UTF-8",
    "nullValue": "",
    "ignoreLeadingWhiteSpace": "false",
    "ignoreTrailingWhiteSpace": "false",
}
PERMISSIVE: Final = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_RECORD}


def read_schema(contract: DatasetContract) -> StructType:
    """Contract columns as nullable text, plus the corrupt-line column."""
    columns = [StructField(name, StringType(), True) for name in contract.column_names]
    return StructType([*columns, StructField(CORRUPT_RECORD, StringType(), True)])


def _relative(column: str) -> Column:
    return F.regexp_extract(F.col(column), _RELATIVE, 1)


def read_files(spark: SparkSession, contract: DatasetContract, paths: Sequence[str]) -> DataFrame:
    """The files' records as text, with ``_corrupt_record``, ``_source_file`` (full path) and
    ``_relative_path`` (the landing-relative name used as the file's key)."""
    reader = spark.read.schema(read_schema(contract)).options(**PERMISSIVE)
    if contract.file_format is FileFormat.CSV:
        raw = reader.options(**CSV_OPTIONS).csv(list(paths))
    else:
        raw = reader.option("encoding", "UTF-8").json(list(paths))
    return raw.select(
        *contract.column_names,
        CORRUPT_RECORD,
        F.col("_metadata.file_path").alias(SOURCE_FILE),
        _relative("_metadata.file_path").alias(RELATIVE_PATH),
    )


@dataclass(frozen=True, slots=True)
class FileStats:
    rows: int
    corrupt_rows: int


def file_stats(records: DataFrame, contract: DatasetContract) -> dict[str, FileStats]:
    """Rows and corrupt lines per file, in one aggregation (one row per file collected).

    A contract column is aggregated too: Spark refuses queries over raw JSON/CSV that reference
    only the corrupt-record column.
    """
    stats = records.groupBy(RELATIVE_PATH).agg(
        F.count(F.lit(1)).alias("rows"),
        F.count(CORRUPT_RECORD).alias("corrupt"),
        F.count(contract.column_names[0]).alias("_keyed"),
    )
    return {r[RELATIVE_PATH]: FileStats(int(r["rows"]), int(r["corrupt"])) for r in stats.collect()}


def json_key_problems(
    spark: SparkSession, contract: DatasetContract, paths: Sequence[str]
) -> dict[str, str]:
    """Files whose JSON keys differ from the contract: an unexpected key, or a contract key in
    no record of the file. Collects (file, key names), bounded by files x contract size."""
    lines = spark.read.schema("value STRING").text(list(paths))
    keys = (
        lines.select(
            _relative("_metadata.file_path").alias(RELATIVE_PATH),
            F.explode(F.json_object_keys("value")).alias("key"),
        )
        .groupBy(RELATIVE_PATH)
        .agg(F.collect_set("key").alias("keys"))
    )
    expected, problems = set(contract.column_names), {}
    for row in keys.collect():
        delivered = set(row["keys"])
        unexpected, missing = sorted(delivered - expected), sorted(expected - delivered)
        if unexpected or missing:
            problems[row[RELATIVE_PATH]] = f"unexpected keys {unexpected}; missing keys {missing}"
    return problems
