"""Read landing-zone files exactly as delivered (thin I/O).

Every column is read as text with the contract's explicit schema — no inference, no
casting, no trimming. Structural problems fail the whole load (FAILFAST): a CSV header
that differs from the contract, a malformed JSON line, or a JSON key set that differs from
the contract is a CRITICAL error, never silently "fixed".
"""

from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from retail_lakehouse.ingestion.source_contracts import DatasetContract, FileFormat

SOURCE_FILE_COLUMN: Final = "_source_file"

CSV_OPTIONS: Final = {
    "header": "true",
    "enforceSchema": "false",  # the header must match the contract column names
    "mode": "FAILFAST",
    "quote": '"',
    "escape": '"',  # RFC 4180: quotes inside values are doubled
    "encoding": "UTF-8",
    "nullValue": "",  # source convention: empty = NULL
    "ignoreLeadingWhiteSpace": "false",
    "ignoreTrailingWhiteSpace": "false",
}
JSON_OPTIONS: Final = {"mode": "FAILFAST", "encoding": "UTF-8"}
JSON_LINE_SCHEMA: Final = "value STRING"


class SourceContractError(ValueError):
    """The delivered columns differ from the source contract (a contract change: CRITICAL)."""


def source_schema(contract: DatasetContract) -> StructType:
    """Contract columns, in order, all nullable text (types are applied in Silver)."""
    return StructType([StructField(name, StringType(), True) for name in contract.column_names])


def check_json_keys(spark: SparkSession, contract: DatasetContract, path: str) -> None:
    """Fail when a JSON key is not in the contract, or a contract key is in no record.

    A JSON reader with an explicit schema silently ignores unknown keys, so a new source field
    would be lost in Bronze. An explicit ``null`` is data; a key absent from every record is a
    removed column. The collected set is the distinct key *names* (bounded by the contract
    size), never the data.
    """
    keys = (
        spark.read.schema(JSON_LINE_SCHEMA)
        .text(path)
        .select(F.explode(F.json_object_keys("value")).alias("key"))
        .distinct()
    )
    delivered = {row["key"] for row in keys.collect()}
    if not delivered:  # no record (an empty delivery): no key to compare, nothing to lose
        return
    expected = set(contract.column_names)
    problems = []
    if unexpected := sorted(delivered - expected):
        problems.append(f"unexpected keys: {', '.join(unexpected)}")
    if missing := sorted(expected - delivered):
        problems.append(f"missing keys: {', '.join(missing)}")
    if problems:
        raise SourceContractError(f"{contract.name} ({path}): {'; '.join(problems)}")


def read_source(spark: SparkSession, contract: DatasetContract, path: str) -> DataFrame:
    """Contract columns as text plus ``_source_file`` (from the file-source ``_metadata``)."""
    reader = spark.read.schema(source_schema(contract))
    if contract.file_format is FileFormat.CSV:
        raw = reader.options(**CSV_OPTIONS).csv(path)
    else:
        check_json_keys(spark, contract, path)
        raw = reader.options(**JSON_OPTIONS).json(path)
    return raw.select(
        *contract.column_names, F.col("_metadata.file_path").alias(SOURCE_FILE_COLUMN)
    )
