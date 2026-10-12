"""``to_bronze``: source values untouched, metadata added, nothing filtered or fixed."""

from datetime import UTC, date, datetime, timedelta, timezone

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

from retail_lakehouse.bronze import bronze_schema, source_schema, to_bronze
from retail_lakehouse.ingestion.source_contracts import RETAIL_SOURCE_CONTRACTS

SUPPLIERS = RETAIL_SOURCE_CONTRACTS["suppliers"]
INGESTED_AT = datetime(2026, 1, 15, 23, 30, tzinfo=UTC)
SOURCE_FILE = "file:/landing/erp/suppliers/suppliers_initial.csv"
RAW_SCHEMA = StructType(  # as returned by read_source
    [*source_schema(SUPPLIERS).fields, StructField("_source_file", StringType(), False)]
)

# Deliberately dirty rows: Bronze must keep all of them exactly as delivered.
RAW_ROWS = [
    ("SUP-0001", "Silva Ltda", "contato.sup0001@example.net", "Recife", "PE", "12", "2020-01-01"),
    ("SUP-0002", " Souza Ltda ", "contato.sup0002 @example.net", "Natal", "RN", "-3", "31/02/2020"),
    ("SUP-0002", " Souza Ltda ", "contato.sup0002 @example.net", "Natal", "RN", "-3", "31/02/2020"),
    ("SUP-0004", None, None, "", "SP", "abc", None),
]


def _raw(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([(*row, SOURCE_FILE) for row in RAW_ROWS], schema=RAW_SCHEMA)


def test_source_values_and_duplicates_are_preserved(spark: SparkSession) -> None:
    # Act
    bronze = to_bronze(_raw(spark), SUPPLIERS, batch_id="initial", ingestion_timestamp=INGESTED_AT)

    # Assert
    expected = spark.createDataFrame(RAW_ROWS, schema=source_schema(SUPPLIERS))
    assertDataFrameEqual(bronze.select(*SUPPLIERS.column_names), expected)


def test_metadata_columns_are_added(spark: SparkSession) -> None:
    bronze = to_bronze(_raw(spark), SUPPLIERS, batch_id="initial", ingestion_timestamp=INGESTED_AT)

    rendered = bronze.select(
        F.date_format("_ingestion_timestamp", "yyyy-MM-dd HH:mm:ss").alias("ingested_at"),
        "_source_file",
        "_batch_id",
        "_ingestion_date",
    ).distinct()
    assertDataFrameEqual(
        rendered,
        [
            Row(
                ingested_at="2026-01-15 23:30:00",
                _source_file=SOURCE_FILE,
                _batch_id="initial",
                _ingestion_date=date(2026, 1, 15),
            )
        ],
    )


def test_ingestion_date_is_the_utc_date(spark: SparkSession) -> None:
    brasilia_evening = datetime(2026, 1, 15, 22, 0, tzinfo=timezone(timedelta(hours=-3)))

    bronze = to_bronze(_raw(spark), SUPPLIERS, batch_id="b", ingestion_timestamp=brasilia_evening)

    [row] = bronze.select("_ingestion_date").distinct().collect()
    assert row["_ingestion_date"] == date(2026, 1, 16)


def test_output_schema_is_the_bronze_contract(spark: SparkSession) -> None:
    bronze = to_bronze(_raw(spark), SUPPLIERS, batch_id="initial", ingestion_timestamp=INGESTED_AT)
    assertSchemaEqual(bronze.schema, bronze_schema(SUPPLIERS))


def test_empty_input_stays_empty(spark: SparkSession) -> None:
    empty = spark.createDataFrame([], schema=RAW_SCHEMA)
    bronze = to_bronze(empty, SUPPLIERS, batch_id="initial", ingestion_timestamp=INGESTED_AT)
    assert bronze.isEmpty()


def test_naive_timestamp_is_rejected(spark: SparkSession) -> None:
    naive = datetime(2026, 1, 15)  # noqa: DTZ001 - naive on purpose
    with pytest.raises(ValueError, match="timezone"):
        to_bronze(_raw(spark), SUPPLIERS, batch_id="initial", ingestion_timestamp=naive)


def test_blank_batch_id_is_rejected(spark: SparkSession) -> None:
    with pytest.raises(ValueError, match="batch_id"):
        to_bronze(_raw(spark), SUPPLIERS, batch_id=" ", ingestion_timestamp=INGESTED_AT)
