"""Reading the landing zone: explicit text schema, every row and value as delivered."""

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

from retail_lakehouse.bronze import landing_path, source_schema
from retail_lakehouse.ingestion.readers import (
    SOURCE_FILE_COLUMN,
    SourceContractError,
    read_source,
)
from retail_lakehouse.ingestion.source_contracts import RETAIL_SOURCE_CONTRACTS
from retail_lakehouse.synthetic import MemorySink

DATASETS = sorted(RETAIL_SOURCE_CONTRACTS)


@pytest.mark.parametrize("dataset", DATASETS)
def test_reader_returns_contract_columns_as_text_plus_source_file(
    spark: SparkSession, landing_root: Path, dataset: str
) -> None:
    contract = RETAIL_SOURCE_CONTRACTS[dataset]

    raw = read_source(spark, contract, landing_path(landing_root.as_posix(), dataset, "initial"))

    assertSchemaEqual(
        raw.schema,
        StructType(
            [*source_schema(contract).fields, StructField(SOURCE_FILE_COLUMN, StringType(), False)]
        ),
    )


@pytest.mark.parametrize("dataset", DATASETS)
def test_reader_preserves_every_row_and_value(
    spark: SparkSession, landing_root: Path, landing_rows: MemorySink, dataset: str
) -> None:
    contract = RETAIL_SOURCE_CONTRACTS[dataset]
    path = landing_path(landing_root.as_posix(), dataset, "initial")

    raw = read_source(spark, contract, path)

    expected = spark.createDataFrame(
        [tuple(row.values()) for row in landing_rows.rows[dataset]], schema=source_schema(contract)
    )
    assertDataFrameEqual(raw.drop(SOURCE_FILE_COLUMN), expected)  # duplicates & anomalies included


def test_source_file_points_to_the_landing_file(spark: SparkSession, landing_root: Path) -> None:
    contract = RETAIL_SOURCE_CONTRACTS["stores"]
    raw = read_source(spark, contract, landing_path(landing_root.as_posix(), "stores", "initial"))

    files = {row[SOURCE_FILE_COLUMN] for row in raw.select(SOURCE_FILE_COLUMN).distinct().collect()}

    assert len(files) == 1
    assert files.pop().endswith("erp/stores/stores_initial.csv")


def test_csv_with_unexpected_header_fails_the_load(spark: SparkSession, tmp_path: Path) -> None:
    contract = RETAIL_SOURCE_CONTRACTS["stores"]
    bad = tmp_path / "stores.csv"
    bad.write_text(
        "store_id,store_name,store_kind,city,state,region,opened_at\n"
        "STR-001,Loja,physical,Recife,PE,Nordeste,2020-01-01\n",
        encoding="utf-8",
    )

    with pytest.raises(Exception, match=r"(?i)header"):  # JVM error type varies
        read_source(spark, contract, bad.as_posix()).collect()


def test_malformed_json_line_fails_the_load(spark: SparkSession, tmp_path: Path) -> None:
    contract = RETAIL_SOURCE_CONTRACTS["orders"]
    bad = tmp_path / "orders.jsonl"
    # First line complete (every contract key), second line truncated.
    bad.write_text(_order_line() + '\n{"order_id": \n', encoding="utf-8")

    with pytest.raises(Exception, match=r"(?i)malformed"):  # JVM error type varies
        read_source(spark, contract, bad.as_posix()).collect()


def _order_line(**changes: str | None) -> str:
    """One orders JSON line with every contract key; ``changes`` adds keys, ``None`` drops one."""
    row = dict.fromkeys(RETAIL_SOURCE_CONTRACTS["orders"].column_names, "x")
    for key, value in changes.items():
        if value is None:
            row.pop(key)
        else:
            row[key] = value
    return json.dumps(row)


def test_json_with_an_unexpected_key_fails_the_load(spark: SparkSession, tmp_path: Path) -> None:
    """Regression (compliance audit): the JSON reader dropped unknown keys silently.

    Medallion rule: a new source column is a contract change, so the load fails (CRITICAL)
    instead of losing the field. CSV already failed on a different header; JSON did not.
    """
    source = tmp_path / "orders.jsonl"
    lines = [_order_line(), _order_line(coupon_code="BF10")]
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(SourceContractError, match="unexpected keys: coupon_code"):
        read_source(spark, RETAIL_SOURCE_CONTRACTS["orders"], source.as_posix())


def test_json_without_a_contract_key_fails_the_load(spark: SparkSession, tmp_path: Path) -> None:
    source = tmp_path / "orders.jsonl"
    source.write_text(_order_line(currency=None) + "\n", encoding="utf-8")

    with pytest.raises(SourceContractError, match="missing keys: currency"):
        read_source(spark, RETAIL_SOURCE_CONTRACTS["orders"], source.as_posix())


def test_json_explicit_null_is_still_data(spark: SparkSession, tmp_path: Path) -> None:
    line = json.loads(_order_line())
    line["currency"] = None
    source = tmp_path / "orders.jsonl"
    source.write_text(json.dumps(line) + "\n", encoding="utf-8")

    [row] = read_source(spark, RETAIL_SOURCE_CONTRACTS["orders"], source.as_posix()).collect()

    assert row["currency"] is None
