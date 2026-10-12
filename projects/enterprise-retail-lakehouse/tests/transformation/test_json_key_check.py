"""The JSON key check of the Retail readers: drift fails, an empty delivery does not."""

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from retail_lakehouse.ingestion.readers import SourceContractError, check_json_keys
from retail_lakehouse.ingestion.source_contracts import RETAIL_SOURCE_CONTRACTS

ORDERS = RETAIL_SOURCE_CONTRACTS["orders"]


def _file(tmp_path: Path, lines: list[dict[str, str]]) -> str:
    path = tmp_path / "orders_b.jsonl"
    path.write_text("".join(json.dumps(line) + "\n" for line in lines), encoding="utf-8")
    return path.as_posix()


def test_an_empty_delivery_has_no_key_to_compare(spark: SparkSession, tmp_path: Path) -> None:
    """Regression: an incremental batch without orders writes an empty file; every contract key
    was reported missing and the whole Bronze run failed."""
    check_json_keys(spark, ORDERS, _file(tmp_path, []))


def test_an_unexpected_key_is_still_drift(spark: SparkSession, tmp_path: Path) -> None:
    row = dict.fromkeys(ORDERS.column_names, "x") | {"coupon": "X"}

    with pytest.raises(SourceContractError, match="unexpected keys: coupon"):
        check_json_keys(spark, ORDERS, _file(tmp_path, [row]))
