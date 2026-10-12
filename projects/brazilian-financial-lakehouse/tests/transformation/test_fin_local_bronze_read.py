"""Local Bronze (JSON Lines from LocalBronzeSink) read back by Spark with the explicit schema.

The local files store ``attributes`` as a JSON object and timestamps as ISO 8601 with
microseconds and offset; both must survive the read (no silent NULLs).
Regression: the reader used a Hadoop glob, which kills the JVM on Windows (winutils).
"""

from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from financial_lakehouse.silver.pipeline import read_bronze_json
from financial_lakehouse.storage import LocalBronzeSink
from financial_lakehouse.transformation import BronzeRecord

REQUESTED = datetime(2026, 10, 6, 15, 37, 41, 102411, tzinfo=UTC)


def test_local_bronze_round_trips_through_spark(spark: SparkSession, tmp_path: Path) -> None:
    record = BronzeRecord.create(
        source="bcb_ptax",
        series_id="EUR",
        indicator_id="eur_brl_ptax",
        reference="2025-01-02 13:10:25.056",
        reference_date=date(2025, 1, 2),
        value="6.42500",
        attributes={"tipoBoletim": "Fechamento"},
        request_timestamp=REQUESTED,
        ingestion_timestamp=REQUESTED,
        run_id="R1",
        raw_file="raw.json",
        raw_sha256="abc",
    )
    LocalBronzeSink(tmp_path).merge([record])

    [row] = read_bronze_json(spark, tmp_path.as_posix()).collect()

    assert (row.record_key, row.value, row.reference_date) == (
        record.record_key,
        "6.42500",
        date(2025, 1, 2),
    )
    assert row.request_timestamp == REQUESTED.astimezone().replace(tzinfo=None)  # local naive
    assert row.ingestion_timestamp is not None
    assert '"tipoBoletim"' in row.attributes and "Fechamento" in row.attributes


def test_an_empty_bronze_root_fails_clearly(spark: SparkSession, tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="no Bronze files"):
        read_bronze_json(spark, tmp_path)
