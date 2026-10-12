"""DeltaBronzeSink: insert-only MERGE of new versions (needs Delta: run in the Linux container)."""

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

from pyspark.sql import SparkSession

from financial_lakehouse.storage import DeltaBronzeSink
from financial_lakehouse.transformation import BronzeRecord

TS = datetime(2026, 10, 6, 14, 0, tzinfo=UTC)


def _record(value: str) -> BronzeRecord:
    return BronzeRecord.create(
        source="bcb_sgs",
        series_id="189",
        indicator_id="igpm_monthly",
        reference="01/02/2025",
        reference_date=date(2025, 2, 1),
        value=value,
        attributes={},
        request_timestamp=TS,
        ingestion_timestamp=TS,
        run_id="R1",
        raw_file="raw.json",
        raw_sha256="abc",
    )


def test_delta_merge_is_idempotent_and_keeps_revisions(delta_spark: SparkSession) -> None:
    delta_spark.sql("CREATE SCHEMA IF NOT EXISTS fin_bronze_it")
    sink = DeltaBronzeSink(delta_spark, "fin_bronze_it.bronze_indicator_observations")

    assert sink.merge([_record("1.06"), _record("1.06")]) == 1
    assert sink.merge([_record("1.06")]) == 0
    assert sink.merge([_record("1.07")]) == 1
    assert sink.count() == 2


def test_delta_merge_stores_a_value_returning_after_a_revision(delta_spark: SparkSession) -> None:
    delta_spark.sql("CREATE SCHEMA IF NOT EXISTS fin_bronze_it")
    sink = DeltaBronzeSink(delta_spark, "fin_bronze_it.bronze_revert")
    deliveries = [
        replace(_record(value), ingestion_timestamp=TS + timedelta(days=day))
        for day, value in enumerate(["1.06", "1.07", "1.06", "1.06"])
    ]

    assert [sink.merge([record]) for record in deliveries] == [1, 1, 1, 0]
    assert sink.count() == 3
