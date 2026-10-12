"""Bronze versions -> Silver: the value the source publishes *now* is the one Silver keeps.

Regression for A -> B -> A: Bronze dropped the return to A (its content key already existed),
so Silver kept B. Written by ``LocalBronzeSink`` in three runs, read back with the explicit
schema and standardized by ``build_silver``.
"""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession

from financial_lakehouse.silver import build_silver, catalog_frame
from financial_lakehouse.silver.pipeline import read_bronze_json
from financial_lakehouse.storage import LocalBronzeSink
from financial_lakehouse.transformation import BronzeRecord

FIRST = datetime(2026, 10, 6, 14, 0, tzinfo=UTC)


def _delivery(value: str, day: int) -> BronzeRecord:
    at = FIRST + timedelta(days=day)
    return BronzeRecord.create(
        source="bcb_sgs",
        series_id="189",
        indicator_id="igpm_monthly",
        reference="01/02/2025",
        reference_date=date(2025, 2, 1),
        value=value,
        attributes={},
        request_timestamp=at,
        ingestion_timestamp=at,
        run_id=f"{at:%Y%m%dT%H%M%SZ}",
        raw_file="raw.json",
        raw_sha256="abc",
    )


def test_silver_follows_a_revision_back_to_the_original_value(
    spark: SparkSession, tmp_path: Path
) -> None:
    sink = LocalBronzeSink(tmp_path)
    inserted = [sink.merge([_delivery(v, day)]) for day, v in enumerate(["1.06", "1.10", "1.06"])]
    assert inserted == [1, 1, 1]
    assert sink.merge([_delivery("1.06", 3)]) == 0  # rerun of the revert

    bronze = read_bronze_json(spark, tmp_path)
    result = build_silver(bronze, catalog_frame(spark), execution_timestamp=FIRST)

    [row] = result.silver.collect()
    assert (row.value, row.revision_count) == (Decimal("1.06"), 3)
    assert result.quarantine.count() == 0
