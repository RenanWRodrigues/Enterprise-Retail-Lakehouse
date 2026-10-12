"""Customer versions (input of SCD Type 2): a new version only when an attribute changes.

The CRM export has no change timestamp: the first version starts at ``registered_at`` and a
later change starts when the batch that revealed it was ingested.
"""

from datetime import UTC, date, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from retail_lakehouse.silver import silver_table_schema
from retail_lakehouse.silver.standardize import customer_versions

REGISTERED = datetime(2024, 2, 1, 12, tzinfo=UTC)
FIRST_BATCH = datetime(2026, 1, 15, 12, tzinfo=UTC)
SECOND_BATCH = datetime(2026, 2, 15, 12, tzinfo=UTC)


def _version(
    customer_id: str, segment: str, effective_from: datetime, at: datetime
) -> dict[str, Any]:
    return {
        "customer_id": customer_id,
        "city": "Recife",
        "state": "PE",
        "region": "Nordeste",
        "segment": segment,
        "birth_date": date(1990, 4, 20),
        "registered_at": REGISTERED,
        "effective_from": effective_from,
        "_ingestion_timestamp": at,
        "_source_file": f"customers_{at:%Y%m}.csv",
        "_batch_id": f"{at:%Y%m}",
    }


def _frame(spark: SparkSession, rows: list[dict[str, Any]]) -> DataFrame:
    schema = silver_table_schema("customers")
    return spark.createDataFrame([tuple(r[n] for n in schema.fieldNames()) for r in rows], schema)


def _versions(
    spark: SparkSession, observed: list[dict[str, Any]], stored: list[dict[str, Any]]
) -> dict[str, tuple[str, datetime]]:
    result = customer_versions(_frame(spark, observed), _frame(spark, stored))
    return {
        r["customer_id"]: (r["segment"], r["effective_from"].replace(tzinfo=None))
        for r in result.collect()
    }


def _local(instant: datetime) -> datetime:
    return instant.astimezone(tz=None).replace(tzinfo=None)  # how collect() renders instants


def test_new_change_and_unchanged_customers(spark: SparkSession) -> None:
    stored = [
        _version("CUS-SAME", "standard", REGISTERED, FIRST_BATCH),
        _version("CUS-CHANGED", "standard", REGISTERED, FIRST_BATCH),
    ]
    observed = [
        _version("CUS-NEW", "standard", REGISTERED, SECOND_BATCH),
        _version("CUS-SAME", "standard", REGISTERED, SECOND_BATCH),
        _version("CUS-CHANGED", "premium", REGISTERED, SECOND_BATCH),
    ]

    versions = _versions(spark, observed, stored)

    assert versions == {
        "CUS-NEW": ("standard", _local(REGISTERED)),  # first version: registration
        "CUS-CHANGED": ("premium", _local(SECOND_BATCH)),  # change: when it was observed
    }  # CUS-SAME: identical to its current version, nothing to insert


def test_the_latest_stored_version_is_the_reference(spark: SparkSession) -> None:
    """Back to an earlier value is a change: it is compared with the current version only."""
    stored = [
        _version("CUS-1", "standard", REGISTERED, FIRST_BATCH),
        _version("CUS-1", "premium", FIRST_BATCH, FIRST_BATCH),
    ]
    observed = [_version("CUS-1", "standard", REGISTERED, SECOND_BATCH)]

    assert _versions(spark, observed, stored) == {"CUS-1": ("standard", _local(SECOND_BATCH))}
