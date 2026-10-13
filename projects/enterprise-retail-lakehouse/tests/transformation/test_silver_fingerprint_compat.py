"""``_row_hash`` stays byte-identical across the move to ``lakehouse_common.fingerprint``.

``_row_hash`` is persisted in the quarantine and is its MERGE key: a different value would
insert every quarantined row again. ``GOLDEN`` was recorded with the Retail implementation
before the move (2026-10-13); the reference recomputes it in Python from the contract.
"""

import hashlib
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from retail_lakehouse.bronze import bronze_schema
from retail_lakehouse.ingestion.source_contracts import RETAIL_SOURCE_CONTRACTS
from retail_lakehouse.silver.standardize import prepare

AT = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)
SHIPMENT = {
    "shipment_id": "SHP-1", "order_id": "ORD-1", "carrier_id": "CAR-01",
    "shipment_status": "in_transit", "shipped_at": "2025-03-06T08:00:00-03:00",
    "delivered_at": None, "freight_amount": "12.50",
}  # fmt: skip
GOLDEN = "2c3c4d77f37b7874d81d766b1e254a990f5236a30ea591d9a7666861f515a2fa"


def test_row_hash_is_unchanged(spark: SparkSession) -> None:
    contract = RETAIL_SOURCE_CONTRACTS["shipments"]
    names = contract.column_names
    row = (*(SHIPMENT[n] for n in names), AT, "landing/shipments_b1", "b1", AT.date())
    bronze = spark.createDataFrame([row], bronze_schema(contract))

    [prepared] = prepare(bronze, "shipments").select("_row_hash").collect()

    texts = [*(SHIPMENT[n] or "\u0000" for n in names), "b1"]  # no "" in the row
    reference = hashlib.sha256("\u001f".join(texts).encode("utf-8")).hexdigest()
    assert prepared["_row_hash"] == reference == GOLDEN
