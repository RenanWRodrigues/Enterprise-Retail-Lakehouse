"""``_content_hash`` stays byte-identical across the move to ``lakehouse_common.fingerprint``.

``_content_hash`` is stored in Silver: resend detection and the Silver MERGE compare against it,
so a different value would turn every resend of a stored reading into a conflict. ``GOLDEN`` was
recorded with the IoT implementation before the move (2026-10-13); the reference recomputes it
in Python from the trimmed measurement text (NULL for blank, U+0000 in its position).
"""

import hashlib
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from iot_lakehouse.bronze.ingest import bronze_schema
from iot_lakehouse.ingestion.source_contracts import IOT_SOURCE_CONTRACTS
from iot_lakehouse.silver.standardize import CONTENT_HASH, MEASUREMENT_COLUMNS, normalize

AT = datetime(2026, 10, 8, 12, tzinfo=UTC)
READING = {
    "reading_id": " R-1 ", "sensor_id": "SEN-T", "event_timestamp": "2025-01-03T10:00:00Z",
    "ingestion_timestamp": "2025-01-03T10:05:00Z", "reading_value": "20.0", "unit": "  ",
    "_ingestion_timestamp": AT, "_source_file": "/landing/f1.json.gz", "_batch_id": "b1",
    "_ingestion_date": AT.date(), "_file_id": "f1", "_source_system": "historian",
}  # fmt: skip
GOLDEN = "d109b593dffb653697cb1d555c8b88f414afe12846f3a4d18f11a7e505251460"


def test_content_hash_is_unchanged(spark: SparkSession) -> None:
    schema = bronze_schema(IOT_SOURCE_CONTRACTS["sensor_readings"])
    bronze = spark.createDataFrame([tuple(READING[f.name] for f in schema.fields)], schema)

    [row] = normalize(bronze).select(CONTENT_HASH).collect()

    texts = [str(READING[c]).strip() or "\u0000" for c in MEASUREMENT_COLUMNS]  # blank unit -> NULL
    reference = hashlib.sha256("\u001f".join(texts).encode("utf-8")).hexdigest()
    assert row[CONTENT_HASH] == reference == GOLDEN
