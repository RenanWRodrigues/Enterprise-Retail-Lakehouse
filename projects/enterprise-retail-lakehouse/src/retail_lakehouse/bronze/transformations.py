"""Bronze transformation: source columns untouched + ingestion metadata.

Bronze never filters, deduplicates, trims, casts or corrects: duplicates, NULLs and
invalid values reach the table exactly as delivered (they are handled in Silver).
"""

from datetime import UTC, datetime
from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

from retail_lakehouse.ingestion.readers import SOURCE_FILE_COLUMN, source_schema
from retail_lakehouse.ingestion.source_contracts import DatasetContract

INGESTION_TIMESTAMP_COLUMN: Final = "_ingestion_timestamp"
BATCH_ID_COLUMN: Final = "_batch_id"
INGESTION_DATE_COLUMN: Final = "_ingestion_date"
METADATA_COLUMNS: Final = (
    INGESTION_TIMESTAMP_COLUMN,
    SOURCE_FILE_COLUMN,
    BATCH_ID_COLUMN,
    INGESTION_DATE_COLUMN,
)


def bronze_schema(contract: DatasetContract) -> StructType:
    """Schema of a Bronze table: the source text columns plus non-null metadata."""
    return StructType(
        [
            *source_schema(contract).fields,
            StructField(INGESTION_TIMESTAMP_COLUMN, TimestampType(), False),
            StructField(SOURCE_FILE_COLUMN, StringType(), False),
            StructField(BATCH_ID_COLUMN, StringType(), False),
            StructField(INGESTION_DATE_COLUMN, DateType(), False),
        ]
    )


def to_bronze(
    raw: DataFrame,
    contract: DatasetContract,
    *,
    batch_id: str,
    ingestion_timestamp: datetime,
) -> DataFrame:
    """Add ``_ingestion_timestamp``, ``_batch_id`` and ``_ingestion_date`` (UTC date).

    ``raw`` must carry the contract columns and ``_source_file`` (see ``read_source``).
    The timestamp is injected by the caller, so the transformation is deterministic.
    """
    if ingestion_timestamp.tzinfo is None:
        raise ValueError("ingestion_timestamp must be timezone-aware (use UTC)")
    if not batch_id.strip():
        raise ValueError("batch_id must not be blank")
    ingested_at = ingestion_timestamp.astimezone(UTC)
    return raw.select(
        *contract.column_names,
        F.lit(ingested_at).cast(TimestampType()).alias(INGESTION_TIMESTAMP_COLUMN),
        F.col(SOURCE_FILE_COLUMN),
        F.lit(batch_id).alias(BATCH_ID_COLUMN),
        F.lit(ingested_at.date()).alias(INGESTION_DATE_COLUMN),
    )
