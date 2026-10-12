"""Bronze layer: source data preserved as delivered, plus ingestion metadata.

See docs/BRONZE.md (project) and the ``medallion`` skill for what Bronze may and may not do.
"""

from retail_lakehouse.bronze.ingest import (
    BRONZE_TABLES,
    BronzeTarget,
    IngestionResult,
    IngestionStatus,
    batch_already_ingested,
    ingest_all,
    ingest_dataset,
    landing_path,
)
from retail_lakehouse.bronze.settings import BronzeSettings, load_bronze_settings
from retail_lakehouse.bronze.transformations import METADATA_COLUMNS, bronze_schema, to_bronze
from retail_lakehouse.ingestion.readers import source_schema

__all__ = [
    "BRONZE_TABLES",
    "METADATA_COLUMNS",
    "BronzeSettings",
    "BronzeTarget",
    "IngestionResult",
    "IngestionStatus",
    "batch_already_ingested",
    "bronze_schema",
    "ingest_all",
    "ingest_dataset",
    "landing_path",
    "load_bronze_settings",
    "source_schema",
    "to_bronze",
]
