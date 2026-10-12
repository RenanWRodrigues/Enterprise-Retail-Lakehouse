"""Ingestion: indicator catalog and the incremental pipeline (APIs -> raw landing -> Bronze)."""

from financial_lakehouse.catalog import INDICATORS, Indicator, SourceKind, indicator
from financial_lakehouse.ingestion.pipeline import (
    IncrementalIngestion,
    IndicatorResult,
    IngestionConfig,
    IngestionFailedError,
    IngestionReport,
    IngestionSettings,
    RunMetrics,
    build_incremental_ingestion,
    load_settings,
)

__all__ = [
    "INDICATORS",
    "IncrementalIngestion",
    "Indicator",
    "IndicatorResult",
    "IngestionConfig",
    "IngestionFailedError",
    "IngestionReport",
    "IngestionSettings",
    "RunMetrics",
    "SourceKind",
    "build_incremental_ingestion",
    "indicator",
    "load_settings",
]
