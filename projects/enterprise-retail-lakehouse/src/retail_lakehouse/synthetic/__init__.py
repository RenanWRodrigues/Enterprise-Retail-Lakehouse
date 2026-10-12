"""Deterministic synthetic source systems for the retail lakehouse (no Faker).

Generates the 11 source datasets described in docs/DATA_CONTRACTS.md, with controlled,
registered anomalies. Usage: docs/DATA_GENERATION.md, or
``python -m retail_lakehouse.synthetic --output data/landing/retail``.
"""

from retail_lakehouse.synthetic.anomalies import AnomalyRecord
from retail_lakehouse.synthetic.config import (
    SUPPORTED_ANOMALIES,
    AnomalyRule,
    AnomalyType,
    GenerationConfig,
    load_config,
)
from retail_lakehouse.synthetic.generator import GenerationSummary, generate_retail_dataset
from retail_lakehouse.synthetic.sinks import FileSink, MemorySink, Sink

__all__ = [
    "SUPPORTED_ANOMALIES",
    "AnomalyRecord",
    "AnomalyRule",
    "AnomalyType",
    "FileSink",
    "GenerationConfig",
    "GenerationSummary",
    "MemorySink",
    "Sink",
    "generate_retail_dataset",
    "load_config",
]
