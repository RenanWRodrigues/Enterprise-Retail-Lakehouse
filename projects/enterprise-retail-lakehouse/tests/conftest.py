"""Retail test fixtures.

Small synthetic datasets are generated once per session (generation is pure Python,
but reused by several modules). Every supported anomaly is enabled at a rate high
enough to appear at small scale; a clean twin allows "with vs. without" comparisons.
"""

import platform
from collections.abc import Mapping
from dataclasses import replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from retail_lakehouse.silver.schemas import SILVER_SCHEMAS
from retail_lakehouse.synthetic import (
    SUPPORTED_ANOMALIES,
    AnomalyRule,
    FileSink,
    GenerationConfig,
    MemorySink,
    generate_retail_dataset,
)
from retail_lakehouse.synthetic.reference_data import CITIES

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import DataType

SMALL_ANOMALY_RATE = 0.1

SMALL_CONFIG = GenerationConfig(
    seed=7,
    start_date=date(2024, 1, 1),
    end_date=date(2025, 12, 31),
    customers=400,
    products=150,
    stores=5,
    suppliers=40,
    orders=2_000,
    anomalies=tuple(
        AnomalyRule(
            anomaly_type=anomaly_type, dataset=dataset, column=column, rate=SMALL_ANOMALY_RATE
        )
        for anomaly_type, dataset, column in SUPPORTED_ANOMALIES
    ),
)


@pytest.fixture(scope="session")
def small_config() -> GenerationConfig:
    return SMALL_CONFIG


@pytest.fixture(scope="session")
def small_dataset() -> MemorySink:
    """Small dataset with every supported anomaly enabled."""
    sink = MemorySink()
    generate_retail_dataset(SMALL_CONFIG, sink)
    return sink


@pytest.fixture(scope="session")
def clean_dataset() -> MemorySink:
    """Same configuration and seed, anomalies disabled."""
    sink = MemorySink()
    generate_retail_dataset(replace(SMALL_CONFIG, anomalies=()), sink)
    return sink


# Landing zone for Spark tests: fewer rows (each Spark action costs seconds on Windows),
# anomalies kept so Bronze can be shown to preserve them.
LANDING_CONFIG = replace(
    SMALL_CONFIG, customers=120, products=60, stores=4, suppliers=20, orders=300
)
SECOND_BATCH_CONFIG = replace(LANDING_CONFIG, seed=8, batch_id="batch_002")


@pytest.fixture(scope="session")
def landing_config() -> GenerationConfig:
    return LANDING_CONFIG


@pytest.fixture(scope="session")
def landing_rows() -> MemorySink:
    """Exactly what the ``initial`` landing batch contains, row by row."""
    sink = MemorySink()
    generate_retail_dataset(LANDING_CONFIG, sink)
    return sink


@pytest.fixture(scope="session")
def landing_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Landing zone with two batches: ``initial`` and ``batch_002``."""
    root = tmp_path_factory.mktemp("landing")
    for config in (LANDING_CONFIG, SECOND_BATCH_CONFIG):
        with FileSink(root, config=config) as sink:
            generate_retail_dataset(config, sink)
    return root


@pytest.fixture(scope="session")
def second_batch_rows() -> MemorySink:
    sink = MemorySink()
    generate_retail_dataset(SECOND_BATCH_CONFIG, sink)
    return sink


# --- Gold inputs ---------------------------------------------------------------------------
# Gold tests consume the Silver *contract* built from the generator's CLEAN output (no
# anomalies). tests/integration/test_silver_build.py proves the real Silver, built from the clean
# landing zone, is identical to it.
GOLD_SOURCE_CONFIG = replace(LANDING_CONFIG, anomalies=())
CALENDAR_START = GOLD_SOURCE_CONFIG.start_date
CALENDAR_END = GOLD_SOURCE_CONFIG.end_date + timedelta(days=31)
_STATE_REGION = {city.state: city.region for city in CITIES}


def _typed(value: str | None, data_type: "DataType") -> object:
    from pyspark.sql import types as T  # noqa: PLC0415, N812 - lazy JVM import

    if value is None:
        return None
    converters: dict[type, object] = {
        T.StringType: str,
        T.IntegerType: int,
        T.DecimalType: Decimal,
        T.DateType: date.fromisoformat,
        T.TimestampType: datetime.fromisoformat,
        T.BooleanType: lambda v: v == "true",
    }
    return converters[type(data_type)](value)  # type: ignore[operator]


def _silver_row(entity: str, row: Mapping[str, str | None]) -> tuple[object, ...]:
    values = dict(row)
    if entity == "customers":  # Silver enriches region and opens the first SCD2 version
        values["region"] = _STATE_REGION[str(row["state"])]
        values["effective_from"] = row["registered_at"]
    return tuple(_typed(values[f.name], f.dataType) for f in SILVER_SCHEMAS[entity].fields)


@pytest.fixture(scope="session")
def gold_source_rows() -> MemorySink:
    """Clean source rows behind ``silver_tables`` (the oracle for Gold expectations)."""
    sink = MemorySink()
    generate_retail_dataset(GOLD_SOURCE_CONFIG, sink)
    return sink


@pytest.fixture(scope="session")
def silver_tables(spark: "SparkSession", gold_source_rows: MemorySink) -> dict[str, "DataFrame"]:
    return {
        entity: spark.createDataFrame(
            [_silver_row(entity, row) for row in gold_source_rows.rows[entity]], schema=schema
        )
        for entity, schema in SILVER_SCHEMAS.items()
    }


@pytest.fixture(scope="session")
def gold_calendar() -> tuple[date, date]:
    return CALENDAR_START, CALENDAR_END


FAR_FUTURE_ON_WINDOWS = (
    "PySpark converts TIMESTAMP to datetime with the local-time C API, which on Windows rejects "
    "years beyond 3000 (OSError 22); SCD2 rows end at 9999-12-31. Gold tests run on Linux "
    "(GitHub Actions, docker/tests.Dockerfile)."
)


@pytest.fixture(scope="session")
def gold_tables(silver_tables: dict[str, "DataFrame"]) -> dict[str, "DataFrame"]:
    from retail_lakehouse.gold import build_gold  # noqa: PLC0415 - lazy JVM import

    if platform.system() == "Windows":
        pytest.skip(FAR_FUTURE_ON_WINDOWS)

    return build_gold(silver_tables, calendar_start=CALENDAR_START, calendar_end=CALENDAR_END)
