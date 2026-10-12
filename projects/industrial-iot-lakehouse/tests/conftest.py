"""IoT test fixtures: a tiny, anomaly-rich configuration (one equipment per type, 3 days)."""

import csv
import gzip
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from iot_lakehouse.ingestion.source_contracts import IOT_SOURCE_CONTRACTS
from iot_lakehouse.synthetic import IotGenerationConfig, MasterData, generate_master_data

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

TINY_CONFIG = IotGenerationConfig(
    seed=11,
    start=datetime(2025, 3, 1, tzinfo=UTC),
    days=3,
    sampling_interval_seconds=300,
    equipment_per_type=1,
    failure_rate_per_year=240.0,  # ~2 per equipment in 3 days: every behaviour shows up
    preventive_interval_days=1,
    spike_rate=0.01,
    impossible_rate=0.005,
    duplicate_rate=0.02,
    out_of_order_rate=0.02,
    late_rate=0.01,
    comm_outages_per_sensor_month=20.0,
    drift_sensor_share=0.5,
    lateness_threshold_minutes=60,
)


@pytest.fixture(scope="session")
def iot_config() -> IotGenerationConfig:
    return TINY_CONFIG


@pytest.fixture(scope="session")
def iot_master() -> MasterData:
    return generate_master_data(TINY_CONFIG)


@pytest.fixture(scope="session")
def iot_readings(spark: "SparkSession", iot_master: MasterData) -> "DataFrame":
    """Labelled readings (contract columns + ground truth + internal columns), lazy."""
    from iot_lakehouse.synthetic import build_readings  # noqa: PLC0415 - lazy JVM import

    return build_readings(spark, TINY_CONFIG, iot_master)


# --- Bronze landing files ------------------------------------------------------------------
# Tiny landing zones in the generator's layout (no Spark): each test states exactly which files
# exist. Values are text as the sources deliver them.

EQUIPMENT_ROW: dict[str, str] = {
    "equipment_id": "EQ-1",
    "equipment_type": "pump",
    "model": "P-100",
    "manufacturer": "Acme",
    "site_id": "S1",
    "site_name": "Plant 1",
    "state": "SP",
    "installed_at": "2020-01-15",
    "rated_power_kw": "55.0",
}


def _reading(reading_id: str, value: Any = 12.5, **overrides: Any) -> dict[str, Any]:
    return {
        "reading_id": reading_id,
        "sensor_id": "SEN-1",
        "event_timestamp": "2025-01-03T10:00:00Z",
        "ingestion_timestamp": "2025-01-03T10:00:05Z",
        "reading_value": value,
        "unit": "C",
        **overrides,
    }


def _write_csv(
    root: Path, dataset: str, batch: str, rows: list[dict[str, str]],
    header: list[str] | None = None,
) -> Path:  # fmt: skip
    path = root / "cmms" / dataset / f"{dataset}_{batch}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = header or list(IOT_SOURCE_CONTRACTS[dataset].column_names)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=columns, lineterminator="\n", extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(rows)
    return path


def _readings_path(root: Path, batch: str, day: str, name: str) -> Path:
    path = root / "historian" / "sensor_readings" / f"batch_id={batch}" / f"ingestion_date={day}"
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{name}.json.gz"


def _write_readings(root: Path, batch: str, day: str, name: str, lines: list[Any]) -> Path:
    """One gzip JSON Lines file; a ``str`` line is written as is (to make malformed JSON)."""
    path = _readings_path(root, batch, day, name)
    text = "".join((line if isinstance(line, str) else json.dumps(line)) + "\n" for line in lines)
    path.write_bytes(gzip.compress(text.encode("utf-8"), mtime=0))
    return path


class LandingKit:
    """The helpers above, handed to tests as a fixture."""

    equipment_row = EQUIPMENT_ROW
    reading = staticmethod(_reading)
    write_csv = staticmethod(_write_csv)
    readings_path = staticmethod(_readings_path)
    write_readings = staticmethod(_write_readings)


@pytest.fixture
def landing_kit() -> LandingKit:
    return LandingKit()
