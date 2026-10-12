"""IoT generation configuration: defaults produce a large volume; invalid values are rejected."""

from dataclasses import replace
from pathlib import Path

import pytest

from iot_lakehouse.synthetic import IotGenerationConfig, estimated_readings, load_config

CONFIG_FILE = Path("projects/industrial-iot-lakehouse/config/data_generation.toml")


def test_repository_config_targets_a_large_volume(repo_root: Path) -> None:
    config = load_config(repo_root / CONFIG_FILE)
    assert estimated_readings(config) >= 10_000_000


def test_estimated_readings_is_sensors_times_steps() -> None:
    config = IotGenerationConfig(
        days=1, sampling_interval_seconds=3600, equipment_per_type=1, lateness_threshold_minutes=120
    )
    sensors = 8 + 6 + 6 + 5
    assert estimated_readings(config) == sensors * 24


@pytest.mark.parametrize(
    "changes",
    [
        {"days": 0},
        {"sampling_interval_seconds": 30},
        {"equipment_per_type": 0},
        {"spike_rate": 1.5},
        {"duplicate_rate": -0.1},
        {"lateness_threshold_minutes": 1},  # must exceed the sampling interval
        {"drift_sensor_share": 2.0},
        {"failure_rate_per_year": -1.0},
    ],
)
def test_invalid_configuration_is_rejected(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        replace(IotGenerationConfig(), **changes)  # type: ignore[arg-type]


def test_start_must_be_timezone_aware() -> None:
    from datetime import datetime  # noqa: PLC0415

    with pytest.raises(ValueError, match="timezone"):
        IotGenerationConfig(start=datetime(2025, 1, 1))  # noqa: DTZ001 - naive on purpose
