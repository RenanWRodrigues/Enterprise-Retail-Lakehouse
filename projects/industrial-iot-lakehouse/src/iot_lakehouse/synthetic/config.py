"""IoT generation configuration (``config/data_generation.toml``)."""

import tomllib
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final, Self

from iot_lakehouse.synthetic.physics import OPERATING_PROFILES

MIN_INTERVAL_SECONDS: Final = 60
MAX_INTERVAL_SECONDS: Final = 3600
SECONDS_PER_DAY: Final = 86_400
MAX_EQUIPMENT: Final = 9_999  # EQ-NNNN
_RATES: Final = (
    "spike_rate",
    "impossible_rate",
    "duplicate_rate",
    "out_of_order_rate",
    "late_rate",
    "drift_sensor_share",
)


@dataclass(frozen=True, slots=True)
class IotGenerationConfig:
    """Defaults: 120 equipment, 750 sensors, 30 days at 1-minute sampling (~32M readings)."""

    seed: int = 20250101
    start: datetime = datetime(2025, 1, 1, tzinfo=UTC)
    days: int = 30
    sampling_interval_seconds: int = 60
    equipment_per_type: int = 30
    failure_rate_per_year: float = 6.0
    preventive_interval_days: int = 14
    spike_rate: float = 0.0005
    impossible_rate: float = 0.0001
    duplicate_rate: float = 0.002
    out_of_order_rate: float = 0.003
    late_rate: float = 0.001
    comm_outages_per_sensor_month: float = 1.0
    drift_sensor_share: float = 0.05
    lateness_threshold_minutes: int = 60
    batch_id: str = "initial"

    def __post_init__(self) -> None:
        if self.start.tzinfo is None:
            raise ValueError("start must be timezone-aware (UTC)")
        if self.days < 1 or self.equipment_per_type < 1 or self.preventive_interval_days < 1:
            raise ValueError("days, equipment_per_type and preventive_interval_days must be >= 1")
        if self.equipment_per_type * len(OPERATING_PROFILES) > MAX_EQUIPMENT:
            raise ValueError(f"at most {MAX_EQUIPMENT} equipment")
        if not MIN_INTERVAL_SECONDS <= self.sampling_interval_seconds <= MAX_INTERVAL_SECONDS:
            bounds = f"[{MIN_INTERVAL_SECONDS}, {MAX_INTERVAL_SECONDS}]"
            raise ValueError(f"sampling_interval_seconds must be within {bounds}")
        if self.lateness_threshold_minutes * 60 <= self.sampling_interval_seconds:
            raise ValueError("lateness_threshold_minutes must exceed the sampling interval")
        for name in _RATES:
            if not 0.0 <= getattr(self, name) <= 1.0:
                raise ValueError(f"{name} must be within [0, 1]")
        if self.failure_rate_per_year < 0 or self.comm_outages_per_sensor_month < 0:
            raise ValueError("event rates must not be negative")
        if not self.batch_id.strip():
            raise ValueError("batch_id must not be blank")

    @property
    def steps(self) -> int:
        """Sampling instants per sensor."""
        return self.days * SECONDS_PER_DAY // self.sampling_interval_seconds

    @property
    def end(self) -> datetime:
        return self.start + timedelta(days=self.days)

    def with_seed(self, seed: int) -> Self:
        return replace(self, seed=seed)


def estimated_readings(config: IotGenerationConfig) -> int:
    """Sensors x sampling instants (before offline gaps and duplicates)."""
    sensors_per_unit = sum(len(sensors) for sensors in OPERATING_PROFILES.values())
    return config.equipment_per_type * sensors_per_unit * config.steps


_KEYS: Final = {f.name for f in fields(IotGenerationConfig)}


def load_config(path: Path) -> IotGenerationConfig:
    """Read ``[generation]`` and ``[anomalies]`` tables into one configuration."""
    with path.open("rb") as handle:
        document: dict[str, Any] = tomllib.load(handle)
    values = {**document.get("generation", {}), **document.get("anomalies", {})}
    unknown = set(values) - _KEYS
    if unknown:
        raise ValueError(f"unknown configuration keys: {sorted(unknown)}")
    return IotGenerationConfig(**values)
