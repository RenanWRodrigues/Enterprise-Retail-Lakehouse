"""Master data and episodes (small, pure Python, deterministic).

Equipment, sensors, failures, maintenance orders and the episodes that shape the readings:
progressive degradation before each failure, calibration drift and offline periods. Each
domain uses its own random stream, so changing one volume never reshuffles another.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import UTC, date, datetime, timedelta
from random import Random
from typing import Final

from iot_lakehouse.ingestion.source_contracts import IOT_SOURCE_CONTRACTS
from iot_lakehouse.synthetic.config import IotGenerationConfig
from iot_lakehouse.synthetic.physics import (
    DRIFT_RELATIVE_CHANGE,
    OPERATING_PROFILES,
    PHYSICAL_LIMITS,
    EquipmentType,
    SensorType,
    failure_modes_for,
)

SITES: Final = (
    ("SITE-01", "Planta Camaçari", "BA"),
    ("SITE-02", "Planta Cubatão", "SP"),
    ("SITE-03", "Planta Betim", "MG"),
    ("SITE-04", "Planta Canoas", "RS"),
    ("SITE-05", "Planta Manaus", "AM"),
)
MANUFACTURERS: Final = ("Fabricante Alfa", "Fabricante Beta", "Fabricante Gama")  # fictitious
MODEL_PREFIX: Final = {
    EquipmentType.PUMP: "PB",
    EquipmentType.ELECTRIC_MOTOR: "ME",
    EquipmentType.COMPRESSOR: "CP",
    EquipmentType.HYDRAULIC_SYSTEM: "HS",
}
SEVERITIES: Final = (("minor", 0.5), ("major", 0.35), ("critical", 0.15))
DEGRADATION_DAYS: Final = (2.0, 7.0)
DOWNTIME_HOURS: Final = (8.0, 72.0)
PREVENTIVE_HOURS: Final = (2.0, 6.0)
COMM_LOSS_MINUTES: Final = (10.0, 360.0)
RESPONSE_MINUTES: Final = (5.0, 60.0)  # failure -> corrective order opened
DISPATCH_MINUTES: Final = (15.0, 240.0)  # order opened -> work started
POWER_MARGIN: Final = (1.1, 1.4)  # rated power vs. nominal consumption
DRIFT_START_SHARE: Final = 0.6  # drifts start within the first 60% of the window
DAYS_PER_YEAR, DAYS_PER_MONTH = 365.0, 30.0


@dataclass(frozen=True, slots=True)
class Equipment:
    equipment_id: str
    equipment_type: EquipmentType
    model: str
    manufacturer: str
    site_id: str
    site_name: str
    state: str
    installed_at: date
    rated_power_kw: float


@dataclass(frozen=True, slots=True)
class Sensor:
    sensor_id: str
    equipment_id: str
    equipment_type: EquipmentType
    sensor_type: SensorType
    sampling_interval_seconds: int


@dataclass(frozen=True, slots=True)
class Failure:
    failure_id: str
    equipment_id: str
    failure_mode: str
    failure_timestamp: datetime
    severity: str
    degradation_start: datetime  # ground truth: not known by the CMMS
    downtime_end: datetime


@dataclass(frozen=True, slots=True)
class MaintenanceOrder:
    maintenance_order_id: str
    equipment_id: str
    order_type: str
    related_failure_id: str | None
    opened_at: datetime
    started_at: datetime
    completed_at: datetime | None
    status: str


@dataclass(frozen=True, slots=True)
class DegradationEpisode:
    failure_id: str
    equipment_id: str
    sensor_type: SensorType
    start: datetime
    end: datetime  # = failure_timestamp
    relative_change: float


@dataclass(frozen=True, slots=True)
class DriftEpisode:
    drift_episode_id: str
    sensor_id: str
    equipment_id: str
    start: datetime
    end: datetime
    relative_change: float


@dataclass(frozen=True, slots=True)
class OfflinePeriod:
    offline_id: str
    equipment_id: str
    sensor_id: str | None  # None = the whole equipment is down
    start: datetime
    end: datetime
    reason: str  # failure_downtime | preventive_maintenance | communication_loss
    reference_id: str | None


def _iso(value: datetime | date | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return value.isoformat()


@dataclass
class MasterData:
    equipment: list[Equipment] = field(default_factory=list)
    sensors: list[Sensor] = field(default_factory=list)
    failures: list[Failure] = field(default_factory=list)
    maintenance_orders: list[MaintenanceOrder] = field(default_factory=list)
    degradation_episodes: list[DegradationEpisode] = field(default_factory=list)
    drift_episodes: list[DriftEpisode] = field(default_factory=list)
    offline_periods: list[OfflinePeriod] = field(default_factory=list)

    def source_rows(self, dataset: str) -> list[dict[str, str | None]]:
        """CMMS rows as text, in contract column order."""
        by_dataset: dict[str, Sequence[object]] = {
            "equipment": self.equipment,
            "sensors": self.sensors,
            "failure_events": self.failures,
            "maintenance_orders": self.maintenance_orders,
        }
        records = by_dataset[dataset]
        rows = []
        for record in records:
            values = _row_values(record)
            rows.append({c: values[c] for c in IOT_SOURCE_CONTRACTS[dataset].column_names})
        return rows


def _row_values(record: object) -> dict[str, str | None]:
    if not is_dataclass(record) or isinstance(record, type):
        raise TypeError(f"expected a dataclass instance, got {type(record).__name__}")
    values: dict[str, str | None] = {}
    for column in fields(record):
        name = column.name
        value = getattr(record, name)
        values[name] = (
            _iso(value) if isinstance(value, date | datetime) or value is None else str(value)
        )
    if isinstance(record, Sensor):
        limit = PHYSICAL_LIMITS[record.sensor_type]
        values |= {
            "unit": limit.unit,
            "physical_min": str(limit.minimum),
            "physical_max": str(limit.maximum),
        }
    return values


def _stream(config: IotGenerationConfig, name: str) -> Random:
    return Random(f"{config.seed}:{name}")


def _between(rng: Random, start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=rng.uniform(0, (end - start).total_seconds()))


def _build_equipment(config: IotGenerationConfig, data: MasterData) -> None:
    rng = _stream(config, "equipment")
    number = 0
    for equipment_type, sensors in OPERATING_PROFILES.items():
        for _ in range(config.equipment_per_type):
            number += 1
            site_id, site_name, state = SITES[(number - 1) % len(SITES)]
            power = sensors[SensorType.POWER_CONSUMPTION].nominal * rng.uniform(*POWER_MARGIN)
            equipment_id = f"EQ-{number:04d}"
            data.equipment.append(
                Equipment(
                    equipment_id=equipment_id,
                    equipment_type=equipment_type,
                    model=f"{MODEL_PREFIX[equipment_type]}-{rng.randint(100, 999)}",
                    manufacturer=rng.choice(MANUFACTURERS),
                    site_id=site_id,
                    site_name=site_name,
                    state=state,
                    installed_at=(config.start - timedelta(days=rng.randint(180, 3650))).date(),
                    rated_power_kw=round(power, 1),
                )
            )
            for sensor_type in sensors:
                data.sensors.append(
                    Sensor(
                        sensor_id=f"SN-{len(data.sensors) + 1:06d}",
                        equipment_id=equipment_id,
                        equipment_type=equipment_type,
                        sensor_type=sensor_type,
                        sampling_interval_seconds=config.sampling_interval_seconds,
                    )
                )


def _build_failures(config: IotGenerationConfig, data: MasterData) -> None:
    """Poisson failures per equipment; degradation + downtime windows never overlap."""
    rng = _stream(config, "failures")
    per_day = config.failure_rate_per_year / DAYS_PER_YEAR
    for equipment in data.equipment:
        if per_day == 0:
            continue
        modes = failure_modes_for(equipment.equipment_type)
        busy_until = config.start - timedelta(days=DEGRADATION_DAYS[1])
        moment = config.start
        while True:
            moment += timedelta(days=rng.expovariate(per_day))
            if moment >= config.end:
                break
            degradation_start = moment - timedelta(days=rng.uniform(*DEGRADATION_DAYS))
            downtime_end = moment + timedelta(hours=rng.uniform(*DOWNTIME_HOURS))
            mode = rng.choice(modes)
            severity = rng.choices([s for s, _ in SEVERITIES], weights=[w for _, w in SEVERITIES])[
                0
            ]
            if degradation_start < busy_until:
                continue  # would overlap the previous failure
            failure = Failure(
                failure_id=f"FL-{len(data.failures) + 1:05d}",
                equipment_id=equipment.equipment_id,
                failure_mode=mode.name,
                failure_timestamp=moment,
                severity=severity,
                degradation_start=degradation_start,
                downtime_end=downtime_end,
            )
            data.failures.append(failure)
            busy_until = downtime_end
            for sensor_type, change in mode.effects:
                data.degradation_episodes.append(
                    DegradationEpisode(
                        failure.failure_id,
                        equipment.equipment_id,
                        sensor_type,
                        degradation_start,
                        moment,
                        change,
                    )
                )


def _next_order_id(data: MasterData) -> str:
    return f"MO-{len(data.maintenance_orders) + 1:06d}"


def _build_maintenance(config: IotGenerationConfig, data: MasterData) -> None:
    """Corrective orders first, then preventive ones: one random stream, drawn in this order."""
    rng = _stream(config, "maintenance")
    _build_corrective(config, data, rng)
    _build_preventive(config, data, rng)


def _build_corrective(config: IotGenerationConfig, data: MasterData, rng: Random) -> None:
    """One corrective order and one downtime offline period per failure."""
    for failure in data.failures:
        opened = failure.failure_timestamp + timedelta(minutes=rng.uniform(*RESPONSE_MINUTES))
        started = opened + timedelta(minutes=rng.uniform(*DISPATCH_MINUTES))
        done = failure.downtime_end <= config.end
        data.maintenance_orders.append(
            MaintenanceOrder(
                _next_order_id(data),
                failure.equipment_id,
                "corrective",
                failure.failure_id,
                opened,
                started,
                failure.downtime_end if done else None,
                "completed" if done else "in_progress",
            )
        )
        data.offline_periods.append(
            OfflinePeriod(
                f"OFF-{len(data.offline_periods) + 1:06d}",
                failure.equipment_id,
                None,
                failure.failure_timestamp,
                failure.downtime_end,
                "failure_downtime",
                failure.failure_id,
            )
        )


def _build_preventive(config: IotGenerationConfig, data: MasterData, rng: Random) -> None:
    """Periodic preventive orders per equipment, skipped when they overlap a failure."""
    interval = timedelta(days=config.preventive_interval_days)
    for equipment in data.equipment:
        busy = [
            (f.degradation_start, f.downtime_end)
            for f in data.failures
            if f.equipment_id == equipment.equipment_id
        ]
        started = config.start + timedelta(days=rng.uniform(0, config.preventive_interval_days))
        while started < config.end:
            finished = started + timedelta(hours=rng.uniform(*PREVENTIVE_HOURS))
            if all(finished <= begin or started >= end for begin, end in busy):
                done = finished <= config.end
                order_id = _next_order_id(data)
                data.maintenance_orders.append(
                    MaintenanceOrder(
                        order_id,
                        equipment.equipment_id,
                        "preventive",
                        None,
                        started - timedelta(days=1),
                        started,
                        finished if done else None,
                        "completed" if done else "in_progress",
                    )
                )
                data.offline_periods.append(
                    OfflinePeriod(
                        f"OFF-{len(data.offline_periods) + 1:06d}",
                        equipment.equipment_id,
                        None,
                        started,
                        finished,
                        "preventive_maintenance",
                        order_id,
                    )
                )
            started += interval


def _build_sensor_episodes(config: IotGenerationConfig, data: MasterData) -> None:
    comm = _stream(config, "communication")
    per_day = config.comm_outages_per_sensor_month / DAYS_PER_MONTH
    for sensor in data.sensors:
        moment = config.start
        while per_day > 0:
            moment += timedelta(days=comm.expovariate(per_day))
            if moment >= config.end:
                break
            data.offline_periods.append(
                OfflinePeriod(
                    f"OFF-{len(data.offline_periods) + 1:06d}",
                    sensor.equipment_id,
                    sensor.sensor_id,
                    moment,
                    moment + timedelta(minutes=comm.uniform(*COMM_LOSS_MINUTES)),
                    "communication_loss",
                    None,
                )
            )

    drift = _stream(config, "drift")
    preventive_starts: dict[str, list[datetime]] = {}
    for order in data.maintenance_orders:
        if order.order_type == "preventive":
            preventive_starts.setdefault(order.equipment_id, []).append(order.started_at)
    latest_start = config.start + (config.end - config.start) * DRIFT_START_SHARE
    for sensor in data.sensors:
        if drift.random() >= config.drift_sensor_share:
            continue
        start = _between(drift, config.start, latest_start)
        # Recalibration at the next preventive maintenance ends the drift.
        end = min(
            (t for t in preventive_starts.get(sensor.equipment_id, []) if t > start),
            default=config.end,
        )
        change = drift.uniform(*DRIFT_RELATIVE_CHANGE) * drift.choice((-1.0, 1.0))
        data.drift_episodes.append(
            DriftEpisode(
                f"DR-{len(data.drift_episodes) + 1:05d}",
                sensor.sensor_id,
                sensor.equipment_id,
                start,
                end,
                change,
            )
        )


def generate_master_data(config: IotGenerationConfig) -> MasterData:
    """All CMMS records and generation episodes for ``config`` (deterministic)."""
    data = MasterData()
    _build_equipment(config, data)
    _build_failures(config, data)
    _build_maintenance(config, data)
    _build_sensor_episodes(config, data)
    return data
