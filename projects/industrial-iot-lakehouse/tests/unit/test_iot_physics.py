"""The physical model is coherent: normal operation, spikes, drift and failures stay possible."""

from pathlib import Path

import pytest

from iot_lakehouse.synthetic.physics import (
    DRIFT_RELATIVE_CHANGE,
    FAILURE_MODES,
    NOISE_EXTREME_SIGMAS,
    OPERATING_PROFILES,
    PHYSICAL_LIMITS,
    SPIKE_SIGMAS,
    EquipmentType,
    SensorType,
    failure_modes_for,
)

DOC = Path("projects/industrial-iot-lakehouse/docs/DATA_CONTRACTS.md")
PROFILES = [
    (equipment, sensor, profile)
    for equipment, sensors in OPERATING_PROFILES.items()
    for sensor, profile in sensors.items()
]


def test_every_sensor_type_has_physical_limits() -> None:
    assert set(PHYSICAL_LIMITS) == set(SensorType)
    assert all(limit.minimum < limit.maximum for limit in PHYSICAL_LIMITS.values())


def test_every_equipment_type_has_a_profile_and_failure_modes() -> None:
    assert set(OPERATING_PROFILES) == set(EquipmentType)
    assert all(failure_modes_for(equipment) for equipment in EquipmentType)


def test_every_sensor_type_is_used_by_some_equipment() -> None:
    used = {sensor for sensors in OPERATING_PROFILES.values() for sensor in sensors}
    assert used == set(SensorType)


@pytest.mark.parametrize(("equipment", "sensor", "profile"), PROFILES, ids=str)
def test_normal_operation_plus_spikes_and_drift_stays_within_limits(
    equipment: EquipmentType, sensor: SensorType, profile: object
) -> None:
    limit = PHYSICAL_LIMITS[sensor]
    nominal, sd, diurnal = profile.nominal, profile.noise_sd, profile.diurnal_amplitude  # type: ignore[attr-defined]
    swing = nominal * (diurnal + DRIFT_RELATIVE_CHANGE[1])
    lowest = nominal - swing - NOISE_EXTREME_SIGMAS * sd  # spikes only go up
    highest = nominal + swing + (NOISE_EXTREME_SIGMAS + SPIKE_SIGMAS[1]) * sd
    assert limit.minimum < lowest
    assert highest < limit.maximum


@pytest.mark.parametrize("mode", FAILURE_MODES, ids=lambda m: f"{m.equipment_type}-{m.name}")
def test_failure_effects_are_anomalous_but_physically_possible(mode: object) -> None:
    profiles = OPERATING_PROFILES[mode.equipment_type]  # type: ignore[attr-defined]
    for sensor, change in mode.effects:  # type: ignore[attr-defined]
        assert sensor in profiles, f"{mode.equipment_type} has no {sensor} sensor"  # type: ignore[attr-defined]
        profile, limit = profiles[sensor], PHYSICAL_LIMITS[sensor]
        swing = profile.diurnal_amplitude + DRIFT_RELATIVE_CHANGE[1]
        sd = profile.noise_sd
        highest = (
            profile.nominal * (1 + change + swing) + (NOISE_EXTREME_SIGMAS + SPIKE_SIGMAS[1]) * sd
        )
        lowest = profile.nominal * (1 + change - swing) - NOISE_EXTREME_SIGMAS * sd
        assert abs(change) * profile.nominal > NOISE_EXTREME_SIGMAS * sd, (
            "effect must exceed the noise"
        )
        assert limit.minimum < lowest
        assert highest < limit.maximum


def _fmt(value: float) -> str:
    return str(int(value)) if value == int(value) else str(value)


@pytest.mark.parametrize("sensor", list(SensorType))
def test_documented_limits_match_the_code(repo_root: Path, sensor: SensorType) -> None:
    doc = (repo_root / DOC).read_text(encoding="utf-8")
    limit = PHYSICAL_LIMITS[sensor]
    assert f"| `{sensor}` | {limit.unit} | {_fmt(limit.minimum)} | {_fmt(limit.maximum)} |" in doc
