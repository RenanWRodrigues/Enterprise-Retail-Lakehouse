"""Physical model: sensor limits, normal operating profiles and failure modes.

Documented in docs/DATA_CONTRACTS.md; consistency (profiles inside limits, failure effects
physically possible) is verified by tests/unit/test_iot_physics.py.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Final


class EquipmentType(StrEnum):
    PUMP = "pump"
    ELECTRIC_MOTOR = "electric_motor"
    COMPRESSOR = "compressor"
    HYDRAULIC_SYSTEM = "hydraulic_system"


class SensorType(StrEnum):
    TEMPERATURE = "temperature"
    VIBRATION = "vibration"
    PRESSURE = "pressure"
    FLOW_RATE = "flow_rate"
    RPM = "rpm"
    CURRENT = "current"
    VOLTAGE = "voltage"
    POWER_CONSUMPTION = "power_consumption"


@dataclass(frozen=True, slots=True)
class PhysicalLimit:
    unit: str
    minimum: float
    maximum: float


@dataclass(frozen=True, slots=True)
class SensorProfile:
    nominal: float
    noise_sd: float
    diurnal_amplitude: float  # relative amplitude of the daily load cycle


@dataclass(frozen=True, slots=True)
class FailureMode:
    name: str
    equipment_type: EquipmentType
    effects: tuple[tuple[SensorType, float], ...]  # relative change reached at failure time


ST, ET = SensorType, EquipmentType

PHYSICAL_LIMITS: Final[dict[SensorType, PhysicalLimit]] = {
    ST.TEMPERATURE: PhysicalLimit("degC", -40.0, 200.0),
    ST.VIBRATION: PhysicalLimit("mm/s", 0.0, 50.0),
    ST.PRESSURE: PhysicalLimit("bar", 0.0, 350.0),
    ST.FLOW_RATE: PhysicalLimit("m3/h", 0.0, 500.0),
    ST.RPM: PhysicalLimit("rpm", 0.0, 6000.0),
    ST.CURRENT: PhysicalLimit("A", 0.0, 500.0),
    ST.VOLTAGE: PhysicalLimit("V", 0.0, 690.0),
    ST.POWER_CONSUMPTION: PhysicalLimit("kW", 0.0, 1000.0),
}


def _p(nominal: float, noise_sd: float, diurnal: float = 0.0) -> SensorProfile:
    return SensorProfile(nominal, noise_sd, diurnal)


# Which sensors each equipment type carries, and how they behave in normal operation.
OPERATING_PROFILES: Final[dict[EquipmentType, dict[SensorType, SensorProfile]]] = {
    ET.PUMP: {
        ST.TEMPERATURE: _p(55.0, 1.2, 0.03),
        ST.VIBRATION: _p(2.8, 0.25),
        ST.PRESSURE: _p(6.0, 0.15, 0.02),
        ST.FLOW_RATE: _p(120.0, 3.0, 0.05),
        ST.RPM: _p(1750.0, 8.0),
        ST.CURRENT: _p(32.0, 0.8, 0.04),
        ST.VOLTAGE: _p(380.0, 2.0),
        ST.POWER_CONSUMPTION: _p(18.5, 0.5, 0.04),
    },
    ET.ELECTRIC_MOTOR: {
        ST.TEMPERATURE: _p(65.0, 1.5, 0.03),
        ST.VIBRATION: _p(2.2, 0.2),
        ST.RPM: _p(1780.0, 6.0),
        ST.CURRENT: _p(45.0, 1.0, 0.05),
        ST.VOLTAGE: _p(380.0, 2.0),
        ST.POWER_CONSUMPTION: _p(22.0, 0.6, 0.05),
    },
    ET.COMPRESSOR: {
        ST.TEMPERATURE: _p(85.0, 2.0, 0.04),
        ST.VIBRATION: _p(4.0, 0.35),
        ST.PRESSURE: _p(8.5, 0.2, 0.03),
        ST.RPM: _p(2950.0, 10.0),
        ST.CURRENT: _p(65.0, 1.5, 0.05),
        ST.POWER_CONSUMPTION: _p(37.0, 1.0, 0.05),
    },
    ET.HYDRAULIC_SYSTEM: {
        ST.TEMPERATURE: _p(48.0, 1.0, 0.02),
        ST.VIBRATION: _p(1.8, 0.15),
        ST.PRESSURE: _p(180.0, 3.0, 0.02),
        ST.FLOW_RATE: _p(60.0, 1.5, 0.04),
        ST.POWER_CONSUMPTION: _p(22.0, 0.6, 0.04),
    },
}

FAILURE_MODES: Final[tuple[FailureMode, ...]] = (
    FailureMode("bearing_wear", ET.PUMP, ((ST.VIBRATION, 2.5), (ST.TEMPERATURE, 0.25))),
    FailureMode(
        "cavitation", ET.PUMP, ((ST.VIBRATION, 1.2), (ST.PRESSURE, -0.35), (ST.FLOW_RATE, -0.30))
    ),
    FailureMode(
        "seal_leak",
        ET.PUMP,
        ((ST.PRESSURE, -0.30), (ST.FLOW_RATE, -0.25), (ST.POWER_CONSUMPTION, 0.20)),
    ),
    FailureMode("bearing_wear", ET.ELECTRIC_MOTOR, ((ST.VIBRATION, 2.0), (ST.TEMPERATURE, 0.20))),
    FailureMode(
        "winding_insulation",
        ET.ELECTRIC_MOTOR,
        ((ST.TEMPERATURE, 0.40), (ST.CURRENT, 0.25), (ST.POWER_CONSUMPTION, 0.20)),
    ),
    FailureMode(
        "valve_failure",
        ET.COMPRESSOR,
        ((ST.PRESSURE, -0.30), (ST.POWER_CONSUMPTION, 0.20), (ST.TEMPERATURE, 0.15)),
    ),
    FailureMode("overheating", ET.COMPRESSOR, ((ST.TEMPERATURE, 0.35), (ST.CURRENT, 0.15))),
    FailureMode(
        "pump_wear",
        ET.HYDRAULIC_SYSTEM,
        ((ST.PRESSURE, -0.25), (ST.FLOW_RATE, -0.30), (ST.VIBRATION, 1.0)),
    ),
    FailureMode("fluid_leak", ET.HYDRAULIC_SYSTEM, ((ST.PRESSURE, -0.40), (ST.FLOW_RATE, -0.20))),
)

# Bounds used by the generator for statistical behaviour (not physical limits).
SPIKE_SIGMAS: Final = (6.0, 12.0)  # upward transient size, in noise standard deviations
NOISE_EXTREME_SIGMAS: Final = 6.0  # practical bound of Gaussian noise (P ~ 2e-9)
DRIFT_RELATIVE_CHANGE: Final = (0.05, 0.15)  # total calibration drift over an episode


def failure_modes_for(equipment_type: EquipmentType) -> tuple[FailureMode, ...]:
    """Failure modes that can affect ``equipment_type`` (and the sensors each one moves)."""
    return tuple(mode for mode in FAILURE_MODES if mode.equipment_type is equipment_type)
