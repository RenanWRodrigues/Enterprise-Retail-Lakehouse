"""Units: valid conversions to the sensor's canonical unit, everything else is a unit error.

The canonical unit of a reading is its sensor's unit in the CMMS master data (``sensors.unit``,
docs/DATA_CONTRACTS.md). A reading delivered in another unit **of the same quantity** with an
exact, known conversion (table below) is converted and marked ``unit_converted``; the delivered
value and unit are kept. Any other unit (another quantity, a typo, an unknown symbol) is left as
delivered, so the shared rule ``unit_mismatch`` quarantines the reading. No guessing: ``"C"`` is
not assumed to be Celsius.
"""

from dataclasses import dataclass
from typing import Final

from pyspark.sql import Column
from pyspark.sql import functions as F


@dataclass(frozen=True, slots=True)
class Conversion:
    """``canonical = delivered * scale + offset``."""

    scale: float
    offset: float = 0.0


# (delivered unit, canonical unit) -> conversion. Exact definitions only.
CONVERSIONS: Final[dict[tuple[str, str], Conversion]] = {
    ("degF", "degC"): Conversion(5 / 9, -32 * 5 / 9),
    ("K", "degC"): Conversion(1.0, -273.15),
    ("psi", "bar"): Conversion(0.0689475729),
    ("kPa", "bar"): Conversion(0.01),
    ("MPa", "bar"): Conversion(10.0),
    ("L/s", "m3/h"): Conversion(3.6),
    ("m3/s", "m3/h"): Conversion(3600.0),
    ("in/s", "mm/s"): Conversion(25.4),
    ("mA", "A"): Conversion(0.001),
    ("kA", "A"): Conversion(1000.0),
    ("kV", "V"): Conversion(1000.0),
    ("W", "kW"): Conversion(0.001),
    ("MW", "kW"): Conversion(1000.0),
}
# Spellings of the same unit (not conversions).
ALIASES: Final[dict[str, str]] = {"°C": "degC", "°F": "degF"}


def normalized_unit(unit: Column) -> Column:
    """Trimmed, with known spellings mapped to their symbol (``°C`` -> ``degC``)."""
    trimmed = F.trim(unit)
    mapped = trimmed
    for spelling, symbol in ALIASES.items():
        mapped = F.when(trimmed == spelling, F.lit(symbol)).otherwise(mapped)
    return mapped


def is_convertible(unit: Column, canonical: Column) -> Column:
    """TRUE when ``unit`` differs from ``canonical`` and the table converts it."""
    pairs = [(unit == F.lit(src)) & (canonical == F.lit(dst)) for src, dst in CONVERSIONS]
    return F.coalesce(F.greatest(*pairs) if len(pairs) > 1 else pairs[0], F.lit(False))


def to_canonical(value: Column, unit: Column, canonical: Column) -> tuple[Column, Column]:
    """``(value, unit)`` in the canonical unit when convertible; otherwise unchanged."""
    converted = value
    for (src, dst), rule in CONVERSIONS.items():
        matches = (unit == F.lit(src)) & (canonical == F.lit(dst))
        converted = F.when(matches, value * F.lit(rule.scale) + F.lit(rule.offset)).otherwise(
            converted
        )
    return converted, F.when(is_convertible(unit, canonical), canonical).otherwise(unit)
