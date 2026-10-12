"""IoT quality suite for sensor readings (declarations; evaluation in lakehouse_common.quality).

Critical rules requested for IoT (docs/DATA_QUALITY.md §8):

- nonexistent sensor: ``sensor_id`` absent from the sensor master data is WARNING (ADR 007:
  master data may arrive late) and escalates to CRITICAL above 5% of the readings;
- invalid timestamp: NULL, earlier than ``MIN_VALID_TIMESTAMP`` (device clock reset) or later
  than its own ingestion plus ``CLOCK_SKEW_TOLERANCE`` (a reading from the future): ERROR.
  *Late* readings (event long before ingestion) are valid data, not a quality failure;
- physically impossible values: outside the sensor's ``[physical_min, physical_max]``, NaN or
  infinite, or with a unit different from the sensor's: ERROR (quarantined);
- duplicates: gateway resends repeat ``reading_id`` (DATA_CONTRACTS.md). ``deduplicate`` keeps
  the first arrival (``duplicate_rows`` = input - output); ``Unique(reading_id)`` is then a
  CRITICAL gate, and two *different* ids for the same sensor and instant are ERROR.
"""

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DataType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from iot_lakehouse.gold.schemas import FOREIGN_KEYS as GOLD_FOREIGN_KEYS
from iot_lakehouse.gold.schemas import GRAIN as GOLD_GRAIN
from iot_lakehouse.ingestion.source_contracts import IOT_SOURCE_CONTRACTS
from lakehouse_common.quality import (
    CheckSet,
    CustomCondition,
    NotNull,
    QualityCheck,
    ReferentialIntegrity,
    SchemaMatches,
    Severity,
    Unique,
    keep_first,
)

SYSTEMIC_ORPHAN_RATE: Final = 0.05
MIN_VALID_TIMESTAMP: Final = datetime(2000, 1, 1, tzinfo=UTC)
CLOCK_SKEW_TOLERANCE: Final = timedelta(minutes=5)
SENSOR_ATTRIBUTES: Final = ("sensor_unit", "physical_min", "physical_max")
LOGICAL_TYPES: Final[dict[str, DataType]] = {
    "string": StringType(),
    "integer": IntegerType(),
    "double": DoubleType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}
CRITICAL, ERROR, WARNING = Severity.CRITICAL, Severity.ERROR, Severity.WARNING


def contract_schema(dataset: str) -> StructType:
    """Typed Spark schema of a source contract (logical types -> Spark types)."""
    contract = IOT_SOURCE_CONTRACTS[dataset]
    return StructType(
        [StructField(c.name, LOGICAL_TYPES[c.logical_type], c.nullable) for c in contract.columns]
    )


READINGS_SCHEMA: Final = contract_schema("sensor_readings")


def deduplicate_readings(readings: DataFrame) -> DataFrame:
    """One row per ``reading_id``: the first arrival (resends are exact copies)."""
    return keep_first(readings, ("reading_id",), (F.col("ingestion_timestamp").asc_nulls_last(),))


def with_sensor_attributes(readings: DataFrame, sensors: DataFrame) -> DataFrame:
    """Readings plus their sensor's unit and physical limits (left join, N:1 on sensor_id)."""
    attributes = sensors.select(
        "sensor_id",
        F.col("unit").alias("sensor_unit"),
        F.col("physical_min").cast("double").alias("physical_min"),
        F.col("physical_max").cast("double").alias("physical_max"),
    ).dropDuplicates(["sensor_id"])
    return readings.join(F.broadcast(attributes), "sensor_id", "left").select(
        *readings.columns, *SENSOR_ATTRIBUTES
    )


def reading_row_rules() -> list[QualityCheck]:
    """Row-level rules of a reading (ERROR: the row is quarantined), declared once.

    Used by ``iot_readings_suite`` and by the Silver standardization (quarantine). They read the
    typed columns and the sensor attributes (``with_sensor_attributes``); ``reading_value`` and
    ``unit`` are in the sensor's canonical unit when the Silver converted them.
    """
    earliest = MIN_VALID_TIMESTAMP.strftime("%Y-%m-%d %H:%M:%S")
    skew = int(CLOCK_SKEW_TOLERANCE.total_seconds())
    return [
        NotNull("sensor_id", severity=ERROR, name="missing_sensor"),
        NotNull("event_timestamp", severity=ERROR, name="missing_event_timestamp"),
        CustomCondition(
            name="invalid_event_timestamp",
            condition=(
                f"event_timestamp IS NULL OR (event_timestamp >= TIMESTAMP '{earliest}' "
                f"AND (ingestion_timestamp IS NULL OR event_timestamp <= "
                f"ingestion_timestamp + INTERVAL {skew} SECONDS))"
            ),
            severity=ERROR,
        ),
        NotNull("reading_value", severity=ERROR, name="missing_value"),
        CustomCondition(
            name="physically_impossible_value",
            condition=(
                "reading_value IS NULL OR (NOT isnan(reading_value) "
                "AND reading_value NOT IN (double('inf'), double('-inf')) "
                "AND (physical_min IS NULL OR reading_value >= physical_min) "
                "AND (physical_max IS NULL OR reading_value <= physical_max))"
            ),
            severity=ERROR,
        ),
        CustomCondition(
            name="unit_mismatch",
            condition="sensor_unit IS NULL OR unit = sensor_unit",
            severity=ERROR,
        ),
    ]


def _readings_checks(sensors: DataFrame) -> list[QualityCheck]:
    missing_sensor, *row_rules = reading_row_rules()
    return [
        NotNull("reading_id", severity=CRITICAL),
        Unique(("reading_id",), severity=CRITICAL, name="duplicate__reading_id"),
        Unique(("sensor_id", "event_timestamp"), severity=ERROR, name="conflicting_readings"),
        missing_sensor,
        ReferentialIntegrity(
            "sensor_id", sensors, "sensor_id", severity=WARNING, name="unknown_sensor"
        ),
        ReferentialIntegrity(
            "sensor_id",
            sensors,
            "sensor_id",
            severity=CRITICAL,
            threshold=SYSTEMIC_ORPHAN_RATE,
            name="unknown_sensor_systemic",
        ),
        *row_rules,
    ]


def iot_readings_suite(readings: DataFrame, sensors: DataFrame) -> list[CheckSet]:
    """Contract check on the readings as delivered, then the row rules on the enriched rows.

    ``readings`` should already be de-duplicated (``deduplicate_readings``).
    """
    schema = SchemaMatches(READINGS_SCHEMA, severity=CRITICAL, name="schema__sensor_readings")
    return [
        CheckSet("silver_sensor_readings", readings, [schema]),
        CheckSet(
            "silver_sensor_readings",
            with_sensor_attributes(readings, sensors),
            _readings_checks(sensors),
        ),
    ]


_RELIABILITY_RULES: Final = (
    ("availability_out_of_range", "availability IS NULL OR availability BETWEEN 0 AND 1"),
    (
        "negative_or_inconsistent_time",
        "planned_seconds >= 0 AND unplanned_seconds >= 0 AND operating_seconds >= 0 "
        "AND operating_seconds <= scheduled_seconds AND scheduled_seconds <= observed_seconds",
    ),
    ("mtbf_without_failures", "(failures = 0) = (mtbf_seconds IS NULL)"),
    ("mttr_without_repairs", "(repairs = 0) = (mttr_seconds IS NULL)"),
)


def _gold_checks(name: str, tables: Mapping[str, DataFrame]) -> list[QualityCheck]:
    """Grain, keys and foreign keys of a Gold table, plus the reliability invariants (CRITICAL)."""
    grain = GOLD_GRAIN[name]
    checks: list[QualityCheck] = [
        Unique(grain, severity=CRITICAL, name=f"grain__{name}"),
        *(NotNull(column, severity=CRITICAL) for column in grain),
    ]
    for column, dimension in GOLD_FOREIGN_KEYS.get(name, ()):
        key = GOLD_GRAIN[dimension][0]
        checks.append(
            ReferentialIntegrity(
                column, tables[dimension], key, severity=CRITICAL, name=f"fk__{column}__{dimension}"
            )
        )
    if name in ("agg_equipment_daily", "agg_equipment_reliability"):
        checks += [
            CustomCondition(name=rule, condition=condition, severity=CRITICAL)
            for rule, condition in _RELIABILITY_RULES
        ]
    return checks


def iot_gold_suite(tables: Mapping[str, DataFrame]) -> list[CheckSet]:
    """The publication gate: every Gold table, dimensions first (nothing published on CRITICAL)."""
    missing = sorted(set(GOLD_GRAIN) - set(tables))
    if missing:
        raise ValueError(f"missing Gold tables: {missing}")
    return [CheckSet(name, tables[name], _gold_checks(name, tables)) for name in GOLD_GRAIN]
