"""Bronze readings -> Silver: pure ``DataFrame -> DataFrame`` steps (docs/SILVER.md).

1. ``normalize``: typed columns (UTC timestamps, double value, trimmed text); delivered text kept;
2. ``enrich``: sensor attributes; units converted to the sensor's canonical unit when exact;
3. rules (``silver_reading_rules``): unparseable text, then the shared row rules
   (``quality.suites.reading_row_rules``): ERROR rows go to quarantine with every reason;
4. ``deduplicate``: per ``reading_id`` in arrival order, the first arrival wins. A later copy with
   the same content is a ``duplicate``; with other content it is quarantined
   (``conflicting_resend``). An id already in Silver counts as the first arrival;
5. ``with_time_flags``: lateness and out-of-order arrival are flags. No reading is dropped for
   being late or out of order (there is no watermark).

Every Bronze row ends in exactly one of: valid, quarantine, duplicate (``SilverBatch``).
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Final

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, TimestampType

from iot_lakehouse.quality.suites import reading_row_rules
from iot_lakehouse.silver.units import is_convertible, normalized_unit, to_canonical
from lakehouse_common.quality import (
    CustomCondition,
    NotNull,
    QualityCheck,
    Severity,
    keep_first,
    split_by_quality,
)
from lakehouse_common.quality.runner import EXECUTION_TIMESTAMP_COLUMN, FAILED_CHECKS_COLUMN

CONFLICTING_RESEND: Final = "conflicting_resend"
CONTENT_HASH: Final = "_content_hash"
SOURCE_COLUMNS: Final = (
    "reading_id", "sensor_id", "event_timestamp", "ingestion_timestamp", "reading_value", "unit",
)  # fmt: skip
LINEAGE: Final = ("_file_id", "_source_file", "_batch_id")
_SRC: Final = "_src_"
_ARRIVAL: Final = "_arrival"
MEASUREMENT_COLUMNS: Final = ("reading_id", "sensor_id", "event_timestamp", "reading_value", "unit")
_SEPARATOR, _NULL = "\u001f", "\u0000"  # unit separator; a NULL marker distinct from ""
ERROR = Severity.ERROR


def _blank_to_null(column: Column) -> Column:
    trimmed = F.trim(column)
    return F.when(trimmed == "", None).otherwise(trimmed)


def _content_hash(text: dict[str, Column]) -> Column:
    """SHA-256 of the measurement as delivered (not of the arrival time): resend identity."""
    parts = [F.coalesce(text[c], F.lit(_NULL)) for c in MEASUREMENT_COLUMNS]
    return F.sha2(F.concat_ws(_SEPARATOR, *parts), 256)


def normalize(bronze: DataFrame) -> DataFrame:
    """Typed reading columns; the delivered text stays in ``_src_*`` (for the rules and the
    quarantine). ``_content_hash`` identifies the measurement as delivered (resend detection)."""
    text = {name: _blank_to_null(F.col(name)) for name in SOURCE_COLUMNS}
    return bronze.select(
        *(F.col(name).alias(f"{_SRC}{name}") for name in SOURCE_COLUMNS),
        text["reading_id"].alias("reading_id"),
        text["sensor_id"].alias("sensor_id"),
        text["event_timestamp"].try_cast(TimestampType()).alias("event_timestamp"),
        text["ingestion_timestamp"].try_cast(TimestampType()).alias("ingestion_timestamp"),
        text["reading_value"].try_cast("double").alias("source_value"),
        normalized_unit(F.col("unit")).alias("source_unit"),
        _content_hash(text).alias(CONTENT_HASH),
        *LINEAGE,
        F.col("_ingestion_timestamp").alias("bronze_ingestion_timestamp"),
    )


def sensor_reference(bronze_sensors: DataFrame) -> DataFrame:
    """Typed sensor attributes from the CMMS Bronze: the latest delivery per ``sensor_id``."""
    latest = keep_first(
        bronze_sensors,
        ("sensor_id",),
        (F.col("_ingestion_timestamp").desc(), F.col("_file_id").desc()),
    )
    return latest.select(
        F.trim("sensor_id").alias("sensor_id"),
        normalized_unit(F.col("unit")).alias("sensor_unit"),
        F.col("physical_min").try_cast("double").alias("physical_min"),
        F.col("physical_max").try_cast("double").alias("physical_max"),
    )


def enrich(normalized: DataFrame, sensors: DataFrame) -> DataFrame:
    """Sensor attributes (left join N:1 on sensor_id) and the value in the canonical unit."""
    joined = normalized.join(F.broadcast(sensors), "sensor_id", "left")
    value, unit = to_canonical(F.col("source_value"), F.col("source_unit"), F.col("sensor_unit"))
    return joined.select(
        "*",
        value.alias("reading_value"),
        unit.alias("unit"),
        is_convertible(F.col("source_unit"), F.col("sensor_unit")).alias("unit_converted"),
        F.col("sensor_unit").isNull().alias("unknown_sensor"),
    )


def _parsed(name: str, delivered: str, parsed: str) -> CustomCondition:
    """Delivered text that is present but does not parse (absent text: the NotNull rules)."""
    raw = f"{_SRC}{delivered}"
    return CustomCondition(
        name=f"unparseable_{name}",
        condition=f"{raw} IS NULL OR trim({raw}) = '' OR {parsed} IS NOT NULL",
        severity=ERROR,
    )


def silver_reading_rules() -> list[QualityCheck]:
    """Silver-only checks on the delivered text, then the shared row rules (all ERROR)."""
    return [
        NotNull("reading_id", severity=ERROR, name="missing_reading_id"),
        _parsed("event_timestamp", "event_timestamp", "event_timestamp"),
        _parsed("ingestion_timestamp", "ingestion_timestamp", "ingestion_timestamp"),
        _parsed("value", "reading_value", "source_value"),
        *reading_row_rules(),
    ]


def _arrival_order() -> list[Column]:
    """Total order of arrival: historian, Bronze, file, content (deterministic tie-break)."""
    return [
        F.col("ingestion_timestamp").asc_nulls_last(),
        F.col("bronze_ingestion_timestamp").asc(),
        F.col("_file_id").asc(),
        F.col(CONTENT_HASH).asc(),
    ]


@dataclass(frozen=True)
class Deduplicated:
    first: DataFrame  # first arrival of every reading_id not in Silver yet
    duplicates: DataFrame  # later copies with the same content (counted, not written)
    conflicts: DataFrame  # later copies with other content (quarantined)


def deduplicate(valid: DataFrame, published: DataFrame | None) -> Deduplicated:
    """First arrival wins, within the batch and across runs (``published``: reading_id and
    content hash already in Silver, which always arrived first)."""
    ranked = valid.withColumn(
        _ARRIVAL, F.row_number().over(Window.partitionBy("reading_id").orderBy(*_arrival_order()))
    )
    winner = ranked.where(F.col(_ARRIVAL) == 1).select(
        "reading_id", F.col(CONTENT_HASH).alias("_winner_hash")
    )
    if published is not None:  # an id already published: its stored content is the winner
        stored = published.select("reading_id", F.col(CONTENT_HASH).alias("_stored_hash"))
        winner = winner.join(stored, "reading_id", "left").select(
            "reading_id", F.coalesce("_stored_hash", "_winner_hash").alias("_winner_hash")
        )
        ranked = ranked.join(
            stored.select("reading_id", F.lit(True).alias("_is_stored")), "reading_id", "left"
        )
    else:
        ranked = ranked.withColumn("_is_stored", F.lit(None).cast("boolean"))
    rows = ranked.join(winner, "reading_id")
    is_first = (F.col(_ARRIVAL) == 1) & F.col("_is_stored").isNull()
    same = F.col(CONTENT_HASH) == F.col("_winner_hash")
    drop = [_ARRIVAL, "_is_stored", "_winner_hash"]
    return Deduplicated(
        first=rows.where(is_first).drop(*drop),
        duplicates=rows.where(~is_first & same).drop(*drop),
        conflicts=rows.where(~is_first & ~same).drop(*drop),
    )


def with_time_flags(
    readings: DataFrame, latest_event_by_sensor: DataFrame | None, *, late_after_seconds: int
) -> DataFrame:
    """``lateness_seconds`` (historian arrival - event), ``is_late`` and ``arrived_out_of_order``
    (the event is older than one already seen for the sensor: earlier in this batch's arrival
    order, or already in Silver). Flags only: nothing is dropped."""
    previous = F.max("event_timestamp").over(
        Window.partitionBy("sensor_id")
        .orderBy(*_arrival_order())
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    flagged = readings.withColumn("_previous_event", previous)
    if latest_event_by_sensor is not None:
        flagged = flagged.join(latest_event_by_sensor, "sensor_id", "left").withColumn(
            "_previous_event", F.greatest("_previous_event", "_stored_event")
        )
    else:
        flagged = flagged.withColumn("_stored_event", F.lit(None).cast(TimestampType()))
    lateness = F.unix_timestamp("ingestion_timestamp") - F.unix_timestamp("event_timestamp")
    return flagged.select(
        *readings.columns,
        lateness.cast("long").alias("lateness_seconds"),
        (lateness > F.lit(late_after_seconds)).alias("is_late"),
        F.coalesce(F.col("event_timestamp") < F.col("_previous_event"), F.lit(False)).alias(
            "arrived_out_of_order"
        ),
    )


@dataclass(frozen=True)
class SilverBatch:
    valid: DataFrame  # one row per new reading_id
    quarantine: DataFrame  # the Bronze row as delivered + reasons
    duplicates: DataFrame


def _as_quarantine(frame: DataFrame, at: datetime, reasons: Column | None = None) -> DataFrame:
    failed = reasons if reasons is not None else F.col(FAILED_CHECKS_COLUMN)
    return frame.select(
        *(F.col(f"{_SRC}{name}").alias(name) for name in SOURCE_COLUMNS),
        *LINEAGE,
        "bronze_ingestion_timestamp",
        CONTENT_HASH,
        failed.cast(ArrayType(StringType())).alias(FAILED_CHECKS_COLUMN),
        F.lit(at).cast(TimestampType()).alias(EXECUTION_TIMESTAMP_COLUMN),
    )


@dataclass(frozen=True)
class SilverHistory:
    """What the Silver already holds, as needed by a new batch (``None``: nothing yet)."""

    published: DataFrame | None = None  # reading_id, _content_hash
    latest_event_by_sensor: DataFrame | None = None  # sensor_id, _stored_event


def build_silver(
    bronze: DataFrame,
    sensors: DataFrame,
    history: SilverHistory,
    *,
    late_after_seconds: int,
    execution_timestamp: datetime,
) -> SilverBatch:
    """Every Bronze row into exactly one of valid, quarantine or duplicates."""
    enriched = enrich(normalize(bronze), sensors)
    valid, rejected = split_by_quality(
        enriched, silver_reading_rules(), execution_timestamp=execution_timestamp
    )
    dedup = deduplicate(valid, history.published)
    timed = with_time_flags(
        dedup.first, history.latest_event_by_sensor, late_after_seconds=late_after_seconds
    )
    quarantine = _as_quarantine(rejected, execution_timestamp).unionByName(
        _as_quarantine(dedup.conflicts, execution_timestamp, F.array(F.lit(CONFLICTING_RESEND)))
    )
    return SilverBatch(timed, quarantine, dedup.duplicates)
