"""Baseline degradation detector: daily sensor means against their own recent history.

Separate from the quality rules: those judge *records* (impossible values, units, timestamps);
this judges *behaviour* (a sensor drifting away from its own normal). Explainable by design:

1. daily mean per sensor (averages out noise and the daily load cycle);
2. baseline = median of the sensor's daily means on the previous ``baseline_days`` days of the
   same uninterrupted run (a day without readings, e.g. a stop, restarts the history);
3. relative deviation = |mean - baseline| / |baseline|; a sensor-day is an alert when the
   deviation reaches ``threshold`` and the baseline has at least ``min_history_days`` days;
4. an equipment-day is an alert when any of its sensors is; consecutive alert days are one
   alert event.

Input: valid readings (``sensor_id``, ``event_timestamp``, ``reading_value``) and the sensor
master data. Ground-truth labels are refused (``LeakageError``): the detector never sees them.
"""

from dataclasses import dataclass
from datetime import date
from typing import Final

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

GROUND_TRUTH_COLUMNS: Final = frozenset(
    {"failure_id", "drift_episode_id", "is_spike", "is_impossible", "is_duplicate", "copy_index",
     "degradation_change", "degradation_progress", "drift_offset", "drift_progress", "clean_value"}
)  # fmt: skip


class LeakageError(ValueError):
    """A ground-truth column reached the detector's input."""


@dataclass(frozen=True, slots=True)
class DetectorParams:
    threshold: float  # relative deviation that raises an alert (0.10 = 10%)
    baseline_days: int = 7
    min_history_days: int = 3

    def __post_init__(self) -> None:
        if (
            self.threshold <= 0
            or self.min_history_days < 1
            or self.baseline_days < self.min_history_days
        ):
            raise ValueError("threshold > 0 and 1 <= min_history_days <= baseline_days")


def refuse_labels(frame: DataFrame) -> None:
    leaked = sorted(GROUND_TRUTH_COLUMNS & set(frame.columns))
    if leaked:
        raise LeakageError(f"ground-truth columns in the detector input: {leaked}")


def daily_means(readings: DataFrame) -> DataFrame:
    """(sensor_id, day, mean): one row per sensor and UTC day with readings."""
    refuse_labels(readings)
    return readings.groupBy("sensor_id", F.to_date("event_timestamp").alias("day")).agg(
        F.avg("reading_value").alias("mean")
    )


def sensor_day_scores(means: DataFrame, params: DetectorParams) -> DataFrame:
    """Each sensor-day with its baseline, deviation and alert flag."""
    by_day = Window.partitionBy("sensor_id").orderBy("day")
    gap = F.datediff("day", F.lag("day").over(by_day)) > 1
    runs = means.withColumn(
        "_run", F.sum(F.when(gap, 1).otherwise(0)).over(by_day)
    )  # a missing day starts a new uninterrupted run
    history = (
        Window.partitionBy("sensor_id", "_run")
        .orderBy(F.col("day").cast("timestamp").cast("long"))
        .rangeBetween(-params.baseline_days * 86_400, -1)
    )
    scored = runs.withColumn("baseline", F.percentile_approx("mean", 0.5).over(history)).withColumn(
        "history_days", F.count("mean").over(history)
    )
    deviation = F.abs(F.col("mean") - F.col("baseline")) / F.abs(F.col("baseline"))
    enough = (F.col("history_days") >= params.min_history_days) & (F.col("baseline") != 0)
    return scored.select(
        "sensor_id", "day", "mean", "baseline", "history_days",
        F.when(enough, deviation).alias("deviation"),
        F.coalesce(enough & (deviation >= params.threshold), F.lit(False)).alias("alert"),
    )  # fmt: skip


def equipment_alert_days(scores: DataFrame, sensors: DataFrame) -> DataFrame:
    """(equipment_id, day, alert, sensors_alerting): any alerting sensor alerts its equipment."""
    joined = scores.join(F.broadcast(sensors.select("sensor_id", "equipment_id")), "sensor_id")
    return joined.groupBy("equipment_id", "day").agg(
        F.max(F.col("alert").cast("int")).cast("boolean").alias("alert"),
        F.sum(F.col("alert").cast("int")).alias("sensors_alerting"),
    )


@dataclass(frozen=True, slots=True, order=True)
class AlertEvent:
    equipment_id: str
    first_day: date
    last_day: date  # inclusive


def alert_events(days: list[tuple[str, date, bool]]) -> list[AlertEvent]:
    """Consecutive alert days of an equipment merged into one event (pure, sorted)."""
    events: list[AlertEvent] = []
    for equipment_id, day, alert in sorted(days):
        if not alert:
            continue
        last = events[-1] if events else None
        if last and last.equipment_id == equipment_id and (day - last.last_day).days == 1:
            events[-1] = AlertEvent(equipment_id, last.first_day, day)
        else:
            events.append(AlertEvent(equipment_id, day, day))
    return events
