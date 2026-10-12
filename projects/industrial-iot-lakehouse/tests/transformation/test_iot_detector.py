"""Baseline detector on small daily series with known answers (no ground truth involved)."""

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta

import pytest
from pyspark.sql import DataFrame, SparkSession

from iot_lakehouse.evaluation.detector import (
    DetectorParams,
    LeakageError,
    daily_means,
    equipment_alert_days,
    sensor_day_scores,
)

PARAMS = DetectorParams(threshold=0.05, baseline_days=7, min_history_days=3)
Series = Mapping[str, Sequence[float | None]]  # sensor -> daily means from 2025-01-01


def _means(spark: SparkSession, series: Series) -> DataFrame:
    """Daily means from 2025-01-01; ``None`` = a day without readings."""
    rows = [
        (sensor, date(2025, 1, 1) + timedelta(days=i), value)
        for sensor, values in series.items()
        for i, value in enumerate(values)
        if value is not None
    ]
    return spark.createDataFrame(rows, "sensor_id string, day date, mean double")


def _alert_days(spark: SparkSession, series: Series) -> dict[str, list[int]]:
    scores = sensor_day_scores(_means(spark, series), PARAMS).where("alert").collect()
    alerts: dict[str, list[int]] = {s: [] for s in series}
    for row in sorted(scores, key=lambda r: (r.sensor_id, r.day)):
        alerts[row.sensor_id].append(row.day.day)
    return alerts


def test_a_drift_away_from_the_recent_baseline_alerts(spark: SparkSession) -> None:
    ramp = [100.0] * 7 + [102.0, 106.0, 112.0]  # +2%, +6%, +12% over the 100 baseline

    assert _alert_days(spark, {"S": ramp}) == {"S": [9, 10]}


def test_a_stable_sensor_never_alerts(spark: SparkSession) -> None:
    assert _alert_days(spark, {"S": [100.0, 101.0, 99.5, 100.4] * 3}) == {"S": []}


def test_no_alert_before_the_minimum_history(spark: SparkSession) -> None:
    assert _alert_days(spark, {"S": [100.0, 150.0, 200.0]}) == {"S": []}  # 0, 1, 2 days of history


def test_a_day_without_readings_restarts_the_baseline(spark: SparkSession) -> None:
    after_stop = [100.0] * 5 + [None] + [130.0] * 4  # a new normal after the stop

    assert _alert_days(spark, {"S": after_stop}) == {"S": []}


def test_an_equipment_alerts_when_any_of_its_sensors_does(spark: SparkSession) -> None:
    series = {"T": [100.0] * 7 + [120.0], "P": [10.0] * 8}
    sensors = spark.createDataFrame(
        [("T", "E1"), ("P", "E1")], "sensor_id string, equipment_id string"
    )

    days = equipment_alert_days(sensor_day_scores(_means(spark, series), PARAMS), sensors)

    [last] = days.where("day = DATE'2025-01-08'").collect()
    assert (last.alert, last.sensors_alerting) == (True, 1)


def test_daily_means_average_each_sensor_day(spark: SparkSession) -> None:
    at = datetime(2025, 1, 1, tzinfo=UTC)
    readings = spark.createDataFrame(
        [("S", at, 10.0), ("S", at + timedelta(hours=6), 20.0), ("S", at + timedelta(days=1), 5.0)],
        "sensor_id string, event_timestamp timestamp, reading_value double",
    )

    assert sorted((r.day.day, r.mean) for r in daily_means(readings).collect()) == [
        (1, 15.0),
        (2, 5.0),
    ]


def test_ground_truth_columns_are_refused(spark: SparkSession) -> None:
    leaked = spark.createDataFrame(
        [("S", datetime(2025, 1, 1, tzinfo=UTC), 1.0, "FL-00001")],
        "sensor_id string, event_timestamp timestamp, reading_value double, failure_id string",
    )

    with pytest.raises(LeakageError, match="failure_id"):
        daily_means(leaked)
