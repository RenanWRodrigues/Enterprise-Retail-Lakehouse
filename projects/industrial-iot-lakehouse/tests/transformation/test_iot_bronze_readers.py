"""Reading landing files as text, and the per-file checks Spark does (no Delta needed)."""

from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from iot_lakehouse.bronze.landing import discover
from iot_lakehouse.bronze.readers import (
    RELATIVE_PATH,
    SOURCE_FILE,
    FileStats,
    file_stats,
    json_key_problems,
    read_files,
)
from iot_lakehouse.ingestion.source_contracts import IOT_SOURCE_CONTRACTS

Kit = Any  # tests/conftest.py::LandingKit
READINGS = IOT_SOURCE_CONTRACTS["sensor_readings"]
DAY = "2025-01-03"


def _paths(root: Path, dataset: str) -> list[str]:
    return [f.path.as_posix() for f in discover(root, dataset)]


def test_records_are_read_as_delivered_text_with_their_file(
    spark: SparkSession, tmp_path: Path, landing_kit: Kit
) -> None:
    landing_kit.write_readings(
        tmp_path, "b1", DAY, "p0", [landing_kit.reading("R1", 0.1), landing_kit.reading("R2", -3)]
    )
    [landing] = discover(tmp_path, "sensor_readings")

    rows = read_files(spark, READINGS, _paths(tmp_path, "sensor_readings")).collect()

    assert sorted((r.reading_id, r.reading_value) for r in rows) == [("R1", "0.1"), ("R2", "-3")]
    assert {r[RELATIVE_PATH] for r in rows} == {landing.relative_path}  # same key as discovery
    assert all(r[SOURCE_FILE].endswith("p0.json.gz") for r in rows)


def test_a_malformed_json_line_is_counted_against_its_file_only(
    spark: SparkSession, tmp_path: Path, landing_kit: Kit
) -> None:
    good = [landing_kit.reading("R1"), landing_kit.reading("R2")]
    landing_kit.write_readings(tmp_path, "b1", DAY, "good", good)
    landing_kit.write_readings(
        tmp_path, "b1", DAY, "bad", [landing_kit.reading("R3"), '{"reading_id": "R4",']
    )
    names = {f.path.name: f.relative_path for f in discover(tmp_path, "sensor_readings")}

    stats = file_stats(read_files(spark, READINGS, _paths(tmp_path, "sensor_readings")), READINGS)

    assert stats == {
        names["good.json.gz"]: FileStats(rows=2, corrupt_rows=0),
        names["bad.json.gz"]: FileStats(rows=2, corrupt_rows=1),
    }


def test_json_key_drift_is_named_per_file(
    spark: SparkSession, tmp_path: Path, landing_kit: Kit
) -> None:
    reading = landing_kit.reading
    landing_kit.write_readings(tmp_path, "b1", DAY, "ok", [reading("R1")])
    landing_kit.write_readings(tmp_path, "b1", DAY, "new_key", [reading("R2", quality_flag="G")])
    no_unit = {k: v for k, v in reading("R3").items() if k != "unit"}
    landing_kit.write_readings(tmp_path, "b1", DAY, "no_unit", [no_unit])
    names = {f.path.name: f.relative_path for f in discover(tmp_path, "sensor_readings")}

    problems = json_key_problems(spark, READINGS, _paths(tmp_path, "sensor_readings"))

    assert problems == {
        names["new_key.json.gz"]: "unexpected keys ['quality_flag']; missing keys []",
        names["no_unit.json.gz"]: "unexpected keys []; missing keys ['unit']",
    }
