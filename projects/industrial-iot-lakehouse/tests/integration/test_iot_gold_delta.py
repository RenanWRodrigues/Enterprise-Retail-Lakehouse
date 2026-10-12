"""IoT Gold on Delta, end to end from landing files: publication, rerun, gate failure.

landing -> Bronze -> Silver (readings and CMMS) -> Gold, small files with known results. Needs
Delta: test container (Linux).
"""

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from iot_lakehouse.bronze.ingest import BronzeTarget
from iot_lakehouse.bronze.pipeline import run_bronze
from iot_lakehouse.gold import pipeline as gold_pipeline
from iot_lakehouse.gold.build import Period, build_gold
from iot_lakehouse.gold.pipeline import GoldSettings, run_gold
from iot_lakehouse.gold.schemas import TABLE_ORDER
from iot_lakehouse.silver.cmms import run_cmms_silver
from iot_lakehouse.silver.pipeline import SilverSettings, SilverTarget, run_silver
from lakehouse_common.observability import InMemoryObservabilitySink, RunStatus
from lakehouse_common.quality import CriticalQualityError

Kit = Any  # tests/conftest.py::LandingKit
AT = datetime(2026, 10, 9, 12, tzinfo=UTC)
PERIOD = Period(date(2025, 1, 3), date(2025, 1, 5))
SENSOR = {
    "sensor_id": "SEN-1", "equipment_id": "EQ-1", "sensor_type": "temperature", "unit": "degC",
    "physical_min": "-40", "physical_max": "200", "sampling_interval_seconds": "300",
}  # fmt: skip
FAILURE = {"failure_id": "F-1", "equipment_id": "EQ-1", "failure_mode": "bearing",
           "failure_timestamp": "2025-01-03T22:00:00Z", "severity": "major"}  # fmt: skip
REPAIR = {"maintenance_order_id": "MO-1", "equipment_id": "EQ-1", "order_type": "corrective",
          "related_failure_id": "F-1", "opened_at": "2025-01-03T22:10:00Z",
          "started_at": "2025-01-03T22:30:00Z", "completed_at": "2025-01-04T02:00:00Z",
          "status": "completed"}  # fmt: skip


def _lake(spark: SparkSession, root: Path, kit: Kit, prefix: str) -> GoldSettings:
    kit.write_csv(root, "equipment", "b1", [kit.equipment_row])
    kit.write_csv(root, "sensors", "b1", [SENSOR])
    kit.write_csv(root, "failure_events", "b1", [FAILURE])
    kit.write_csv(root, "maintenance_orders", "b1", [REPAIR])
    kit.write_readings(root, "b1", "2025-01-03", "p0", [kit.reading("R1", 20.0, unit="degC")])
    bronze, silver_ns = BronzeTarget(f"{prefix}_bronze"), f"{prefix}_silver"
    sink = InMemoryObservabilitySink()
    run_bronze(spark, landing_root=root, target=bronze, ingestion_timestamp=AT, sink=sink)
    run_silver(spark, settings=SilverSettings(bronze, SilverTarget(silver_ns), 3600),
               ingestion_timestamp=AT, sink=sink)  # fmt: skip
    run_cmms_silver(spark, bronze=bronze, silver_namespace=silver_ns, at=AT, sink=sink)
    return GoldSettings(silver_ns, f"{prefix}_gold", PERIOD)


def _versions(spark: SparkSession, namespace: str) -> dict[str, int]:
    versions = {}
    for name in TABLE_ORDER:
        row = spark.sql(f"DESCRIBE HISTORY {namespace}.{name} LIMIT 1").select("version").first()
        versions[name] = int(row["version"]) if row else -1
    return versions


def test_the_gold_is_published_from_landing_files_and_a_rerun_is_identical(
    delta_spark: SparkSession, tmp_path: Path, landing_kit: Kit
) -> None:
    settings = _lake(delta_spark, tmp_path, landing_kit, "iot_gold_e2e")
    sink = InMemoryObservabilitySink()

    rows = run_gold(delta_spark, settings=settings, now=AT, sink=sink)

    reliability = delta_spark.table(
        f"{settings.gold_namespace}.agg_equipment_reliability"
    ).collect()
    [eq1] = [r for r in reliability if r.equipment_id == "EQ-1"]
    # failure 22:00 -> repaired 02:00 next day: 4 h stop split over two days; 48 h observed
    assert (eq1.unplanned_seconds, eq1.failures, eq1.repairs, eq1.mttr_seconds) == (
        4 * 3600,
        1,
        1,
        4 * 3600.0,
    )
    assert eq1.availability == pytest.approx(44 / 48)
    [fact] = delta_spark.table(f"{settings.gold_namespace}.fact_equipment_failures").collect()
    assert (fact.repair_status, fact._batch_id) == ("completed", "b1")  # traceable to the landing
    assert [r.status for r in sink.runs] == [RunStatus.SUCCEEDED] and sum(rows.values()) > 0
    first = {n: delta_spark.table(f"{settings.gold_namespace}.{n}") for n in TABLE_ORDER}
    snapshot = {n: f.collect() for n, f in first.items()}

    run_gold(delta_spark, settings=settings, now=AT + timedelta(hours=1), sink=sink)

    for name in TABLE_ORDER:
        again = delta_spark.table(f"{settings.gold_namespace}.{name}").collect()
        assert sorted(map(str, again)) == sorted(map(str, snapshot[name])), name


def test_a_gate_failure_keeps_the_previous_gold(
    delta_spark: SparkSession, tmp_path: Path, landing_kit: Kit, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _lake(delta_spark, tmp_path, landing_kit, "iot_gold_gate")
    run_gold(delta_spark, settings=settings, now=AT, sink=InMemoryObservabilitySink())
    versions = _versions(delta_spark, settings.gold_namespace)

    def duplicated_daily(silver: dict[str, DataFrame], period: Period) -> dict[str, DataFrame]:
        tables = build_gold(silver, period)
        daily = tables["agg_equipment_daily"]
        return {**tables, "agg_equipment_daily": daily.unionByName(daily.limit(1))}

    monkeypatch.setattr(gold_pipeline, "build_gold", duplicated_daily)
    sink = InMemoryObservabilitySink()
    with pytest.raises(CriticalQualityError, match="grain__agg_equipment_daily"):
        run_gold(delta_spark, settings=settings, now=AT + timedelta(hours=1), sink=sink)

    assert _versions(delta_spark, settings.gold_namespace) == versions  # nothing replaced
    assert [r.status for r in sink.runs] == [RunStatus.FAILED]
