"""Bronze (Delta) -> Silver MERGE (incremental, idempotent) -> Gold (needs Delta: container)."""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession

from financial_lakehouse.gold import GOLD_SCHEMAS
from financial_lakehouse.gold.pipeline import run_gold
from financial_lakehouse.silver import pipeline as silver_pipeline
from financial_lakehouse.silver.pipeline import (
    QUARANTINE_TABLE,
    RECONCILIATION_TABLE,
    SILVER_TABLE,
    run_silver,
)
from financial_lakehouse.silver.standardize import SilverResult, build_silver
from financial_lakehouse.storage import DeltaBronzeSink
from financial_lakehouse.transformation import BronzeRecord
from lakehouse_common.observability import RunStatus
from lakehouse_common.quality import CriticalQualityError

NS = "fin_medallion_it"
FIRST, SECOND = datetime(2026, 10, 5, 12, tzinfo=UTC), datetime(2026, 10, 6, 12, tzinfo=UTC)


def _igpm(month: int, value: str, at: datetime) -> BronzeRecord:
    return BronzeRecord.create(
        source="bcb_sgs",
        series_id="189",
        indicator_id="igpm_monthly",
        reference=f"01/{month:02d}/2025",
        reference_date=date(2025, month, 1),
        value=value,
        attributes={},
        request_timestamp=at,
        ingestion_timestamp=at,
        run_id=f"{at:%Y%m%d}",
        raw_file="raw.json",
        raw_sha256="abc",
    )


def test_silver_is_incremental_idempotent_and_feeds_gold(delta_spark: SparkSession) -> None:
    delta_spark.sql(f"CREATE SCHEMA IF NOT EXISTS {NS}")
    bronze_table = f"{NS}.bronze_indicator_observations"
    bronze = DeltaBronzeSink(delta_spark, bronze_table)
    bronze.merge([_igpm(1, "0.27", FIRST), _igpm(2, "1.06", FIRST), _igpm(3, "abc", FIRST)])

    first = run_silver(delta_spark, bronze_table=bronze_table, namespace=NS, now=FIRST)
    assert (first.silver_rows, first.quarantined_rows, first.after_version) == (2, 1, None)
    assert first.status is RunStatus.SUCCEEDED_WITH_ALERTS  # invalid_decimal is ERROR: alert

    again = run_silver(delta_spark, bronze_table=bronze_table, namespace=NS, now=FIRST)
    assert (again.after_version, again.bronze_rows_read) == (first.bronze_version, 0)  # no commit
    assert again.status is RunStatus.SUCCEEDED  # the invalid row is not reported again
    assert delta_spark.table(f"{NS}.{SILVER_TABLE}").count() == 2

    bronze.merge([_igpm(2, "1.10", SECOND)])  # a revision of February
    revised = run_silver(delta_spark, bronze_table=bronze_table, namespace=NS, now=SECOND)
    assert revised.bronze_rows_read == 2  # only the versions of the touched key (February)
    february = delta_spark.table(f"{NS}.{SILVER_TABLE}").where("reference_date = '2025-02-01'")
    assert [(r.value, r.revision_count) for r in february.collect()] == [(Decimal("1.10"), 2)]
    assert delta_spark.table(f"{NS}.{QUARANTINE_TABLE}").count() == 1  # not duplicated
    # 2 consumed rows: February 1.10 valid, February 1.06 superseded
    assert {k: v for k, v in revised.destinations.items() if v} == {
        "valid": 1,
        "superseded_revision": 1,
    }
    evidence = delta_spark.table(f"{NS}.{RECONCILIATION_TABLE}").where(
        f"run_id = '{SECOND:%Y%m%dT%H%M%SZ}'"
    )
    rows = evidence.collect()
    assert {r.destination: r.rows for r in rows if r.rows} == {
        "valid": 1,
        "superseded_revision": 1,
    }
    assert {(r.bronze_rows, r.balanced, r.from_version, r.bronze_version) for r in rows} == {
        (2, True, first.bronze_version, revised.bronze_version)
    }
    assert {tuple(r.bronze_run_ids) for r in rows} == {("20261005", "20261006")}

    gold = run_gold(delta_spark, silver_table=f"{NS}.{SILVER_TABLE}", gold_namespace=NS, now=SECOND)
    assert gold.status is RunStatus.SUCCEEDED
    for name in GOLD_SCHEMAS:
        assert delta_spark.table(f"{NS}.{name}").columns == GOLD_SCHEMAS[name].fieldNames()
    written = sum(delta_spark.table(f"{NS}.{name}").count() for name in GOLD_SCHEMAS)
    assert gold.output_rows == written  # counted by the quality pass, not by extra actions

    runs = delta_spark.table(f"{NS}.pipeline_runs").orderBy("started_at").collect()
    # Progress by Delta version (M6): the invalid March row alerts once, when it first arrives.
    assert [(r.pipeline_name, r.status) for r in runs] == [
        ("financial_silver", "SUCCEEDED_WITH_ALERTS"),
        ("financial_silver", "SUCCEEDED"),
        ("financial_silver", "SUCCEEDED"),
        ("financial_gold", "SUCCEEDED"),
    ]
    assert delta_spark.table(f"{NS}.data_quality_results").where("status <> 'PASS'").count() >= 1


def test_a_lost_row_fails_the_run_and_publishes_nothing(
    delta_spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Simulated defect: a step drops one valid row. The run must stop before the MERGE."""
    ns = "fin_reconciliation_it"
    delta_spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ns}")
    bronze_table = f"{ns}.bronze_indicator_observations"
    DeltaBronzeSink(delta_spark, bronze_table).merge(
        [_igpm(1, "0.27", FIRST), _igpm(2, "1.06", FIRST)]
    )
    build = build_silver  # the real one; the pipeline gets a lossy wrapper

    def lossy(bronze: DataFrame, catalog: DataFrame, **kwargs: datetime) -> SilverResult:
        result = build(bronze, catalog, **kwargs)
        return SilverResult(result.silver.limit(1), result.quarantine, result.excluded)

    monkeypatch.setattr(silver_pipeline, "build_silver", lossy)

    with pytest.raises(CriticalQualityError, match="reconciliation"):
        run_silver(delta_spark, bronze_table=bronze_table, namespace=ns, now=FIRST)

    assert not delta_spark.catalog.tableExists(f"{ns}.{SILVER_TABLE}")  # nothing published
    [run] = delta_spark.table(f"{ns}.pipeline_runs").collect()
    assert run.status == "FAILED" and "reconciliation" in run.error_message
    evidence = delta_spark.table(f"{ns}.{RECONCILIATION_TABLE}").collect()
    assert {(r.bronze_rows, r.balanced) for r in evidence} == {(2, False)}
    [check] = (
        delta_spark.table(f"{ns}.data_quality_results")
        .where("check_name = 'reconciliation'")
        .collect()
    )
    assert check.status == "CRITICAL"
