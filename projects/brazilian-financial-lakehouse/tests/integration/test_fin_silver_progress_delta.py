"""Silver progress by Bronze Delta version (Change Data Feed): no loss, no repeated effects.

Needs Delta: run in the test container (Linux). Each test owns its schema. Failures are
injected by replacing one pipeline step for its first call only, as a crash would.
"""

import logging
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from financial_lakehouse.silver import pipeline as silver_pipeline
from financial_lakehouse.silver.pipeline import (
    QUARANTINE_TABLE,
    RECONCILIATION_TABLE,
    SILVER_TABLE,
    SilverRunReport,
    run_silver,
)
from financial_lakehouse.silver.progress import (
    CHECKPOINT_TABLE,
    BronzeContractError,
    ChangeFeedDisabledError,
    ChangesUnavailableError,
    Checkpoint,
    CheckpointAheadError,
    commit_checkpoint,
    read_checkpoint,
)
from financial_lakehouse.silver.standardize import SilverResult, build_silver
from financial_lakehouse.storage import DeltaBronzeSink
from financial_lakehouse.storage.bronze import BRONZE_DDL
from financial_lakehouse.transformation import BronzeRecord
from lakehouse_common.observability import RunStatus

START = datetime(2026, 10, 5, 12, tzinfo=UTC)


class CrashError(RuntimeError):
    """An injected infrastructure failure."""


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
        run_id=f"{at:%Y%m%d%H}",
        raw_file="raw.json",
        raw_sha256="abc",
    )


class Lake:
    """One test's Bronze table, Silver namespace and run clock."""

    def __init__(self, spark: SparkSession, namespace: str) -> None:
        self.spark, self.ns = spark, namespace
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
        self.bronze_table = f"{namespace}.bronze_indicator_observations"
        self.bronze = DeltaBronzeSink(spark, self.bronze_table)
        self.clock = START

    def tick(self) -> datetime:
        self.clock += timedelta(hours=1)
        return self.clock

    def run(self) -> SilverRunReport:
        return run_silver(
            self.spark, bronze_table=self.bronze_table, namespace=self.ns, now=self.tick()
        )

    def count(self, table: str) -> int:
        return self.spark.table(f"{self.ns}.{table}").count()

    def checkpoint(self) -> int | None:
        return read_checkpoint(
            self.spark,
            f"{self.ns}.{CHECKPOINT_TABLE}",
            pipeline="financial_silver",
            source=self.bronze_table,
        )

    def statuses(self) -> list[str]:
        runs = self.spark.table(f"{self.ns}.pipeline_runs").orderBy("started_at", "finished_at")
        return [r.status for r in runs.collect()]


@pytest.fixture
def lake(delta_spark: SparkSession, request: pytest.FixtureRequest) -> Lake:
    return Lake(delta_spark, f"fin_progress_{request.node.name[5:40]}")


@pytest.fixture
def alerts(caplog: pytest.LogCaptureFixture) -> Iterator[Callable[[], list[str]]]:
    with caplog.at_level(logging.WARNING, logger="lakehouse.alerts"):
        yield lambda: [r.getMessage() for r in caplog.records if r.name == "lakehouse.alerts"]


def _fail_once(monkeypatch: pytest.MonkeyPatch, name: str, real: Callable[..., Any]) -> list[int]:
    calls: list[int] = []

    def crash_then_real(*args: Any, **kwargs: Any) -> Any:
        calls.append(1)
        if len(calls) == 1:
            raise CrashError(f"injected failure in {name}")
        return real(*args, **kwargs)

    monkeypatch.setattr(silver_pipeline, name, crash_then_real)
    return calls


def test_first_run_then_no_commit_then_a_new_commit(
    lake: Lake, alerts: Callable[[], list[str]]
) -> None:
    lake.bronze.merge([_igpm(1, "0.27", lake.clock), _igpm(2, "abc", lake.clock)])

    first = lake.run()
    assert (first.after_version, first.bronze_rows_read) == (None, 2)
    assert lake.checkpoint() == first.bronze_version
    assert len(alerts()) == 1  # invalid_decimal on February, once

    idle = lake.run()
    assert (idle.after_version, idle.bronze_version, idle.bronze_rows_read) == (
        first.bronze_version, first.bronze_version, 0,
    )  # fmt: skip
    assert idle.status is RunStatus.SUCCEEDED and len(alerts()) == 1

    lake.bronze.merge([_igpm(3, "0.50", lake.tick())])  # February is in quarantine: a hole
    third = lake.run()
    assert (third.after_version, third.bronze_rows_read, third.silver_rows) == (
        first.bronze_version, 1, 1,
    )  # fmt: skip
    new_alerts = alerts()[1:]
    assert len(new_alerts) == 1 and "unexpected_gaps" in new_alerts[0]  # new problem: reported

    lake.bronze.merge([_igpm(4, "0.40", lake.tick())])  # contiguous to March
    fourth = lake.run()
    assert fourth.status is RunStatus.SUCCEEDED and len(alerts()) == 2  # old gap not repeated
    assert (lake.count(SILVER_TABLE), lake.count(QUARANTINE_TABLE)) == (3, 1)
    evidence = lake.spark.table(f"{lake.ns}.{RECONCILIATION_TABLE}").collect()
    assert {r.balanced for r in evidence} == {True}  # M1 holds on every run


def test_a_bronze_commit_during_the_run_waits_for_the_next_run(
    lake: Lake, monkeypatch: pytest.MonkeyPatch
) -> None:
    lake.bronze.merge([_igpm(1, "0.27", lake.clock)])

    def build_then_write_bronze(
        bronze: DataFrame, catalog: DataFrame, **kw: datetime
    ) -> SilverResult:
        lake.bronze.merge([_igpm(4, "0.40", lake.clock)])  # committed while Silver runs
        return build_silver(bronze, catalog, **kw)

    monkeypatch.setattr(silver_pipeline, "build_silver", build_then_write_bronze)
    during = lake.run()
    monkeypatch.setattr(silver_pipeline, "build_silver", build_silver)

    assert during.bronze_rows_read == 1  # pinned: the April commit is not read
    assert lake.checkpoint() == during.bronze_version  # nor committed
    after = lake.run()
    assert (after.after_version, after.bronze_rows_read, after.silver_rows) == (
        during.bronze_version, 1, 1,
    )  # fmt: skip
    assert lake.count(SILVER_TABLE) == 2  # April arrives, nothing lost


def test_a_failure_before_publication_changes_nothing_and_the_rerun_completes(
    lake: Lake, monkeypatch: pytest.MonkeyPatch, alerts: Callable[[], list[str]]
) -> None:
    lake.bronze.merge([_igpm(1, "0.27", lake.clock), _igpm(2, "abc", lake.clock)])
    _fail_once(monkeypatch, "merge_silver", silver_pipeline.merge_silver)

    with pytest.raises(CrashError):
        lake.run()
    assert lake.checkpoint() is None
    assert not lake.spark.catalog.tableExists(f"{lake.ns}.{SILVER_TABLE}")

    retry = lake.run()
    assert retry.after_version is None and lake.checkpoint() == retry.bronze_version
    assert (lake.count(SILVER_TABLE), lake.count(QUARANTINE_TABLE)) == (1, 1)
    # nothing was recorded by the failed attempt, so its alert is legitimately raised again
    assert len(alerts()) == 2
    assert lake.statuses() == ["FAILED", "SUCCEEDED_WITH_ALERTS"]


def test_a_failure_after_publication_repeats_the_range_without_repeating_effects(
    lake: Lake, monkeypatch: pytest.MonkeyPatch, alerts: Callable[[], list[str]]
) -> None:
    lake.bronze.merge([_igpm(1, "0.27", lake.clock), _igpm(2, "abc", lake.clock)])
    first = lake.run()
    lake.bronze.merge([_igpm(1, "0.30", lake.tick()), _igpm(3, "xyz", lake.clock)])
    calls = _fail_once(monkeypatch, "commit_checkpoint", commit_checkpoint)

    with pytest.raises(CrashError):
        lake.run()  # Silver and quarantine published, checkpoint not advanced
    assert lake.checkpoint() == first.bronze_version
    published = (lake.count(SILVER_TABLE), lake.count(QUARANTINE_TABLE))
    alerts_after_crash = len(alerts())

    retry = lake.run()
    assert len(calls) == 2 and retry.after_version == first.bronze_version
    assert lake.checkpoint() == retry.bronze_version
    assert (lake.count(SILVER_TABLE), lake.count(QUARANTINE_TABLE)) == published == (1, 2)
    assert len(alerts()) == alerts_after_crash  # March was reported by the crashed attempt
    assert retry.status is RunStatus.SUCCEEDED
    january = lake.spark.table(f"{lake.ns}.{SILVER_TABLE}").collect()
    assert [(r.value, r.revision_count) for r in january] == [(Decimal("0.30"), 2)]


def test_a_deleted_bronze_row_stops_the_run(lake: Lake) -> None:
    lake.bronze.merge([_igpm(1, "0.27", lake.clock), _igpm(2, "1.06", lake.clock)])
    first = lake.run()
    delete = f"DELETE FROM {lake.bronze_table} WHERE reference_date = '2025-02-01'"  # noqa: S608
    lake.spark.sql(delete)  # test-owned table, constant predicate

    with pytest.raises(BronzeContractError, match=r"\['delete'\]"):
        lake.run()
    assert lake.checkpoint() == first.bronze_version
    assert lake.statuses() == ["SUCCEEDED", "FAILED"]


def test_a_checkpoint_after_the_bronze_is_refused(lake: Lake) -> None:
    lake.bronze.merge([_igpm(1, "0.27", lake.clock)])
    lake.run()
    commit_checkpoint(
        lake.spark,
        f"{lake.ns}.{CHECKPOINT_TABLE}",
        Checkpoint("financial_silver", lake.bronze_table, 99, "manual", lake.tick()),
    )

    with pytest.raises(CheckpointAheadError, match="checkpoint version 99"):
        lake.run()


def test_versions_without_change_data_are_refused_with_the_procedure(lake: Lake) -> None:
    lake.spark.sql(f"CREATE TABLE {lake.bronze_table} ({BRONZE_DDL}) USING DELTA")  # no CDF
    lake.bronze.merge([_igpm(1, "0.27", lake.clock)])
    first = lake.run()  # first run: a snapshot, no feed needed
    lake.bronze.merge([_igpm(2, "1.06", lake.tick())])

    with pytest.raises(ChangeFeedDisabledError, match="ALTER TABLE"):
        lake.run()
    lake.spark.sql(
        f"ALTER TABLE {lake.bronze_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )
    with pytest.raises(ChangesUnavailableError, match="Recovery"):
        lake.run()  # the pending versions were written before the feed existed
    assert lake.checkpoint() == first.bronze_version
