"""Raw -> Bronze -> Silver -> Gold with Delta (Linux: Delta needs Hadoop native helpers).

The generator registers every anomaly it injects (``MemorySink.anomalies``): that registry is
the oracle for what Silver must quarantine or de-duplicate. The clean twin of the landing zone
must produce exactly the Silver contract the Gold tests already use (``silver_tables``).
"""

from collections import Counter
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from lakehouse_common.observability import InMemoryObservabilitySink, PipelineRun, RunStatus
from lakehouse_common.quality import CriticalQualityError, Status
from retail_lakehouse.bronze import BronzeTarget, ingest_all
from retail_lakehouse.gold import GoldTarget, build_gold
from retail_lakehouse.gold.pipeline import write_gold
from retail_lakehouse.silver import SILVER_SCHEMAS
from retail_lakehouse.silver.pipeline import (
    ENTITY_ORDER,
    SilverSettings,
    SilverTarget,
    run_silver,
)
from retail_lakehouse.synthetic import (
    AnomalyType,
    FileSink,
    GenerationConfig,
    MemorySink,
    generate_retail_dataset,
)

BRONZE_AT = datetime(2026, 1, 15, 12, tzinfo=UTC)
SILVER_AT = BRONZE_AT + timedelta(hours=1)
SECOND_BRONZE_AT = BRONZE_AT + timedelta(days=31)
SECOND_SILVER_AT = SECOND_BRONZE_AT + timedelta(hours=1)
REASONS = {  # anomaly (type, dataset, column) -> quarantine reason
    (AnomalyType.INVALID_DATE, "orders", "order_timestamp"): "invalid__order_timestamp",
    (AnomalyType.INVALID_DATE, "customers", "birth_date"): "future__birth_date",
    (AnomalyType.INVALID_DATE, "shipments", "delivered_at"): "delivered_before_shipped",
    (AnomalyType.NEGATIVE_PRICE, "products", "list_price"): "negative__list_price",
    (AnomalyType.NEGATIVE_PRICE, "order_items", "unit_price"): "negative__unit_price",
    (AnomalyType.INVALID_QUANTITY, "order_items", "quantity"): "non_positive__quantity",
    (AnomalyType.INVALID_QUANTITY, "inventory_movements", "quantity"): "zero__quantity",
}


def _settings(prefix: str) -> SilverSettings:
    return SilverSettings(bronze=BronzeTarget(f"{prefix}_bronze"), target=SilverTarget(prefix))


def _bronze(
    spark: SparkSession, root: Path, settings: SilverSettings, batch: str, at: datetime
) -> None:
    ingest_all(
        spark,
        landing_root=root.as_posix(),
        batch_id=batch,
        target=settings.bronze,
        ingestion_timestamp=at,
        sink=InMemoryObservabilitySink(),
    )


# The shared landing injects every anomaly at 10% (to show Bronze preserves them): orders of
# quarantined customers plus injected orphans exceed the 5% systemic threshold, so Silver stops
# (test_a_systemic_break_stops_the_entity_before_writing). At 1% every quarantine reason still
# appears and orphans stay below 5% (measured for both seeds: 2.3% of orders, 3.1% of lines).
REALISTIC_RATE = 0.01


def _with_rate(config: GenerationConfig, rate: float) -> GenerationConfig:
    return replace(config, anomalies=tuple(replace(rule, rate=rate) for rule in config.anomalies))


@pytest.fixture(scope="module")
def silver_configs(landing_config: GenerationConfig) -> tuple[GenerationConfig, GenerationConfig]:
    first = _with_rate(landing_config, REALISTIC_RATE)
    return first, replace(first, seed=8, batch_id="batch_002")


@pytest.fixture(scope="module")
def silver_landing(
    tmp_path_factory: pytest.TempPathFactory, silver_configs: tuple[GenerationConfig, ...]
) -> Path:
    root = tmp_path_factory.mktemp("silver_landing")
    for config in silver_configs:
        with FileSink(root, config=config) as sink:
            generate_retail_dataset(config, sink)
    return root


@pytest.fixture(scope="module")
def silver_rows(silver_configs: tuple[GenerationConfig, ...]) -> MemorySink:
    """Exactly what the ``initial`` batch of ``silver_landing`` contains, with its registry."""
    sink = MemorySink()
    generate_retail_dataset(silver_configs[0], sink)
    return sink


@pytest.fixture(scope="module")
def anomalous(
    delta_spark: SparkSession, silver_landing: Path
) -> tuple[SilverSettings, list[PipelineRun]]:
    settings = _settings("silver_it")
    _bronze(delta_spark, silver_landing, settings, "initial", BRONZE_AT)
    runs = run_silver(
        delta_spark, settings=settings, sink=InMemoryObservabilitySink(), now=SILVER_AT
    )
    return settings, runs


def _table(spark: SparkSession, settings: SilverSettings, entity: str) -> DataFrame:
    return spark.table(settings.target.table(entity))


def _quarantine(spark: SparkSession, settings: SilverSettings, entity: str) -> DataFrame:
    return spark.table(settings.target.quarantine(entity))


def test_every_entity_reconciles(anomalous: tuple[SilverSettings, list[PipelineRun]]) -> None:
    _, runs = anomalous
    assert [r.table.rsplit(".", 1)[1] for r in runs] == [f"silver_{e}" for e in ENTITY_ORDER]
    for run in runs:
        assert run.status in ("SUCCEEDED", "SUCCEEDED_WITH_ALERTS"), (run.table, run.error_message)
        accounted = (run.output_rows or 0) + (run.rejected_rows or 0) + (run.duplicate_rows or 0)
        assert run.input_rows == accounted, run.table


def test_duplicates_are_exactly_the_injected_resends(
    anomalous: tuple[SilverSettings, list[PipelineRun]], silver_rows: MemorySink
) -> None:
    _, runs = anomalous
    injected = Counter(
        a.dataset
        for a in silver_rows.anomalies
        if a.anomaly_type in (AnomalyType.DUPLICATE_RECORD, AnomalyType.DUPLICATE_PAYMENT)
    )
    removed = {r.table.rsplit(".", 1)[1].removeprefix("silver_"): r.duplicate_rows for r in runs}
    for dataset, resends in injected.items():
        assert removed[dataset] == resends, dataset


@pytest.mark.parametrize(("anomaly", "reason"), list(REASONS.items()), ids=str)
def test_each_invalid_value_is_quarantined_with_its_reason(
    delta_spark: SparkSession,
    anomalous: tuple[SilverSettings, list[PipelineRun]],
    silver_rows: MemorySink,
    anomaly: tuple[AnomalyType, str, str],
    reason: str,
) -> None:
    settings, _ = anomalous
    anomaly_type, dataset, column = anomaly
    injected = sum(
        1
        for a in silver_rows.anomalies
        if (a.anomaly_type, a.dataset, a.column) == (anomaly_type, dataset, column)
    )
    quarantined = (
        _quarantine(delta_spark, settings, dataset)
        .where(F.array_contains("_dq_failed_checks", reason))
        .count()
    )
    assert injected > 0
    assert quarantined == injected


def test_unknown_references_stay_in_silver(
    delta_spark: SparkSession,
    anomalous: tuple[SilverSettings, list[PipelineRun]],
    silver_rows: MemorySink,
) -> None:
    """ADR 007: an order of an unknown customer is kept (WARNING) and maps to -1 in Gold."""
    settings, _ = anomalous
    orphan_ids = {
        a.injected_value
        for a in silver_rows.anomalies
        if (a.anomaly_type, a.dataset, a.column)
        == (AnomalyType.ORPHAN_FOREIGN_KEY, "orders", "customer_id")
    }
    kept = {
        r.customer_id
        for r in _table(delta_spark, settings, "orders").select("customer_id").collect()
    }
    assert orphan_ids and orphan_ids <= kept


def test_silver_feeds_gold_through_the_publication_gate(
    delta_spark: SparkSession,
    anomalous: tuple[SilverSettings, list[PipelineRun]],
    gold_calendar: tuple[date, date],
) -> None:
    """The Silver of an anomalous landing yields a Gold the gate accepts (orphans: WARNING)."""
    settings, _ = anomalous
    silver = {e: _table(delta_spark, settings, e) for e in SILVER_SCHEMAS}
    start, end = gold_calendar
    gold = build_gold(silver, calendar_start=start, calendar_end=end)
    target, runs = GoldTarget("gold_from_silver"), InMemoryObservabilitySink()

    write_gold(gold, target, run_id="G1", sink=runs, now=SILVER_AT)

    published = {name: delta_spark.table(target.table(name)) for name in gold}
    assert published["fact_sales"].count() == silver["order_items"].count()  # one row per line
    assert published["fact_payments"].count() == silver["payments"].count()
    gate = runs.runs[0]
    assert gate.status is RunStatus.SUCCEEDED_WITH_ALERTS  # orphans kept by Silver (ADR 007)
    assert all(r.status is not Status.CRITICAL for r in gate.quality)


def test_rerun_processes_nothing_and_changes_nothing(
    delta_spark: SparkSession, anomalous: tuple[SilverSettings, list[PipelineRun]]
) -> None:
    settings, _ = anomalous
    before = {e: _table(delta_spark, settings, e).count() for e in ENTITY_ORDER}

    again = run_silver(
        delta_spark, settings=settings, sink=InMemoryObservabilitySink(), now=SILVER_AT
    )

    assert {(r.input_rows, r.output_rows) for r in again} == {(0, 0)}  # zero-records runs
    assert {e: _table(delta_spark, settings, e).count() for e in ENTITY_ORDER} == before


def test_second_batch_adds_customer_versions_and_updates_entities(
    delta_spark: SparkSession,
    anomalous: tuple[SilverSettings, list[PipelineRun]],
    silver_landing: Path,
) -> None:
    """batch_002 regenerates the same ids with another seed: attributes change everywhere."""
    settings, _ = anomalous
    customers_before = _table(delta_spark, settings, "customers").count()
    _bronze(delta_spark, silver_landing, settings, "batch_002", SECOND_BRONZE_AT)

    runs = run_silver(
        delta_spark, settings=settings, sink=InMemoryObservabilitySink(), now=SECOND_SILVER_AT
    )

    assert all(r.status != "FAILED" for r in runs), [r.error_message for r in runs]
    customers = _table(delta_spark, settings, "customers")
    assert customers.count() > customers_before  # changed customers opened new versions
    assert customers.groupBy("customer_id", "effective_from").count().where("count > 1").isEmpty()
    later = customers.where(F.col("_batch_id") == "batch_002")
    assert (
        later.where(F.col("effective_from") != F.col("_ingestion_timestamp"))
        .where(F.col("effective_from") != F.col("registered_at"))
        .isEmpty()
    )  # a version starts at registration (new customer) or at the batch (change)
    orders = _table(delta_spark, settings, "orders")
    assert not orders.where(F.col("_batch_id") == "batch_002").isEmpty()  # SCD1 updates


def test_clean_landing_gives_exactly_the_silver_contract(
    delta_spark: SparkSession,
    tmp_path_factory: pytest.TempPathFactory,
    landing_config: GenerationConfig,
    silver_tables: dict[str, DataFrame],
) -> None:
    """Silver built from the clean twin equals the oracle the Gold tests are written against."""
    root = tmp_path_factory.mktemp("clean_landing")
    clean = replace(landing_config, anomalies=())
    with FileSink(root, config=clean) as sink:
        generate_retail_dataset(clean, sink)
    settings = _settings("silver_clean_it")
    _bronze(delta_spark, root, settings, "initial", BRONZE_AT)

    runs = run_silver(
        delta_spark, settings=settings, sink=InMemoryObservabilitySink(), now=SILVER_AT
    )

    assert {r.rejected_rows for r in runs} == {0}
    for entity, expected in silver_tables.items():
        actual = _table(delta_spark, settings, entity).select(*SILVER_SCHEMAS[entity].fieldNames())
        assertDataFrameEqual(actual, expected)


def test_a_systemic_break_stops_the_entity_before_writing(
    delta_spark: SparkSession, landing_root: Path
) -> None:
    """At 10% corruption, orders of quarantined customers plus injected orphans exceed 5%:
    ``unknown_customer_systemic`` is CRITICAL, orders are not written, the run is recorded.
    """
    settings = _settings("silver_critical_it")
    _bronze(delta_spark, landing_root, settings, "initial", BRONZE_AT)
    sink = InMemoryObservabilitySink()

    with pytest.raises(CriticalQualityError, match="unknown_customer_systemic"):
        run_silver(delta_spark, settings=settings, sink=sink, now=SILVER_AT)

    failed = sink.runs[-1]
    assert (failed.table, failed.status) == (settings.target.table("orders"), "FAILED")
    assert _table(delta_spark, settings, "orders").isEmpty()  # stopped before the MERGE
    assert not _table(delta_spark, settings, "customers").isEmpty()  # earlier entities kept
