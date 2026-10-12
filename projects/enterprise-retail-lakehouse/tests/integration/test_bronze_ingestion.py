"""Bronze ingestion into Delta tables: preservation, metadata, idempotency, protection.

Requires Delta Lake (Linux or Windows with HADOOP_HOME): run in the test container.
"""

import shutil
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from lakehouse_common.observability import InMemoryObservabilitySink
from retail_lakehouse.bronze import (
    BronzeTarget,
    IngestionResult,
    IngestionStatus,
    bronze_schema,
    ingest_all,
    ingest_dataset,
    landing_path,
    source_schema,
)
from retail_lakehouse.ingestion.source_contracts import RETAIL_SOURCE_CONTRACTS
from retail_lakehouse.synthetic import MemorySink

INGESTED_AT = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)
DATASETS = sorted(RETAIL_SOURCE_CONTRACTS)


def _versions(spark: SparkSession, table: str) -> int:
    return spark.sql(f"DESCRIBE HISTORY {table}").count()


@pytest.fixture(scope="module")
def initial_run(delta_spark: SparkSession, landing_root: Path) -> list[IngestionResult]:
    return ingest_all(
        delta_spark,
        landing_root=landing_root.as_posix(),
        batch_id="initial",
        target=BronzeTarget("bronze_it"),
        ingestion_timestamp=INGESTED_AT,
        sink=InMemoryObservabilitySink(),
    )


def test_every_dataset_lands_in_its_bronze_table(
    delta_spark: SparkSession, initial_run: list[IngestionResult]
) -> None:
    assert {r.table_name for r in initial_run} == {f"bronze_it.bronze_{d}" for d in DATASETS}
    assert {r.status for r in initial_run} == {IngestionStatus.INGESTED}
    for result in initial_run:
        assert delta_spark.catalog.tableExists(result.table_name)


@pytest.mark.parametrize("dataset", DATASETS)
def test_table_schema_is_the_bronze_contract(
    delta_spark: SparkSession, initial_run: list[IngestionResult], dataset: str
) -> None:
    table = delta_spark.table(f"bronze_it.bronze_{dataset}")
    expected = bronze_schema(RETAIL_SOURCE_CONTRACTS[dataset])
    assert [(f.name, f.dataType) for f in table.schema] == [(f.name, f.dataType) for f in expected]


@pytest.mark.parametrize("dataset", DATASETS)
def test_rows_are_preserved_exactly(
    delta_spark: SparkSession,
    initial_run: list[IngestionResult],
    landing_rows: MemorySink,
    dataset: str,
) -> None:
    contract = RETAIL_SOURCE_CONTRACTS[dataset]
    expected_rows = landing_rows.rows[dataset]
    [result] = [r for r in initial_run if r.dataset == dataset]

    table = delta_spark.table(result.table_name).select(*contract.column_names)

    assert result.rows_written == len(expected_rows)
    expected = delta_spark.createDataFrame(
        [tuple(row.values()) for row in expected_rows], schema=source_schema(contract)
    )
    assertDataFrameEqual(table, expected)


@pytest.mark.parametrize("dataset", DATASETS)
def test_metadata_is_populated(
    delta_spark: SparkSession, initial_run: list[IngestionResult], dataset: str
) -> None:
    contract = RETAIL_SOURCE_CONTRACTS[dataset]
    table = delta_spark.table(f"bronze_it.bronze_{dataset}")

    [row] = table.agg(
        F.collect_set("_batch_id").alias("batches"),
        F.collect_set("_source_file").alias("files"),
        F.collect_set(F.date_format("_ingestion_timestamp", "yyyy-MM-dd HH:mm:ss")).alias("ts"),
        F.collect_set("_ingestion_date").alias("dates"),
    ).collect()

    assert row["batches"] == ["initial"]
    assert len(row["files"]) == 1
    assert row["files"][0].endswith(
        f"{contract.source_system}/{dataset}/{dataset}_initial.{contract.file_format}"
    )
    assert row["ts"] == ["2026-01-15 12:00:00"]
    assert row["dates"] == [date(2026, 1, 15)]


def test_rerunning_the_same_batch_is_a_no_op(delta_spark: SparkSession, landing_root: Path) -> None:
    target = BronzeTarget("bronze_rerun")
    root = landing_root.as_posix()
    runs = InMemoryObservabilitySink()
    first = ingest_all(
        delta_spark,
        landing_root=root,
        batch_id="initial",
        target=target,
        ingestion_timestamp=INGESTED_AT,
        sink=runs,
    )
    versions = {r.table_name: _versions(delta_spark, r.table_name) for r in first}
    counts = {r.table_name: delta_spark.table(r.table_name).count() for r in first}

    second = ingest_all(
        delta_spark,
        landing_root=root,
        batch_id="initial",
        target=target,
        ingestion_timestamp=datetime(2026, 1, 16, tzinfo=UTC),
        sink=runs,
    )

    assert {r.status for r in second} == {IngestionStatus.SKIPPED}
    assert all(r.rows_written == 0 for r in second)
    first_runs, rerun = runs.runs[: len(first)], runs.runs[len(first) :]
    assert {(r.project, r.layer, r.status) for r in runs.runs} == {
        ("retail", "bronze", "SUCCEEDED")
    }
    assert all(r.output_rows and r.output_rows > 0 for r in first_runs)
    assert {(r.input_rows, r.output_rows) for r in rerun} == {(0, 0)}  # zero-records runs
    for table, version_count in versions.items():
        assert _versions(delta_spark, table) == version_count, f"{table} got a new commit"
        assert delta_spark.table(table).count() == counts[table]


def test_same_batch_cannot_be_ingested_twice(
    delta_spark: SparkSession, landing_root: Path, landing_rows: MemorySink
) -> None:
    target = BronzeTarget("bronze_protection")
    results = [
        ingest_dataset(
            delta_spark,
            "customers",
            landing_root=landing_root.as_posix(),
            batch_id="initial",
            target=target,
            ingestion_timestamp=INGESTED_AT,
        )
        for _ in range(2)
    ]

    assert [r.status for r in results] == [IngestionStatus.INGESTED, IngestionStatus.SKIPPED]
    assert delta_spark.table(results[0].table_name).count() == len(landing_rows.rows["customers"])


def test_a_new_batch_is_appended(
    delta_spark: SparkSession,
    landing_root: Path,
    landing_rows: MemorySink,
    second_batch_rows: MemorySink,
) -> None:
    target = BronzeTarget("bronze_batches")
    for batch_id in ("initial", "batch_002"):
        ingest_dataset(
            delta_spark,
            "orders",
            landing_root=landing_root.as_posix(),
            batch_id=batch_id,
            target=target,
            ingestion_timestamp=INGESTED_AT,
        )

    per_batch = {
        row["_batch_id"]: row["count"]
        for row in delta_spark.table(target.table("orders")).groupBy("_batch_id").count().collect()
    }
    assert per_batch == {
        "initial": len(landing_rows.rows["orders"]),
        "batch_002": len(second_batch_rows.rows["orders"]),
    }


def test_bronze_tables_are_append_only(
    delta_spark: SparkSession, initial_run: list[IngestionResult]
) -> None:
    table = "bronze_it.bronze_stores"
    before = delta_spark.table(table).count()

    with pytest.raises(Exception, match=r"(?i)append"):  # JVM error type varies
        delta_spark.sql(f"DELETE FROM {table} WHERE store_id = 'STR-001'")  # noqa: S608 - constant

    assert delta_spark.table(table).count() == before


def test_structural_error_aborts_without_writing(
    delta_spark: SparkSession, landing_root: Path, tmp_path: Path
) -> None:
    broken_root = tmp_path / "landing"
    source = Path(landing_path(landing_root.as_posix(), "stores", "initial"))
    target_file = Path(landing_path(broken_root.as_posix(), "stores", "initial"))
    target_file.parent.mkdir(parents=True)
    shutil.copy(source, target_file)
    lines = target_file.read_text(encoding="utf-8").splitlines()
    target_file.write_text(
        "\n".join([lines[0].replace("region", "zone"), *lines[1:]]) + "\n", encoding="utf-8"
    )
    target = BronzeTarget("bronze_broken")

    with pytest.raises(Exception, match=r"(?i)header"):  # JVM error type varies
        ingest_dataset(
            delta_spark,
            "stores",
            landing_root=broken_root.as_posix(),
            batch_id="initial",
            target=target,
            ingestion_timestamp=INGESTED_AT,
        )

    assert not delta_spark.catalog.tableExists(target.table("stores"))
