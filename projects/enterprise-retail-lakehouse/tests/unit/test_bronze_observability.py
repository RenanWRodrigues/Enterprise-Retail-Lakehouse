"""Retail Bronze is observed per dataset: success, skipped batch (zero records) and failure.

``ingest_dataset`` (Spark + Delta) is replaced by a fake; the Delta path is asserted in
``tests/integration/test_bronze_ingestion.py``. What is tested here is the wiring.
"""

from datetime import UTC, datetime
from typing import Any

import pytest

from lakehouse_common.observability import InMemoryObservabilitySink
from retail_lakehouse.bronze import BronzeTarget, IngestionResult, IngestionStatus, ingest
from retail_lakehouse.bronze.ingest import ingest_all

AT = datetime(2026, 10, 7, 12, tzinfo=UTC)
TARGET = BronzeTarget("retail_bronze")


def _fake(monkeypatch: pytest.MonkeyPatch, outcomes: dict[str, int | Exception]) -> None:
    def ingest_dataset(spark: Any, dataset: str, **kwargs: Any) -> IngestionResult:
        outcome = outcomes[dataset]
        if isinstance(outcome, Exception):
            raise outcome
        status = IngestionStatus.INGESTED if outcome else IngestionStatus.SKIPPED
        return IngestionResult(dataset, TARGET.table(dataset), "b1", "path", status, outcome)

    monkeypatch.setattr(ingest, "ingest_dataset", ingest_dataset)


def _run(datasets: list[str], runs: InMemoryObservabilitySink) -> list[IngestionResult]:
    return ingest_all(
        None,  # type: ignore[arg-type]
        landing_root="landing",
        batch_id="b1",
        target=TARGET,
        ingestion_timestamp=AT,
        sink=runs,
        datasets=datasets,
    )


def test_one_run_per_dataset_with_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake(monkeypatch, {"customers": 120, "orders": 0})
    runs = InMemoryObservabilitySink()

    _run(["customers", "orders"], runs)

    recorded = {r.table: r for r in runs.runs}
    customers = recorded["retail_bronze.bronze_customers"]
    assert (customers.pipeline_name, customers.project, customers.layer) == (
        "retail_bronze",
        "retail",
        "bronze",
    )
    assert (customers.run_id, customers.status) == ("20261007T120000Z", "SUCCEEDED")
    assert (customers.input_rows, customers.output_rows, customers.rejected_rows) == (120, 120, 0)
    orders = recorded["retail_bronze.bronze_orders"]  # batch already ingested: zero records
    assert (orders.status, orders.input_rows, orders.output_rows) == ("SUCCEEDED", 0, 0)


def test_a_failing_dataset_is_recorded_and_stops_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake(monkeypatch, {"customers": 120, "orders": FileNotFoundError("orders_b1.jsonl")})
    runs = InMemoryObservabilitySink()

    with pytest.raises(FileNotFoundError):
        _run(["customers", "orders", "payments"], runs)

    assert [(r.table, r.status) for r in runs.runs] == [
        ("retail_bronze.bronze_customers", "SUCCEEDED"),
        ("retail_bronze.bronze_orders", "FAILED"),
    ]  # payments never started
    assert runs.runs[1].error_message == "FileNotFoundError: orders_b1.jsonl"
