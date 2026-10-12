"""Intentional anomalies: configured, injected, registered — and nothing else changes."""

from collections import Counter
from dataclasses import replace

import pytest

from retail_lakehouse.ingestion.source_contracts import RETAIL_SOURCE_CONTRACTS
from retail_lakehouse.synthetic import (
    SUPPORTED_ANOMALIES,
    AnomalyType,
    GenerationConfig,
    MemorySink,
    generate_retail_dataset,
)

Row = dict[str, str | None]
VALUE_ANOMALIES = {
    AnomalyType.NULL_VALUE,
    AnomalyType.INVALID_EMAIL,
    AnomalyType.NEGATIVE_PRICE,
    AnomalyType.INVALID_QUANTITY,
    AnomalyType.ORPHAN_FOREIGN_KEY,
    AnomalyType.INVALID_DATE,
}


def _key(dataset: str, row: Row) -> str:
    return "|".join(str(row[c]) for c in RETAIL_SOURCE_CONTRACTS[dataset].business_key)


@pytest.mark.parametrize(("anomaly_type", "dataset", "column"), SUPPORTED_ANOMALIES)
def test_every_supported_anomaly_is_injected_and_registered(
    small_dataset: MemorySink, anomaly_type: AnomalyType, dataset: str, column: str | None
) -> None:
    registered = [
        a
        for a in small_dataset.anomalies
        if (a.anomaly_type, a.dataset, a.column) == (anomaly_type, dataset, column)
    ]
    assert registered, f"no {anomaly_type} in {dataset}.{column}"


def test_registered_value_anomalies_are_present_in_the_data(small_dataset: MemorySink) -> None:
    for anomaly in small_dataset.anomalies:
        if anomaly.anomaly_type not in VALUE_ANOMALIES:
            continue
        rows = [
            r
            for r in small_dataset.rows[anomaly.dataset]
            if _key(anomaly.dataset, r) == anomaly.record_key
        ]
        assert rows, f"{anomaly.anomaly_id}: record not found"
        assert all(r[str(anomaly.column)] == anomaly.injected_value for r in rows), anomaly
        assert anomaly.original_value != anomaly.injected_value


@pytest.mark.parametrize(
    ("anomaly_type", "dataset", "column", "is_anomalous"),
    [
        (AnomalyType.NULL_VALUE, "customers", "email", lambda v: v is None),
        (
            AnomalyType.INVALID_EMAIL,
            "customers",
            "email",
            lambda v: (
                v is not None
                and (v.count("@") != 1 or " " in v or v.endswith("@") or v.startswith("@"))
            ),
        ),
        (AnomalyType.NEGATIVE_PRICE, "order_items", "unit_price", lambda v: v.startswith("-")),
        (AnomalyType.INVALID_QUANTITY, "order_items", "quantity", lambda v: int(v) <= 0),
    ],
)
def test_injected_values_are_really_invalid(
    small_dataset: MemorySink,
    anomaly_type: AnomalyType,
    dataset: str,
    column: str,
    is_anomalous: object,
) -> None:
    injected = [
        a.injected_value
        for a in small_dataset.anomalies
        if (a.anomaly_type, a.dataset, a.column) == (anomaly_type, dataset, column)
    ]
    assert injected and all(is_anomalous(v) for v in injected)  # type: ignore[operator]


def test_invalid_dates_are_unparseable_or_incoherent(small_dataset: MemorySink) -> None:
    contracts = RETAIL_SOURCE_CONTRACTS
    for anomaly in small_dataset.anomalies:
        if anomaly.anomaly_type is not AnomalyType.INVALID_DATE:
            continue
        if anomaly.dataset == "orders":
            assert not contracts["orders"].column("order_timestamp").accepts(anomaly.injected_value)
        elif anomaly.dataset == "customers":
            assert str(anomaly.injected_value) > "2030", "birth date in the future"
        else:  # shipments.delivered_at before shipped_at
            [shipment] = [
                s for s in small_dataset.rows["shipments"] if s["shipment_id"] == anomaly.record_key
            ]
            assert str(shipment["delivered_at"]) < str(shipment["shipped_at"])


def test_anomaly_counts_follow_configured_rates(
    small_dataset: MemorySink, clean_dataset: MemorySink, small_config: GenerationConfig
) -> None:
    counts = Counter((a.anomaly_type, a.dataset, a.column) for a in small_dataset.anomalies)
    for rule in small_config.anomalies:
        population = len(clean_dataset.rows[rule.dataset])
        expected = rule.rate * population
        observed = counts[(rule.anomaly_type, rule.dataset, rule.column)]
        # Value anomalies may be skipped when the cell was already altered or is not applicable.
        assert observed <= expected * 2 + 5, rule
        assert observed >= expected * 0.3, rule


def test_rows_not_touched_by_anomalies_are_identical_to_the_clean_run(
    small_dataset: MemorySink, clean_dataset: MemorySink
) -> None:
    touched = Counter((a.dataset, a.record_key) for a in small_dataset.anomalies)
    for dataset, clean_rows in clean_dataset.rows.items():
        dirty = [
            r for r in small_dataset.rows[dataset] if (dataset, _key(dataset, r)) not in touched
        ]
        clean = [r for r in clean_rows if (dataset, _key(dataset, r)) not in touched]
        assert dirty == clean, dataset


def test_duplicates_add_rows_and_nothing_else_changes_volumes(
    small_dataset: MemorySink, clean_dataset: MemorySink
) -> None:
    duplicates = Counter(
        a.dataset
        for a in small_dataset.anomalies
        if a.anomaly_type in {AnomalyType.DUPLICATE_RECORD, AnomalyType.DUPLICATE_PAYMENT}
    )
    for dataset, clean_rows in clean_dataset.rows.items():
        assert len(small_dataset.rows[dataset]) == len(clean_rows) + duplicates[dataset], dataset


def test_disabled_anomalies_produce_no_registry(clean_dataset: MemorySink) -> None:
    assert clean_dataset.anomalies == []


def test_rate_one_hits_every_record(small_config: GenerationConfig) -> None:
    rule = replace(
        small_config.anomalies[0],
        anomaly_type=AnomalyType.NULL_VALUE,
        dataset="customers",
        column="email",
        rate=1.0,
    )
    sink = MemorySink()
    generate_retail_dataset(replace(small_config, anomalies=(rule,)), sink)
    assert all(row["email"] is None for row in sink.rows["customers"])


def test_supported_anomalies_cover_the_requested_catalogue() -> None:
    assert {t.value for t, _, _ in SUPPORTED_ANOMALIES} == {
        "duplicate_record",
        "null_value",
        "invalid_email",
        "negative_price",
        "invalid_quantity",
        "orphan_foreign_key",
        "duplicate_payment",
        "invalid_date",
    }
