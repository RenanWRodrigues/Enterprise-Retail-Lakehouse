"""Silver reconciliation: every consumed Bronze row has exactly one counted destination.

bronze_consumed = valid + quarantine + duplicate + non_closing_bulletin + superseded_revision
                  + redundant_latest_version            (docs/SILVER.md, "Reconciliação")

Each scenario uses its own (indicator, reference_date) keys, so the expected count of every
destination follows from the rows alone. Counts come from the DataFrame of each destination.
"""

from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from financial_lakehouse.silver import build_silver, catalog_frame
from financial_lakehouse.silver.standardize import (
    DESTINATIONS,
    DUPLICATE,
    NON_CLOSING,
    QUARANTINE,
    REDUNDANT,
    SUPERSEDED,
    VALID,
    SilverResult,
    destination_counts,
    reconcile_silver,
)
from lakehouse_common.observability import InMemoryObservabilitySink, Layer, RunStatus, track_run
from lakehouse_common.quality import CriticalQualityError, Status

RUN_AT = datetime(2026, 10, 8, 15, tzinfo=UTC)
EARLY = datetime(2026, 9, 1, 12, tzinfo=UTC)
MIDDLE = datetime(2026, 9, 15, 12, tzinfo=UTC)
LATE = datetime(2026, 10, 6, 12, tzinfo=UTC)
Factory = Callable[..., dict[str, Any]]
Frame = Callable[[list[dict[str, Any]]], DataFrame]


def _ptax(make: Factory) -> list[dict[str, Any]]:
    def bulletin(kind: str, value: str, time: str) -> dict[str, Any]:
        return make(
            run_id="ptax", source="bcb_ptax", series_id="EUR", indicator_id="eur_brl_ptax",
            reference=f"2025-01-02 {time}", reference_date=date(2025, 1, 2), value=value,
            attributes=f'{{"tipoBoletim": "{kind}"}}',
        )  # fmt: skip

    return [
        bulletin("Abertura", "6.40", "10:10"),
        bulletin("Intermediário", "6.41", "11:10"),
        bulletin("Fechamento", "6.4250", "13:10"),
    ]


def _igpm(make: Factory, month: int, value: str | None, **extra: Any) -> dict[str, Any]:
    return make(
        reference=f"01/{month:02d}/2020", reference_date=date(2020, month, 1), value=value, **extra
    )


def _revisions(make: Factory) -> list[dict[str, Any]]:
    return [
        _igpm(make, 1, "1.00", request_timestamp=EARLY),
        _igpm(make, 1, "1.05", request_timestamp=MIDDLE),
        _igpm(make, 1, "1.10", request_timestamp=LATE),
    ]


def _duplicates(make: Factory) -> list[dict[str, Any]]:
    return [_igpm(make, 2, "0.50"), _igpm(make, 2, "0.50")]


def _invalid(make: Factory) -> list[dict[str, Any]]:
    return [_igpm(make, 3, "abc"), _igpm(make, 3, "abc"), _igpm(make, 6, None)]


def _conflict(make: Factory) -> list[dict[str, Any]]:
    return [
        _igpm(make, 4, "1.00", request_timestamp=EARLY),
        _igpm(make, 4, "1.06"),  # same latest request,
        _igpm(make, 4, "1.07"),  # two different values
    ]


def _redundant(make: Factory) -> list[dict[str, Any]]:
    # Same latest request and value, other attributes: another record_key, same observation.
    return [_igpm(make, 5, "1.06"), _igpm(make, 5, "1.06", attributes='{"note": "x"}')]


SCENARIOS: dict[str, tuple[Callable[[Factory], list[dict[str, Any]]], dict[str, int]]] = {
    "ptax_opening_and_closing": (_ptax, {VALID: 1, NON_CLOSING: 2}),
    "successive_revisions": (_revisions, {VALID: 1, SUPERSEDED: 2}),
    "technical_duplicates": (_duplicates, {VALID: 1, DUPLICATE: 1}),
    "invalid_rows": (_invalid, {QUARANTINE: 3}),  # rules run first: an invalid twin is quarantined
    "conflicting_latest_values": (_conflict, {QUARANTINE: 2, SUPERSEDED: 1}),
    "redundant_latest_version": (_redundant, {VALID: 1, REDUNDANT: 1}),
}


def _expected(partial: dict[str, int]) -> dict[str, int]:
    return {destination: partial.get(destination, 0) for destination in DESTINATIONS}


def _standardize(spark: SparkSession, frame: DataFrame) -> SilverResult:
    return build_silver(frame, catalog_frame(spark), execution_timestamp=RUN_AT)


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_each_scenario_lands_in_its_own_destinations(
    spark: SparkSession, bronze_row: Factory, bronze_frame: Frame, scenario: str
) -> None:
    rows, expected = SCENARIOS[scenario]
    bronze = bronze_frame(rows(bronze_row))

    counts = destination_counts(_standardize(spark, bronze))

    assert counts == _expected(expected)
    assert reconcile_silver(bronze.count(), counts).balanced


@pytest.fixture(scope="module")
def mixed(bronze_row: Factory, bronze_frame: Frame) -> DataFrame:
    return bronze_frame([row for build, _ in SCENARIOS.values() for row in build(bronze_row)])


def test_a_mixed_batch_reconciles_every_category(spark: SparkSession, mixed: DataFrame) -> None:
    counts = destination_counts(_standardize(spark, mixed))
    balance = reconcile_silver(mixed.count(), counts)

    assert counts == {
        VALID: 4, QUARANTINE: 5, DUPLICATE: 1, NON_CLOSING: 2, SUPERSEDED: 3, REDUNDANT: 1,
    }  # fmt: skip
    assert balance.balanced and balance.input_rows == sum(counts.values()) == 16
    assert balance.describe() == (
        "16 Bronze rows = 4 valid + 5 quarantine + 1 duplicate + 2 non_closing_bulletin "
        "+ 3 superseded_revision + 1 redundant_latest_version"
    )


def test_valid_counts_the_logical_rows_selected_not_the_rows_merged(
    spark: SparkSession, mixed: DataFrame
) -> None:
    result = _standardize(spark, mixed)

    assert destination_counts(result)[VALID] == result.silver.count()
    assert result.silver.select("indicator_id", "reference_date").distinct().count() == 4


def test_a_lost_row_fails_critical_and_the_run_is_failed(
    spark: SparkSession, mixed: DataFrame
) -> None:
    result = _standardize(spark, mixed)
    lossy = SilverResult(result.silver.limit(3), result.quarantine, result.excluded)
    sink = InMemoryObservabilitySink()

    check = reconcile_silver(mixed.count(), destination_counts(lossy)).to_check_result(
        table_name="silver_indicator_observations", execution_timestamp=RUN_AT
    )
    with (
        pytest.raises(CriticalQualityError, match="reconciliation"),
        track_run(
            pipeline_name="financial_silver", run_id="R1", project="financial",
            layer=Layer.SILVER, table="silver_indicator_observations", sink=sink,
        ) as step,
    ):  # fmt: skip
        step.record_quality([check])
        pytest.fail("nothing may be written after a failed reconciliation")

    assert (check.status, check.failed_rows) == (Status.CRITICAL, 1)
    assert check.details is not None and check.details.startswith("16 Bronze rows != 3 valid")
    [run] = sink.runs
    assert run.status is RunStatus.FAILED


def test_a_rerun_gives_the_same_destinations_and_rows(
    spark: SparkSession, mixed: DataFrame
) -> None:
    first, second = _standardize(spark, mixed), _standardize(spark, mixed)

    assert destination_counts(first) == destination_counts(second)
    assert first.silver.exceptAll(second.silver).isEmpty()
    assert second.silver.exceptAll(first.silver).isEmpty()


def test_an_empty_batch_reconciles_to_zero(spark: SparkSession, bronze_frame: Frame) -> None:
    counts = destination_counts(_standardize(spark, bronze_frame([])))

    assert counts == _expected({})
    assert reconcile_silver(0, counts).balanced
