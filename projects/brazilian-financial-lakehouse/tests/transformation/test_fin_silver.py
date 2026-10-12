"""Silver standardization: decimals, dates, nulls, duplicates, frequencies, revisions.

Every scenario is one or more Bronze rows labelled by ``run_id`` (the case name), each with its
own (indicator, reference_date) key so scenarios cannot interfere. The batch is standardized
once per module: each Spark job costs seconds locally, the assertions stay one test per rule.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from financial_lakehouse.silver import (
    SILVER_SCHEMA,
    build_silver,
    catalog_frame,
    versions_of_touched_keys,
)

RUN_AT = datetime(2026, 10, 6, 15, tzinfo=UTC)
EARLY, LATE = datetime(2026, 9, 1, 12, tzinfo=UTC), datetime(2026, 10, 6, 12, tzinfo=UTC)
Factory = Callable[..., dict[str, Any]]
Frame = Callable[[list[dict[str, Any]]], DataFrame]

QUARANTINE_CASES: dict[str, tuple[dict[str, Any], str]] = {
    "letters": ({"value": "abc"}, "invalid_decimal"),
    "decimal_comma": ({"value": "1,06"}, "invalid_decimal"),  # the APIs deliver a dot
    "nine_decimals": ({"value": "0.123456789"}, "invalid_decimal"),  # would be rounded
    "null_value": ({"value": None}, "missing_value"),
    "blank_value": ({"value": ""}, "missing_value"),
    "ibge_not_available": ({"value": "..."}, "missing_value"),
    "ibge_suppressed": ({"value": "X"}, "missing_value"),
    "unknown_indicator": ({"indicator_id": "selic_meta"}, "unknown_indicator"),
    "null_reference_date": ({"reference_date": None}, "missing_reference_date"),
    "monthly_not_day_one": ({"reference_date": date(2025, 2, 15)}, "misaligned_reference_date"),
    "quarter_not_aligned": (
        {"indicator_id": "gdp_growth_yoy", "reference_date": date(2025, 2, 1)},
        "misaligned_reference_date",
    ),
    "future": ({"reference_date": date(2026, 11, 1)}, "future_reference_date"),
    "zero_exchange_rate": (
        {"indicator_id": "usd_brl_sell", "reference_date": date(2025, 2, 3), "value": "0"},
        "non_positive_value",
    ),
    "negative_balance": ({"indicator_id": "credit_total", "value": "-5"}, "non_positive_value"),
}
DECIMAL_CASES: dict[str, tuple[str, date, str, Decimal]] = {
    "fx_trailing_zeros": ("usd_brl_sell", date(2025, 2, 3), "6.42500", Decimal("6.42500")),
    "negative_rate": ("igpm_monthly", date(2019, 1, 1), "-0.34", Decimal("-0.34")),
    "large_balance": (
        "credit_total",
        date(2025, 2, 1),
        "6543210987654.12",
        Decimal("6543210987654.12"),
    ),
    "blanks_trimmed": ("selic_daily", date(2025, 2, 3), " 0.055131 ", Decimal("0.055131")),
}
PERIOD_CASES: dict[str, tuple[str, date, tuple[date, date]]] = {
    "daily": ("selic_daily", date(2025, 2, 4), (date(2025, 2, 4), date(2025, 2, 4))),
    "monthly_leap_year": ("ipca_monthly", date(2024, 2, 1), (date(2024, 2, 1), date(2024, 2, 29))),
    "quarterly": ("gdp_growth_yoy", date(2025, 4, 1), (date(2025, 4, 1), date(2025, 6, 30))),
    "rolling_quarter": (
        "unemployment_rate",
        date(2025, 3, 1),
        (date(2025, 1, 1), date(2025, 3, 31)),
    ),
}


def _local(moment: datetime) -> datetime:
    """PySpark returns TIMESTAMP values as naive datetimes in the local time zone."""
    return moment.astimezone().replace(tzinfo=None)


def _scenarios(make: Factory) -> list[dict[str, Any]]:
    rows = [make(run_id="catalog")]
    rows += [make(run_id=case, **overrides) for case, (overrides, _) in QUARANTINE_CASES.items()]
    rows += [
        make(run_id=case, indicator_id=i, reference_date=d, value=text)
        for case, (i, d, text, _) in DECIMAL_CASES.items()
    ]
    rows += [
        make(run_id=case, indicator_id=i, reference_date=d)
        for case, (i, d, _) in PERIOD_CASES.items()
    ]

    def ptax(kind: str, value: str, time: str) -> dict[str, Any]:
        return make(
            run_id="ptax",
            source="bcb_ptax",
            series_id="EUR",
            indicator_id="eur_brl_ptax",
            reference=f"2025-01-02 {time}",
            reference_date=date(2025, 1, 2),
            value=value,
            attributes=f'{{"tipoBoletim": "{kind}", "cotacaoCompra": "6.4"}}',
        )

    rows += [
        ptax("Abertura", "6.40", "10:10"),
        ptax("Intermediário", "6.41", "11:10"),
        ptax("Fechamento", "6.4250", "13:10"),
    ]

    def igpm(case: str, month: int, value: str, **extra: Any) -> dict[str, Any]:
        reference_date = date(2020, month, 1)
        return make(
            run_id=case,
            reference=f"01/{month:02d}/2020",
            reference_date=reference_date,
            value=value,
            **extra,
        )

    rows += [
        igpm("revision", 1, "1.10", request_timestamp=LATE),
        igpm("revision", 1, "1.06", request_timestamp=EARLY),
    ]
    rows += [igpm("duplicate", 2, "0.50"), igpm("duplicate", 2, "0.50")]
    rows += [
        igpm("conflict", 3, "1.00", request_timestamp=EARLY),
        igpm("conflict", 3, "1.06"),
        igpm("conflict", 3, "1.07"),
    ]
    rows += [
        igpm("no_request_time", 4, "1.06", request_timestamp=None, ingestion_timestamp=EARLY),
        igpm("no_request_time", 4, "1.10", request_timestamp=None, ingestion_timestamp=LATE),
    ]
    return rows


@dataclass(frozen=True)
class Outcome:
    bronze: list[dict[str, Any]]
    silver: dict[str, list[Row]]  # by case (via record_key)
    quarantine: dict[str, list[Row]]  # by case (run_id)
    schema: list[tuple[str, Any]]


@pytest.fixture(scope="module")
def outcome(spark: SparkSession, bronze_row: Factory, bronze_frame: Frame) -> Outcome:
    rows = _scenarios(bronze_row)
    result = build_silver(bronze_frame(rows), catalog_frame(spark), execution_timestamp=RUN_AT)
    case_of = {row["record_key"]: row["run_id"] for row in rows}
    silver: dict[str, list[Row]] = {}
    for row in result.silver.collect():
        silver.setdefault(case_of[row.record_key], []).append(row)
    quarantine: dict[str, list[Row]] = {}
    for row in result.quarantine.collect():
        quarantine.setdefault(row.run_id, []).append(row)
    schema = [(f.name, f.dataType) for f in result.silver.schema.fields]
    return Outcome(rows, silver, quarantine, schema)


def test_silver_has_the_contract_schema(outcome: Outcome) -> None:
    assert outcome.schema == [(f.name, f.dataType) for f in SILVER_SCHEMA.fields]


def test_conceptual_columns_come_from_the_catalog(outcome: Outcome) -> None:
    [row] = outcome.silver["catalog"]
    assert (row.indicator_id, row.indicator_name, row.unit, row.frequency, row.source) == (
        "igpm_monthly",
        "IGP-M (FGV) - variação mensal",
        "%",
        "monthly",
        "bcb_sgs",
    )
    assert row.updated_at == _local(LATE)  # request time of the winning version
    assert "catalog" not in outcome.quarantine


@pytest.mark.parametrize("case", DECIMAL_CASES)
def test_decimal_text_is_parsed_exactly(outcome: Outcome, case: str) -> None:
    [row] = outcome.silver[case]
    assert row.value == DECIMAL_CASES[case][3]
    assert case not in outcome.quarantine


@pytest.mark.parametrize("case", QUARANTINE_CASES)
def test_invalid_rows_are_quarantined_with_the_reason(outcome: Outcome, case: str) -> None:
    [rejected] = outcome.quarantine[case]
    assert rejected._dq_failed_checks == [QUARANTINE_CASES[case][1]]
    assert rejected._dq_execution_timestamp == _local(RUN_AT)
    assert case not in outcome.silver


def test_quarantine_keeps_the_bronze_lineage(outcome: Outcome) -> None:
    [rejected] = outcome.quarantine["letters"]
    [source] = [r for r in outcome.bronze if r["run_id"] == "letters"]
    assert (rejected.record_key, rejected.raw_file, rejected.value) == (
        source["record_key"],
        source["raw_file"],
        "abc",
    )


@pytest.mark.parametrize("case", PERIOD_CASES)
def test_period_bounds_follow_the_frequency(outcome: Outcome, case: str) -> None:
    [row] = outcome.silver[case]
    assert (row.period_start, row.period_end) == PERIOD_CASES[case][2]


def test_ptax_keeps_only_the_closing_bulletin(outcome: Outcome) -> None:
    [row] = outcome.silver["ptax"]
    assert (row.value, row.reference_date) == (Decimal("6.4250"), date(2025, 1, 2))
    assert "ptax" not in outcome.quarantine  # other bulletins are excluded, not invalid


def test_latest_revision_wins_and_history_is_counted(outcome: Outcome) -> None:
    [row] = outcome.silver["revision"]
    assert (row.value, row.revision_count, row.updated_at) == (Decimal("1.10"), 2, _local(LATE))


def test_exact_duplicates_collapse(outcome: Outcome) -> None:
    [row] = outcome.silver["duplicate"]
    assert row.revision_count == 1


def test_different_values_in_the_same_latest_request_are_a_conflict(outcome: Outcome) -> None:
    assert "conflict" not in outcome.silver  # never resolved arbitrarily
    assert sorted((r.value, r._dq_failed_checks) for r in outcome.quarantine["conflict"]) == [
        ("1.06", ["conflicting_values"]),
        ("1.07", ["conflicting_values"]),
    ]


def test_missing_request_timestamp_falls_back_to_ingestion_time(outcome: Outcome) -> None:
    [row] = outcome.silver["no_request_time"]
    assert (row.value, row.updated_at) == (Decimal("1.10"), _local(LATE))


def test_one_row_per_indicator_and_date(outcome: Outcome) -> None:
    keys = [(r.indicator_id, r.reference_date) for rows in outcome.silver.values() for r in rows]
    assert len(keys) == len(set(keys))


def test_incremental_input_rereads_every_version_of_touched_keys_only(
    bronze_row: Factory, bronze_frame: Frame
) -> None:
    old_feb = bronze_row(value="1.06", ingestion_timestamp=EARLY)
    new_feb = bronze_row(value="1.10", ingestion_timestamp=LATE)
    old_mar = bronze_row(
        reference="01/03/2025", reference_date=date(2025, 3, 1), ingestion_timestamp=EARLY
    )
    bronze = bronze_frame([old_feb, new_feb, old_mar])

    selected = versions_of_touched_keys(bronze, bronze_frame([new_feb])).collect()

    assert {r.record_key for r in selected} == {old_feb["record_key"], new_feb["record_key"]}


def test_the_input_follows_the_commits_not_the_ingestion_time(
    bronze_row: Factory, bronze_frame: Frame
) -> None:
    """Regression of the former timestamp watermark: rows of one Bronze run share its start as
    ``ingestion_timestamp``, so a row committed after a Silver read looked already consumed
    (``>``) or forced the whole run to be re-read and re-alerted (``>=``). The input is now the
    keys of the uncommitted Bronze versions, whatever their timestamps."""
    run_start = datetime(2026, 10, 6, 12, 0, 5, tzinfo=UTC)
    seen = bronze_row(ingestion_timestamp=run_start)  # consumed by the previous Silver run
    written_later = bronze_row(
        indicator_id="ipca_monthly",
        series_id="1737/63",
        source="ibge_agregados",
        reference="202502",
        ingestion_timestamp=run_start,  # same Bronze run, in a later Delta commit
    )
    bronze = bronze_frame([seen, written_later])

    selected = versions_of_touched_keys(bronze, bronze_frame([written_later])).collect()

    assert {r.record_key for r in selected} == {written_later["record_key"]}
