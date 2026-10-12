"""Financial test fixtures.

HTTP: a scripted fake transport and recorded official payloads. No test touches the network.
Payloads in tests/unit/fixtures/ are real responses captured from the official APIs on
2026-10-05 (see docs/data_sources.md).

Spark (Silver/Gold): a Bronze row factory and a synthetic Silver.

The synthetic Silver is deterministic and small (2024-01 .. 2026-02, daily series up to Friday
2026-02-13 so February 2026 is a partial month). Tests compute expected values from the same
rows with plain Python (independent oracle).
"""

from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from random import Random
from typing import Any

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from financial_lakehouse.catalog import indicator
from financial_lakehouse.clients.http import HttpClient, HttpResponse, RetryPolicy
from financial_lakehouse.gold import build_gold
from financial_lakehouse.silver import BRONZE_SCHEMA, SILVER_SCHEMA
from financial_lakehouse.transformation.bronze_records import record_key_of

FIXTURES = Path(__file__).parent / "unit" / "fixtures"
Outcome = HttpResponse | Exception


class FakeTransport:
    """Returns scripted outcomes in order (or per URL-prefix route) and records every call."""

    def __init__(self, outcomes: list[Outcome] | None = None) -> None:
        self.outcomes = list(outcomes or [])
        self.routes: list[tuple[Callable[[str], bool], Callable[[str], Outcome]]] = []
        self.calls: list[str] = []

    def route(self, predicate: Callable[[str], bool], responder: Callable[[str], Outcome]) -> None:
        self.routes.append((predicate, responder))

    def __call__(self, url: str, timeout: float) -> HttpResponse:
        self.calls.append(url)
        outcome: Outcome | None = None
        for predicate, responder in self.routes:
            if predicate(url):
                outcome = responder(url)
                break
        if outcome is None:
            if not self.outcomes:
                raise AssertionError(f"unexpected request: {url}")
            outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def response(
    status: int = 200, body: bytes = b"[]", headers: dict[str, str] | None = None
) -> HttpResponse:
    return HttpResponse(
        url="https://example.invalid", status=status, headers=headers or {}, body=body
    )


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


@pytest.fixture
def transport() -> FakeTransport:
    return FakeTransport()


@pytest.fixture
def sleeps() -> list[float]:
    return []


@pytest.fixture
def http(transport: FakeTransport, sleeps: list[float]) -> HttpClient:
    """Client wired to the fake transport; sleeping is recorded, never real."""
    return HttpClient(
        transport=transport,
        policy=RetryPolicy(max_attempts=4, base_delay_seconds=1.0, max_delay_seconds=8.0),
        sleep=sleeps.append,
        rng=Random(0),
    )


@pytest.fixture
def respond() -> Callable[..., HttpResponse]:
    """Factory for fake responses: ``respond(status, body, headers)``."""
    return response


@pytest.fixture
def payload() -> Callable[[str], bytes]:
    """Recorded official payload by file name (tests/unit/fixtures/)."""
    return fixture_bytes


# --- Spark: synthetic Silver, Gold and Bronze factories ----------------------------------

SilverRow = dict[str, Any]
FIRST_DAY, LAST_DAILY = date(2024, 1, 1), date(2026, 2, 13)
UPDATED = datetime(2026, 2, 14, 12, 0, tzinfo=UTC)
IPCA = [
    "0.42",
    "0.83",
    "0.16",
    "0.38",
    "0.46",
    "0.21",
    "0.38",
    "-0.02",
    "0.44",
    "0.56",
    "0.39",
    "0.52",
]
IGPM = [
    "0.07",
    "-0.52",
    "-0.47",
    "0.31",
    "0.89",
    "0.81",
    "0.61",
    "0.29",
    "0.62",
    "1.52",
    "1.30",
    "0.94",
]
SATURDAY = 5


def _months(first: date, count: int) -> list[date]:
    return [
        date(first.year + (first.month - 1 + i) // 12, (first.month - 1 + i) % 12 + 1, 1)
        for i in range(count)
    ]


def _last_day(month: date) -> date:
    following = _months(month, 2)[1]
    return following - timedelta(days=1)


def selic_target_on(day: date) -> Decimal:
    if day < date(2024, 5, 9):
        return Decimal("11.75")
    if day < date(2025, 1, 30):
        return Decimal("10.50")
    return Decimal("13.25")


def _row(indicator_id: str, reference: date, value: Decimal, start: date, end: date) -> SilverRow:
    item = indicator(indicator_id)
    return {
        "indicator_id": indicator_id,
        "indicator_name": item.name,
        "reference_date": reference,
        "value": value,
        "unit": item.unit,
        "frequency": item.frequency,
        "source": str(item.source),
        "updated_at": UPDATED,
        "period_start": start,
        "period_end": end,
        "series_id": indicator_id,
        "revision_count": 1,
        "record_key": f"{indicator_id}-{reference}",
        "bronze_ingestion_timestamp": UPDATED,
    }


def synthetic_silver_rows() -> list[SilverRow]:
    rows: list[SilverRow] = []
    days = [FIRST_DAY + timedelta(days=i) for i in range((LAST_DAILY - FIRST_DAY).days + 1)]
    for day in days:
        target = selic_target_on(day)
        rows.append(_row("selic_target", day, target, day, day))
        if day.weekday() >= SATURDAY:
            continue
        month_index = (day.year - 2024) * 12 + day.month - 1
        daily = ((1 + float(target) / 100) ** (1 / 252) - 1) * 100
        usd = Decimal("5.0000") + Decimal("0.05") * month_index + Decimal(day.day) / 1000
        rows += [
            _row("selic_daily", day, Decimal(f"{daily:.6f}"), day, day),
            _row("usd_brl_sell", day, usd, day, day),
            _row(
                "eur_brl_ptax", day, (usd * Decimal("1.08")).quantize(Decimal("0.0001")), day, day
            ),
            _row("international_reserves", day, Decimal(350000 + 100 * month_index), day, day),
        ]
    for i, month in enumerate(_months(date(2024, 1, 1), 25)):  # 2024-01 .. 2026-01
        end = _last_day(month)
        rows.append(_row("ipca_monthly", month, Decimal(IPCA[i % 12]), month, end))
        rows.append(_row("igpm_monthly", month, Decimal(IGPM[i % 12]), month, end))
    for i, month in enumerate(_months(date(2024, 1, 1), 24)):  # 2024-01 .. 2025-12
        end = _last_day(month)
        rows.append(_row("monetary_base", month, Decimal(400_000_000 + 1_000_000 * i), month, end))
        rows.append(_row("credit_total", month, Decimal(6_000_000 + 50_000 * i), month, end))
        rolling_start = _months(date(2023, 11, 1), i + 1)[-1]  # rolling quarter ends in `month`
        rows.append(
            _row(
                "unemployment_rate", month, Decimal("7.8") - Decimal("0.1") * i, rolling_start, end
            )
        )
    for i, quarter in enumerate(_months(date(2024, 1, 1), 21)[::3]):  # 2024Q1 .. 2025Q3
        end = _last_day(_months(quarter, 3)[2])
        rows.append(
            _row("gdp_growth_yoy", quarter, Decimal("2.5") + Decimal("0.3") * i, quarter, end)
        )
    return rows


@pytest.fixture(scope="session")
def silver_rows() -> list[SilverRow]:
    return synthetic_silver_rows()


@pytest.fixture(scope="session")
def silver_frame(spark: SparkSession, silver_rows: list[SilverRow]) -> DataFrame:
    names = SILVER_SCHEMA.fieldNames()
    frame = spark.createDataFrame([tuple(r[n] for n in names) for r in silver_rows], SILVER_SCHEMA)
    return frame.cache()  # read by every Gold table


@pytest.fixture(scope="session")
def gold_rows(silver_frame: DataFrame) -> dict[str, list[Row]]:
    """Each Gold table of the synthetic Silver, computed once (the tables are tiny)."""
    return {name: frame.collect() for name, frame in build_gold(silver_frame).items()}


@pytest.fixture(scope="session")
def gold_tables(
    spark: SparkSession, silver_frame: DataFrame, gold_rows: dict[str, list[Row]]
) -> dict[str, DataFrame]:
    """The same tables as local DataFrames: SQL tests query them without recomputing Gold.

    Re-created from the collected rows (not ``cache()``): Spark 4.0 failed to resolve columns
    when several cached Gold plans shared sub-plans ("key not found" in InMemoryRelation).
    """
    schemas = {name: frame.schema for name, frame in build_gold(silver_frame).items()}
    return {name: spark.createDataFrame(gold_rows[name], schemas[name]) for name in schemas}


BronzeFactory = Callable[..., dict[str, Any]]


@pytest.fixture(scope="session")
def bronze_row() -> BronzeFactory:
    """A valid Bronze row (SGS IGP-M) whose fields can be overridden; key derived from content."""

    def make(**overrides: Any) -> dict[str, Any]:
        row: dict[str, Any] = {
            "source": "bcb_sgs",
            "series_id": "189",
            "indicator_id": "igpm_monthly",
            "reference": "01/02/2025",
            "reference_date": date(2025, 2, 1),
            "value": "1.06",
            "attributes": "{}",
            "request_timestamp": datetime(2026, 10, 6, 12, tzinfo=UTC),
            "ingestion_timestamp": datetime(2026, 10, 6, 12, 0, 5, tzinfo=UTC),
            "run_id": "R1",
            "raw_file": "raw.json",
            "raw_sha256": "abc",
        } | overrides
        row.setdefault(
            "record_key",
            # Overrides may change the indicator/date without touching series_id/reference, so
            # they are part of the test key too (in production they are implied by the content).
            record_key_of(
                row["source"],
                row["series_id"],
                row["reference"],
                str(row["value"]),
                {
                    "attributes": row["attributes"],
                    "indicator_id": row["indicator_id"],
                    "reference_date": str(row["reference_date"]),
                },
            ),
        )
        return row

    return make


@pytest.fixture(scope="session")
def bronze_frame(spark: SparkSession) -> Callable[[list[dict[str, Any]]], DataFrame]:
    names = BRONZE_SCHEMA.fieldNames()

    def make(rows: list[dict[str, Any]]) -> DataFrame:
        return spark.createDataFrame([tuple(r[n] for n in names) for r in rows], BRONZE_SCHEMA)

    return make


@pytest.fixture(scope="module")
def gold_views(spark: SparkSession, gold_tables: dict[str, DataFrame]) -> Iterator[None]:
    for name, frame in gold_tables.items():
        frame.createOrReplaceTempView(name)
    yield
    for name in gold_tables:
        spark.catalog.dropTempView(name)
