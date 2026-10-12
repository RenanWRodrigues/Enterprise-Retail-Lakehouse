"""Financial quality suites (declarations; evaluation in lakehouse_common.quality).

Critical rules requested for Financial (docs/DATA_QUALITY.md §8):

- duplicated dates: one value per (indicator_id, reference_date) — CRITICAL gate on Silver;
- nulls: ``value``/``reference_date`` NULL in Silver — CRITICAL (Silver quarantines NULL values
  before, so a NULL here is a bug);
- unexpected gaps: missing periods per series — WARNING, checked on the *full* Silver table after
  the merge (an incremental batch holds only the keys it touched);
- invalid values: not an exact decimal, non-positive price/balance, misaligned or future date —
  ERROR, quarantined by the Silver row rules; implausible magnitudes — WARNING;
- schema changes: Bronze and Silver contracts (``SchemaMatches``) — CRITICAL.
"""

from collections.abc import Mapping
from typing import Final

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from financial_lakehouse.gold.schemas import GOLD_SCHEMAS
from financial_lakehouse.silver.schemas import BRONZE_SCHEMA, SILVER_SCHEMA
from financial_lakehouse.silver.standardize import (
    DAILY,
    FREQUENCIES,
    QUARTERLY,
    ROW_RULES,
)
from lakehouse_common.quality import (
    AcceptedValues,
    CheckSet,
    CustomCondition,
    NotNull,
    QualityCheck,
    SchemaMatches,
    SequenceGaps,
    Severity,
    Unique,
)

CRITICAL, WARNING = Severity.CRITICAL, Severity.WARNING
BRONZE_TABLE: Final = "bronze_indicator_observations"
SILVER_TABLE: Final = "silver_indicator_observations"

CALENDAR_DAILY: Final = frozenset({"selic_target"})  # published for every calendar day
BUSINESS_DAY_MAX_GAP_DAYS: Final = 5
"""Without a holiday calendar: Friday -> Wednesday (Carnival) is the longest normal gap."""
MONTHS_PER_QUARTER: Final = 3

PLAUSIBLE_RANGES: Final[dict[str, tuple[float, float]]] = {
    "selic_target": (0.0, 50.0),  # % a.a.
    "selic_daily": (0.0, 0.2),  # % a.d.
    "ipca_monthly": (-2.0, 5.0),  # % a.m.
    "igpm_monthly": (-5.0, 8.0),  # % a.m.
    "gdp_growth_yoy": (-15.0, 20.0),  # %
    "usd_brl_sell": (1.0, 15.0),  # R$/US$
    "eur_brl_ptax": (1.0, 15.0),  # R$/EUR
    "international_reserves": (10_000.0, 1_000_000.0),  # US$ milhões
    "unemployment_rate": (2.0, 25.0),  # %
}
"""Outside these bounds a value is suspicious (WARNING), not impossible: alert a human."""

GOLD_GRAIN: Final[dict[str, tuple[str, ...]]] = {
    name: (schema.fieldNames()[0],) for name, schema in GOLD_SCHEMAS.items()
}


def with_gap_columns(silver: DataFrame) -> DataFrame:
    """``_period_index`` (day or month ordinal) and ``_max_gap`` (allowed step) per row."""
    frequency, day = F.col("frequency"), F.col("reference_date")
    months = F.year(day) * 12 + F.month(day)
    days = F.datediff(day, F.lit("1970-01-01"))
    max_gap = (
        F.when(F.col("indicator_id").isin(*CALENDAR_DAILY), 1)
        .when(frequency == DAILY, BUSINESS_DAY_MAX_GAP_DAYS)
        .when(frequency == QUARTERLY, MONTHS_PER_QUARTER)
        .otherwise(1)
    )
    return silver.withColumn(
        "_period_index", F.when(frequency == DAILY, days).otherwise(months)
    ).withColumn("_max_gap", max_gap)


def _plausibility() -> CustomCondition:
    cases = " ".join(
        f"WHEN '{indicator}' THEN value BETWEEN {low} AND {high}"
        for indicator, (low, high) in PLAUSIBLE_RANGES.items()
    )
    return CustomCondition(
        name="implausible_value",
        condition=f"value IS NULL OR (CASE indicator_id {cases} ELSE TRUE END)",
        severity=WARNING,
    )


def bronze_suite(bronze: DataFrame) -> list[CheckSet]:
    """Source schema change stops the run before anything is parsed."""
    schema = SchemaMatches(BRONZE_SCHEMA, severity=CRITICAL, name="schema__bronze")
    return [CheckSet(BRONZE_TABLE, bronze, [schema])]


def standardization_suite(candidates: DataFrame) -> list[CheckSet]:
    """The Silver row rules (ERROR: rows quarantined), measured on the rows they judge."""
    return [CheckSet(f"{SILVER_TABLE}__candidates", candidates, list(ROW_RULES))]


def silver_gate_checks() -> list[QualityCheck]:
    """CRITICAL contract of a Silver batch (schema, key, nulls, frequency) plus a plausibility
    WARNING.
    """
    return [
        SchemaMatches(SILVER_SCHEMA, severity=CRITICAL, name="schema__silver"),
        Unique(("indicator_id", "reference_date"), severity=CRITICAL, name="duplicate_dates"),
        NotNull("value", severity=CRITICAL),
        NotNull("reference_date", severity=CRITICAL),
        AcceptedValues("frequency", FREQUENCIES, severity=CRITICAL),
        _plausibility(),
    ]


def silver_suite(silver_batch: DataFrame, new_rows: DataFrame | None = None) -> list[CheckSet]:
    """Pre-write gate on the batch about to be merged.

    With ``new_rows`` (the batch rows whose version is not in Silver yet), the plausibility
    WARNING is measured on them only: an implausible value is reported when it first arrives,
    not again each time its key is reprocessed. The CRITICAL contract always covers the batch.
    """
    checks = silver_gate_checks()
    if new_rows is None:
        return [CheckSet(SILVER_TABLE, silver_batch, checks)]
    blocking = [check for check in checks if check.severity is CRITICAL]
    warnings = [check for check in checks if check.severity is not CRITICAL]
    return [
        CheckSet(SILVER_TABLE, silver_batch, blocking),
        CheckSet(SILVER_TABLE, new_rows, warnings),
    ]


GAPS_CHECK: Final = "unexpected_gaps"
_GAP_FLAG, _PREVIOUS_DATE = "_gap_before", "_previous_reference_date"


def gap_events(silver_table: DataFrame) -> DataFrame:
    """Each Silver row with its previous date and whether a gap precedes it (whole history)."""
    gaps = SequenceGaps(("indicator_id",), "_period_index", "_max_gap")
    previous = F.lag("reference_date").over(
        Window.partitionBy("indicator_id").orderBy("_period_index")
    )
    return gaps.annotate(
        with_gap_columns(silver_table).withColumn(_PREVIOUS_DATE, previous), _GAP_FLAG
    )


def silver_table_suite(
    silver_table: DataFrame, new_keys: DataFrame | None = None
) -> list[CheckSet]:
    """Post-merge checks that need the whole history (WARNING only: the data is written).

    A gap is the hole between a row and its previous row. With ``new_keys`` (the
    ``indicator_id, reference_date`` this run inserted), only gaps with a new row at either end
    are reported: a gap appears when a key is inserted next to a hole, so an old gap is not
    reported again. Without ``new_keys``, every gap of the table is checked.
    """
    events = gap_events(silver_table)
    if new_keys is not None:
        keys = new_keys.select("indicator_id", "reference_date").distinct()
        after = events.join(keys, ["indicator_id", "reference_date"], "left_semi")
        before = events.join(
            keys.withColumnRenamed("reference_date", _PREVIOUS_DATE),
            ["indicator_id", _PREVIOUS_DATE],
            "left_semi",
        )
        events = after.unionByName(before).dropDuplicates(["indicator_id", "reference_date"])
    gaps = CustomCondition(name=GAPS_CHECK, condition=f"NOT {_GAP_FLAG}", severity=WARNING)
    return [CheckSet(SILVER_TABLE, events, [gaps])]


def gold_suite(tables: Mapping[str, DataFrame]) -> list[CheckSet]:
    """Contract and grain of every Gold table (CRITICAL: nothing is replaced on failure)."""
    return [
        CheckSet(
            name,
            frame,
            [
                SchemaMatches(GOLD_SCHEMAS[name], severity=CRITICAL, name=f"schema__{name}"),
                Unique(GOLD_GRAIN[name], severity=CRITICAL, name=f"grain__{name}"),
                NotNull(GOLD_GRAIN[name][0], severity=CRITICAL),
            ],
        )
        for name, frame in tables.items()
    ]
