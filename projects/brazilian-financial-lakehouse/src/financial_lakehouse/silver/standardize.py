"""Bronze -> Silver: one validated, typed value per (indicator, reference date).

Pure ``DataFrame -> DataFrame`` functions (no I/O). Rules (docs/SILVER.md):

1. official observation only: PTAX keeps the closing bulletin (``Fechamento``);
2. row rules (quarantine on failure): known indicator, reference date present, aligned with the
   frequency and not in the future, value present and an exact decimal, positive where it must be;
3. exact duplicates (same ``record_key``) collapse;
4. revisions: the version delivered by the latest request wins; ``revision_count`` keeps how many
   versions exist; different values delivered by the *same* latest request are a conflict and go
   to quarantine instead of being resolved arbitrarily;
5. explicit period bounds per frequency (daily, monthly, quarterly, rolling quarter).

Reconciliation (docs/SILVER.md): every consumed Bronze row has exactly one destination, decided
by the first step that takes it out, in this order: ``non_closing_bulletin`` (step 1),
``quarantine`` (row rules, step 2), ``duplicate`` (step 3), ``superseded_revision`` and
``quarantine`` for conflicts, ``redundant_latest_version`` (step 4), else ``valid``. Each
destination is its own DataFrame, counted separately; the counts must add up to the input.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Final

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, TimestampType

from financial_lakehouse.catalog import INDICATORS, Indicator, SourceKind
from financial_lakehouse.silver.schemas import (
    BRONZE_SCHEMA,
    CATALOG_SCHEMA,
    SILVER_SCHEMA,
    VALUE_TYPE,
)
from lakehouse_common.quality import (
    CustomCondition,
    QualityCheck,
    Reconciliation,
    split_by_quality,
)
from lakehouse_common.quality.runner import EXECUTION_TIMESTAMP_COLUMN, FAILED_CHECKS_COLUMN

DAILY, MONTHLY, QUARTERLY, ROLLING_QUARTER = "daily", "monthly", "quarterly", "rolling_quarter"
FREQUENCIES: Final = (DAILY, MONTHLY, QUARTERLY, ROLLING_QUARTER)
PTAX_CLOSING_BULLETIN: Final = "Fechamento"
DECIMAL_TEXT: Final = r"^-?[0-9]{1,16}(\.[0-9]{1,8})?$"  # fits VALUE_TYPE exactly: no rounding
IBGE_MISSING_MARKERS: Final = ("-", "..", "...", "X")  # SIDRA symbols: not a number
POSITIVE_ONLY: Final = (
    "usd_brl_sell",
    "eur_brl_ptax",
    "international_reserves",
    "monetary_base",
    "credit_total",
)
CONFLICTING_VALUES: Final = "conflicting_values"
KEY: Final = ("indicator_id", "reference_date")

# Destinations of a consumed Bronze row (reconciliation categories, mutually exclusive).
VALID: Final = "valid"  # selected as the observation of its key (logical, before the MERGE)
QUARANTINE: Final = "quarantine"  # failed a row rule, or a conflicting latest value
DUPLICATE: Final = "duplicate"  # same record_key as another valid row: technical re-delivery
NON_CLOSING: Final = "non_closing_bulletin"  # PTAX opening/intermediate bulletin (step 1)
SUPERSEDED: Final = "superseded_revision"  # an older version of a key with a newer delivery
REDUNDANT: Final = "redundant_latest_version"  # same latest request and value, other record_key
DESTINATIONS: Final = (VALID, QUARANTINE, DUPLICATE, NON_CLOSING, SUPERSEDED, REDUNDANT)
_DESTINATION: Final = "_destination"
_PARSED, _TEXT = "_value_parsed", "_value_text"


def catalog_frame(spark: SparkSession, indicators: Sequence[Indicator] = INDICATORS) -> DataFrame:
    """Catalog attributes joined to Bronze (N:1 on indicator_id): name, unit, frequency."""
    rows = [(i.indicator_id, i.name, i.unit, i.frequency) for i in indicators]
    return spark.createDataFrame(rows, CATALOG_SCHEMA)


def _is_official() -> Column:
    """TRUE for the rows kept by step 1; a PTAX row without bulletin type is not official."""
    bulletin = F.get_json_object(F.col("attributes"), "$.tipoBoletim")
    is_ptax = F.col("source") == F.lit(str(SourceKind.BCB_PTAX))
    return F.coalesce(~is_ptax | (bulletin == F.lit(PTAX_CLOSING_BULLETIN)), F.lit(False))


def select_official_observations(bronze: DataFrame) -> DataFrame:
    """PTAX publishes several bulletins a day; the official rate of the day is the closing one."""
    return bronze.filter(_is_official())


def non_closing_bulletins(bronze: DataFrame) -> DataFrame:
    """The Bronze rows step 1 leaves out: PTAX bulletins other than ``Fechamento``."""
    return bronze.filter(~_is_official())


def _sql_list(values: Sequence[str]) -> str:
    return ", ".join(f"'{value}'" for value in values)


MISSING = f"({_TEXT} IS NULL OR {_TEXT} = '' OR {_TEXT} IN ({_sql_list(IBGE_MISSING_MARKERS)}))"
ROW_RULES: Final[tuple[QualityCheck, ...]] = (
    CustomCondition(name="unknown_indicator", condition="indicator_name IS NOT NULL"),
    CustomCondition(name="missing_reference_date", condition="reference_date IS NOT NULL"),
    CustomCondition(
        name="misaligned_reference_date",
        condition=(
            f"reference_date IS NULL OR indicator_name IS NULL OR frequency = '{DAILY}' "
            f"OR (day(reference_date) = 1 AND (frequency <> '{QUARTERLY}' "
            "OR month(reference_date) IN (1, 4, 7, 10)))"
        ),
    ),
    CustomCondition(
        name="future_reference_date",
        condition="reference_date IS NULL OR reference_date <= to_date(ingestion_timestamp)",
    ),
    CustomCondition(name="missing_value", condition=f"NOT {MISSING}"),
    CustomCondition(name="invalid_decimal", condition=f"{MISSING} OR {_PARSED} IS NOT NULL"),
    CustomCondition(
        name="non_positive_value",
        condition=(
            f"{_PARSED} IS NULL OR indicator_id NOT IN ({_sql_list(POSITIVE_ONLY)}) "
            f"OR {_PARSED} > 0"
        ),
    ),
)
"""Each name is the quarantine reason recorded in ``_dq_failed_checks``."""


def with_parsed_value(bronze: DataFrame, catalog: DataFrame) -> DataFrame:
    """Catalog attributes (N:1 on indicator_id) plus the value parsed only if exact."""
    text = F.trim(F.col("value"))
    return bronze.join(F.broadcast(catalog), "indicator_id", "left").select(
        *bronze.columns,
        "indicator_name",
        "unit",
        "frequency",
        text.alias(_TEXT),
        F.when(text.rlike(DECIMAL_TEXT), text.cast(VALUE_TYPE)).alias(_PARSED),
    )


def period_bounds(frequency: Column, reference_date: Column) -> tuple[Column, Column]:
    """Daily: the day. Monthly: the month. Quarterly: 3 months from the reference.
    Rolling quarter: the 3 months *ending* in the reference month (IBGE PNAD convention)."""
    start = F.when(frequency == ROLLING_QUARTER, F.add_months(reference_date, -2)).otherwise(
        reference_date
    )
    end = (
        F.when(frequency == DAILY, reference_date)
        .when(frequency == QUARTERLY, F.last_day(F.add_months(reference_date, 2)))
        .otherwise(F.last_day(reference_date))
    )
    return start, end


@dataclass(frozen=True, slots=True)
class SilverResult:
    """Published rows, quarantined rows and the Bronze rows left out by a documented rule.

    ``excluded`` maps each exclusion (``DUPLICATE``, ``NON_CLOSING``, ``SUPERSEDED``,
    ``REDUNDANT``) to its Bronze rows, so every destination can be counted on its own.
    """

    silver: DataFrame
    quarantine: DataFrame
    excluded: Mapping[str, DataFrame] = field(default_factory=dict)


def _quarantine_columns() -> list[Column]:
    return [
        *(F.col(field.name) for field in BRONZE_SCHEMA.fields),
        F.col(FAILED_CHECKS_COLUMN).cast(ArrayType(StringType())),
        F.col(EXECUTION_TIMESTAMP_COLUMN).cast(TimestampType()),
    ]


def resolve_revisions(valid: DataFrame, execution_timestamp: datetime) -> SilverResult:
    """Latest delivered version per key; conflicting latest versions are quarantined."""
    key = Window.partitionBy(*KEY)
    seen_at = F.coalesce(F.col("request_timestamp"), F.col("ingestion_timestamp"))
    copies = Window.partitionBy("record_key").orderBy("ingestion_timestamp", "run_id")
    duplicates = valid.withColumn("_copy", F.row_number().over(copies)).filter(F.col("_copy") > 1)
    versions = (
        valid.dropDuplicates(["record_key"])
        .withColumn("_seen_at", seen_at)
        .withColumn("_latest_at", F.max("_seen_at").over(key))
        .withColumn("revision_count", F.count(F.lit(1)).over(key).cast("int"))
    )
    superseded = versions.filter(F.col("_seen_at") < F.col("_latest_at"))
    latest = versions.filter(F.col("_seen_at") == F.col("_latest_at")).withColumn(
        "_latest_values", F.size(F.collect_set(_PARSED).over(key))
    )
    conflicts = (
        latest.filter(F.col("_latest_values") > 1)
        .withColumn(FAILED_CHECKS_COLUMN, F.array(F.lit(CONFLICTING_VALUES)))
        .withColumn(EXECUTION_TIMESTAMP_COLUMN, F.lit(execution_timestamp))
        .select(*_quarantine_columns())
    )
    order = Window.partitionBy(*KEY).orderBy(
        F.col("ingestion_timestamp").desc(), F.col("record_key").desc()
    )
    ranked = latest.filter(F.col("_latest_values") == 1).withColumn(
        "_rank", F.row_number().over(order)
    )
    winners = ranked.filter(F.col("_rank") == 1)
    redundant = ranked.filter(F.col("_rank") > 1)
    start, end = period_bounds(F.col("frequency"), F.col("reference_date"))
    silver = winners.select(
        "indicator_id",
        "indicator_name",
        "reference_date",
        F.col(_PARSED).alias("value"),
        "unit",
        "frequency",
        "source",
        F.col("_seen_at").alias("updated_at"),
        start.alias("period_start"),
        end.alias("period_end"),
        "series_id",
        "revision_count",
        "record_key",
        F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
    )
    excluded = {DUPLICATE: duplicates, SUPERSEDED: superseded, REDUNDANT: redundant}
    return SilverResult(
        silver, conflicts, {name: _bronze_rows(rows) for name, rows in excluded.items()}
    )


def _bronze_rows(frame: DataFrame) -> DataFrame:
    return frame.select(*BRONZE_SCHEMA.fieldNames())


def build_silver(
    bronze: DataFrame, catalog: DataFrame, *, execution_timestamp: datetime
) -> SilverResult:
    """Standardized series (``SILVER_SCHEMA`` order) and every rejected Bronze row with reasons."""
    candidates = with_parsed_value(select_official_observations(bronze), catalog)
    valid, rejected = split_by_quality(
        candidates, ROW_RULES, execution_timestamp=execution_timestamp
    )
    resolved = resolve_revisions(valid, execution_timestamp)
    silver = resolved.silver.select(
        *(F.col(field.name).cast(field.dataType) for field in SILVER_SCHEMA.fields)
    )
    quarantine = rejected.select(*_quarantine_columns()).unionByName(resolved.quarantine)
    excluded = {NON_CLOSING: non_closing_bulletins(bronze), **resolved.excluded}
    return SilverResult(silver, quarantine, excluded)


def destination_counts(result: SilverResult) -> dict[str, int]:
    """Rows per destination (``DESTINATIONS`` order), each counted from its own DataFrame.

    One Spark action: the destinations are unioned as labels and grouped. ``VALID`` is the
    number of logical observations selected for the MERGE, not the rows the MERGE changes.
    """
    frames = {VALID: result.silver, QUARANTINE: result.quarantine, **result.excluded}
    unknown = sorted(set(frames) - set(DESTINATIONS))
    if unknown:
        raise ValueError(f"undeclared Silver destinations: {unknown}")
    labels = [frame.select(F.lit(name).alias(_DESTINATION)) for name, frame in frames.items()]
    union = labels[0]
    for frame in labels[1:]:
        union = union.unionByName(frame)
    # at most len(DESTINATIONS) rows: a bounded collect
    counted = {
        row[_DESTINATION]: int(row["count"])
        for row in union.groupBy(_DESTINATION).count().collect()
    }
    return {name: counted.get(name, 0) for name in DESTINATIONS}


def reconcile_silver(bronze_rows: int, counts: Mapping[str, int]) -> Reconciliation:
    """The Silver equation: ``bronze_consumed = sum of the DESTINATIONS counts``."""
    return Reconciliation(bronze_rows, {name: counts.get(name, 0) for name in DESTINATIONS})


def versions_of_touched_keys(bronze: DataFrame, changed: DataFrame) -> DataFrame:
    """Incremental input: every Bronze version of the keys that ``changed`` rows touch.

    ``changed`` are the rows of the Bronze commits not yet consumed (``silver.progress``). All
    versions of a touched key are read so revision resolution and ``revision_count`` stay exact;
    untouched keys are not reprocessed. Progress is by Delta version, not by ingestion time.
    """
    touched = (
        changed.select(*KEY)
        .distinct()
        .withColumnRenamed("indicator_id", "_indicator_id")
        .withColumnRenamed("reference_date", "_reference_date")
    )
    condition = (F.col("indicator_id") == F.col("_indicator_id")) & F.col(
        "reference_date"
    ).eqNullSafe(F.col("_reference_date"))
    return bronze.join(touched, condition, "left_semi")


def not_in(frame: DataFrame, recorded: DataFrame | None, columns: Sequence[str]) -> DataFrame:
    """Rows of ``frame`` whose ``columns`` are absent from ``recorded`` (``None``: all rows).

    The event identity used to alert once: a problem already recorded (a record_key already in
    quarantine, an observation already in Silver) is not reported again.
    """
    if recorded is None:
        return frame
    return frame.join(recorded.select(*columns).distinct(), list(columns), "left_anti")
