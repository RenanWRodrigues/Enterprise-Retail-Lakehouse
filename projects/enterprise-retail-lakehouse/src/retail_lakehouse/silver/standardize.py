"""Bronze -> Silver for one entity batch: pure ``DataFrame -> DataFrame`` functions (no I/O).

Rules (docs/SILVER.md), all derived from the source contract plus ``silver_row_rules``:

1. exact duplicates (the same delivered row twice in a batch) collapse to one;
2. contract checks on the delivered text: required value (``missing__<col>``), format that parses
   (``invalid__<col>``), accepted value (``out_of_domain__<col>``); business rules on the typed
   values (``negative__…``, ``discount_exceeds_line``…). A failing row goes to quarantine with
   every reason, never fixed silently. Only columns that reach Silver are checked: personal data
   that Silver does not carry (name, e-mail) cannot quarantine a customer;
3. the same business key with different content in one batch is a conflict: every version goes
   to quarantine (``conflicting_values``) instead of being resolved arbitrarily;
4. remaining rows with the same key and the same Silver content collapse (they differed only in
   columns Silver does not keep);
5. typing: text trimmed, empty -> NULL, timestamps with offset -> UTC instants, decimals exact.

Each output is its own DataFrame, so ``input = valid + quarantine + duplicates`` is checked from
independent counts (``BatchCounts.reconcile``) rather than being true by construction.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Final

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType, TimestampType

from lakehouse_common.quality import (
    CustomCondition,
    Dimension,
    QualityCheck,
    Reconciliation,
    Severity,
    split_by_quality,
)
from lakehouse_common.quality.runner import EXECUTION_TIMESTAMP_COLUMN, FAILED_CHECKS_COLUMN
from retail_lakehouse.bronze.transformations import METADATA_COLUMNS, bronze_schema
from retail_lakehouse.ingestion.source_contracts import (
    RETAIL_SOURCE_CONTRACTS,
    ColumnContract,
    LogicalType,
)
from retail_lakehouse.quality.suites import silver_row_rules
from retail_lakehouse.silver.reference import STATE_REGIONS
from retail_lakehouse.silver.schemas import ROW_HASH_COLUMN, SILVER_SCHEMAS, silver_table_schema

SOURCE_PREFIX: Final = "_src_"
CONFLICTING_VALUES: Final = "conflicting_values"
DATE_FORMAT: Final = "yyyy-MM-dd"
# The source writes "yyyy-MM-ddTHH:mm:ss±hh:mm"; the "T" is replaced by a space so the pattern
# needs no quoted literal (the regex still requires the "T").
TIMESTAMP_FORMAT: Final = "yyyy-MM-dd HH:mm:ssXXX"
_DAY: Final = "[0-9]{4}-[0-9]{2}-[0-9]{2}"
PATTERNS: Final = {
    LogicalType.INTEGER: "^-?[0-9]+$",
    LogicalType.DECIMAL: "^-?[0-9]+[.][0-9]{2}$",
    LogicalType.DATE: f"^{_DAY}$",
    LogicalType.TIMESTAMP: f"^{_DAY}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}[+-][0-9]{{2}}:[0-9]{{2}}$",
}
_PARSES: Final = {
    LogicalType.INTEGER: "try_cast({source} AS INT) IS NOT NULL",
    LogicalType.DECIMAL: "try_cast({source} AS DECIMAL(12,2)) IS NOT NULL",
    LogicalType.DATE: f"try_to_timestamp({{source}}, '{DATE_FORMAT}') IS NOT NULL",
    LogicalType.TIMESTAMP: (
        f"try_to_timestamp(replace({{source}}, 'T', ' '), '{TIMESTAMP_FORMAT}') IS NOT NULL"
    ),
}
# Required by the source contract, but tolerated as NULL because Gold has a defined answer:
# a product without supplier points to the unknown supplier; an unknown birth date gives the
# "unknown" age band.
NULL_TOLERATED: Final = frozenset({("products", "supplier_id"), ("customers", "birth_date")})
STATE_COLUMN: Final = "state"
CUSTOMER_ATTRIBUTES: Final = ("city", "state", "region", "segment", "birth_date", "registered_at")
_QUARANTINE_OWN: Final = (ROW_HASH_COLUMN, FAILED_CHECKS_COLUMN, EXECUTION_TIMESTAMP_COLUMN)
_RANK: Final = "_silver_rank"
_CONTENT: Final = "_silver_content"
_VERSIONS: Final = "_silver_versions"


@dataclass(frozen=True, slots=True)
class BatchCounts:
    """Counts of one entity batch, each taken from its own DataFrame."""

    input_rows: int
    output_rows: int
    rejected_rows: int
    duplicate_rows: int

    def reconcile(self) -> None:
        """``input = valid + quarantine + duplicates`` (medallion invariant), or raise
        ``ReconciliationError`` (shared ``lakehouse_common.quality.Reconciliation``)."""
        Reconciliation(
            self.input_rows,
            {
                "valid": self.output_rows,
                "quarantined": self.rejected_rows,
                "duplicates": self.duplicate_rows,
            },
        ).check()


@dataclass(frozen=True)
class StandardizedBatch:
    valid: DataFrame  # silver_table_schema(entity)
    quarantine: DataFrame  # quarantine_table_schema(entity)
    duplicates: DataFrame  # one row per removed duplicate (row fingerprint only)


def silver_source_columns(entity: str) -> list[ColumnContract]:
    """Source columns that reach Silver under the same name (derived columns excluded)."""
    silver = set(SILVER_SCHEMAS[entity].fieldNames())
    return [c for c in RETAIL_SOURCE_CONTRACTS[entity].columns if c.name in silver]


def quarantine_table_schema(entity: str) -> StructType:
    """The Bronze row as delivered, its fingerprint, the failed checks and when they ran."""
    return StructType(
        [
            *bronze_schema(RETAIL_SOURCE_CONTRACTS[entity]).fields,
            StructField(ROW_HASH_COLUMN, StringType(), False),
            StructField(FAILED_CHECKS_COLUMN, ArrayType(StringType()), False),
            StructField(EXECUTION_TIMESTAMP_COLUMN, TimestampType(), False),
        ]
    )


def _source(name: str) -> str:
    return f"`{SOURCE_PREFIX}{name}`"


def _validity(logical_type: LogicalType, source: str) -> str | None:
    if logical_type is LogicalType.BOOLEAN:
        return f"{source} IN ('true', 'false')"
    if logical_type not in PATTERNS:
        return None
    parses = _PARSES[logical_type].format(source=source)
    return f"{source} RLIKE '{PATTERNS[logical_type]}' AND {parses}"


def _domain(column: ColumnContract) -> tuple[str, ...]:
    if column.accepted_values:
        return column.accepted_values
    if column.name == STATE_COLUMN:
        return tuple(sorted(STATE_REGIONS))
    return ()


def _rule(name: str, condition: str, column: str, dimension: Dimension) -> QualityCheck:
    return CustomCondition(
        name=name,
        condition=condition,
        severity=Severity.ERROR,  # the row goes to quarantine, the batch continues
        dimension=dimension,
        column=column,
    )


def contract_rules(entity: str) -> list[QualityCheck]:
    """Required, format and domain checks on the delivered text of every Silver column."""
    rules: list[QualityCheck] = []
    for column in silver_source_columns(entity):
        name, source = column.name, _source(column.name)
        if not column.nullable and (entity, name) not in NULL_TOLERATED:
            condition = f"{source} IS NOT NULL AND trim({source}) <> ''"
            rules.append(_rule(f"missing__{name}", condition, name, Dimension.COMPLETENESS))
        if validity := _validity(column.logical_type, source):
            condition = f"{source} IS NULL OR trim({source}) = '' OR ({validity})"
            rules.append(_rule(f"invalid__{name}", condition, name, Dimension.VALIDITY))
        if domain := _domain(column):
            values = ", ".join(f"'{value}'" for value in domain)
            condition = f"{source} IS NULL OR trim({source}) IN ({values})"
            rules.append(_rule(f"out_of_domain__{name}", condition, name, Dimension.VALIDITY))
    return rules


def _typed(column: ColumnContract) -> Column:
    text = F.trim(F.col(f"{SOURCE_PREFIX}{column.name}"))
    value = F.when(text == "", None).otherwise(text)
    typed = {
        LogicalType.STRING: value,
        LogicalType.INTEGER: value.try_cast("int"),
        LogicalType.DECIMAL: value.try_cast("decimal(12,2)"),
        LogicalType.DATE: F.to_date(F.try_to_timestamp(value, F.lit(DATE_FORMAT))),
        LogicalType.TIMESTAMP: F.try_to_timestamp(
            F.replace(value, F.lit("T"), F.lit(" ")), F.lit(TIMESTAMP_FORMAT)
        ),
        LogicalType.BOOLEAN: F.when(value == "true", True).when(value == "false", False),
    }
    return typed[column.logical_type].alias(column.name)


def _derived(entity: str) -> list[Column]:
    if entity != "customers":
        return []
    regions = F.create_map(*(F.lit(item) for pair in STATE_REGIONS.items() for item in pair))
    return [
        regions[F.col(STATE_COLUMN)].alias("region"),  # N:1 reference lookup (27 states)
        F.col("registered_at").alias("effective_from"),  # first version; see customer_versions
    ]


def prepare(bronze: DataFrame, entity: str) -> DataFrame:
    """Delivered text under ``_src_<col>``, metadata, row fingerprint and typed Silver columns."""
    names = RETAIL_SOURCE_CONTRACTS[entity].column_names
    renamed = bronze.select(
        *(F.col(name).alias(f"{SOURCE_PREFIX}{name}") for name in names),
        *METADATA_COLUMNS,
        F.xxhash64(*names, "_batch_id").cast("string").alias(ROW_HASH_COLUMN),
    )
    typed = renamed.select("*", *(_typed(c) for c in silver_source_columns(entity)))
    return typed.select("*", *_derived(entity))


def _ranked(frame: DataFrame, partition: list[str], order: list[Column]) -> DataFrame:
    window = Window.partitionBy(*partition).orderBy(*order)
    return frame.withColumn(_RANK, F.row_number().over(window))


def _quarantine_rows(frame: DataFrame, entity: str) -> DataFrame:
    """Back to the delivered column names (``_src_<col>`` -> ``<col>``), typed as Bronze."""
    columns = []
    for field in quarantine_table_schema(entity).fields:
        own = field.name in _QUARANTINE_OWN or field.name in METADATA_COLUMNS
        source = field.name if own else f"{SOURCE_PREFIX}{field.name}"
        columns.append(F.col(source).cast(field.dataType).alias(field.name))
    return frame.select(*columns)


def standardize(
    bronze: DataFrame, entity: str, *, execution_timestamp: datetime
) -> StandardizedBatch:
    """Valid Silver rows, quarantined Bronze rows with reasons, and the removed duplicates."""
    key = list(RETAIL_SOURCE_CONTRACTS[entity].business_key)

    exact = _ranked(prepare(bronze, entity), [ROW_HASH_COLUMN], [F.col("_source_file")])
    delivered = exact.filter(F.col(_RANK) == 1).drop(_RANK)
    exact_duplicates = exact.filter(F.col(_RANK) > 1)

    checks = [*contract_rules(entity), *silver_row_rules(entity)]
    valid, rejected = split_by_quality(delivered, checks, execution_timestamp=execution_timestamp)

    contents = F.xxhash64(*SILVER_SCHEMAS[entity].fieldNames())
    tagged = valid.withColumn(_CONTENT, contents).withColumn(
        _VERSIONS, F.size(F.collect_set(_CONTENT).over(Window.partitionBy(*key)))
    )
    conflicts = (
        tagged.filter(F.col(_VERSIONS) > 1)
        .withColumn(FAILED_CHECKS_COLUMN, F.array(F.lit(CONFLICTING_VALUES)))
        .withColumn(EXECUTION_TIMESTAMP_COLUMN, F.lit(execution_timestamp).cast(TimestampType()))
    )
    single = _ranked(
        tagged.filter(F.col(_VERSIONS) == 1),
        key,
        [F.col("_source_file"), F.col(ROW_HASH_COLUMN)],
    )
    kept = single.filter(F.col(_RANK) == 1)
    key_duplicates = single.filter(F.col(_RANK) > 1)

    output = silver_table_schema(entity)
    quarantine = _quarantine_rows(rejected, entity).unionByName(_quarantine_rows(conflicts, entity))
    duplicates = exact_duplicates.select(ROW_HASH_COLUMN).unionByName(
        key_duplicates.select(ROW_HASH_COLUMN)
    )
    return StandardizedBatch(
        valid=kept.select(*(F.col(f.name).cast(f.dataType) for f in output.fields)),
        quarantine=quarantine,
        duplicates=duplicates,
    )


def customer_versions(observed: DataFrame, stored: DataFrame) -> DataFrame:
    """New customer versions to insert: a customer first seen, or one whose attributes changed.

    The CRM export has no change timestamp. The first version starts at ``registered_at``; a
    change starts at the ingestion of the batch that revealed it. Each observation is compared
    with the customer's *latest* stored version (N:1), so returning to an earlier value is a
    change too.
    """
    latest = (
        _ranked(stored, ["customer_id"], [F.col("effective_from").desc()])
        .filter(F.col(_RANK) == 1)
        .select(
            F.col("customer_id").alias("_current_id"),
            *(F.col(c).alias(f"_current_{c}") for c in CUSTOMER_ATTRIBUTES),
        )
    )
    joined = observed.join(latest, F.col("customer_id") == F.col("_current_id"), "left")  # N:1
    unchanged = F.lit(True)
    for column in CUSTOMER_ATTRIBUTES:
        unchanged = unchanged & F.col(column).eqNullSafe(F.col(f"_current_{column}"))
    is_new = F.col("_current_id").isNull()
    effective = F.when(is_new, F.col("registered_at")).otherwise(F.col("_ingestion_timestamp"))
    output = silver_table_schema("customers")
    return (
        joined.filter(is_new | ~unchanged)
        .withColumn("effective_from", effective)
        .select(*(F.col(f.name).cast(f.dataType) for f in output.fields))
    )
