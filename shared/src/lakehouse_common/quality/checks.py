"""Check catalog.

Row-level checks add a non-null boolean *failure flag* column with native Spark
expressions, so the runner can count every check in one aggregation and reuse the
same flags for quarantine. Dataset-level checks see the row count and the DataFrame
itself (its schema), never individual rows.

NULL semantics: only ``NotNull`` (and ``CustomCondition``) fail on NULL. Other checks
ignore NULL values so a single defect is never counted by several checks.
"""

import operator
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import ClassVar, Final

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from lakehouse_common.quality.results import Dimension, Severity

MIN_THRESHOLD: Final = 0.0
MAX_THRESHOLD: Final = 1.0
DUPLICATE_MIN_COUNT: Final = 2

RangeBound = int | float | Decimal | date | datetime


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _is_inverted(low: RangeBound, high: RangeBound) -> bool:
    try:
        return bool(operator.gt(low, high))
    except TypeError as error:
        raise ValueError("min_value and max_value must be comparable") from error


def _flag(condition: Column) -> Column:
    """Failure flags are never NULL: an unknown outcome is not a failure here."""
    return F.coalesce(condition, F.lit(False))


@dataclass(frozen=True, kw_only=True)
class QualityCheck(ABC):
    """Common options: ``severity``, ``threshold`` (tolerated failure rate), ``name``."""

    check_type: ClassVar[str]

    severity: Severity = Severity.ERROR
    threshold: float = MIN_THRESHOLD
    name: str | None = None
    dimension: Dimension = Dimension.VALIDITY

    def __post_init__(self) -> None:
        _require(
            MIN_THRESHOLD <= self.threshold <= MAX_THRESHOLD,
            f"threshold must be within [0, 1], got {self.threshold}",
        )
        _require(self.name is None or bool(self.name.strip()), "check name must not be blank")

    @property
    @abstractmethod
    def column_name(self) -> str | None:
        """Checked column(s), comma-separated; ``None`` for dataset-level checks."""

    @property
    def check_name(self) -> str:
        if self.name:
            return self.name
        if self.column_name is None:
            return self.check_type
        return f"{self.check_type}__{self.column_name.replace(',', '__')}"


class RowCheck(QualityCheck, ABC):
    """A check whose failures can be attributed to individual rows."""

    @abstractmethod
    def annotate(self, df: DataFrame, flag_column: str) -> DataFrame:
        """Return ``df`` plus ``flag_column`` (TRUE = row fails), same rows, same columns."""


class PredicateCheck(RowCheck, ABC):
    """A row check expressed as a single column expression."""

    @abstractmethod
    def failure_condition(self) -> Column:
        """Boolean expression that is TRUE for failing rows."""

    def annotate(self, df: DataFrame, flag_column: str) -> DataFrame:
        return df.withColumn(flag_column, _flag(self.failure_condition()))


@dataclass(frozen=True, slots=True)
class DatasetOutcome:
    passed: bool
    details: str | None = None


class DatasetCheck(QualityCheck, ABC):
    """A check on the dataset as a whole."""

    def evaluate(self, df: DataFrame, rows_checked: int) -> DatasetOutcome:
        """Default: decided by the row count alone (``passes``)."""
        del df
        return DatasetOutcome(self.passes(rows_checked))

    @abstractmethod
    def passes(self, rows_checked: int) -> bool: ...


def _require_column(column: str) -> None:
    _require(bool(column.strip()), "column must not be blank")


@dataclass(frozen=True)
class NotNull(PredicateCheck):
    check_type: ClassVar[str] = "not_null"

    column: str
    blank_is_null: bool = False
    dimension: Dimension = field(default=Dimension.COMPLETENESS, kw_only=True)

    def __post_init__(self) -> None:
        super().__post_init__()
        _require_column(self.column)

    @property
    def column_name(self) -> str:
        return self.column

    def failure_condition(self) -> Column:
        value = F.col(self.column)
        if self.blank_is_null:
            return value.isNull() | (F.trim(value) == "")
        return value.isNull()


@dataclass(frozen=True)
class Unique(RowCheck):
    """Every row sharing a key with another row fails. Rows with a NULL key are ignored."""

    check_type: ClassVar[str] = "unique"

    columns: tuple[str, ...]
    dimension: Dimension = field(default=Dimension.UNIQUENESS, kw_only=True)

    def __post_init__(self) -> None:
        super().__post_init__()
        _require(bool(self.columns), "unique needs at least one column")
        for column in self.columns:
            _require_column(column)

    @property
    def column_name(self) -> str:
        return ",".join(self.columns)

    def annotate(self, df: DataFrame, flag_column: str) -> DataFrame:
        key_is_complete = F.lit(True)
        for column in self.columns:
            key_is_complete = key_is_complete & F.col(column).isNotNull()
        occurrences = F.count(F.lit(1)).over(Window.partitionBy(*self.columns))
        return df.withColumn(
            flag_column, _flag(key_is_complete & (occurrences >= DUPLICATE_MIN_COUNT))
        )


@dataclass(frozen=True)
class AcceptedValues(PredicateCheck):
    """Exact, case-sensitive membership. NULL is ignored (see ``NotNull``)."""

    check_type: ClassVar[str] = "accepted_values"

    column: str
    values: tuple[object, ...]

    def __post_init__(self) -> None:
        super().__post_init__()
        _require_column(self.column)
        _require(bool(self.values), "accepted_values needs at least one value")

    @property
    def column_name(self) -> str:
        return self.column

    def failure_condition(self) -> Column:
        value = F.col(self.column)
        return value.isNotNull() & ~value.isin(*self.values)


@dataclass(frozen=True)
class Range(PredicateCheck):
    """Bounds are inclusive by default; either bound may be omitted. NULL is ignored."""

    check_type: ClassVar[str] = "range"

    column: str
    min_value: RangeBound | None = None
    max_value: RangeBound | None = None
    inclusive: bool = True

    def __post_init__(self) -> None:
        super().__post_init__()
        _require_column(self.column)
        _require(
            self.min_value is not None or self.max_value is not None,
            "range needs min_value and/or max_value",
        )
        if self.min_value is not None and self.max_value is not None:
            _require(
                not _is_inverted(self.min_value, self.max_value),
                "min_value must not exceed max_value",
            )

    @property
    def column_name(self) -> str:
        return self.column

    def failure_condition(self) -> Column:
        value = F.col(self.column)
        out_of_range = F.lit(False)
        if self.min_value is not None:
            low = F.lit(self.min_value)
            out_of_range = out_of_range | (value < low if self.inclusive else value <= low)
        if self.max_value is not None:
            high = F.lit(self.max_value)
            out_of_range = out_of_range | (value > high if self.inclusive else value >= high)
        return value.isNotNull() & out_of_range


@dataclass(frozen=True)
class Regex(PredicateCheck):
    """The (Java) regular expression must match the *whole* value. NULL is ignored."""

    check_type: ClassVar[str] = "regex"

    column: str
    pattern: str

    def __post_init__(self) -> None:
        super().__post_init__()
        _require_column(self.column)
        _require(bool(self.pattern), "pattern must not be empty")

    @property
    def column_name(self) -> str:
        return self.column

    def failure_condition(self) -> Column:
        value = F.col(self.column)
        return value.isNotNull() & ~value.rlike(f"^(?:{self.pattern})$")


@dataclass(frozen=True, eq=False)
class ReferentialIntegrity(RowCheck):
    """Non-null ``column`` values must exist in ``reference.reference_column``.

    Reference keys are de-duplicated before the left join, so the join is N:1 and
    never multiplies rows. NULL foreign keys are ignored (see ``NotNull``).
    """

    check_type: ClassVar[str] = "referential_integrity"

    column: str
    reference: DataFrame
    reference_column: str
    dimension: Dimension = field(default=Dimension.REFERENTIAL_INTEGRITY, kw_only=True)

    def __post_init__(self) -> None:
        super().__post_init__()
        _require_column(self.column)
        _require_column(self.reference_column)

    @property
    def column_name(self) -> str:
        return self.column

    def annotate(self, df: DataFrame, flag_column: str) -> DataFrame:
        key_column = f"{flag_column}_reference_key"
        reference_keys = (
            self.reference.select(F.col(self.reference_column).alias(key_column))
            .where(F.col(key_column).isNotNull())
            .distinct()
        )
        joined = df.join(reference_keys, F.col(self.column) == F.col(key_column), "left")  # N:1
        orphan = F.col(self.column).isNotNull() & F.col(key_column).isNull()
        return joined.withColumn(flag_column, _flag(orphan)).drop(key_column)


@dataclass(frozen=True)
class RowCount(DatasetCheck):
    """Dataset-level: the number of rows must fall within ``[min_rows, max_rows]``."""

    check_type: ClassVar[str] = "row_count"

    min_rows: int | None = None
    max_rows: int | None = None
    dimension: Dimension = field(default=Dimension.VOLUME, kw_only=True)

    def __post_init__(self) -> None:
        super().__post_init__()
        _require(
            self.min_rows is not None or self.max_rows is not None,
            "row_count needs min_rows and/or max_rows",
        )
        for bound in (self.min_rows, self.max_rows):
            _require(bound is None or bound >= 0, "row bounds must not be negative")
        if self.min_rows is not None and self.max_rows is not None:
            _require(self.min_rows <= self.max_rows, "min_rows must not exceed max_rows")

    @property
    def column_name(self) -> None:
        return None

    def passes(self, rows_checked: int) -> bool:
        above_min = self.min_rows is None or rows_checked >= self.min_rows
        below_max = self.max_rows is None or rows_checked <= self.max_rows
        return above_min and below_max


@dataclass(frozen=True)
class CustomCondition(PredicateCheck):
    """``condition`` is a Spark SQL boolean, TRUE for valid rows. NULL counts as failure."""

    check_type: ClassVar[str] = "custom_condition"

    # Required here (optional in the base). field() drops the inherited default None.
    name: str = field()
    condition: str
    column: str | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        super().__post_init__()
        _require(bool(self.condition.strip()), "condition must not be blank")

    @property
    def column_name(self) -> str | None:
        return self.column

    def failure_condition(self) -> Column:
        return ~F.coalesce(F.expr(self.condition), F.lit(False))


@dataclass(frozen=True)
class SchemaMatches(DatasetCheck):
    """Dataset-level: the DataFrame has exactly the expected columns and types (schema drift).

    Column order is ignored (it does not change meaning) and so is nullability by default
    (Spark does not preserve it reliably through transformations). ``details`` lists the
    missing, unexpected and retyped columns.
    """

    check_type: ClassVar[str] = "schema_matches"

    expected: StructType
    check_nullability: bool = False
    dimension: Dimension = field(default=Dimension.CONSISTENCY, kw_only=True)

    @property
    def column_name(self) -> None:
        return None

    def passes(self, rows_checked: int) -> bool:
        raise NotImplementedError("schema_matches needs the DataFrame: the runner calls evaluate()")

    def evaluate(self, df: DataFrame, rows_checked: int) -> DatasetOutcome:
        del rows_checked
        expected = {f.name: f for f in self.expected.fields}
        actual = {f.name: f for f in df.schema.fields}
        problems = []
        if missing := sorted(set(expected) - set(actual)):
            problems.append(f"missing: {', '.join(missing)}")
        if unexpected := sorted(set(actual) - set(expected)):
            problems.append(f"unexpected: {', '.join(unexpected)}")
        retyped = [
            f"{name} ({expected[name].dataType.simpleString()} -> "
            f"{actual[name].dataType.simpleString()})"
            for name in sorted(set(expected) & set(actual))
            if expected[name].dataType != actual[name].dataType
        ]
        if retyped:
            problems.append(f"type changed: {', '.join(retyped)}")
        if self.check_nullability:
            relaxed = sorted(
                name
                for name in set(expected) & set(actual)
                if not expected[name].nullable and actual[name].nullable
            )
            if relaxed:
                problems.append(f"became nullable: {', '.join(relaxed)}")
        return DatasetOutcome(not problems, "; ".join(problems) or None)


@dataclass(frozen=True)
class SequenceGaps(RowCheck):
    """A row fails when the step from the previous row of its partition exceeds the allowed gap.

    ``order_column`` is an integer ordinal (day number, month number, epoch seconds...);
    ``max_gap`` is a constant or the name of a column with each row's allowed step, so one check
    covers partitions of different frequencies. The first row of a partition never fails and
    rows with a NULL ordinal are ignored.
    """

    check_type: ClassVar[str] = "sequence_gaps"

    partition_by: tuple[str, ...]
    order_column: str
    max_gap: int | str
    dimension: Dimension = field(default=Dimension.COMPLETENESS, kw_only=True)

    def __post_init__(self) -> None:
        super().__post_init__()
        _require(bool(self.order_column), "sequence_gaps needs an order column")
        _require(isinstance(self.max_gap, str) or self.max_gap >= 1, "max_gap must be >= 1")

    @property
    def column_name(self) -> str:
        return self.order_column

    def annotate(self, df: DataFrame, flag_column: str) -> DataFrame:
        window = Window.partitionBy(*self.partition_by).orderBy(F.col(self.order_column))
        previous = F.lag(F.col(self.order_column)).over(window)
        allowed = F.col(self.max_gap) if isinstance(self.max_gap, str) else F.lit(self.max_gap)
        return df.withColumn(flag_column, _flag((F.col(self.order_column) - previous) > allowed))
