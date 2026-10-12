"""Controlled, registered anomaly injection.

A record is selected for a rule when ``score(seed, rule, business_key) < rate``. The score
comes from a cryptographic hash, so selection is deterministic and independent of every
other random draw: enabling, disabling or re-rating one rule never changes other records.
Anomalies alter rows only at emission time; derived values (payment amounts, stock) are
always computed from clean data.
"""

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final, Protocol

from retail_lakehouse.ingestion.source_contracts import RETAIL_SOURCE_CONTRACTS
from retail_lakehouse.synthetic.config import SUPPORTED_ANOMALIES, AnomalyRule, AnomalyType

Row = dict[str, str | None]
DUPLICATE_MARKER: Final = "<duplicate row>"
DUPLICATE_TYPES: Final = frozenset({AnomalyType.DUPLICATE_RECORD, AnomalyType.DUPLICATE_PAYMENT})
_HASH_BYTES: Final = 8
_HASH_SPACE: Final = 2 ** (8 * _HASH_BYTES)
_HALF: Final = 0.5
_FUTURE_BIRTH_YEAR: Final = 2090


@dataclass(frozen=True, slots=True)
class AnomalyRecord:
    anomaly_id: str
    anomaly_type: AnomalyType
    dataset: str
    column: str | None
    record_key: str
    original_value: str | None
    injected_value: str | None


class AnomalySink(Protocol):
    def write(self, dataset: str, row: Mapping[str, str | None]) -> None: ...
    def record_anomaly(self, anomaly: AnomalyRecord) -> None: ...


class _NotApplicable:
    """Returned by a mutator when the cell cannot carry the anomaly (e.g. already NULL)."""


NOT_APPLICABLE: Final = _NotApplicable()
Mutation = str | None | _NotApplicable
Mutator = Callable[[Row, str, float], Mutation]


def score(seed: int, rule: AnomalyRule, record_key: str, salt: str = "") -> float:
    """Deterministic pseudo-random number in [0, 1) for (seed, rule, record)."""
    payload = f"{seed}|{rule.anomaly_type}|{rule.dataset}|{rule.column}|{record_key}|{salt}"
    digest = hashlib.blake2b(payload.encode("utf-8"), digest_size=_HASH_BYTES).digest()
    return int.from_bytes(digest, "big") / _HASH_SPACE


def _pick(options: list[str], variant: float) -> str:
    return options[int(variant * len(options))]


def _null(row: Row, column: str, variant: float) -> Mutation:
    return None if row[column] is not None else NOT_APPLICABLE


def _invalid_email(row: Row, column: str, variant: float) -> Mutation:
    value = row[column]
    if value is None or "@" not in value:
        return NOT_APPLICABLE
    local, domain = value.split("@", 1)
    variants = [
        f"{local}#{domain}",
        f"{local}@",
        f"@{domain}",
        f"{local}@@{domain}",
        f"{local} @{domain}",
    ]
    return _pick(variants, variant)


def _negative(row: Row, column: str, variant: float) -> Mutation:
    value = row[column]
    return NOT_APPLICABLE if value is None or value.startswith("-") else f"-{value}"


def _invalid_quantity(row: Row, column: str, variant: float) -> Mutation:
    value = row[column]
    if value is None:
        return NOT_APPLICABLE
    magnitude = value.lstrip("-")
    return "0" if variant < _HALF else f"-{magnitude}"


def _zero_quantity(row: Row, column: str, variant: float) -> Mutation:
    """Stock movements are signed, so the invalid value is zero (contract: quantity != 0)."""
    return NOT_APPLICABLE if row[column] in (None, "0") else "0"


def _orphan(row: Row, column: str, variant: float) -> Mutation:
    """Same id shape, number starting with 9: outside the generated id range."""
    value = row[column]
    if value is None or "-" not in value:
        return NOT_APPLICABLE
    prefix, digits = value.split("-", 1)
    width = len(digits) - 1
    return f"{prefix}-9{int(variant * 10**width):0{width}d}"


def _malformed_timestamp(row: Row, column: str, variant: float) -> Mutation:
    value = row[column]
    if value is None:
        return NOT_APPLICABLE
    day, time = value.split("T")
    year, month, dom = day.split("-")
    variants = [
        f"{year}-02-30T{time}",  # impossible calendar day
        f"{year}-13-{dom}T{time}",  # impossible month
        f"{dom}/{month}/{year} {time[:8]}",  # wrong format
    ]
    return _pick(variants, variant)


def _future_birth_date(row: Row, column: str, variant: float) -> Mutation:
    value = row[column]
    if value is None:
        return NOT_APPLICABLE
    _, month, dom = value.split("-")
    return f"{_FUTURE_BIRTH_YEAR + int(variant * 9)}-{month}-{dom}"


def _delivered_before_shipped(row: Row, column: str, variant: float) -> Mutation:
    if row[column] is None or row["shipped_at"] is None:
        return NOT_APPLICABLE
    shipped = datetime.fromisoformat(row["shipped_at"])
    return (shipped - timedelta(days=1 + int(variant * 3))).isoformat(timespec="seconds")


MUTATORS: Final[dict[tuple[AnomalyType, str, str], Mutator]] = {
    (AnomalyType.NULL_VALUE, "customers", "email"): _null,
    (AnomalyType.NULL_VALUE, "customers", "birth_date"): _null,
    (AnomalyType.NULL_VALUE, "products", "supplier_id"): _null,
    (AnomalyType.INVALID_EMAIL, "customers", "email"): _invalid_email,
    (AnomalyType.INVALID_EMAIL, "suppliers", "email"): _invalid_email,
    (AnomalyType.NEGATIVE_PRICE, "products", "list_price"): _negative,
    (AnomalyType.NEGATIVE_PRICE, "order_items", "unit_price"): _negative,
    (AnomalyType.INVALID_QUANTITY, "order_items", "quantity"): _invalid_quantity,
    (AnomalyType.INVALID_QUANTITY, "inventory_movements", "quantity"): _zero_quantity,
    (AnomalyType.ORPHAN_FOREIGN_KEY, "products", "category_id"): _orphan,
    (AnomalyType.ORPHAN_FOREIGN_KEY, "orders", "customer_id"): _orphan,
    (AnomalyType.ORPHAN_FOREIGN_KEY, "order_items", "product_id"): _orphan,
    (AnomalyType.ORPHAN_FOREIGN_KEY, "payments", "order_id"): _orphan,
    (AnomalyType.INVALID_DATE, "orders", "order_timestamp"): _malformed_timestamp,
    (AnomalyType.INVALID_DATE, "customers", "birth_date"): _future_birth_date,
    (AnomalyType.INVALID_DATE, "shipments", "delivered_at"): _delivered_before_shipped,
}


def record_key(dataset: str, row: Mapping[str, str | None]) -> str:
    """Business key of a row, ``|``-joined (as in the anomaly registry)."""
    return "|".join(str(row[c]) for c in RETAIL_SOURCE_CONTRACTS[dataset].business_key)


class AnomalyInjector:
    """Applies configured rules to rows on their way to the sink and registers each hit."""

    def __init__(self, seed: int, rules: tuple[AnomalyRule, ...], sink: AnomalySink) -> None:
        order = {target: index for index, target in enumerate(SUPPORTED_ANOMALIES)}
        active = sorted(
            (rule for rule in rules if rule.rate > 0),
            key=lambda rule: order[(rule.anomaly_type, rule.dataset, rule.column)],
        )
        self._rules: dict[str, list[AnomalyRule]] = {}
        for rule in active:
            self._rules.setdefault(rule.dataset, []).append(rule)
        self._seed = seed
        self._sink = sink
        self._next_id = 1

    def emit(self, dataset: str, row: Row) -> None:
        rules = self._rules.get(dataset)
        if not rules:
            self._sink.write(dataset, row)
            return
        key = record_key(dataset, row)
        altered: set[str] = set()
        duplicate_rule: AnomalyRule | None = None
        for rule in rules:
            if score(self._seed, rule, key) >= rule.rate:
                continue
            if rule.anomaly_type in DUPLICATE_TYPES:
                duplicate_rule = rule
                continue
            column = str(rule.column)
            if column in altered:
                continue
            mutator = MUTATORS[(rule.anomaly_type, rule.dataset, column)]
            injected = mutator(row, column, score(self._seed, rule, key, salt="variant"))
            if isinstance(injected, _NotApplicable):
                continue
            self._register(rule, key, original=row[column], injected=injected)
            row[column] = injected
            altered.add(column)
        self._sink.write(dataset, row)
        if duplicate_rule is not None:
            self._register(duplicate_rule, key, original=None, injected=DUPLICATE_MARKER)
            self._sink.write(dataset, dict(row))

    def _register(
        self, rule: AnomalyRule, key: str, *, original: str | None, injected: str | None
    ) -> None:
        self._sink.record_anomaly(
            AnomalyRecord(
                anomaly_id=f"ANM-{self._next_id:07d}",
                anomaly_type=rule.anomaly_type,
                dataset=rule.dataset,
                column=rule.column,
                record_key=key,
                original_value=original,
                injected_value=injected,
            )
        )
        self._next_id += 1
