"""Generation configuration: volumes, calendar window, seed and anomaly rules."""

import tomllib
from dataclasses import dataclass, fields
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

MIN_STORES: Final = 2  # at least one physical store plus the online store


class AnomalyType(StrEnum):
    DUPLICATE_RECORD = "duplicate_record"
    NULL_VALUE = "null_value"
    INVALID_EMAIL = "invalid_email"
    NEGATIVE_PRICE = "negative_price"
    INVALID_QUANTITY = "invalid_quantity"
    ORPHAN_FOREIGN_KEY = "orphan_foreign_key"
    DUPLICATE_PAYMENT = "duplicate_payment"
    INVALID_DATE = "invalid_date"


# (type, dataset, column). The order is also the order of application per record:
# value anomalies first (one per cell), duplicates last (they copy the altered row).
SUPPORTED_ANOMALIES: Final[tuple[tuple[AnomalyType, str, str | None], ...]] = (
    (AnomalyType.NULL_VALUE, "customers", "email"),
    (AnomalyType.NULL_VALUE, "customers", "birth_date"),
    (AnomalyType.NULL_VALUE, "products", "supplier_id"),
    (AnomalyType.INVALID_EMAIL, "customers", "email"),
    (AnomalyType.INVALID_EMAIL, "suppliers", "email"),
    (AnomalyType.NEGATIVE_PRICE, "products", "list_price"),
    (AnomalyType.NEGATIVE_PRICE, "order_items", "unit_price"),
    (AnomalyType.INVALID_QUANTITY, "order_items", "quantity"),
    (AnomalyType.INVALID_QUANTITY, "inventory_movements", "quantity"),
    (AnomalyType.ORPHAN_FOREIGN_KEY, "products", "category_id"),
    (AnomalyType.ORPHAN_FOREIGN_KEY, "orders", "customer_id"),
    (AnomalyType.ORPHAN_FOREIGN_KEY, "order_items", "product_id"),
    (AnomalyType.ORPHAN_FOREIGN_KEY, "payments", "order_id"),
    (AnomalyType.INVALID_DATE, "orders", "order_timestamp"),
    (AnomalyType.INVALID_DATE, "customers", "birth_date"),
    (AnomalyType.INVALID_DATE, "shipments", "delivered_at"),
    (AnomalyType.DUPLICATE_RECORD, "customers", None),
    (AnomalyType.DUPLICATE_RECORD, "order_items", None),
    (AnomalyType.DUPLICATE_PAYMENT, "payments", None),
)

# Digits of each generated identifier. Orphan keys start with "9", so every volume must
# stay below 9 * 10 ** (digits - 1) to keep orphans outside the real id range.
ID_DIGITS: Final = {
    "categories": 3,
    "suppliers": 4,
    "stores": 3,
    "products": 6,
    "customers": 7,
    "orders": 8,
}


@dataclass(frozen=True, slots=True)
class AnomalyRule:
    anomaly_type: AnomalyType
    dataset: str
    column: str | None
    rate: float

    def __post_init__(self) -> None:
        if not 0.0 <= self.rate <= 1.0:
            raise ValueError(f"anomaly rate must be within [0, 1], got {self.rate}")
        if (self.anomaly_type, self.dataset, self.column) not in SUPPORTED_ANOMALIES:
            raise ValueError(
                f"unsupported anomaly: {self.anomaly_type} on {self.dataset}.{self.column}"
            )


def _max_volume(dataset: str) -> int:
    return int(9 * 10 ** (ID_DIGITS[dataset] - 1)) - 1


@dataclass(frozen=True, slots=True)
class GenerationConfig:
    """Defaults reproduce the portfolio volumes **without** anomalies (see config/*.toml)."""

    seed: int = 20240101
    start_date: date = date(2024, 1, 1)
    end_date: date = date(2025, 12, 31)
    customers: int = 50_000
    products: int = 2_000
    stores: int = 30
    suppliers: int = 100
    orders: int = 100_000
    batch_id: str = "initial"
    anomalies: tuple[AnomalyRule, ...] = ()

    def __post_init__(self) -> None:
        if self.end_date <= self.start_date:
            raise ValueError("end_date must be after start_date")
        for dataset in ("customers", "products", "suppliers", "orders"):
            volume = getattr(self, dataset)
            if not 1 <= volume <= _max_volume(dataset):
                raise ValueError(f"{dataset} must be within [1, {_max_volume(dataset)}]")
        if not MIN_STORES <= self.stores <= _max_volume("stores"):
            raise ValueError(f"stores must be within [{MIN_STORES}, {_max_volume('stores')}]")
        if not self.batch_id.strip():
            raise ValueError("batch_id must not be blank")
        targets = [(r.anomaly_type, r.dataset, r.column) for r in self.anomalies]
        if len(targets) != len(set(targets)):
            raise ValueError("duplicate anomaly rules")


_GENERATION_KEYS: Final = {f.name for f in fields(GenerationConfig)} - {"anomalies"}


def load_config(path: Path) -> GenerationConfig:
    """Read a TOML file with a ``[generation]`` table and ``[[anomalies]]`` entries."""
    with path.open("rb") as handle:
        document: dict[str, Any] = tomllib.load(handle)
    generation = dict(document.get("generation", {}))
    unknown = set(generation) - _GENERATION_KEYS
    if unknown:
        raise ValueError(f"unknown generation keys: {sorted(unknown)}")
    rules = tuple(
        AnomalyRule(
            anomaly_type=AnomalyType(entry["type"]),
            dataset=entry["dataset"],
            column=entry.get("column"),
            rate=float(entry["rate"]),
        )
        for entry in document.get("anomalies", [])
    )
    return GenerationConfig(**generation, anomalies=rules)
