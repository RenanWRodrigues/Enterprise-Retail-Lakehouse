"""Contracts of the retail source systems (landing zone, before Bronze).

Single source of truth for dataset names, column order, logical types, business keys and
relationships; documented in ``docs/DATA_CONTRACTS.md`` (kept in sync by tests). Values
arrive as text — see ``docs/SOURCE_SYSTEMS.md`` for serialisation conventions.
"""

import re
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import Final


class LogicalType(StrEnum):
    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal(12,2)"
    DATE = "date"
    TIMESTAMP = "timestamp"
    BOOLEAN = "boolean"


class FileFormat(StrEnum):
    CSV = "csv"
    JSONL = "jsonl"


class SourceSystem(StrEnum):
    ERP = "erp"
    CRM = "crm"
    ECOMMERCE = "ecommerce"
    PAYMENTS = "payments"
    LOGISTICS = "logistics"


_INTEGER: Final = re.compile(r"^-?\d+$")
_DECIMAL: Final = re.compile(r"^-?\d+\.\d{2}$")
_DATE: Final = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TIMESTAMP: Final = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$")
_BOOLEANS: Final = frozenset({"true", "false"})


def _parses(logical_type: LogicalType, value: str) -> bool:  # noqa: PLR0911 - one branch per type
    if logical_type is LogicalType.STRING:
        return value.strip() != ""
    if logical_type is LogicalType.INTEGER:
        return bool(_INTEGER.match(value))
    if logical_type is LogicalType.DECIMAL:
        return bool(_DECIMAL.match(value))
    if logical_type is LogicalType.BOOLEAN:
        return value in _BOOLEANS
    pattern, parser = (
        (_DATE, date.fromisoformat)
        if logical_type is LogicalType.DATE
        else (_TIMESTAMP, datetime.fromisoformat)
    )
    if not pattern.match(value):
        return False
    try:
        parser(value)
    except ValueError:
        return False
    return True


@dataclass(frozen=True, slots=True)
class ColumnContract:
    name: str
    logical_type: LogicalType
    nullable: bool = False
    accepted_values: tuple[str, ...] = ()
    pii: bool = False

    def accepts(self, value: str | None) -> bool:
        """True when a text value honours type, nullability and domain."""
        if value is None:
            return self.nullable
        if not _parses(self.logical_type, value):
            return False
        return not self.accepted_values or value in self.accepted_values


@dataclass(frozen=True, slots=True)
class ForeignKey:
    column: str
    references_dataset: str
    references_column: str


@dataclass(frozen=True, slots=True)
class DatasetContract:
    name: str
    source_system: SourceSystem
    file_format: FileFormat
    business_key: tuple[str, ...]
    columns: tuple[ColumnContract, ...]
    foreign_keys: tuple[ForeignKey, ...] = ()

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    def column(self, name: str) -> ColumnContract:
        for column in self.columns:
            if column.name == name:
                return column
        raise KeyError(f"{self.name} has no column {name!r}")


STR, INT, DEC = LogicalType.STRING, LogicalType.INTEGER, LogicalType.DECIMAL
DATE, TS, BOOL = LogicalType.DATE, LogicalType.TIMESTAMP, LogicalType.BOOLEAN

STORE_TYPES: Final = ("physical", "online")
REGIONS: Final = ("Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul")
SEGMENTS: Final = ("standard", "premium")
CHANNELS: Final = ("store", "online", "app")
ORDER_STATUSES: Final = ("created", "paid", "shipped", "delivered", "cancelled", "returned")
PAYMENT_METHODS: Final = ("credit_card", "debit_card", "pix", "boleto", "cash")
PAYMENT_STATUSES: Final = ("approved", "declined", "refunded")
SHIPMENT_STATUSES: Final = ("in_transit", "delivered", "returned")
CARRIERS: Final = ("CAR-01", "CAR-02", "CAR-03", "CAR-04")
MOVEMENT_TYPES: Final = ("opening_balance", "purchase_receipt", "sale", "return")


def _fk(column: str, dataset: str) -> ForeignKey:
    return ForeignKey(column, dataset, column)


_CONTRACTS: Final = (
    DatasetContract(
        name="categories",
        source_system=SourceSystem.ERP,
        file_format=FileFormat.CSV,
        business_key=("category_id",),
        columns=(
            ColumnContract("category_id", STR),
            ColumnContract("category_name", STR),
            ColumnContract("parent_category_id", STR, nullable=True),
        ),
        foreign_keys=(ForeignKey("parent_category_id", "categories", "category_id"),),
    ),
    DatasetContract(
        name="suppliers",
        source_system=SourceSystem.ERP,
        file_format=FileFormat.CSV,
        business_key=("supplier_id",),
        columns=(
            ColumnContract("supplier_id", STR),
            ColumnContract("supplier_name", STR),
            ColumnContract("email", STR),
            ColumnContract("city", STR),
            ColumnContract("state", STR),
            ColumnContract("lead_time_days", INT),
            ColumnContract("created_at", DATE),
        ),
    ),
    DatasetContract(
        name="stores",
        source_system=SourceSystem.ERP,
        file_format=FileFormat.CSV,
        business_key=("store_id",),
        columns=(
            ColumnContract("store_id", STR),
            ColumnContract("store_name", STR),
            ColumnContract("store_type", STR, accepted_values=STORE_TYPES),
            ColumnContract("city", STR),
            ColumnContract("state", STR),
            ColumnContract("region", STR, accepted_values=REGIONS),
            ColumnContract("opened_at", DATE),
        ),
    ),
    DatasetContract(
        name="products",
        source_system=SourceSystem.ERP,
        file_format=FileFormat.CSV,
        business_key=("product_id",),
        columns=(
            ColumnContract("product_id", STR),
            ColumnContract("sku", STR),
            ColumnContract("product_name", STR),
            ColumnContract("category_id", STR),
            ColumnContract("supplier_id", STR),
            ColumnContract("brand", STR),
            ColumnContract("list_price", DEC),
            ColumnContract("cost_price", DEC),
            ColumnContract("is_active", BOOL),
            ColumnContract("created_at", TS),
            ColumnContract("updated_at", TS),
        ),
        foreign_keys=(_fk("category_id", "categories"), _fk("supplier_id", "suppliers")),
    ),
    DatasetContract(
        name="customers",
        source_system=SourceSystem.CRM,
        file_format=FileFormat.CSV,
        business_key=("customer_id",),
        columns=(
            ColumnContract("customer_id", STR),
            ColumnContract("first_name", STR, pii=True),
            ColumnContract("last_name", STR, pii=True),
            ColumnContract("email", STR, pii=True),
            ColumnContract("birth_date", DATE, pii=True),
            ColumnContract("city", STR),
            ColumnContract("state", STR),
            ColumnContract("segment", STR, accepted_values=SEGMENTS),
            ColumnContract("registered_at", TS),
        ),
    ),
    DatasetContract(
        name="orders",
        source_system=SourceSystem.ECOMMERCE,
        file_format=FileFormat.JSONL,
        business_key=("order_id",),
        columns=(
            ColumnContract("order_id", STR),
            ColumnContract("customer_id", STR),
            ColumnContract("store_id", STR),
            ColumnContract("channel", STR, accepted_values=CHANNELS),
            ColumnContract("order_status", STR, accepted_values=ORDER_STATUSES),
            ColumnContract("order_timestamp", TS),
            ColumnContract("currency", STR, accepted_values=("BRL",)),
            ColumnContract("freight_amount", DEC),
        ),
        foreign_keys=(_fk("customer_id", "customers"), _fk("store_id", "stores")),
    ),
    DatasetContract(
        name="order_items",
        source_system=SourceSystem.ECOMMERCE,
        file_format=FileFormat.JSONL,
        business_key=("order_id", "line_number"),
        columns=(
            ColumnContract("order_id", STR),
            ColumnContract("line_number", INT),
            ColumnContract("product_id", STR),
            ColumnContract("quantity", INT),
            ColumnContract("unit_price", DEC),
            ColumnContract("discount_amount", DEC),
        ),
        foreign_keys=(_fk("order_id", "orders"), _fk("product_id", "products")),
    ),
    DatasetContract(
        name="payments",
        source_system=SourceSystem.PAYMENTS,
        file_format=FileFormat.JSONL,
        business_key=("payment_id",),
        columns=(
            ColumnContract("payment_id", STR),
            ColumnContract("order_id", STR),
            ColumnContract("payment_method", STR, accepted_values=PAYMENT_METHODS),
            ColumnContract("installments", INT),
            ColumnContract("amount", DEC),
            ColumnContract("payment_status", STR, accepted_values=PAYMENT_STATUSES),
            ColumnContract("processed_at", TS),
        ),
        foreign_keys=(_fk("order_id", "orders"),),
    ),
    DatasetContract(
        name="shipments",
        source_system=SourceSystem.LOGISTICS,
        file_format=FileFormat.CSV,
        business_key=("shipment_id",),
        columns=(
            ColumnContract("shipment_id", STR),
            ColumnContract("order_id", STR),
            ColumnContract("carrier_id", STR, accepted_values=CARRIERS),
            ColumnContract("shipment_status", STR, accepted_values=SHIPMENT_STATUSES),
            ColumnContract("shipped_at", TS),
            ColumnContract("delivered_at", TS, nullable=True),
            ColumnContract("freight_amount", DEC),
        ),
        foreign_keys=(_fk("order_id", "orders"),),
    ),
    DatasetContract(
        name="inventory",
        source_system=SourceSystem.ERP,
        file_format=FileFormat.CSV,
        business_key=("snapshot_date", "store_id", "product_id"),
        columns=(
            ColumnContract("snapshot_date", DATE),
            ColumnContract("store_id", STR),
            ColumnContract("product_id", STR),
            ColumnContract("quantity_on_hand", INT),
            ColumnContract("reorder_point", INT),
        ),
        foreign_keys=(_fk("store_id", "stores"), _fk("product_id", "products")),
    ),
    DatasetContract(
        name="inventory_movements",
        source_system=SourceSystem.ERP,
        file_format=FileFormat.CSV,
        business_key=("movement_id",),
        columns=(
            ColumnContract("movement_id", STR),
            ColumnContract("store_id", STR),
            ColumnContract("product_id", STR),
            ColumnContract("movement_type", STR, accepted_values=MOVEMENT_TYPES),
            ColumnContract("quantity", INT),
            ColumnContract("movement_timestamp", TS),
            ColumnContract("reference_id", STR, nullable=True),
        ),
        foreign_keys=(_fk("store_id", "stores"), _fk("product_id", "products")),
    ),
)

RETAIL_SOURCE_CONTRACTS: Final[dict[str, DatasetContract]] = {c.name: c for c in _CONTRACTS}
