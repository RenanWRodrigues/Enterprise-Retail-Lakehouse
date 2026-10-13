"""Retail quality suite: what is checked on the Silver entities (the framework decides how).

Critical rules requested for Retail (docs/DATA_QUALITY.md §8):

- orders without customer: NULL ``customer_id`` (ERROR, quarantined); a ``customer_id`` absent
  from customers is WARNING (ADR 007: Gold maps it to the unknown member) and escalates to
  CRITICAL above 5% of the rows (systemic break, e.g. customers not loaded);
- nonexistent products: same pattern on ``order_items.product_id``;
- negative values: prices, discounts, freight and payments below zero, quantity below 1 (ERROR);
- duplicates: business keys are de-duplicated first (``BUSINESS_KEYS``), then ``Unique`` is a
  CRITICAL gate; ``SchemaMatches`` (CRITICAL) stops on a changed contract.

``silver_row_rules`` holds the row-level business rules (ERROR) of every entity. The Silver
standardization uses them to quarantine rows; the suite reuses the same checks as its gate, so
each rule is declared once.

``retail_gold_suite`` is the Gold publication gate (docs/DATA_QUALITY.md §8): the contract in
``gold/schemas.py`` checked on every candidate table before any table is published.
"""

from collections.abc import Mapping
from typing import Final

from pyspark.sql import DataFrame

from lakehouse_common.quality import (
    CheckSet,
    CustomCondition,
    Dimension,
    NotNull,
    QualityCheck,
    Range,
    ReferentialIntegrity,
    SchemaMatches,
    Severity,
    Unique,
)
from retail_lakehouse.gold.keys import UNKNOWN_KEY
from retail_lakehouse.gold.schemas import (
    EVENT_DATES,
    FOREIGN_KEYS,
    GOLD_SCHEMAS,
    GRAIN,
    LATE_MASTER_DATA_KEYS,
    NATURAL_KEYS,
)
from retail_lakehouse.silver.schemas import SILVER_SCHEMAS

SYSTEMIC_ORPHAN_RATE: Final = 0.05
ORPHAN_EXAMPLES: Final = 5  # orphan keys listed in a finding's details
BUSINESS_KEYS: Final[dict[str, tuple[str, ...]]] = {
    "categories": ("category_id",),
    "suppliers": ("supplier_id",),
    "stores": ("store_id",),
    "customers": ("customer_id", "effective_from"),  # one row per version
    "products": ("product_id",),
    "orders": ("order_id",),
    "order_items": ("order_id", "line_number"),
    "payments": ("payment_id",),
    "shipments": ("shipment_id",),
    "inventory": ("snapshot_date", "store_id", "product_id"),
    "inventory_movements": ("movement_id",),
}
CRITICAL, ERROR, WARNING = Severity.CRITICAL, Severity.ERROR, Severity.WARNING


def _contract(entity: str) -> list[QualityCheck]:
    keys = BUSINESS_KEYS[entity]
    return [
        SchemaMatches(SILVER_SCHEMAS[entity], severity=CRITICAL, name=f"schema__{entity}"),
        *(NotNull(key, severity=CRITICAL) for key in keys),
        Unique(keys, severity=CRITICAL, name=f"duplicate__{entity}"),
    ]


def _orphans(column: str, reference: DataFrame, entity: str) -> list[QualityCheck]:
    """NULL key = ERROR; key absent from ``reference`` = WARNING, CRITICAL above 5% (ADR 007).

    Findings list a few orphan keys (``details``) for diagnosis.
    """
    return [
        NotNull(column, severity=ERROR, name=f"missing_{entity}"),
        ReferentialIntegrity(
            column,
            reference,
            column,
            severity=WARNING,
            name=f"unknown_{entity}",
            examples=ORPHAN_EXAMPLES,
        ),
        ReferentialIntegrity(
            column,
            reference,
            column,
            severity=CRITICAL,
            threshold=SYSTEMIC_ORPHAN_RATE,
            name=f"unknown_{entity}_systemic",
            examples=ORPHAN_EXAMPLES,
        ),
    ]


def _not_negative(*columns: str) -> list[QualityCheck]:
    return [
        Range(column, min_value=0, severity=ERROR, name=f"negative__{column}") for column in columns
    ]


REFERENCES: Final[dict[str, tuple[str, ...]]] = {
    "orders": ("customers",),
    "order_items": ("products", "orders"),
    "payments": ("orders",),
    "shipments": ("orders",),
    "inventory_movements": ("products", "stores"),
}


def _at_least_one(column: str) -> QualityCheck:
    return Range(column, min_value=1, severity=ERROR, name=f"non_positive__{column}")


def silver_row_rules(entity: str) -> list[QualityCheck]:
    """Row-level business rules (ERROR: the row goes to quarantine) on Silver column names.

    NULL never fails here: required columns are enforced by the standardization contract checks.
    """
    rules: dict[str, list[QualityCheck]] = {
        "suppliers": _not_negative("lead_time_days"),
        "customers": [
            CustomCondition(
                name="future__birth_date",  # born after registering: an impossible birth date
                condition="birth_date IS NULL OR registered_at IS NULL "
                "OR birth_date <= CAST(registered_at AS DATE)",
                severity=ERROR,
            )
        ],
        "products": _not_negative("list_price", "cost_price"),
        "orders": _not_negative("freight_amount"),
        "order_items": [
            _at_least_one("quantity"),
            *_not_negative("unit_price", "discount_amount"),
            CustomCondition(
                name="discount_exceeds_line",
                condition="discount_amount IS NULL OR quantity IS NULL OR unit_price IS NULL "
                "OR discount_amount <= quantity * unit_price",
                severity=ERROR,
            ),
        ],
        "payments": [*_not_negative("amount"), _at_least_one("installments")],
        "shipments": [
            *_not_negative("freight_amount"),
            CustomCondition(
                name="delivered_before_shipped",
                condition="delivered_at IS NULL OR shipped_at IS NULL "
                "OR delivered_at >= shipped_at",
                severity=ERROR,
            ),
        ],
        "inventory": _not_negative("quantity_on_hand", "reorder_point"),
        "inventory_movements": [
            CustomCondition(
                name="zero__quantity",  # movements are signed; zero moves nothing
                condition="quantity IS NULL OR quantity <> 0",
                severity=ERROR,
            )
        ],
    }
    return rules.get(entity, [])


def _entity_checks(entity: str, tables: Mapping[str, DataFrame]) -> list[QualityCheck]:
    checks = [*_contract(entity), *silver_row_rules(entity)]
    if entity == "orders":
        return [*checks, *_orphans("customer_id", tables["customers"], "customer")]
    if entity == "order_items":
        return [
            *checks,
            *_orphans("product_id", tables["products"], "product"),
            ReferentialIntegrity(
                "order_id", tables["orders"], "order_id", severity=WARNING, name="unknown_order"
            ),
        ]
    if entity == "payments":
        return [
            *checks,
            ReferentialIntegrity(
                "order_id", tables["orders"], "order_id", severity=WARNING, name="unknown_order"
            ),
        ]
    if entity == "shipments":
        return [*checks, *_orphans("order_id", tables["orders"], "order")]
    if entity == "inventory_movements":
        # reference_id is optional and points to different documents: not a foreign key
        return [
            *checks,
            *_orphans("product_id", tables["products"], "product"),
            *_orphans("store_id", tables["stores"], "store"),
        ]
    return checks


def retail_entity_suite(
    entity: str, frame: DataFrame, references: Mapping[str, DataFrame]
) -> list[CheckSet]:
    """The checks of one entity only; ``references`` are its foreign-key targets.

    Used by the Silver run, which gates one entity batch at a time against the current Silver
    tables (a reference table is not re-checked, so its own references are not needed).
    """
    if entity not in BUSINESS_KEYS:
        raise ValueError(f"no quality suite for: {entity}")
    missing = [r for r in REFERENCES.get(entity, ()) if r not in references]
    if missing:
        raise ValueError(f"{entity} needs reference tables {missing}")
    tables = {**references, entity: frame}
    return [CheckSet(f"silver_{entity}", frame, _entity_checks(entity, tables))]


def retail_silver_suite(tables: Mapping[str, DataFrame]) -> list[CheckSet]:
    """Check sets for the Silver entities in ``tables`` (keyed by entity name).

    An entity whose foreign keys are checked needs its reference entities in ``tables`` too.
    """
    unknown = sorted(set(tables) - set(BUSINESS_KEYS))
    if unknown:
        raise ValueError(f"no quality suite for: {unknown}")
    for entity, references in REFERENCES.items():
        missing = [r for r in references if entity in tables and r not in tables]
        if missing:
            raise ValueError(f"{entity} needs reference tables {missing}")
    return [
        CheckSet(f"silver_{entity}", tables[entity], _entity_checks(entity, tables))
        for entity in BUSINESS_KEYS
        if entity in tables
    ]


def _gold_keys(name: str, tables: Mapping[str, DataFrame]) -> list[QualityCheck]:
    """Required keys and referential integrity of one Gold table (all CRITICAL).

    The unknown member (-1) is a regular dimension row, so a fact pointing to it passes the
    foreign-key check; only the late master-data keys also report it, as WARNING.
    """
    foreign_keys = FOREIGN_KEYS.get(name, ())
    required = dict.fromkeys((*GRAIN[name], *(fk.column for fk in foreign_keys)))
    checks: list[QualityCheck] = [NotNull(column, severity=CRITICAL) for column in required]
    if name in NATURAL_KEYS:
        surrogate, natural = GRAIN[name][0], NATURAL_KEYS[name]
        checks.append(
            CustomCondition(
                name=f"natural_key__{natural}",
                condition=f"{surrogate} = {UNKNOWN_KEY} OR {natural} IS NOT NULL",
                column=natural,
                severity=CRITICAL,
                dimension=Dimension.COMPLETENESS,
            )
        )
    for fk in foreign_keys:
        checks.append(
            ReferentialIntegrity(
                fk.column,
                tables[fk.dimension],
                fk.key,
                severity=CRITICAL,
                name=f"fk__{fk.column}__{fk.dimension}",
            )
        )
        if fk.column in LATE_MASTER_DATA_KEYS and name.startswith("fact_"):
            checks.append(
                CustomCondition(
                    name=f"unknown_member__{fk.column}",
                    condition=f"{fk.column} IS NULL OR {fk.column} <> {UNKNOWN_KEY}",
                    column=fk.column,
                    severity=WARNING,
                    dimension=Dimension.REFERENTIAL_INTEGRITY,
                )
            )
    for key, event_date in EVENT_DATES.get(name, {}).items():
        checks.append(
            CustomCondition(
                name=f"out_of_calendar__{event_date}",
                condition=f"{key} IS NULL OR {key} <> {UNKNOWN_KEY} OR {event_date} IS NULL",
                column=event_date,
                severity=WARNING,
            )
        )
    return checks


INVENTORY_RECONCILIATION: Final = "inventory_reconciliation"
SYSTEMIC_INVENTORY_MISMATCH_RATE: Final = 0.05  # share of snapshot keys (same rule as orphans)


def inventory_reconciliation_suite(frame: DataFrame) -> list[CheckSet]:
    """R3 on ``gold.inventory.inventory_reconciliation``: exact match, in units (tolerance 0).

    A mismatched key is ERROR: a count error or an unrecorded/quarantined movement, an inventory
    finding that does not invalidate the sales facts (alert, Gold published). Above 5% of the
    keys it is CRITICAL: the ledger or the snapshot load is broken and the stock metrics
    would be wrong (nothing published, previous Gold kept).
    """
    condition = "difference = 0"
    return [
        CheckSet(
            INVENTORY_RECONCILIATION,
            frame,
            [
                CustomCondition(
                    name="r3__snapshot_matches_movements",
                    condition=condition,
                    column="quantity_on_hand",
                    severity=ERROR,
                    dimension=Dimension.CONSISTENCY,
                ),
                CustomCondition(
                    name="r3__snapshot_matches_movements_systemic",
                    condition=condition,
                    column="quantity_on_hand",
                    severity=CRITICAL,
                    threshold=SYSTEMIC_INVENTORY_MISMATCH_RATE,
                    dimension=Dimension.CONSISTENCY,
                ),
            ],
        )
    ]


def retail_gold_suite(
    tables: Mapping[str, DataFrame], *, inventory_reconciliation: DataFrame | None = None
) -> list[CheckSet]:
    """The publication gate of every Gold table, dimensions first (``GOLD_SCHEMAS`` order).

    CRITICAL: schema (columns and types), duplicated grain, NULL key, dangling foreign key,
    dimension member without natural key. WARNING: facts linked to the unknown customer, product
    or store (orphans the contract keeps, ADR 007), and facts dated outside the calendar (unknown
    date key, event date kept). ``run_suite`` stops at a schema failure.
    ``inventory_reconciliation`` adds R3 (``inventory_reconciliation_suite``) after the tables.
    """
    missing = sorted(set(GOLD_SCHEMAS) - set(tables))
    if missing:
        raise ValueError(f"missing Gold tables: {missing}")
    r3 = (
        []
        if inventory_reconciliation is None
        else inventory_reconciliation_suite(inventory_reconciliation)
    )
    return [
        CheckSet(
            name,
            tables[name],
            [
                SchemaMatches(GOLD_SCHEMAS[name], severity=CRITICAL, name=f"schema__{name}"),
                Unique(GRAIN[name], severity=CRITICAL, name=f"grain__{name}"),
                *_gold_keys(name, tables),
            ],
        )
        for name in GOLD_SCHEMAS
    ] + r3
