"""Incremental retail batches (CDC) on top of a full batch, without changing the full mode.

A **plan** (``config/incremental.toml``) names the base configuration (the full batch) and the
incremental batches in order. Batch N is produced by:

1. rebuilding the base master data from the base configuration (deterministic: same IDs);
2. replaying the master-data changes of batches 1..N-1 (nothing is read from disk);
3. applying batch N: a share of customers changed (segment, city), a share of products changed
   (price, category within the same department), new customers and products, new orders of
   existing entities in the batch window ``(previous as_of, as_of]``.

The batch is a CDC export: only changed or new customers and products, plus the window's new
orders, items, payments, shipments and stock movements. Every change is recorded with its real
date in ``_changes_<batch_id>.csv`` (ground truth: the CRM export itself has no change date).

Identifiers: new customers and products continue the base numbering; new transactions use the
batch as namespace (``ORD-<batch_id>-000001``), so they never collide with the full batch.
Each change has a deterministic ``event_id`` and each batch a ``batch_fingerprint``.
"""

import hashlib
import json
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from random import Random
from typing import Final

from retail_lakehouse.synthetic.config import GenerationConfig, load_config
from retail_lakehouse.synthetic.generator import _Transactions
from retail_lakehouse.synthetic.master_data import (
    Customer,
    MasterData,
    Product,
    build_master_data,
    money,
    ts,
)
from retail_lakehouse.synthetic.reference_data import CITIES, DEPARTMENTS, City
from retail_lakehouse.synthetic.seasonality import BRASILIA, sample_order_timestamps
from retail_lakehouse.synthetic.sinks import Sink

CHANGE_COLUMNS: Final = (
    "event_id", "batch_id", "entity", "entity_id", "change_type", "attribute",
    "old_value", "new_value", "changed_at",
)  # fmt: skip
CUSTOMER_CHANGES: Final = (("city",), ("segment",), ("city", "segment"))
CUSTOMER_CHANGE_WEIGHTS: Final = (0.50, 0.35, 0.15)
PRODUCT_PRICE_CHANGE: Final = (0.05, 0.15)  # relative, up or down
PRODUCT_CATEGORY_CHANGE_RATE: Final = 0.3  # the other changes are price-only
SEGMENTS: Final = ("standard", "premium")
MAX_CUSTOMERS, MAX_PRODUCTS = 8_999_999, 899_999  # below the orphan-key range ("9...")
BATCH_STOCK: Final = 50  # units per store and product at the start of a batch window
Window = tuple[date, date]  # first and last day (inclusive)


@dataclass(frozen=True, slots=True)
class BatchPlan:
    batch_id: str
    as_of: date  # last day of the batch window (inclusive)
    customer_change_rate: float = 0.0
    product_change_rate: float = 0.0
    new_customers: int = 0
    new_products: int = 0
    orders: int = 0

    def __post_init__(self) -> None:
        for rate in (self.customer_change_rate, self.product_change_rate):
            if not 0.0 <= rate <= 1.0:
                raise ValueError(f"change rates must be within [0, 1] ({self.batch_id})")
        if min(self.new_customers, self.new_products, self.orders) < 0:
            raise ValueError(f"volumes must not be negative ({self.batch_id})")


@dataclass(frozen=True)
class IncrementalPlan:
    base: GenerationConfig
    batches: tuple[BatchPlan, ...]

    def __post_init__(self) -> None:
        ids = [b.batch_id for b in self.batches]
        if len(set(ids)) != len(ids) or self.base.batch_id in ids:
            raise ValueError("batch ids must be unique and differ from the base batch")
        previous = self.base.end_date
        for batch in self.batches:
            if batch.as_of <= previous:
                raise ValueError(f"{batch.batch_id}: as_of must be after the previous window")
            previous = batch.as_of

    def index(self, batch_id: str) -> int:
        for position, batch in enumerate(self.batches):
            if batch.batch_id == batch_id:
                return position
        raise ValueError(
            f"unknown batch {batch_id!r}; planned: {[b.batch_id for b in self.batches]}"
        )

    def window(self, position: int) -> tuple[date, date]:
        """First and last day of the batch window."""
        before = self.base.end_date if position == 0 else self.batches[position - 1].as_of
        return before + timedelta(days=1), self.batches[position].as_of

    def fingerprint(self, position: int) -> str:
        """Identity of the batch: the base configuration and every batch up to this one."""
        payload = {
            "base": {k: str(v) for k, v in asdict(self.base).items()},
            "batches": [
                {k: str(v) for k, v in asdict(b).items()} for b in self.batches[: position + 1]
            ],
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def load_plan(path: Path) -> IncrementalPlan:
    with path.open("rb") as handle:
        doc = tomllib.load(handle)
    base = load_config(path.parent / str(doc["base_config"]))
    return IncrementalPlan(base, tuple(BatchPlan(**entry) for entry in doc.get("batches", [])))


@dataclass(frozen=True, slots=True)
class Change:
    event_id: str
    batch_id: str
    entity: str
    entity_id: str
    change_type: str  # update | insert
    attribute: str
    old_value: str | None
    new_value: str | None
    changed_at: str


def event_id(batch_id: str, entity: str, entity_id: str, attribute: str) -> str:
    """Deterministic: the same change of the same batch always has the same id."""
    raw = f"{batch_id}|{entity}|{entity_id}|{attribute}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]


def _updated(
    batch_id: str, subject: tuple[str, str], attribute: str,
    values: tuple[str | None, str | None], at: datetime,
) -> Change:  # fmt: skip
    """``subject`` = (entity, entity_id); ``values`` = (old, new)."""
    (entity, entity_id), (old, new) = subject, values
    eid = event_id(batch_id, entity, entity_id, attribute)
    return Change(eid, batch_id, entity, entity_id, "update", attribute, old, new, ts(at))


def _inserted(batch_id: str, entity: str, entity_id: str, at: datetime) -> Change:
    eid = event_id(batch_id, entity, entity_id, "*")
    return Change(eid, batch_id, entity, entity_id, "insert", "*", None, None, ts(at))


Row = dict[str, str | None]


@dataclass
class MasterState:
    """Current master data (rows as the source exports them, and the generator's objects)."""

    customers: dict[str, Row]
    products: dict[str, Row]
    master: MasterData
    leaf_department: dict[str, int] = field(default_factory=dict)


def base_state(config: GenerationConfig) -> MasterState:
    """The full batch's master data, clean (anomalies belong to the full batch's files)."""
    rows: dict[str, list[Row]] = {}
    master = build_master_data(
        config, lambda dataset, row: rows.setdefault(dataset, []).append(dict(row))
    )
    departments = {leaf.category_id: leaf.department_index for leaf in master.leaves}
    return MasterState(
        {r["customer_id"] or "": r for r in rows["customers"]},
        {r["product_id"] or "": r for r in rows["products"]},
        master,
        departments,
    )


def _rng(plan: IncrementalPlan, batch_id: str, purpose: str) -> Random:
    return Random(f"{plan.base.seed}:incremental:{batch_id}:{purpose}")


def _moments(rng: Random, first: date, last: date, count: int) -> list[datetime]:
    """``count`` distinct-second instants in the window, sorted (Brasília time)."""
    start = datetime.combine(first, time(), BRASILIA)
    seconds = int(
        (datetime.combine(last + timedelta(days=1), time(), BRASILIA) - start).total_seconds()
    )
    return sorted(start + timedelta(seconds=s) for s in rng.sample(range(seconds), count))


def _other_city(rng: Random, current: str | None) -> City:
    candidates = [c for c in CITIES if c.name != current]
    return rng.choices(candidates, weights=[c.weight for c in candidates])[0]


Emit = Callable[[str, Row], None]


def _change_customers(
    state: MasterState, batch: BatchPlan, rng: Random, moments: list[datetime], emit: Emit
) -> list[Change]:
    chosen = rng.sample(
        sorted(state.customers), round(len(state.customers) * batch.customer_change_rate)
    )
    changes = []
    for customer_id, at in zip(chosen, moments, strict=True):
        row = dict(state.customers[customer_id])
        attributes = rng.choices(CUSTOMER_CHANGES, weights=CUSTOMER_CHANGE_WEIGHTS)[0]
        updates: dict[str, str] = {}
        if "city" in attributes:
            city = _other_city(rng, row["city"])
            updates |= {"city": city.name, "state": city.state}
        if "segment" in attributes:
            updates["segment"] = SEGMENTS[1] if row["segment"] == SEGMENTS[0] else SEGMENTS[0]
        for attribute, value in updates.items():
            changes.append(
                _updated(
                    batch.batch_id,
                    ("customer", customer_id),
                    attribute,
                    (row[attribute], value),
                    at,
                )
            )
        row |= updates
        state.customers[customer_id] = row
        emit("customers", row)
    return changes


def _change_products(
    state: MasterState, batch: BatchPlan, rng: Random, moments: list[datetime], emit: Emit
) -> list[Change]:
    active = sorted(pid for pid, r in state.products.items() if r["is_active"] == "true")
    chosen = rng.sample(active, round(len(active) * batch.product_change_rate))
    changes = []
    for product_id, at in zip(chosen, moments, strict=True):
        row = dict(state.products[product_id])
        updates: dict[str, str] = {}
        department = state.leaf_department[row["category_id"] or ""]
        siblings = [
            c
            for c, d in state.leaf_department.items()
            if d == department and c != row["category_id"]
        ]
        if siblings and rng.random() < PRODUCT_CATEGORY_CHANGE_RATE:
            updates["category_id"] = rng.choice(sorted(siblings))
        else:
            factor = 1 + rng.choice((-1, 1)) * rng.uniform(*PRODUCT_PRICE_CHANGE)
            updates["list_price"] = (
                f"{money(float(Decimal(row['list_price'] or '0')) * factor):.2f}"
            )
        for attribute, value in updates.items():
            changes.append(
                _updated(
                    batch.batch_id, ("product", product_id), attribute, (row[attribute], value), at
                )
            )
        row |= {**updates, "updated_at": ts(at)}
        state.products[product_id] = row
        emit("products", row)
    return changes


def _new_customers(
    state: MasterState, batch: BatchPlan, window: Window, rng: Random, emit: Emit
) -> list[Change]:
    first, last = window
    start = len(state.customers) + 1
    if start + batch.new_customers - 1 > MAX_CUSTOMERS:
        raise ValueError("too many customers for the id range")
    changes = []
    for number, at in enumerate(_moments(rng, first, last, batch.new_customers), start=start):
        city = rng.choices(CITIES, weights=[c.weight for c in CITIES])[0]
        customer_id = f"CUS-{number:07d}"
        row: Row = {
            "customer_id": customer_id, "first_name": "Cliente", "last_name": f"Novo {number}",
            "email": f"cliente.novo{number}@example.com",
            "birth_date": (first - timedelta(days=rng.randint(20 * 365, 70 * 365))).isoformat(),
            "city": city.name, "state": city.state, "segment": SEGMENTS[0], "registered_at": ts(at),
        }  # fmt: skip
        state.customers[customer_id] = row
        state.master.customers.append(Customer(customer_id, city, at))
        state.master.customer_registrations.append(at)
        changes.append(_inserted(batch.batch_id, "customer", customer_id, at))
        emit("customers", row)
    return changes


def _new_products(
    state: MasterState, batch: BatchPlan, window: Window, rng: Random, emit: Emit
) -> list[Change]:
    first, last = window
    start = len(state.products) + 1
    if start + batch.new_products - 1 > MAX_PRODUCTS:
        raise ValueError("too many products for the id range")
    leaves = state.master.leaves
    changes = []
    for number, at in enumerate(_moments(rng, first, last, batch.new_products), start=start):
        leaf = rng.choice(leaves)
        price = Decimal(int(rng.uniform(leaf.min_price, leaf.max_price))) + Decimal("0.90")
        product_id = f"PRD-{number:06d}"
        row: Row = {
            "product_id": product_id, "sku": f"SKU-{leaf.category_id[4:]}-{number:06d}",
            "product_name": f"{leaf.name} Novo {number}", "category_id": leaf.category_id,
            "supplier_id": rng.choice(state.master.supplier_ids), "brand": "Nova",
            "list_price": f"{price:.2f}", "cost_price": f"{money(float(price) * 0.6):.2f}",
            "is_active": "true", "created_at": ts(at), "updated_at": ts(at),
        }  # fmt: skip
        state.products[product_id] = row
        multi = DEPARTMENTS[leaf.department_index].multi_unit
        state.master.products.append(Product(product_id, price, True, multi, 1.0))
        changes.append(_inserted(batch.batch_id, "product", product_id, at))
        emit("products", row)
    return changes


def _sync_products(state: MasterState) -> None:
    """The generator's product objects follow the current prices (orders use them)."""
    state.master.products = [
        replace(p, list_price=Decimal(state.products[p.product_id]["list_price"] or "0"))
        for p in state.master.products
    ]


def _sync_customers(state: MasterState) -> None:
    """The generator's customer objects follow the current city (store choice uses it)."""
    by_name = {c.name: c for c in CITIES}
    state.master.customers = [
        replace(c, city=by_name[state.customers[c.customer_id]["city"] or ""])
        for c in state.master.customers
    ]


def apply_master_changes(
    plan: IncrementalPlan, state: MasterState, position: int, emit: Emit
) -> list[Change]:
    """Master-data changes of one batch, applied to ``state``; their rows go to ``emit``."""
    batch = plan.batches[position]
    window = plan.window(position)
    first, last = window
    rng = _rng(plan, batch.batch_id, "master")
    changes = _change_customers(state, batch, rng, _moments(rng, first, last, round(
        len(state.customers) * batch.customer_change_rate)), emit)  # fmt: skip
    active = sum(r["is_active"] == "true" for r in state.products.values())
    changes += _change_products(state, batch, rng, _moments(rng, first, last, round(
        active * batch.product_change_rate)), emit)  # fmt: skip
    changes += _new_customers(state, batch, window, rng, emit)
    changes += _new_products(state, batch, window, rng, emit)
    _sync_customers(state)
    _sync_products(state)
    return changes


class _BatchTransactions(_Transactions):
    """The full generator's order lifecycle, in the batch window, with batch-scoped ids.

    No opening balances and no snapshot: stock is a continuing balance (sales, receipts and
    returns are the batch's movements).
    """

    def __init__(
        self, config: GenerationConfig, master: MasterData, emit: Emit, batch_id: str
    ) -> None:
        super().__init__(config, master, emit)
        self._batch_id = batch_id

    def _next_id(self, prefix: str, digits: int) -> str:
        del digits
        self._counters[prefix] += 1
        return f"{prefix}-{self._batch_id}-{self._counters[prefix]:06d}"

    def run(self) -> None:
        for store in self._master.stores:
            for product in self._assortment[store.store_id]:
                key = (store.store_id, product.product_id)
                self._target[key] = self._balance[key] = BATCH_STOCK
        for ordered_at in self._order_times():
            self._order(ordered_at)

    def _order_times(self) -> list[datetime]:
        return sample_order_timestamps(
            self._rng, self._config.start_date, self._config.end_date, self._config.orders
        )


@dataclass(frozen=True)
class IncrementalSummary:
    batch_id: str
    batch_fingerprint: str
    window: tuple[str, str]
    changes: tuple[Change, ...]


def generate_incremental_batch(
    plan: IncrementalPlan, batch_id: str, sink: Sink
) -> IncrementalSummary:
    """Write one incremental batch to ``sink`` (same plan and batch: same rows)."""
    position = plan.index(batch_id)
    state = base_state(plan.base)
    for earlier in range(position):
        apply_master_changes(plan, state, earlier, lambda dataset, row: None)
    changes = apply_master_changes(plan, state, position, sink.write)
    batch = plan.batches[position]
    first, last = plan.window(position)
    if batch.orders:
        seed = int(hashlib.sha256(f"{plan.base.seed}:{batch_id}".encode()).hexdigest()[:12], 16)
        config = replace(plan.base, seed=seed, start_date=first, end_date=last, orders=batch.orders,
                         batch_id=batch_id, anomalies=())  # fmt: skip
        _BatchTransactions(config, state.master, sink.write, batch_id).run()
    return IncrementalSummary(
        batch_id, plan.fingerprint(position), (first.isoformat(), last.isoformat()), tuple(changes)
    )


def change_rows(changes: tuple[Change, ...]) -> list[Mapping[str, str | None]]:
    return [asdict(change) for change in changes]
