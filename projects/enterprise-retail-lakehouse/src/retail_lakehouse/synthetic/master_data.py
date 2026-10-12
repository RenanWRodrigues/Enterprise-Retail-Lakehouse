"""Master data: categories, suppliers, stores, products and customers.

Each dataset draws from its own random stream (``Random(f"{seed}:{name}")``), so changing
one volume never reshuffles another dataset.
"""

import math
import unicodedata
from bisect import bisect_right
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from random import Random
from typing import Final

from retail_lakehouse.synthetic.config import GenerationConfig
from retail_lakehouse.synthetic.reference_data import (
    BRANDS,
    CITIES,
    CUSTOMER_EMAIL_DOMAINS,
    DEPARTMENTS,
    FIRST_NAMES,
    LAST_NAMES,
    SUPPLIER_SUFFIXES,
    City,
)
from retail_lakehouse.synthetic.seasonality import BRASILIA

CENT: Final = Decimal("0.01")
PRICE_ENDING: Final = Decimal("0.90")
INACTIVE_PRODUCT_RATE: Final = 0.03
PREMIUM_SEGMENT_RATE: Final = 0.15
CUSTOMER_HISTORY_YEARS: Final = 5
MIN_AGE_DAYS: Final = 18 * 365 + 5
MAX_AGE_DAYS: Final = 80 * 365
COST_RATIO: Final = (0.45, 0.75)
LEAD_TIME_DAYS: Final = (2, 30)
MODEL_LETTERS: Final = "ABCDEFGHJKLMNPRSTUVWXZ"
SECONDS_PER_DAY: Final = 86_400

EmitFn = Callable[[str, dict[str, str | None]], None]


def stream(config: GenerationConfig, name: str) -> Random:
    """Independent, deterministic random stream (str seeds are hashed with SHA-512)."""
    return Random(f"{config.seed}:{name}")


def money(value: Decimal | float) -> Decimal:
    return Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)


def ts(value: datetime) -> str:
    return value.isoformat(timespec="seconds")


def _ascii(text: str) -> str:
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()


def _random_datetime(rng: Random, start: datetime, end: datetime) -> datetime:
    seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=rng.randrange(seconds))


def _midnight(day: date) -> datetime:
    return datetime.combine(day, time(), tzinfo=BRASILIA)


@dataclass(frozen=True, slots=True)
class Category:
    category_id: str
    name: str
    department_index: int
    min_price: float
    max_price: float
    weight: float  # share of the catalogue (leaves only)


@dataclass(frozen=True, slots=True)
class Store:
    store_id: str
    is_online: bool
    city: City


@dataclass(frozen=True, slots=True)
class Product:
    product_id: str
    list_price: Decimal
    is_active: bool
    multi_unit: bool
    popularity: float


@dataclass(frozen=True, slots=True)
class Customer:
    customer_id: str
    city: City
    registered_at: datetime


@dataclass
class MasterData:
    leaves: list[Category]
    supplier_ids: list[str]
    stores: list[Store]
    products: list[Product]
    customers: list[Customer]
    customer_registrations: list[datetime]  # sorted, aligned with ``customers``

    def eligible_customers(self, at: datetime) -> int:
        """How many customers were registered at or before ``at``."""
        return bisect_right(self.customer_registrations, at)


def build_categories(emit: EmitFn) -> list[Category]:
    leaves: list[Category] = []
    next_id = len(DEPARTMENTS) + 1
    for index, department in enumerate(DEPARTMENTS, start=1):
        emit("categories", {
            "category_id": f"CAT-{index:03d}",
            "category_name": department.name,
            "parent_category_id": None,
        })  # fmt: skip
    for index, department in enumerate(DEPARTMENTS, start=1):
        share = department.product_share / len(department.subcategories)
        for sub in department.subcategories:
            category_id = f"CAT-{next_id:03d}"
            emit("categories", {
                "category_id": category_id,
                "category_name": sub.name,
                "parent_category_id": f"CAT-{index:03d}",
            })  # fmt: skip
            leaves.append(
                Category(category_id, sub.name, index - 1, sub.min_price, sub.max_price, share)
            )
            next_id += 1
    return leaves


def build_suppliers(config: GenerationConfig, emit: EmitFn) -> list[str]:
    rng = stream(config, "suppliers")
    weights = [city.weight for city in CITIES]
    ids = []
    for number in range(1, config.suppliers + 1):
        supplier_id = f"SUP-{number:04d}"
        city = rng.choices(CITIES, weights=weights)[0]
        emit("suppliers", {
            "supplier_id": supplier_id,
            "supplier_name": f"{rng.choice(LAST_NAMES)} {rng.choice(SUPPLIER_SUFFIXES)} Ltda",
            "email": f"contato.sup{number:04d}@example.net",
            "city": city.name,
            "state": city.state,
            "lead_time_days": str(rng.randint(*LEAD_TIME_DAYS)),
            "created_at": (config.start_date - timedelta(days=rng.randint(200, 3000))).isoformat(),
        })  # fmt: skip
        ids.append(supplier_id)
    return ids


def build_stores(config: GenerationConfig, emit: EmitFn) -> list[Store]:
    rng = stream(config, "stores")
    stores = []
    for number in range(1, config.stores + 1):
        is_online = number == config.stores
        city = CITIES[0] if is_online else CITIES[(number - 1) % len(CITIES)]
        suffix = "" if number <= len(CITIES) else f" {(number - 1) // len(CITIES) + 1}"
        store = Store(f"STR-{number:03d}", is_online, city)
        emit("stores", {
            "store_id": store.store_id,
            "store_name": "E-commerce - CD São Paulo" if is_online else f"Loja {city.name}{suffix}",
            "store_type": "online" if is_online else "physical",
            "city": city.name,
            "state": city.state,
            "region": city.region,
            "opened_at": (config.start_date - timedelta(days=rng.randint(30, 5000))).isoformat(),
        })  # fmt: skip
        stores.append(store)
    return stores


def build_products(
    config: GenerationConfig, leaves: list[Category], supplier_ids: list[str], emit: EmitFn
) -> list[Product]:
    rng = stream(config, "products")
    weights = [leaf.weight for leaf in leaves]
    window_start = _midnight(config.start_date - timedelta(days=4 * 365))
    window_end = _midnight(config.start_date - timedelta(days=30))
    end = _midnight(config.end_date)
    products = []
    for number in range(1, config.products + 1):
        leaf = rng.choices(leaves, weights=weights)[0]
        department = DEPARTMENTS[leaf.department_index]
        raw = math.exp(rng.uniform(math.log(leaf.min_price), math.log(leaf.max_price)))
        list_price = Decimal(int(raw)) + PRICE_ENDING
        cost_price = money(float(list_price) * rng.uniform(*COST_RATIO))
        brand = rng.choice(BRANDS)
        created = _random_datetime(rng, window_start, window_end)
        updated = min(
            created + timedelta(days=rng.randint(0, 600), seconds=rng.randrange(SECONDS_PER_DAY)),
            end,
        )
        is_active = rng.random() >= INACTIVE_PRODUCT_RATE or number == 1
        model = f"{rng.choice(MODEL_LETTERS)}{rng.randint(100, 999)}"
        product = Product(
            product_id=f"PRD-{number:06d}",
            list_price=list_price,
            is_active=is_active,
            multi_unit=department.multi_unit,
            popularity=rng.paretovariate(1.5),
        )
        emit("products", {
            "product_id": product.product_id,
            "sku": f"SKU-{leaf.category_id[4:]}-{number:06d}",
            "product_name": f"{leaf.name} {brand} {model}",
            "category_id": leaf.category_id,
            "supplier_id": rng.choice(supplier_ids),
            "brand": brand,
            "list_price": f"{list_price:.2f}",
            "cost_price": f"{cost_price:.2f}",
            "is_active": "true" if is_active else "false",
            "created_at": ts(created),
            "updated_at": ts(updated),
        })  # fmt: skip
        products.append(product)
    return products


def build_customers(
    config: GenerationConfig, emit: EmitFn
) -> tuple[list[Customer], list[datetime]]:
    rng = stream(config, "customers")
    history_start = _midnight(config.start_date - timedelta(days=CUSTOMER_HISTORY_YEARS * 365))
    window_end = _midnight(config.end_date) + timedelta(days=1)
    # The first customer exists since the beginning, so every order has an eligible customer.
    registrations = sorted(
        [history_start]
        + [_random_datetime(rng, history_start, window_end) for _ in range(config.customers - 1)]
    )
    weights = [city.weight for city in CITIES]
    customers = []
    for number, registered_at in enumerate(registrations, start=1):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        city = rng.choices(CITIES, weights=weights)[0]
        customer = Customer(f"CUS-{number:07d}", city, registered_at)
        birth = config.start_date - timedelta(days=rng.randint(MIN_AGE_DAYS, MAX_AGE_DAYS))
        emit("customers", {
            "customer_id": customer.customer_id,
            "first_name": first,
            "last_name": last,
            "email": f"{_ascii(first)}.{_ascii(last)}{number}@{rng.choice(CUSTOMER_EMAIL_DOMAINS)}",
            "birth_date": birth.isoformat(),
            "city": city.name,
            "state": city.state,
            "segment": "premium" if rng.random() < PREMIUM_SEGMENT_RATE else "standard",
            "registered_at": ts(registered_at),
        })  # fmt: skip
        customers.append(customer)
    return customers, registrations


def build_master_data(config: GenerationConfig, emit: EmitFn) -> MasterData:
    leaves = build_categories(emit)
    supplier_ids = build_suppliers(config, emit)
    stores = build_stores(config, emit)
    products = build_products(config, leaves, supplier_ids, emit)
    customers, registrations = build_customers(config, emit)
    return MasterData(leaves, supplier_ids, stores, products, customers, registrations)
