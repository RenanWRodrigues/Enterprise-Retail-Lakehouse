"""Deterministic retail dataset generation (master data, orders, payments, logistics, stock).

Rows are streamed to a sink as they are produced; only master data and stock balances
are kept in memory. See docs/DATA_GENERATION.md.
"""

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from decimal import Decimal
from itertools import accumulate
from typing import Final

from retail_lakehouse.ingestion.source_contracts import CARRIERS, RETAIL_SOURCE_CONTRACTS
from retail_lakehouse.synthetic.anomalies import AnomalyInjector, AnomalyRecord, Row
from retail_lakehouse.synthetic.config import GenerationConfig
from retail_lakehouse.synthetic.master_data import (
    Customer,
    EmitFn,
    MasterData,
    Product,
    Store,
    build_master_data,
    money,
    stream,
    ts,
)
from retail_lakehouse.synthetic.reference_data import (
    FREE_SHIPPING_FROM,
    FREIGHT_RANGE,
    TRANSIT_DAYS,
)
from retail_lakehouse.synthetic.seasonality import BRASILIA, promotion, sample_order_timestamps
from retail_lakehouse.synthetic.sinks import Sink

ITEM_COUNT_WEIGHTS: Final = (0.18, 0.24, 0.20, 0.14, 0.10, 0.07, 0.04, 0.03)  # 1..8, mean 3.26
CANDIDATES_PER_ITEM: Final = 3  # draws per wanted item before giving up on distinct products
CHANNELS: Final = ("store", "online", "app")
CHANNEL_WEIGHTS: Final = (0.60, 0.25, 0.15)
STORE_PAYMENT_METHODS: Final = (
    ("credit_card", "debit_card", "pix", "cash"),
    (0.45, 0.25, 0.20, 0.10),
)
ONLINE_PAYMENT_METHODS: Final = (("credit_card", "pix", "boleto"), (0.55, 0.30, 0.15))
INSTALLMENTS: Final = (tuple(range(1, 11)), (30, 10, 12, 8, 8, 10, 4, 4, 4, 10))
INSTALLMENT_MIN_AMOUNT: Final = Decimal(100)
SINGLE_UNIT_QUANTITY: Final = ((1, 2, 3), (0.80, 0.15, 0.05))
MULTI_UNIT_MAX: Final = 6
CANCEL_RATE: Final = 0.03
RETURN_RATE: Final = 0.02
DECLINED_RETRY_RATE: Final = 0.02
RETURN_DELAY: Final = timedelta(days=7)
RETRY_DELAY: Final = timedelta(minutes=3)
CUSTOMER_SKEW: Final = 1.6  # >1 concentrates orders on long-standing customers
PHYSICAL_ASSORTMENT_SHARE: Final = 0.5
OPENING_STOCK: Final = {  # (is_online, multi_unit) -> range
    (False, False): (5, 40),
    (False, True): (20, 120),
    (True, False): (30, 200),
    (True, True): (100, 400),
}
REORDER_POINT: Final = {False: 5, True: 20}
ZERO: Final = Decimal("0.00")


@dataclass(frozen=True, slots=True)
class GenerationSummary:
    row_counts: dict[str, int]
    anomaly_counts: dict[str, int]


class _CountingSink:
    def __init__(self, sink: Sink) -> None:
        self._sink = sink
        self.rows: Counter[str] = Counter()
        self.anomalies: Counter[str] = Counter()

    def write(self, dataset: str, row: Mapping[str, str | None]) -> None:
        self.rows[dataset] += 1
        self._sink.write(dataset, row)

    def record_anomaly(self, anomaly: AnomalyRecord) -> None:
        self.anomalies[anomaly.anomaly_type.value] += 1
        self._sink.record_anomaly(anomaly)

    def summary(self) -> GenerationSummary:
        return GenerationSummary(
            row_counts={name: self.rows[name] for name in RETAIL_SOURCE_CONTRACTS},
            anomaly_counts=dict(sorted(self.anomalies.items())),
        )


def _row(dataset: str, **values: str | None) -> Row:
    """Row with the contract's column order (fails fast on a missing column)."""
    return {column: values[column] for column in RETAIL_SOURCE_CONTRACTS[dataset].column_names}


@dataclass(frozen=True, slots=True)
class _PaymentTerms:
    order_id: str
    method: str
    installments: int
    amount: Decimal


@dataclass(frozen=True, slots=True)
class _Line:
    product: Product
    quantity: int
    discount: Decimal

    @property
    def net(self) -> Decimal:
        return self.product.list_price * self.quantity - self.discount


class _Transactions:
    def __init__(self, config: GenerationConfig, master: MasterData, emit: EmitFn) -> None:
        self._config = config
        self._master = master
        self._emit = emit
        self._rng = stream(config, "transactions")
        self._stock_rng = stream(config, "inventory")
        self._window_start = datetime.combine(config.start_date, time(), BRASILIA)
        self._window_end = datetime.combine(config.end_date, time(23, 59, 59), BRASILIA)
        self._counters: Counter[str] = Counter()
        active = [p for p in master.products if p.is_active]
        self._assortment: dict[str, list[Product]] = {}
        self._cum_weights: dict[str, list[float]] = {}
        for store in master.stores:
            products = (
                active
                if store.is_online
                else [p for p in active if self._stock_rng.random() < PHYSICAL_ASSORTMENT_SHARE]
                or active[:1]
            )
            self._assortment[store.store_id] = products
            self._cum_weights[store.store_id] = list(accumulate(p.popularity for p in products))
        self._online_store = master.stores[-1]
        physical = [s for s in master.stores if not s.is_online]
        self._stores_by_state: dict[str, list[Store]] = {}
        self._stores_by_region: dict[str, list[Store]] = {}
        for store in physical:
            self._stores_by_state.setdefault(store.city.state, []).append(store)
            self._stores_by_region.setdefault(store.city.region, []).append(store)
        self._physical = physical
        self._balance: dict[tuple[str, str], int] = {}
        self._target: dict[tuple[str, str], int] = {}

    def _next_id(self, prefix: str, digits: int) -> str:
        self._counters[prefix] += 1
        return f"{prefix}-{self._counters[prefix]:0{digits}d}"

    def run(self) -> None:
        self._opening_balances()
        timestamps = sample_order_timestamps(
            self._rng, self._config.start_date, self._config.end_date, self._config.orders
        )
        for ordered_at in timestamps:
            self._order(ordered_at)
        self._snapshot()

    # --- stock -----------------------------------------------------------------------------

    def _movement(  # noqa: PLR0913, PLR0917 - mirrors the movement record
        self,
        store: Store,
        product: Product,
        kind: str,
        quantity: int,
        at: datetime,
        ref: str | None,
    ) -> None:
        key = (store.store_id, product.product_id)
        self._balance[key] = self._balance.get(key, 0) + quantity
        self._emit("inventory_movements", _row(
            "inventory_movements",
            movement_id=self._next_id("MOV", 9),
            store_id=store.store_id,
            product_id=product.product_id,
            movement_type=kind,
            quantity=str(quantity),
            movement_timestamp=ts(at),
            reference_id=ref,
        ))  # fmt: skip

    def _opening_balances(self) -> None:
        for store in self._master.stores:
            for product in self._assortment[store.store_id]:
                quantity = self._stock_rng.randint(
                    *OPENING_STOCK[(store.is_online, product.multi_unit)]
                )
                self._target[(store.store_id, product.product_id)] = quantity
                self._movement(
                    store, product, "opening_balance", quantity, self._window_start, None
                )

    def _sell(self, store: Store, line: _Line, at: datetime, order_id: str) -> None:
        key = (store.store_id, line.product.product_id)
        balance = self._balance[key]
        if balance < line.quantity:  # on-demand replenishment right before the sale
            receipt = max(self._target[key] - balance, line.quantity - balance)
            arrival = max(at - timedelta(hours=1), self._window_start)
            self._movement(store, line.product, "purchase_receipt", receipt, arrival, None)
        self._movement(store, line.product, "sale", -line.quantity, at, order_id)

    def _snapshot(self) -> None:
        for store in self._master.stores:
            for product in self._assortment[store.store_id]:
                self._emit("inventory", _row(
                    "inventory",
                    snapshot_date=self._config.end_date.isoformat(),
                    store_id=store.store_id,
                    product_id=product.product_id,
                    quantity_on_hand=str(self._balance[(store.store_id, product.product_id)]),
                    reorder_point=str(REORDER_POINT[store.is_online]),
                ))  # fmt: skip

    # --- orders ----------------------------------------------------------------------------

    def _customer(self, at: datetime) -> Customer:
        eligible = self._master.eligible_customers(at)
        return self._master.customers[int(eligible * self._rng.random() ** CUSTOMER_SKEW)]

    def _physical_store(self, customer: Customer) -> Store:
        candidates = (
            self._stores_by_state.get(customer.city.state)
            or self._stores_by_region.get(customer.city.region)
            or self._physical
        )
        return self._rng.choice(candidates)

    def _lines(self, store: Store, at: datetime) -> list[_Line]:
        wanted = self._rng.choices(
            range(1, len(ITEM_COUNT_WEIGHTS) + 1), weights=ITEM_COUNT_WEIGHTS
        )[0]
        candidates = self._rng.choices(
            self._assortment[store.store_id],
            cum_weights=self._cum_weights[store.store_id],
            k=wanted * CANDIDATES_PER_ITEM,
        )
        chosen = list(dict.fromkeys(candidates))[:wanted]  # distinct, first-drawn order
        on_sale, low, high = promotion(at.date())
        lines = []
        for product in chosen:
            if product.multi_unit:
                quantity = self._rng.randint(1, MULTI_UNIT_MAX)
            else:
                quantity = self._rng.choices(*SINGLE_UNIT_QUANTITY)[0]
            discount = ZERO
            if self._rng.random() < on_sale:
                discount = money(
                    float(product.list_price * quantity) * self._rng.uniform(low, high)
                )
            lines.append(_Line(product, quantity, discount))
        return lines

    def _payment(self, terms: _PaymentTerms, status: str, at: datetime) -> None:
        self._emit("payments", _row(
            "payments",
            payment_id=self._next_id("PAY", 8),
            order_id=terms.order_id,
            payment_method=terms.method,
            installments=str(terms.installments),
            amount=f"{terms.amount:.2f}",
            payment_status=status,
            processed_at=ts(at),
        ))  # fmt: skip

    def _order(self, ordered_at: datetime) -> None:  # noqa: PLR0912, PLR0915 - one order lifecycle
        rng = self._rng
        order_id = self._next_id("ORD", 8)
        channel = rng.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]
        is_online = channel != "store"
        customer = self._customer(ordered_at)
        store = self._online_store if is_online else self._physical_store(customer)
        lines = self._lines(store, ordered_at)
        net = sum((line.net for line in lines), ZERO)
        region = customer.city.region
        freight = ZERO
        if is_online and net < FREE_SHIPPING_FROM:
            freight = money(rng.uniform(*FREIGHT_RANGE[region]))
        amount = net + freight

        methods, weights = ONLINE_PAYMENT_METHODS if is_online else STORE_PAYMENT_METHODS
        method = rng.choices(methods, weights=weights)[0]
        installments = 1
        if method == "credit_card" and amount > INSTALLMENT_MIN_AMOUNT:
            installments = rng.choices(*INSTALLMENTS)[0]
        delay = timedelta(seconds=rng.randint(30, 600))
        if method == "boleto":
            delay = timedelta(days=rng.randint(1, 2), seconds=rng.randint(0, 3600))
        paid_at = ordered_at + delay
        cancelled = rng.random() < CANCEL_RATE
        retried = rng.random() < DECLINED_RETRY_RATE
        wants_return = rng.random() < RETURN_RATE
        shipped_at = paid_at + timedelta(hours=rng.randint(4, 48))
        transit_low, transit_high = TRANSIT_DAYS[region]
        delivered_at = shipped_at + timedelta(
            days=rng.randint(transit_low, transit_high), hours=rng.randint(0, 10)
        )
        carrier = rng.choice(CARRIERS)

        returned_at: datetime | None = None
        shipment_status: str | None = None
        if cancelled:
            status = "cancelled"
        elif paid_at > self._window_end:
            status = "created"
        elif not is_online:
            status = "delivered"
        elif shipped_at > self._window_end:
            status = "paid"
        elif delivered_at > self._window_end:
            status, shipment_status = "shipped", "in_transit"
        elif wants_return and delivered_at + RETURN_DELAY <= self._window_end:
            status, shipment_status = "returned", "returned"
            returned_at = delivered_at + RETURN_DELAY
        else:
            status, shipment_status = "delivered", "delivered"

        self._emit("orders", _row(
            "orders",
            order_id=order_id,
            customer_id=customer.customer_id,
            store_id=store.store_id,
            channel=channel,
            order_status=status,
            order_timestamp=ts(ordered_at),
            currency="BRL",
            freight_amount=f"{freight:.2f}",
        ))  # fmt: skip
        for number, line in enumerate(lines, start=1):
            self._emit("order_items", _row(
                "order_items",
                order_id=order_id,
                line_number=str(number),
                product_id=line.product.product_id,
                quantity=str(line.quantity),
                unit_price=f"{line.product.list_price:.2f}",
                discount_amount=f"{line.discount:.2f}",
            ))  # fmt: skip

        terms = _PaymentTerms(order_id, method, installments, amount)
        if status == "cancelled":
            self._payment(terms, "declined", paid_at)
        elif status != "created":
            if retried:
                self._payment(terms, "declined", paid_at)
                paid_at += RETRY_DELAY
            self._payment(terms, "approved", paid_at)
            if returned_at is not None:
                self._payment(terms, "refunded", returned_at)

        if shipment_status is not None:
            self._emit("shipments", _row(
                "shipments",
                shipment_id=self._next_id("SHP", 8),
                order_id=order_id,
                carrier_id=carrier,
                shipment_status=shipment_status,
                shipped_at=ts(shipped_at),
                delivered_at=None if shipment_status == "in_transit" else ts(delivered_at),
                freight_amount=f"{freight:.2f}",
            ))  # fmt: skip

        if status != "cancelled":
            for line in lines:
                self._sell(store, line, ordered_at, order_id)
        if returned_at is not None:
            for line in lines:
                self._movement(store, line.product, "return", line.quantity, returned_at, order_id)


def generate_retail_dataset(config: GenerationConfig, sink: Sink) -> GenerationSummary:
    """Generate every retail source dataset into ``sink``; same config => same output."""
    counting = _CountingSink(sink)
    injector = AnomalyInjector(config.seed, config.anomalies, counting)
    master = build_master_data(config, injector.emit)
    _Transactions(config, master, injector.emit).run()
    return counting.summary()
