"""R3 inventory reconciliation (BUSINESS_RULES.md §3): stock snapshot vs movement ledger.

Both inputs are contracted ERP datasets in Silver: ``inventory`` (one row per snapshot_date,
store_id, product_id) and ``inventory_movements`` (signed quantities). The snapshot is the stock
at the end of ``snapshot_date`` (Brasília business date), so it must equal the sum of the
movements up to that date. Natural keys are used, not Gold surrogate keys: a movement whose
product is unknown to Gold (sk -1) still belongs to a real (store, product) of the snapshot.

The frame is a gate input (``quality.suites.inventory_reconciliation_suite``), not a published
Gold table. docs/audits/inventory-quality-r3.md.
"""

from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from retail_lakehouse.gold.keys import business_date

SNAPSHOT_KEY: Final = ("snapshot_date", "store_id", "product_id")
_DAY: Final = "_movement_day"


def inventory_reconciliation(snapshot: DataFrame, movements: DataFrame) -> DataFrame:
    """One row per key present in the snapshot or in the ledger up to that snapshot date.

    Columns: ``snapshot_date``, ``store_id``, ``product_id``, ``quantity_on_hand`` (NULL: key
    absent from the snapshot), ``movement_balance`` (NULL: no movement up to the date) and
    ``difference = quantity_on_hand - movement_balance``, both absent sides counting as 0 (a key
    missing from the snapshot is not stocked; a key never moved has nothing on hand).
    Movements after ``snapshot_date`` (later batches) do not count. No snapshot: no rows.
    """
    dates = snapshot.select("snapshot_date").distinct()  # one row per snapshot taken
    ledger = (
        movements.select(
            "store_id",
            "product_id",
            "quantity",
            business_date(F.col("movement_timestamp")).alias(_DAY),
        )
        .join(dates, F.col(_DAY) <= F.col("snapshot_date"))  # each movement N:M snapshot dates
        .groupBy(*SNAPSHOT_KEY)
        .agg(F.sum("quantity").alias("movement_balance"))
    )
    on_hand = snapshot.select(*SNAPSHOT_KEY, "quantity_on_hand")
    joined = on_hand.join(ledger, list(SNAPSHOT_KEY), "full_outer")  # 1:1 on the key
    difference = F.coalesce(F.col("quantity_on_hand").cast("long"), F.lit(0)) - F.coalesce(
        F.col("movement_balance"), F.lit(0)
    )
    return joined.select(
        *SNAPSHOT_KEY, "quantity_on_hand", "movement_balance", difference.alias("difference")
    )
