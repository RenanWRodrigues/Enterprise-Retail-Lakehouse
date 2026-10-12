-- Units on hand by store at the end of the period.
-- Definition (docs/BUSINESS_RULES.md §3, R3): balance = SUM of signed movement quantities.
-- Balances are semi-additive: they may be summed across stores/products, NEVER across time.
-- Grain of the result: one row per store.
-- Joins: fact_inventory_movements N:1 dim_store (store_sk).
WITH balance_by_store AS (
    SELECT
        m.store_sk,
        SUM(m.quantity) AS units_on_hand,
        COUNT(DISTINCT m.product_sk) AS products_carried
    FROM fact_inventory_movements AS m
    GROUP BY m.store_sk
)

SELECT
    st.store_id,
    st.store_name,
    st.store_type,
    b.units_on_hand,
    b.products_carried
FROM balance_by_store AS b
INNER JOIN dim_store AS st
    ON b.store_sk = st.store_sk  -- N:1
ORDER BY b.units_on_hand DESC;
