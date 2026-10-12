-- Net revenue, orders and average order value by customer segment AT THE TIME OF THE ORDER.
-- KPIs: net_revenue, average_order_value (docs/BUSINESS_RULES.md §2).
-- Grain of the result: one row per segment.
-- Joins: fact_sales N:1 dim_customer by customer_sk (the SCD2 version valid at the order).
--        Never join by customer_id: customers with several versions would be double counted.
WITH kept_lines AS (
    SELECT
        c.segment,
        f.order_id,
        f.net_amount
    FROM fact_sales AS f
    INNER JOIN dim_customer AS c
        ON f.customer_sk = c.customer_sk  -- N:1
    WHERE f.order_status NOT IN ('cancelled', 'returned')
),

segment_totals AS (
    SELECT
        segment,
        SUM(net_amount) AS net_revenue,
        COUNT(DISTINCT order_id) AS orders
    FROM kept_lines
    GROUP BY segment
)

SELECT
    segment,
    net_revenue,
    orders,
    ROUND(net_revenue / NULLIF(orders, 0), 2) AS average_order_value
FROM segment_totals
ORDER BY net_revenue DESC;
