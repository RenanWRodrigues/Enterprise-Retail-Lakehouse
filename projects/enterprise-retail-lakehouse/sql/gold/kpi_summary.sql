-- KPI summary: every KPI of docs/BUSINESS_RULES.md §2 in one row.
-- Grain of the result: one row (whole period).
-- Run in the Gold schema (e.g. USE retail.gold).
-- Joins: none between facts; the two single-row aggregates are combined 1:1 (CROSS JOIN).
WITH sales_totals AS (
    SELECT
        SUM(CASE WHEN order_status <> 'cancelled' THEN gross_amount ELSE 0 END) AS gross_revenue,
        SUM(CASE WHEN order_status <> 'cancelled' THEN discount_amount ELSE 0 END) AS discount_amount,
        SUM(CASE WHEN order_status = 'returned' THEN net_amount ELSE 0 END) AS returned_amount,
        SUM(
            CASE WHEN order_status NOT IN ('cancelled', 'returned') THEN net_amount ELSE 0 END
        ) AS net_revenue,
        SUM(
            CASE WHEN order_status NOT IN ('cancelled', 'returned') THEN quantity ELSE 0 END
        ) AS quantity_sold,
        COUNT(
            DISTINCT CASE WHEN order_status NOT IN ('cancelled', 'returned') THEN order_id END
        ) AS kept_orders
    FROM fact_sales
),

shipment_totals AS (
    SELECT
        SUM(
            CASE WHEN shipment_status IN ('in_transit', 'delivered') THEN freight_amount ELSE 0 END
        ) AS shipping_cost,
        ROUND(
            AVG(CASE WHEN shipment_status IN ('delivered', 'returned') THEN delivery_days END), 2
        ) AS delivery_days
    FROM fact_shipments
)

SELECT
    s.gross_revenue,
    s.discount_amount,
    s.returned_amount,
    s.net_revenue,
    s.quantity_sold,
    ROUND(s.net_revenue / NULLIF(s.kept_orders, 0), 2) AS average_order_value,
    sh.shipping_cost,
    sh.delivery_days
FROM sales_totals AS s
CROSS JOIN shipment_totals AS sh  -- 1:1 (single-row aggregates)
;
