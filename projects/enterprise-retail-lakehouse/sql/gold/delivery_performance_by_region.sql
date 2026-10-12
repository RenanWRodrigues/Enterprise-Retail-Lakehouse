-- Delivery performance by customer region (at the time of the order).
-- KPIs: shipping_cost, delivery_days (docs/BUSINESS_RULES.md §2); shipments = count of
-- shipments in the shipping_cost scope.
-- Grain of the result: one row per region.
-- Joins: fact_shipments N:1 dim_customer (customer_sk).
WITH shipments_by_region AS (
    SELECT
        c.region,
        s.shipment_status,
        s.freight_amount,
        s.delivery_days
    FROM fact_shipments AS s
    INNER JOIN dim_customer AS c
        ON s.customer_sk = c.customer_sk  -- N:1
)

SELECT
    region,
    COUNT(CASE WHEN shipment_status IN ('in_transit', 'delivered') THEN 1 END) AS shipments,
    SUM(
        CASE WHEN shipment_status IN ('in_transit', 'delivered') THEN freight_amount ELSE 0 END
    ) AS shipping_cost,
    ROUND(
        AVG(CASE WHEN shipment_status IN ('delivered', 'returned') THEN delivery_days END), 2
    ) AS delivery_days
FROM shipments_by_region
GROUP BY region
ORDER BY delivery_days DESC;
