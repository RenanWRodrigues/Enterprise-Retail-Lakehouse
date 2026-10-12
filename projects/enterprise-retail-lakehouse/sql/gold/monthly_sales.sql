-- Monthly sales KPIs (gross_revenue, discount_amount, net_revenue, quantity_sold,
-- average_order_value) — definitions in docs/BUSINESS_RULES.md §2.
-- Grain of the result: one row per (period, date_classification):
--   * one row per calendar month (period = 'yyyy-MM', date_classification = 'IN_CALENDAR');
--   * at most two rows with period = 'UNKNOWN_DATE' (year and month NULL), never mixed into a
--     real month: 'OUT_OF_CALENDAR' (order date known but outside dim_date; it stays in
--     fact_sales.order_date) and 'UNKNOWN_DATE' (no order date, e.g. the order was quarantined).
-- Every sale of fact_sales is counted exactly once: the totals equal kpi_summary.
-- Joins: fact_sales N:1 dim_date (date_key). LEFT: a line is never dropped by the join (the
-- publication gate already guarantees every date_key exists; -1 is the unknown member).
WITH lines_by_period AS (
    SELECT
        d.year,
        d.month,
        COALESCE(d.year_month, 'UNKNOWN_DATE') AS period,
        CASE
            WHEN f.date_key <> -1 AND d.date_key IS NOT NULL THEN 'IN_CALENDAR'
            WHEN f.order_date IS NOT NULL THEN 'OUT_OF_CALENDAR'
            ELSE 'UNKNOWN_DATE'
        END AS date_classification,
        f.order_id,
        f.order_status,
        f.quantity,
        f.gross_amount,
        f.discount_amount,
        f.net_amount
    FROM fact_sales AS f
    LEFT JOIN dim_date AS d
        ON f.date_key = d.date_key  -- N:1
),

period_totals AS (
    SELECT
        year,
        month,
        period,
        date_classification,
        SUM(CASE WHEN order_status <> 'cancelled' THEN gross_amount ELSE 0 END) AS gross_revenue,
        SUM(CASE WHEN order_status <> 'cancelled' THEN discount_amount ELSE 0 END) AS discount_amount,
        SUM(
            CASE WHEN order_status NOT IN ('cancelled', 'returned') THEN net_amount ELSE 0 END
        ) AS net_revenue,
        SUM(
            CASE WHEN order_status NOT IN ('cancelled', 'returned') THEN quantity ELSE 0 END
        ) AS quantity_sold,
        COUNT(
            DISTINCT CASE WHEN order_status NOT IN ('cancelled', 'returned') THEN order_id END
        ) AS orders
    FROM lines_by_period
    GROUP BY year, month, period, date_classification
)

SELECT
    period,
    date_classification,
    year,
    month,
    gross_revenue,
    discount_amount,
    net_revenue,
    quantity_sold,
    orders,
    ROUND(net_revenue / NULLIF(orders, 0), 2) AS average_order_value
FROM period_totals
ORDER BY date_classification = 'IN_CALENDAR' DESC, period, date_classification;
