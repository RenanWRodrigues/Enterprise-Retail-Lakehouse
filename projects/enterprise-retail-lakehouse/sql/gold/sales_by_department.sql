-- Net revenue and units by product department, with share of total net revenue.
-- KPIs: net_revenue, quantity_sold (docs/BUSINESS_RULES.md §2).
-- Grain of the result: one row per department.
-- Joins: fact_sales N:1 dim_product (product_sk).
WITH kept_lines AS (
    SELECT
        p.department_name,
        f.quantity,
        f.net_amount
    FROM fact_sales AS f
    INNER JOIN dim_product AS p
        ON f.product_sk = p.product_sk  -- N:1
    WHERE f.order_status NOT IN ('cancelled', 'returned')
),

department_totals AS (
    SELECT
        department_name,
        SUM(net_amount) AS net_revenue,
        SUM(quantity) AS quantity_sold
    FROM kept_lines
    GROUP BY department_name
)

SELECT
    department_name,
    net_revenue,
    quantity_sold,
    ROUND(100 * net_revenue / SUM(net_revenue) OVER (), 2) AS net_revenue_share_pct
FROM department_totals
ORDER BY net_revenue DESC;
