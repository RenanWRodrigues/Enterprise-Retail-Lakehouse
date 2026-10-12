-- Payment mix: approved amount by payment method and its share of all approved payments.
-- Definitions (docs/BUSINESS_RULES.md §1): approved amount = SUM(amount) of 'approved'
-- events; refunds and declined attempts are reported separately, never netted here.
-- Grain of the result: one row per payment method.
-- Joins: none (fact_payments only, at its own grain).
WITH method_totals AS (
    SELECT
        payment_method,
        SUM(CASE WHEN payment_status = 'approved' THEN amount ELSE 0 END) AS approved_amount,
        COUNT(CASE WHEN payment_status = 'approved' THEN 1 END) AS approved_payments,
        COUNT(CASE WHEN payment_status = 'declined' THEN 1 END) AS declined_attempts,
        SUM(CASE WHEN payment_status = 'refunded' THEN amount ELSE 0 END) AS refunded_amount,
        AVG(CASE WHEN payment_status = 'approved' THEN installments END) AS avg_installments
    FROM fact_payments
    GROUP BY payment_method
)

SELECT
    payment_method,
    approved_amount,
    approved_payments,
    declined_attempts,
    refunded_amount,
    ROUND(avg_installments, 2) AS avg_installments,
    ROUND(100 * approved_amount / SUM(approved_amount) OVER (), 2) AS approved_share_pct
FROM method_totals
ORDER BY approved_amount DESC;
