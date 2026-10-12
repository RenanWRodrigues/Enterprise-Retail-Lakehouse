-- SELIC × câmbio: variações mensais da meta Selic (p.p.) e do USD/BRL (%).
-- Grain do resultado: um mês completo (reference_month) de gold_brazil_macro_monthly.
-- Fonte: gold_brazil_macro_monthly (sem joins).
--
-- Leitura: correlação NÃO implica causalidade. O câmbio depende do diferencial de juros, mas
-- também do dólar global, do risco-país, dos termos de troca e do fluxo de capitais; e o câmbio
-- afeta a inflação, que por sua vez influencia a Selic (causalidade nos dois sentidos).
-- A correlação de 24 meses só é exibida com a janela completa e fica NULL quando uma das séries
-- é constante na janela (por exemplo, Selic sem mudança por 24 meses: correlação indefinida).
WITH complete_months AS (
    SELECT
        reference_month,
        selic_target_pct_year,
        selic_change,
        usd_brl,
        usd_brl_change,
        usd_brl_moving_average_12m,
        CASE WHEN usd_brl_change IS NOT NULL THEN selic_change END AS x_paired,
        CASE WHEN selic_change IS NOT NULL THEN usd_brl_change END AS y_paired
    FROM gold_brazil_macro_monthly
    WHERE NOT is_partial_month
),

rolling AS (
    SELECT
        reference_month,
        selic_target_pct_year,
        selic_change,
        usd_brl,
        usd_brl_change,
        usd_brl_moving_average_12m,
        -- Pearson over paired months, written out with TRY_DIVIDE: CORR raises a division by
        -- zero (ANSI mode) when one series is constant in the window, e.g. an unchanged Selic.
        TRY_DIVIDE(
            COVAR_POP(x_paired, y_paired) OVER last_24_months,
            STDDEV_POP(x_paired) OVER last_24_months * STDDEV_POP(y_paired) OVER last_24_months
        ) AS corr_24m,
        COUNT(x_paired) OVER last_24_months AS paired_months
    FROM complete_months
    WINDOW last_24_months AS (ORDER BY reference_month ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)
)

SELECT
    reference_month,
    selic_target_pct_year,
    selic_change,
    usd_brl,
    usd_brl_change,
    usd_brl_moving_average_12m,
    CASE WHEN paired_months = 24 THEN ROUND(corr_24m, 4) END AS selic_change_usd_change_corr_24m
FROM rolling
ORDER BY reference_month;
