-- SELIC × inflação: meta Selic, Selic acumulada e IPCA em 12 meses, juro real ex-post.
-- Grain do resultado: um mês completo (reference_month) de gold_brazil_macro_monthly.
-- Fonte: gold_brazil_macro_monthly (sem joins).
--
-- Leitura: correlação NÃO implica causalidade. A Selic reage à inflação (função de reação do
-- Copom) e a inflação reage à Selic com defasagens longas e variáveis; as duas respondem a
-- choques comuns (câmbio, commodities, política fiscal, expectativas). selic_inflation_corr_24m
-- mede apenas co-movimento linear em 24 meses, só é exibida com a janela completa e fica NULL
-- quando uma das séries é constante na janela (correlação indefinida);
-- inflation_12m_one_year_later é uma defasagem descritiva, não uma identificação de efeito.
WITH complete_months AS (
    SELECT
        reference_month,
        selic_target_pct_year,
        selic_12m_pct,
        inflation_12m,
        real_interest_rate,
        CASE WHEN inflation_12m IS NOT NULL THEN selic_target_pct_year END AS x_paired,
        CASE WHEN selic_target_pct_year IS NOT NULL THEN inflation_12m END AS y_paired
    FROM gold_brazil_macro_monthly
    WHERE NOT is_partial_month
),

rolling AS (
    SELECT
        reference_month,
        selic_target_pct_year,
        selic_12m_pct,
        inflation_12m,
        real_interest_rate,
        -- Pearson over paired months, written out with TRY_DIVIDE: CORR raises a division by
        -- zero (ANSI mode) when one series is constant in the window, e.g. an unchanged Selic.
        TRY_DIVIDE(
            COVAR_POP(x_paired, y_paired) OVER last_24_months,
            STDDEV_POP(x_paired) OVER last_24_months * STDDEV_POP(y_paired) OVER last_24_months
        ) AS corr_24m,
        COUNT(x_paired) OVER last_24_months AS paired_months,
        LEAD(inflation_12m, 12) OVER (ORDER BY reference_month) AS inflation_12m_one_year_later
    FROM complete_months
    WINDOW last_24_months AS (ORDER BY reference_month ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)
)

SELECT
    reference_month,
    selic_target_pct_year,
    selic_12m_pct,
    inflation_12m,
    real_interest_rate,
    selic_target_pct_year - inflation_12m AS selic_minus_inflation_pp,
    CASE WHEN paired_months = 24 THEN ROUND(corr_24m, 4) END AS selic_inflation_corr_24m,
    inflation_12m_one_year_later
FROM rolling
ORDER BY reference_month;
