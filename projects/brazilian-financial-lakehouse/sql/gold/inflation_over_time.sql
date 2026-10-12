-- Inflação ao longo do tempo: IPCA e IGP-M acumulados por ano-calendário.
-- Grain do resultado: um ano (reference_year) de gold_inflation.
-- Fonte: gold_inflation (sem joins).
-- O acumulado do ano só é exibido com os 12 meses publicados (anos em curso ficam NULL);
-- acumulado = (produto de (1 + taxa mensal / 100) - 1) * 100, via EXP(SUM(LN(...))).
--
-- Leitura: série descritiva. Diferenças entre IPCA e IGP-M refletem cestas e ponderações
-- distintas (o IGP-M pesa preços no atacado), não uma relação de causalidade entre os índices.
WITH monthly AS (
    SELECT
        YEAR(reference_month) AS reference_year,
        ipca_pct,
        igpm_pct,
        inflation_12m
    FROM gold_inflation
),

yearly AS (
    SELECT
        reference_year,
        COUNT(ipca_pct) AS ipca_months,
        COUNT(igpm_pct) AS igpm_months,
        (EXP(SUM(LN(1 + ipca_pct / 100))) - 1) * 100 AS ipca_year_pct,
        (EXP(SUM(LN(1 + igpm_pct / 100))) - 1) * 100 AS igpm_year_pct,
        MAX(ipca_pct) AS ipca_highest_month_pct,
        MIN(ipca_pct) AS ipca_lowest_month_pct,
        COUNT(CASE WHEN ipca_pct < 0 THEN 1 END) AS ipca_deflation_months,
        MAX(inflation_12m) AS ipca_12m_peak
    FROM monthly
    GROUP BY reference_year
)

SELECT
    reference_year,
    ipca_months,
    CASE WHEN ipca_months = 12 THEN ROUND(ipca_year_pct, 4) END AS ipca_year_pct,
    CASE WHEN igpm_months = 12 THEN ROUND(igpm_year_pct, 4) END AS igpm_year_pct,
    ipca_highest_month_pct,
    ipca_lowest_month_pct,
    ipca_deflation_months,
    ipca_12m_peak
FROM yearly
ORDER BY reference_year;
