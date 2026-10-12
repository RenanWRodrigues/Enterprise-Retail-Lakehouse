-- Atividade econômica: PIB (variação contra o mesmo trimestre do ano anterior) e crédito.
-- Grain do resultado: um trimestre (reference_quarter) de gold_economic_activity.
-- Joins: gold_economic_activity 1:1 gold_inflation no último mês do trimestre
-- (reference_month = reference_quarter + 2 meses), para deflacionar o crédito.
-- credit_total_real_yoy_pct_approx = ((1 + crédito a/a / 100) / (1 + IPCA 12m / 100) - 1) * 100
-- é uma aproximação: o saldo de crédito é nominal e o IPCA mede preços ao consumidor.
--
-- Leitura: correlação NÃO implica causalidade. Crédito e PIB se movem juntos porque um alimenta
-- o outro e ambos respondem a juros, renda e confiança; esta consulta só descreve.
SELECT a.reference_quarter,
    a.quarter,
    a.gdp_growth_yoy_pct,
    a.gdp_growth_yoy_moving_average_4q,
    a.credit_total_yoy_pct,
    i.inflation_12m,
    ROUND(
        (
            (1 + a.credit_total_yoy_pct / 100) / (1 + i.inflation_12m / 100) - 1
        ) * 100,
        4
    ) AS credit_total_real_yoy_pct_approx,
    a.monetary_base_yoy_pct
FROM gold_economic_activity AS a
    LEFT JOIN gold_inflation AS i ON i.reference_month = ADD_MONTHS(a.reference_quarter, 2) -- 1:1 (one month per quarter)
ORDER BY a.reference_quarter;
