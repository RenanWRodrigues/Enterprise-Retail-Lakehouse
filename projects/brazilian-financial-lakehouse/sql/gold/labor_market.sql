-- Mercado de trabalho: taxa de desocupação da PNAD Contínua (trimestre móvel).
-- Grain do resultado: um trimestre móvel, identificado pelo seu último mês (reference_month),
-- de gold_labor_market. Fonte: gold_labor_market (sem joins).
-- Trimestres móveis consecutivos compartilham 2 meses: compare com 3 meses antes (trimestres sem
-- sobreposição) ou com 12 meses antes (mesmo trimestre do ano anterior, sem efeito sazonal).
--
-- Leitura: série descritiva. A desocupação reflete atividade, demografia e participação na força
-- de trabalho ao mesmo tempo; correlação com outras séries NÃO implica causalidade.
SELECT
    reference_month,
    period_start,
    period_end,
    unemployment_rate_pct,
    unemployment_change_3m_pp,
    unemployment_change_12m_pp,
    MIN(unemployment_rate_pct) OVER last_12_months AS lowest_rate_12m_pct,
    MAX(unemployment_rate_pct) OVER last_12_months AS highest_rate_12m_pct,
    CASE
        WHEN unemployment_change_12m_pp < 0 THEN 'down_vs_last_year'
        WHEN unemployment_change_12m_pp > 0 THEN 'up_vs_last_year'
        WHEN unemployment_change_12m_pp = 0 THEN 'flat_vs_last_year'
    END AS year_over_year_direction
FROM gold_labor_market
WINDOW last_12_months AS (ORDER BY reference_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)
ORDER BY reference_month;
