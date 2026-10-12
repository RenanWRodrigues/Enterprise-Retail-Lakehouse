-- Câmbio ao longo do tempo: USD/BRL e EUR/BRL (PTAX de fechamento) por ano-calendário.
-- Grain do resultado: um ano (reference_year) de gold_exchange_rates.
-- Fonte: gold_exchange_rates (sem joins). Variação anual = última cotação do ano contra a última
-- do ano anterior; volatilidade = desvio-padrão das variações diárias (%).
--
-- Leitura: série descritiva. Movimentos do câmbio têm muitas causas simultâneas (juros, risco,
-- dólar global, commodities); esta consulta não identifica causalidade.
WITH yearly AS (
    SELECT
        YEAR(reference_date) AS reference_year,
        MAX_BY(usd_brl, CASE WHEN usd_brl IS NOT NULL THEN reference_date END) AS usd_brl_year_end,
        AVG(usd_brl) AS usd_brl_average,
        MIN(usd_brl) AS usd_brl_min,
        MAX(usd_brl) AS usd_brl_max,
        STDDEV_SAMP(usd_brl_change_pct) AS usd_brl_daily_volatility_pct,
        MAX_BY(eur_brl, CASE WHEN eur_brl IS NOT NULL THEN reference_date END) AS eur_brl_year_end,
        AVG(eur_brl) AS eur_brl_average
    FROM gold_exchange_rates
    GROUP BY YEAR(reference_date)
)

SELECT
    reference_year,
    usd_brl_year_end,
    ROUND(
        (usd_brl_year_end / LAG(usd_brl_year_end) OVER (ORDER BY reference_year) - 1) * 100, 4
    ) AS usd_brl_year_change_pct,
    ROUND(usd_brl_average, 4) AS usd_brl_average,
    usd_brl_min,
    usd_brl_max,
    ROUND(usd_brl_daily_volatility_pct, 4) AS usd_brl_daily_volatility_pct,
    eur_brl_year_end,
    ROUND(eur_brl_average, 4) AS eur_brl_average
FROM yearly
ORDER BY reference_year;
