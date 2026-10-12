# Brazilian Financial Data Lakehouse

> **Status: Bronze, Silver e Gold implementados**; Gold validada contra séries oficiais do BCB/IBGE.
> Execução de Silver/Gold no Databricks pendente (ver Status). Resumo:
> [PORTFOLIO_SUMMARY.md](../../docs/PORTFOLIO_SUMMARY.md#03--brazilian-financial-data-lakehouse).

## Problem

Analistas precisam comparar indicadores macroeconômicos brasileiros (Selic, IPCA,
câmbio) com o desempenho de fundos de investimento. As fontes públicas têm formatos
distintos, séries que podem ser **revisadas**, convenções numéricas brasileiras
(`1.234,56`) e dependem do calendário de dias úteis.

Competências demonstradas: ingestão de APIs com retry/backoff/paginação, watermark
com lookback para revisões, calendário de dias úteis, medidas semi-aditivas e não aditivas.

## Architecture

```
src/financial_lakehouse/
├── clients/     clientes HTTP (BCB SGS, BCB PTAX, IBGE) — timeout, retry, backoff
├── ingestion/   pipeline incremental: catálogo → clientes → landing → Bronze, checkpoint, métricas
├── transformation/  payloads → observações em texto e registros Bronze (puro)
├── storage/     landing zone, manifest, métricas, checkpoints, sinks Bronze (Delta/local)
├── silver/      séries padronizadas: decimais exatos, datas por frequência, revisões, quarentena, MERGE
├── gold/        reamostragem mensal, métricas derivadas, tabelas temáticas e painel macro
├── quality/     regras de qualidade do domínio financeiro
└── utils/       helpers do projeto (sem regra de negócio)
notebooks/{bronze,silver,gold}/   orquestração fina (formato fonte Databricks)
config/                           configuração por ambiente
sql/gold/                         consultas analíticas (SELIC × inflação, câmbio, atividade, trabalho)
```

Visão geral do monorepo: [`docs/ARCHITECTURE.md`](../../docs/ARCHITECTURE.md).

## Data

**APIs públicas oficiais, sem credenciais** — 11 indicadores: Selic (meta e diária), IPCA, IGP-M,
PIB, USD/BRL, EUR/BRL (PTAX), reservas internacionais, base monetária, crédito total e desemprego.
Cada endpoint, parâmetro, schema, limitação e data de acesso está em
[docs/data_sources.md](docs/data_sources.md); a ingestão raw em [docs/RAW_INGESTION.md](docs/RAW_INGESTION.md); a carga incremental no Bronze em
[docs/INCREMENTAL_STRATEGY.md](docs/INCREMENTAL_STRATEGY.md); a padronização em [docs/SILVER.md](docs/SILVER.md);
as tabelas, regras de reamostragem e métricas em [docs/GOLD.md](docs/GOLD.md).

| Fonte | Instituição | Indicadores |
|---|---|---|
| SGS (`api.bcb.gov.br`) | Banco Central do Brasil | Selic, IGP-M, USD/BRL, reservas, base monetária, crédito |
| PTAX / Olinda | Banco Central do Brasil | EUR/BRL (todos os boletins) |
| API de agregados v3 | IBGE | IPCA, PIB, taxa de desocupação |

## Medallion Layers

| Camada | Planejado | Estado |
|---|---|---|
| Raw (landing) | respostas exatas das APIs + manifest + observações em texto | ✅ |
| Bronze | `bronze_indicator_observations`: incremental por checkpoint, MERGE insert-only de versões novas (inclusive A → B → A), revisões preservadas, `reprocess_from` para revisões tardias | ✅ |
| Silver | `silver_indicator_observations`: um valor DECIMAL por (indicador, data), revisões resolvidas, PTAX de fechamento, quarentena com motivos | ✅ |
| Gold | `gold_interest_rates`, `gold_exchange_rates`, `gold_inflation`, `gold_economic_activity`, `gold_labor_market`, `gold_brazil_macro_monthly` (ADR 041) | ✅ |

## Testing

`tests/{unit,transformation,data_quality,integration}/` — estratégia em
[`docs/TESTING_STRATEGY.md`](../../docs/TESTING_STRATEGY.md). Testes nunca acessam a
rede: clientes HTTP são substituídos por fakes.

```bash
.venv/Scripts/python -m pytest projects/brazilian-financial-lakehouse
```

## Status

| Item | Estado |
|---|---|
| Estrutura de diretórios | ✅ |
| Fontes oficiais pesquisadas e documentadas | ✅ |
| Clientes HTTP + ingestão raw (landing, manifest, watermarks) | ✅ execução real em 2026-10-06 com os 11 indicadores |
| Bronze incremental (checkpoint, idempotência, métricas, falha controlada) | ✅ testes com transporte HTTP falso; duas execuções reais em 2026-10-06 (2ª: 206 recebidos, 2 inseridos); MERGE Delta executado nos testes de integração no Linux (2026-10-07) |
| Silver (padronização, revisões, quarentena, gate de qualidade, MERGE incremental) | ✅ testes Spark locais; MERGE Delta exercitado no Linux (2026-10-07), com a expectativa de alerta da reexecução ajustada depois ([TEST_REPORT.md](../../docs/TEST_REPORT.md)) |
| Gold (6 tabelas, reamostragem, métricas, juro real, 6 consultas SQL) | ✅ testes contra oráculos Python e contra agregados oficiais (24/24 meses e 252/252 dias idênticos, [GOLD.md §7](docs/GOLD.md)) |

## Roadmap

1. ✅ Pesquisa das fontes oficiais e ingestão raw (clientes HTTP, landing, watermarks).
2. Confirmar acesso de saída à internet no Databricks Free Edition (ou pousar via Volume).
3. ✅ Bronze incremental com checkpoint lógico e MERGE idempotente.
4. ✅ Silver: tipagem, datas por frequência, revisões via `MERGE`, boletim PTAX de fechamento.
5. ✅ Gold: tabelas temáticas, painel mensal, métricas derivadas e consultas analíticas.
6. ✅ Validar a Gold contra séries oficiais acumuladas (IBGE 2265, SGS 4390, 3698, 3696 e 1178).
7. Calendário de dias úteis (lacunas em séries diárias).
