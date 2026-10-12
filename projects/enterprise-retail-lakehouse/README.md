# Enterprise Retail Lakehouse

> **Status:** raw → Bronze → Silver → Gold implementado e testado de ponta a ponta com Delta
> (Linux, 2026-10-07). Ainda não executado no Databricks. Resumo:
> [PORTFOLIO_SUMMARY.md](../../docs/PORTFOLIO_SUMMARY.md#01--enterprise-retail-lakehouse).

## Problem

Uma rede varejista precisa de uma visão confiável e histórica de vendas, clientes,
produtos e lojas para responder: qual a receita por loja/dia/categoria? qual o ticket
médio? quais clientes estão retornando? Os dados de origem chegam com duplicatas,
formatos inconsistentes e atributos de cliente/produto que mudam ao longo do tempo.

Competências demonstradas: modelagem dimensional, SCD Type 2, MERGE idempotente,
qualidade de dados com quarentena, reconciliação contra double counting.

## Architecture

```
src/retail_lakehouse/
├── synthetic/   gerador determinístico dos sistemas de origem (landing zone)
├── ingestion/   contratos das fontes (+ leitura da landing zone na Bronze)
├── bronze/      metadados técnicos + escrita append idempotente
├── silver/      padronização, deduplicação, SCD2, quarentena
├── gold/        fatos, dimensões, agregados
├── quality/     checks de qualidade das tabelas do varejo (usa lakehouse_common.quality)
└── utils/       helpers do projeto (sem regra de negócio)
notebooks/{bronze,silver,gold}/   orquestração fina (formato fonte Databricks)
config/                           configuração por ambiente
sql/                              DDL e consultas Gold
```

Visão geral do monorepo: [`docs/ARCHITECTURE.md`](../../docs/ARCHITECTURE.md).

## Data

Dados **sintéticos e determinísticos** (gerador próprio, sem Faker) de cinco sistemas de
origem — ERP, CRM, e-commerce, gateway de pagamento e logística — em 11 datasets:
~50 mil clientes, 2 mil produtos, 30 lojas, 100 mil pedidos e ~325 mil itens em 24 meses,
com sazonalidade brasileira e anomalias intencionais configuráveis e registradas.
Nenhum dado real de pessoa física.

| Documento | Conteúdo |
|---|---|
| [docs/SOURCE_SYSTEMS.md](docs/SOURCE_SYSTEMS.md) | sistemas, landing zone, convenções de serialização |
| [docs/DATA_CONTRACTS.md](docs/DATA_CONTRACTS.md) | schemas, business keys, relações (ERD), volumes |
| [docs/DATA_GENERATION.md](docs/DATA_GENERATION.md) | como gerar, determinismo, sazonalidade, anomalias, resultados |

## Medallion Layers

| Camada | Tabelas planejadas | Observação |
|---|---|---|
| Bronze | ✅ `bronze_<dataset>` para os 11 datasets ([docs/BRONZE.md](docs/BRONZE.md)) | texto como na fonte + `_ingestion_timestamp`, `_source_file`, `_batch_id`, `_ingestion_date` |
| Silver | ✅ `silver_<entidade>` para as 11 entidades (clientes por versão, entrada da SCD2) + `silver_<entidade>_quarantine` ([docs/SILVER.md](docs/SILVER.md)) | tipado pelo contrato da fonte, duplicatas e conflitos tratados, quarentena com motivos, reconciliação por lote, incremental por tabela de controle |
| Gold | ✅ `dim_customer` (SCD2), `dim_product`, `dim_store`, `dim_supplier`, `dim_date`; `fact_sales`, `fact_payments`, `fact_shipments`, `fact_inventory_movements` | grains em [docs/DIMENSIONAL_MODEL.md](docs/DIMENSIONAL_MODEL.md); KPIs em [docs/BUSINESS_RULES.md](docs/BUSINESS_RULES.md); colunas em [docs/DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md) |

## Testing

`tests/{unit,transformation,data_quality,integration}/` — estratégia em
[`docs/TESTING_STRATEGY.md`](../../docs/TESTING_STRATEGY.md).

```bash
.venv/Scripts/python -m pytest projects/enterprise-retail-lakehouse
```

## Status

| Item | Estado |
|---|---|
| Estrutura de diretórios | ✅ |
| Framework de qualidade (`lakehouse_common.quality`) | ✅ disponível; checks do Retail na Fase 2 |
| Contratos de fonte + gerador de dados | ✅ testes por categoria em [PORTFOLIO_SUMMARY.md](../../docs/PORTFOLIO_SUMMARY.md#01--enterprise-retail-lakehouse) |
| Bronze | ✅ implementada; testes Delta executados no Linux em 2026-10-07 ([docs/BRONZE.md](docs/BRONZE.md)) |
| Silver | ✅ implementada ([docs/SILVER.md](docs/SILVER.md)); quarentena e duplicatas conferidas contra o registro de anomalias do gerador |
| Gold | ✅ modelo, KPIs e SQL analítico; construída a partir das tabelas Silver no teste de integração |
| Testes de pipeline | ✅ raw → Bronze → Silver → Gold com Delta, reexecução idempotente e segundo lote (`tests/integration/test_silver_build.py`) |

## Roadmap

1. ✅ Contratos de fonte e gerador de dados sintéticos determinístico.
2. ✅ Bronze: ingestão com metadados.
3. ✅ Silver: padronização, deduplicação, quarentena, versões de cliente (SCD2).
4. ✅ Gold: modelo dimensional + agregados, reconciliação com Silver.
5. ✅ Testes de integração (reexecução idempotente, lote incremental).
6. Gate de qualidade em tempo de execução na Gold; execução no Databricks.

Depende da Fase 1 (núcleo compartilhado) — ver [`docs/ROADMAP.md`](../../docs/ROADMAP.md).
