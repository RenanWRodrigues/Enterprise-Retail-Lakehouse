# Databricks Data Engineering Portfolio

Três Lakehouses construídos com **Python, PySpark, Spark SQL e Delta Lake** para o
**Databricks Free Edition**: varejo com modelagem dimensional, telemetria industrial e dados
macroeconômicos oficiais do Brasil. O código é testado, observável e reproduzível, e cada número
deste README tem evidência.

```
                    DATA SOURCES
                         │
                    INGESTION
                         │
                      BRONZE
                         │
                      SILVER
                         │
                       GOLD
                         │
              ┌──────────┴──────────┐
              │                     │
        Databricks SQL          Analytics
```

## Projetos

| # | Projeto | Problema | Destaque técnico | Estado |
|---|---|---|---|---|
| 01 | [Enterprise Retail Lakehouse](projects/enterprise-retail-lakehouse/README.md) | receita, ticket e entregas confiáveis a partir de 5 sistemas de origem | Silver com quarentena e versões de cliente, esquema estrela, SCD Type 2 com join point-in-time | ponta a ponta ✅ |
| 02 | [Industrial IoT Lakehouse](projects/industrial-iot-lakehouse/README.md) | detectar degradação de equipamentos em telemetria atrasada, duplicada e ruidosa | gerador determinístico em Spark sem UDF, ground truth de falhas | fontes e qualidade ✅ · camadas ⏳ |
| 03 | [Brazilian Financial Data Lakehouse](projects/brazilian-financial-lakehouse/README.md) | juros, inflação, câmbio e atividade a partir de APIs oficiais que revisam dados | ingestão incremental com versões, reamostragem validada contra séries oficiais | ponta a ponta ✅ |

Resumo de cada projeto (problema, arquitetura, pipeline, modelo, desafios, qualidade, testes e
resultados): **[docs/PORTFOLIO_SUMMARY.md](docs/PORTFOLIO_SUMMARY.md)**.

## Resultados comprovados

| Evidência | Resultado |
|---|---|
| Ingestão incremental contra as APIs oficiais (11 indicadores, 2026-10-06) | 1ª execução: 656 observações inseridas; 2ª: 206 recebidas, **2 inseridas** (zero duplicatas) |
| Gold financeira contra agregados publicados pelo BCB/IBGE | Selic do mês, dólar médio e de fim de mês, IPCA 12 meses: **24/24 meses idênticos**; Selic anualizada: **252/252 dias** |
| Landing do Retail gerada | **1.005.095 linhas** em 11 datasets, **4.318 anomalias** intencionais registradas |
| Silver do Retail contra o registro de anomalias do gerador | linhas em quarentena por motivo e duplicatas removidas **iguais** às anomalias injetadas; Silver da landing limpa **idêntica** ao contrato da Gold |
| Testes (Linux, código atual, 2026-10-07) | **1.212 passed, 0 failed** (unit 469, transformation 163, data quality + contract 517, integration 63) |
| Cobertura | **≥ 95%** (piso: 85%); medida sem a integração da Silver do Retail, que rodou à parte |

Origem e escopo de cada número: [PORTFOLIO_SUMMARY.md](docs/PORTFOLIO_SUMMARY.md),
[FINAL_AUDIT.md](docs/FINAL_AUDIT.md) e [TEST_REPORT.md](docs/TEST_REPORT.md).

## Arquitetura

```mermaid
flowchart LR
    subgraph Fontes
        R1[ERP · CRM · e-commerce<br/>pagamentos · logística]
        I1[CMMS · historian]
        F1[BCB SGS · PTAX · IBGE]
    end
    R1 & I1 & F1 --> B[(Bronze<br/>dado como chegou<br/>+ metadados)]
    B --> S[(Silver<br/>tipado, deduplicado,<br/>quarentena)]
    S --> G[(Gold<br/>fatos, dimensões,<br/>agregados)]
    G --> SQL[Databricks SQL]
    G --> AN[Analytics]
    Q{{qualidade de dados}} -.gate.-> S
    Q -.gate.-> G
    O[[pipeline_runs<br/>data_quality_results]] -.observa.-> B & S & G
```

- **Medallion com fronteiras explícitas.** O Bronze não aplica regra de negócio, a Silver valida
  e põe em quarentena, a Gold tem grain documentado.
- **Idempotência.** MERGE por chave, lote já ingerido é pulado, reconstruções determinísticas.
- **Lógica fora dos notebooks.** Transformações são funções puras `DataFrame -> DataFrame`; os
  notebooks só orquestram.
- **Biblioteca comum** ([`shared/`](shared/README.md)) só com código usado por 2+ projetos:
  qualidade e observabilidade.

## Competências

| Competência | Onde ver |
|---|---|
| Python | clientes HTTP com retry e backoff, geradores determinísticos, mypy strict |
| PySpark | transformações puras, janelas, joins com intervalos, geração em escala sem UDF |
| Spark SQL | consultas Gold com grain e cardinalidade documentados ([Retail](projects/enterprise-retail-lakehouse/sql/gold), [Financial](projects/brazilian-financial-lakehouse/sql/gold)) |
| Delta Lake | MERGE insert-only e upsert, tabelas append-only, histórico |
| Medallion Architecture | Bronze → Silver → Gold nos três projetos |
| Dimensional Modeling | esquema estrela do Retail, SCD Type 2, surrogate keys determinísticas |
| Incremental Processing | checkpoint com lookback, versões de observação, watermark da Silver |
| Data Quality | framework comum PASS/WARNING/ERROR/CRITICAL, quarentena com motivo ([DATA_QUALITY.md](docs/DATA_QUALITY.md)) |
| Testing | 1.212 testes em 5 categorias, CI em cadeia ([TESTING_STRATEGY.md](docs/TESTING_STRATEGY.md), [TEST_REPORT.md](docs/TEST_REPORT.md)) |
| Observability | `pipeline_runs` / `data_quality_results` nos três projetos ([OBSERVABILITY.md](docs/OBSERVABILITY.md)) |
| APIs | BCB SGS, PTAX (OData) e IBGE: timeout, retry, paginação, gzip, falha controlada |
| Time-series Data | frequências diária a trimestral, reamostragem, eventos tardios e fora de ordem (IoT) |

## Como executar

Requisitos: Python 3.12+, Java 17/21 e GNU make. Sem make, os comandos equivalentes estão em
[TESTING_STRATEGY.md §13](docs/TESTING_STRATEGY.md).

```bash
make install            # .venv + dependências de desenvolvimento
make lint               # ruff check, ruff format --check, mypy strict
make test-unit          # Python puro, sem Spark, sem rede
make test               # todos os testes, exceto integration
make test-integration   # Spark + Delta de ponta a ponta (Linux/Docker)
make coverage           # cobertura com piso de 85%
```

A CI (GitHub Actions, sem segredos) roda os mesmos alvos em cadeia: lint → unit →
transformation → data quality → coverage, com integration em job separado.

## Documentação

| Documento | Conteúdo |
|---|---|
| [PORTFOLIO_SUMMARY.md](docs/PORTFOLIO_SUMMARY.md) | os três projetos em detalhe |
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) · [ADR/](docs/ADR/) | layout, convenções, registro de decisões |
| [DATA_QUALITY.md](docs/DATA_QUALITY.md) · [OBSERVABILITY.md](docs/OBSERVABILITY.md) | regras, severidades, métricas de execução |
| [TESTING_STRATEGY.md](docs/TESTING_STRATEGY.md) | categorias de teste, CI, comandos |
| [SECURITY.md](docs/SECURITY.md) · [SECURITY_AUDIT.md](docs/SECURITY_AUDIT.md) | política e auditoria |
| [ROADMAP.md](docs/ROADMAP.md) | fases e próximos passos |
| [CLAUDE.md](CLAUDE.md) | regras de engenharia do repositório |
