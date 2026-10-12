# Industrial IoT Lakehouse

> **Status:** gerador de fontes (CMMS + historian) com ground truth, contratos, suíte de qualidade,
> observabilidade, **Bronze** ([docs/BRONZE.md](docs/BRONZE.md)), **Silver** ([docs/SILVER.md](docs/SILVER.md))
> e **Gold** ([docs/GOLD.md](docs/GOLD.md): MTBF, MTTR, disponibilidade) implementados. Resumo:
> [PORTFOLIO_SUMMARY.md](../../docs/PORTFOLIO_SUMMARY.md#02--industrial-iot-lakehouse).

## Problem

Uma planta industrial monitora equipamentos com sensores (temperatura, vibração,
pressão). As leituras chegam continuamente, às vezes **atrasadas, duplicadas ou fora
de ordem**, e com valores fisicamente impossíveis. A operação precisa de indicadores
confiáveis por equipamento e janela de tempo, além de alertas de anomalia.

Competências demonstradas: ingestão incremental, deduplicação de eventos,
watermarks para eventos tardios, agregação temporal, freshness como regra de qualidade.

## Architecture

```
src/iot_lakehouse/
├── ingestion/   leitura incremental de telemetria (schema explícito)
├── bronze/      metadados técnicos + escrita append idempotente
├── silver/      deduplicação por evento, validação física, quarentena
├── gold/        agregados por janela, disponibilidade, anomalias
├── quality/     regras de qualidade do domínio IoT
└── utils/       helpers do projeto (sem regra de negócio)
notebooks/{bronze,silver,gold}/   orquestração fina (formato fonte Databricks)
config/                           configuração por ambiente
sql/                              DDL e consultas Gold
```

Visão geral do monorepo: [`docs/ARCHITECTURE.md`](../../docs/ARCHITECTURE.md).

## Data

**Gerado** (determinístico, Spark para as leituras): CMMS (`equipment`, `sensors`,
`failure_events`, `maintenance_orders`) + historian (`sensor_readings`, ~32 milhões por padrão),
com ruído, spikes, drift, degradação antes de falhas, períodos offline, duplicatas, eventos fora de
ordem e atrasados e leituras impossíveis — e um **ground truth** separado ligando cada anomalia à
falha conhecida.

| Documento | Conteúdo |
|---|---|
| [docs/DATA_CONTRACTS.md](docs/DATA_CONTRACTS.md) | schemas, relações, limites físicos, perfis, modos de falha |
| [docs/DATA_GENERATION.md](docs/DATA_GENERATION.md) | modelo do sinal, anomalias, ground truth, casos de teste |

## Medallion Layers

| Camada | Tabelas planejadas | Observação |
|---|---|---|
| Bronze | `bronze_<dataset>` para as 5 fontes + `bronze_files` (decisão por arquivo) | ✅ por arquivo, idempotente, quarentena de arquivos ([docs/BRONZE.md](docs/BRONZE.md)) |
| Silver | `silver_sensor_readings`: uma linha por `reading_id` (primeira chegada), unidades canônicas, atraso e fora de ordem marcados (nunca descartados), quarentena com motivos, reconciliação | ✅ ([docs/SILVER.md](docs/SILVER.md)) |
| Gold | `dim_equipment`, `dim_date`, `fact_equipment_failures`, `fact_maintenance`, `agg_equipment_{hourly,daily,reliability}` | ✅ fórmulas documentadas e testadas contra cálculo manual ([docs/GOLD.md](docs/GOLD.md)) |

## Testing

`tests/{unit,transformation,data_quality,integration}/` — estratégia em
[`docs/TESTING_STRATEGY.md`](../../docs/TESTING_STRATEGY.md).

```bash
.venv/Scripts/python -m pytest projects/industrial-iot-lakehouse
```

## Status

| Item | Estado |
|---|---|
| Estrutura de diretórios | ✅ |
| Contratos, limites físicos, modos de falha | ✅ |
| Gerador (Python + Spark) e ground truth | ✅ testado em escala mínima; volume padrão pendente de execução em Linux/Databricks |
| Bronze | ✅ |
| Silver | ✅ |
| Gold | ✅ |
| Detecção e avaliação | ✅ baseline explicável avaliado contra o ground truth ([docs/iot/evaluation](../../docs/iot/evaluation/METHODOLOGY.md)) |

## Roadmap

1. Simulador de telemetria determinístico.
2. Bronze: ingestão incremental (mecanismo confirmado no workspace — ver skill `databricks`).
3. Silver: deduplicação, eventos tardios, validação física, quarentena.
4. Gold: agregados por janela, disponibilidade, alertas.
5. Testes de integração com lotes fora de ordem.

Depende da Fase 1 (núcleo compartilhado) — ver [`docs/ROADMAP.md`](../../docs/ROADMAP.md).
