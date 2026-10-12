# Test Matrix

Testes existentes por área e categoria (coletados em 2026-10-08) e testes previstos para cada item
do [IMPROVEMENT_PLAN.md](IMPROVEMENT_PLAN.md). Categorias e comandos:
[TESTING_STRATEGY.md](../TESTING_STRATEGY.md).

## 1. Testes existentes

| Área | unit | transformation | data_quality | contract | integration | Total |
|---|---:|---:|---:|---:|---:|---:|
| Retail | 130 | 63 | 63 | 174 | 56 | 486 |
| Financial | 165 | 81 | 8 | 0 | 3 | 257 |
| IoT | 85 | 10 | 11 | 0 | 4 | 110 |
| shared | 86 | 9 | 40 | 5 | 0 | 140 |
| `tests/` (governança da raiz) | 3 | 0 | 0 | 216 | 0 | 219 |
| **Total** | **469** | **163** | **122** | **395** | **63** | **1.212** |

Onde cada categoria roda:

| Categoria | Windows | Linux / CI |
|---|---|---|
| unit | ✅ completo | ✅ |
| transformation | ✅ por área (inteiro passa de 590 s); 18 skips da Gold do Retail | ✅ |
| data_quality | ✅; 11 skips da Gold do Retail | ✅ |
| contract | ✅; 54 skips da Gold do Retail | ✅ |
| integration | ❌ exige Delta/Hadoop | ✅ por projeto |

Não há testes end-to-end contra o Databricks nem contra as APIs reais na suíte: as execuções reais
do Financial (2026-10-06) e a validação contra séries oficiais estão registradas em
[TEST_REPORT.md](../TEST_REPORT.md) e usam fixtures capturadas.

### Cobertura por fluxo (existente)

| Fluxo | Retail | Financial | IoT |
|---|---|---|---|
| Geração / fonte | unit + transformation (gerador, anomalias, sinks) | unit (clientes HTTP com transporte falso: timeout, retry, `Retry-After`, gzip) | unit + transformation (física, leituras, writer) |
| Bronze | integration (39) | unit + integration (versões, watermark, rerun, revisão) | — |
| Silver | transformation (29) + integration (15) | transformation + integration | — |
| Gold | transformation + data_quality (reconciliação) | transformation (validação oficial) + integration | — |
| Qualidade | data_quality | data_quality | data_quality (suíte declarada) |
| Observabilidade | integration (`pipeline_runs` por etapa) | integration | unit (geração) |
| Governança | contract (raiz + projeto) | contract (raiz) | contract (raiz) |

## 2. Testes previstos por item

Cada linha começa RED (reproduz o problema) antes da correção.

| Item | Teste | Categoria | Arquivo previsto | Valida |
|---|---|---|---|---|
| M3 | fato com data após `calendar_end` recebe `date_key = -1` | transformation | `retail/tests/transformation/test_gold_facts.py` (novo) | mapeamento |
| M3 | total de `monthly_sales.sql` = total de `kpi_summary.sql` com fato fora do calendário | data_quality | `retail/tests/data_quality/test_gold_reconciliation.py` | regressão da divergência |
| M2 | Gold com linha duplicada no grain falha antes de gravar; run `FAILED` | data_quality | `retail/tests/data_quality/test_retail_gold_suite.py` | gate CRITICAL |
| M2 | fato com FK sem dimensão falha (CRITICAL) | data_quality | idem | integridade referencial |
| M2 | Gold válida grava as mesmas tabelas de hoje | integration | `retail/tests/integration/test_silver_build.py` | não regressão |
| DUP | um único nome de tabela por camada no Financial | contract | `financial/tests/contract/test_table_names.py` (novo diretório) | fonte única |
| M1 | lote com boletins PTAX intradiários e revisões reconcilia `bronze = válidas + quarentena + duplicatas + excluídas` | transformation | `financial/tests/transformation/test_silver_standardize.py` (novo) | contagens |
| M1 | linha removida de propósito faz `reconcile()` levantar erro | unit | `financial/tests/unit/test_silver_counts.py` | detecção |
| M6 | reexecução sem dados novos termina `SUCCEEDED` sem alerta | integration | `financial/tests/integration/test_fin_silver_gold_delta.py` | fim do alerta repetido |
| M6 | linha gravada no Bronze durante a Silver é processada na execução seguinte | integration | idem | regressão do bug original da watermark |
| IoT 4.1 | landing → Bronze idempotente, lote repetido pulado, metadados técnicos | integration | `iot/tests/integration/test_bronze_ingest.py` | Bronze |
| IoT 4.2 | dedup por `reading_id`; tardio aceito; impossível/unidade/timestamp em quarentena com motivo | transformation | `iot/tests/transformation/test_silver_readings.py` | Silver |
| IoT 4.3 | MTBF, MTTR, disponibilidade em casos calculados à mão (inclui janela sem falha e falha atravessando janela) | transformation | `iot/tests/transformation/test_gold_metrics.py` | fórmulas |
| IoT 4.4 | anomalias e falhas detectadas × ground truth do gerador | data_quality | `iot/tests/data_quality/test_ground_truth.py` | validação |
| L2 | CLI `synthetic` gera landing mínima em diretório temporário | unit/transformation | `retail/tests/unit/test_synthetic_cli.py`, `iot/tests/unit/test_synthetic_cli.py` | CLIs |
| M5 | lock presente e coerente com `pyproject.toml` | contract | `tests/contract/test_pinned_dependencies.py` | reprodutibilidade |
| M4 | nenhuma (refatoração sem mudança de comportamento: os testes existentes são a rede) | — | — | — |

## 3. Comandos de verificação por etapa

```bash
# Etapa 1 (Retail) — Windows parcial; Gold e integration no Linux
.venv/Scripts/python -m pytest projects/enterprise-retail-lakehouse -m "unit or data_quality"
make test-integration   # Linux/WSL

# Etapa 2 (Financial)
.venv/Scripts/python -m pytest projects/brazilian-financial-lakehouse -m "not integration"
make test-integration   # Linux/WSL

# Sempre, antes de concluir
.venv/Scripts/python -m ruff check . && .venv/Scripts/python -m ruff format --check . && .venv/Scripts/python -m mypy
.venv/Scripts/python -m pytest -m contract
```
