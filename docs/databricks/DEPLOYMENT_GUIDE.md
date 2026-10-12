# Deployment Guide — Databricks Free Edition (Retail e Financial)

Como preparar, executar e validar os smoke tests no workspace. Nada aqui foi executado no
Databricks ainda: o estado real está em [EXECUTION_REPORT.md](EXECUTION_REPORT.md). Capacidades e
limites: [COMPATIBILITY_MATRIX.md](COMPATIBILITY_MATRIX.md). Erros conhecidos:
[TROUBLESHOOTING.md](TROUBLESHOOTING.md).

## 1. Princípios

- **Isolamento.** O smoke test usa só o ambiente `smoke` dos arquivos de configuração: catálogo
  `workspace`, schemas `smoke_*`, Volumes `/Volumes/workspace/smoke_*`. O ambiente `databricks`
  (persistente: `retail.*`, `financial.*`) não é tocado. O contrato
  `tests/contract/test_databricks_readiness.py` garante que nenhum nome de `smoke` coincide com
  `databricks`.
- **Custo controlado.** Só compute serverless (o único do Free Edition). Amostras pequenas: Retail
  com ~300 pedidos (`config/data_generation_smoke.toml`); Financial a partir de 2025-01-01
  (`[smoke]` de `config/ingestion.toml`). Uma execução por etapa, sem agendamento.
- **Autorização.** Cada passo marcado **[remoto]** cria ou altera recursos no workspace e só roda
  com autorização explícita do responsável.
- **Credenciais.** A CLI usa o perfil local `~/.databrickscfg` (fora do repositório). Nenhum token
  entra em código, notebook, log ou documento ([SECURITY.md](../SECURITY.md)).

## 2. Pré-requisitos (locais)

| Item | Estado verificado em 2026-10-08 |
|---|---|
| Databricks CLI | v1.5.0 instalada |
| Autenticação | perfil `DEFAULT` em `~/.databrickscfg`, host em `cloud.databricks.com`; **não validado** (validar contata o workspace) |
| Variáveis `DATABRICKS_*` | não definidas |
| Dependências no workspace | nenhuma além do `pyspark` do runtime: o código só importa a biblioteca padrão e `pyspark` |
| Bundle / jobs declarativos | não existem; execução por notebooks |

Validar a autenticação **[remoto, só leitura]**: `databricks auth profiles` (mostra se o perfil é
válido, sem exibir o token).

## 3. Código no workspace

Os notebooks importam os pacotes direto do repositório (`sys.path` a partir de `Path.cwd()`), então
o repositório precisa estar num **Git folder** do workspace **[remoto]**:

1. *Workspace → Create → Git folder*, com a URL do repositório (depois do primeiro push).
2. Abrir os notebooks a partir do Git folder; o diretório de trabalho de um notebook em Git folder
   é o diretório dele (a verificar no preflight: o import do pacote falha com
   `ModuleNotFoundError` se não for).

## 4. Ordem de execução

| # | Notebook | O que faz | Remoto |
|---|---|---|---|
| 0 | `shared/notebooks/00_preflight.py` (`namespace = workspace.smoke_preflight`) | detecta runtime, catálogo, schema, Volume, Delta + CDF, saída HTTPS | cria o schema, o Volume `probe` e a tabela `_preflight_cdf` |
| 1 | SQL abaixo | cria schemas e Volumes do smoke | sim |
| 2 | `projects/enterprise-retail-lakehouse/notebooks/00_run_pipeline.py` (`environment = smoke`) | landing sintética no Volume → Bronze → Silver → Gold | grava `workspace.smoke_retail_*` |
| 3 | `projects/brazilian-financial-lakehouse/notebooks/00_run_pipeline.py` (`environment = smoke`, `source = api` ou `replay`) | APIs (ou replay) → Bronze → Silver → Gold | grava `workspace.smoke_financial_*` |
| 4 | consultas da seção 7 | validação | só leitura |

Se `dbutils.notebook.run` não estiver disponível, rodar as etapas à mão na mesma ordem, com os
mesmos parâmetros (`setup/00_generate_landing`, `bronze/01_ingest_bronze`,
`silver/01_build_silver`, `gold/01_build_gold`).

### SQL de preparação [remoto]

```sql
CREATE SCHEMA IF NOT EXISTS workspace.smoke_retail;            -- Volume da landing Retail
CREATE VOLUME IF NOT EXISTS workspace.smoke_retail.landing;
CREATE SCHEMA IF NOT EXISTS workspace.smoke_retail_bronze;
CREATE SCHEMA IF NOT EXISTS workspace.smoke_retail_silver;
CREATE SCHEMA IF NOT EXISTS workspace.smoke_retail_gold;
CREATE SCHEMA IF NOT EXISTS workspace.smoke_financial;         -- Volumes da landing e do replay
CREATE VOLUME IF NOT EXISTS workspace.smoke_financial.landing;
CREATE VOLUME IF NOT EXISTS workspace.smoke_financial.replay;
CREATE SCHEMA IF NOT EXISTS workspace.smoke_financial_bronze;
CREATE SCHEMA IF NOT EXISTS workspace.smoke_financial_silver;
CREATE SCHEMA IF NOT EXISTS workspace.smoke_financial_gold;
```

A Gold do Retail cria também `workspace.smoke_retail_gold_staging` (staging do gate de
publicação, decisão 062).

## 5. Fonte externa: API ou replay

O preflight `outbound_https` diz se o workspace alcança `api.bcb.gov.br`, `olinda.bcb.gov.br` e
`servicodados.ibge.gov.br`.

- **Com acesso:** `source = api`. A execução faz uma janela por indicador desde 2025-01-01 (com o
  lookback do `[smoke]`), sem repetir chamadas: o checkpoint lógico do Bronze evita reler o que
  já foi ingerido.
- **Sem acesso: replay pelo Volume.**
  1. Numa máquina com internet, gravar as respostas reais com o mesmo código:

     ```bash
     PYTHONPATH="projects/brazilian-financial-lakehouse/src;shared/src" \
       python -m financial_lakehouse.ingestion --env local --backfill-start 2025-01-01 \
       --record-responses data/replay/financial
     ```

  2. Copiar para o Volume **[remoto]**:
     `databricks fs cp -r data/replay/financial dbfs:/Volumes/workspace/smoke_financial/replay`.
  3. Rodar com `source = replay`. O notebook usa o instante da gravação
     (`_recorded_at.txt`) como relógio, então pede exatamente as URLs gravadas; uma URL ausente
     falha com `ReplayMissError`, sem ir à rede.

  **Diferença em relação à API:** o replay reproduz o que as APIs responderam **no instante da
  gravação**. Revisões publicadas depois não aparecem, e o tempo de rede não é medido. O relatório
  de execução deve dizer qual fonte foi usada.

## 6. O que é gravado

| Projeto | Tabelas (namespace `smoke`) |
|---|---|
| Retail | `smoke_retail_bronze.bronze_<dataset>` (11), `smoke_retail_silver.silver_<entidade>` e `_quarantine` (11 + 11), `silver_processed_batches`, `smoke_retail_gold.dim_*`/`fact_*` (9), `smoke_retail_gold_staging.*` (9), `pipeline_runs` e `data_quality_results` em cada schema |
| Financial | `smoke_financial_bronze.bronze_indicator_observations` (com CDF), `smoke_financial_silver.silver_indicator_observations`, `_quarantine`, `silver_reconciliation`, `silver_checkpoints`, `smoke_financial_gold.gold_*` (6), `pipeline_runs` e `data_quality_results`; landing e `_state/watermarks.json` no Volume |

## 7. Validação (só leitura)

```sql
-- execuções e status (uma linha por etapa/tabela)
SELECT pipeline_name, `table`, status, input_rows, output_rows, rejected_rows,
       duration_seconds, error_message
FROM workspace.smoke_retail_gold.pipeline_runs ORDER BY started_at;     -- idem nos outros schemas

-- alertas e falhas de qualidade
SELECT check_name, table_name, status, failed_rows, rows_checked, details
FROM workspace.smoke_financial_silver.data_quality_results WHERE status <> 'PASS';

-- reconciliação da Silver financeira (M1)
SELECT run_id, destination, rows, bronze_rows, balanced, from_version, bronze_version
FROM workspace.smoke_financial_silver.silver_reconciliation ORDER BY run_id, destination;

-- progresso por versão (M6)
SELECT * FROM workspace.smoke_financial_silver.silver_checkpoints;

-- Gold do Retail: totais mensais = KPI (M3)
-- rodar sql/gold/monthly_sales.sql e sql/gold/kpi_summary.sql com USE workspace.smoke_retail_gold
```

Critérios de sucesso do smoke:

- nenhuma linha `FAILED` em `pipeline_runs`;
- `balanced = true` em toda a reconciliação;
- Gold do Retail publicada, e não só em staging;
- `monthly_sales` somado igual a `kpi_summary`;
- um segundo `00_run_pipeline` do Financial sem commits novos lê 0 linhas e não alerta.

## 8. Limpeza

Não executada por padrão. Remover os schemas `smoke_*` é destrutivo e exige autorização:
`DROP SCHEMA workspace.<schema> CASCADE` para cada schema da seção 4.
