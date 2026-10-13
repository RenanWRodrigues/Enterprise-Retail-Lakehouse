# Smoke test — Retail e Financial no Databricks Free Edition

Estado em 2026-10-12: **NÃO EXECUTADO.** Nenhum notebook rodou no Databricks; este documento não
tem identificador de execução, versão de runtime nem tempo medido no workspace. A execução está
preparada e bloqueada pelos itens da §1. Preencha as seções 5 e 6 só com dados de uma execução real.

Antecedentes: preparação de 2026-10-08 em [EXECUTION_REPORT.md](EXECUTION_REPORT.md); passo a
passo em [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md); rede em
[network-preflight.md](network-preflight.md).

## 1. Pré-requisitos e bloqueios

| # | Pré-requisito | Estado | O que destrava |
|---|---|---|---|
| B1 | Acesso autorizado ao workspace | **ausente**: sem autorização para executar em workspace remoto (CLAUDE.md, *Evolution Rules*) | autorização explícita: rodar pelo Git folder (você) ou pela Databricks CLI (eu) |
| B2 | Network preflight | **pendente** ([network-preflight.md](network-preflight.md) §3) | rodar `notebooks/setup/00_network_preflight.py`; define `source = api` ou `replay` no Financial |
| B3 | Código no workspace = código validado | **ausente**: o Git folder lê o repositório remoto, e as correções desta revisão não estão nele (40 arquivos sem commit; `5461e80` sem push) | commits e push autorizados, depois *pull* no Git folder |
| B4 | Caminho Delta validado no Linux | **ausente**: o job de integração da CI falha por `Java heap space` ([ci-integration-memory.md](../audits/ci-integration-memory.md)); `test_smoke_rehearsal.py` nunca terminou | causa da CI comprovada e corrigida |
| B5 | Catálogo, schemas e Volumes do smoke | **não verificado** no workspace | preflight compartilhado (`shared/notebooks/00_preflight.py`) e o SQL do DEPLOYMENT_GUIDE §4 |

Rodar com B3 aberto testaria o código de 2026-10-08, e não o desta revisão. Rodar com B4 aberto
coloca no workspace caminhos Delta que nem a CI conseguiu exercitar.

## 2. Ambiente smoke (isolamento)

Sem credenciais: a CLI usa o perfil local `~/.databrickscfg`, fora do repositório e não lido aqui.

| Projeto | Camada | Destino (`[smoke]` nos TOML) |
|---|---|---|
| Retail | landing | `/Volumes/workspace/smoke_retail/landing` |
| Retail | Bronze / Silver / Gold | `workspace.smoke_retail_bronze` / `workspace.smoke_retail_silver` / `workspace.smoke_retail_gold` (+ `_gold_staging`) |
| Financial | landing / replay | `/Volumes/workspace/smoke_financial/landing` / `.../replay` |
| Financial | Bronze / Silver / Gold | `workspace.smoke_financial_bronze` / `workspace.smoke_financial_silver` / `workspace.smoke_financial_gold` |

Nada no `[smoke]` aponta para os destinos de `[databricks]` (`retail.*`, `financial.*`).
`tests/contract/test_databricks_readiness.py` garante isso. O catálogo `workspace` é o padrão do
Free Edition, a confirmar pelo preflight (B5).

## 3. Volume reduzido

- **Retail:** `config/data_generation_smoke.toml`, com 300 pedidos, 120 clientes, 60 produtos, 4 lojas
  e as anomalias de produção, gerados no Volume por `notebooks/setup/00_generate_landing.py`.
- **Financial:** `backfill_start = 2025-01-01` no `[smoke]`, uma janela por indicador.

Nenhuma carga completa.

## 4. Roteiro de execução e verificações

Ordem: preflight compartilhado → network preflight → Retail → Financial. Cada passo usa
`environment = smoke`.

### Retail

| Passo | Notebook | Verificação (SQL no workspace) |
|---|---|---|
| 1–2 | `notebooks/setup/00_generate_landing.py` | arquivos por sistema no Volume |
| 3–4 Bronze | `notebooks/bronze/01_ingest_bronze.py` | `SELECT _batch_id, count(*) FROM workspace.smoke_retail_bronze.bronze_<dataset> GROUP BY 1` = linhas da landing; `pipeline_runs` do Bronze `SUCCEEDED` |
| 5–6 Silver | `notebooks/silver/01_build_silver.py` | `pipeline_runs`: `input_rows = output_rows + rejected_rows + duplicate_rows` por entidade; `silver_<entidade>_quarantine` agrupada por `_dq_failed_checks`; nenhum CRITICAL em `data_quality_results` |
| 7–8 Gold | `notebooks/gold/01_build_gold.py` | grain único (`GROUP BY` grain `HAVING count(*) > 1` vazio); `fk__*` e `r3__*` em `data_quality_results`; linhas por tabela publicada |
| 9 reexecução | Bronze, Silver e Gold de novo, sem landing nova | Bronze: lotes `SKIPPED` e `DESCRIBE HISTORY` sem commit novo; Silver: `input_rows = 0`; Gold: mesmas contagens e chaves |
| 10 | — | erros e métricas de `pipeline_runs` / `data_quality_results` copiados para a §6 |

Alternativa: `notebooks/00_run_pipeline.py` roda as três camadas em sequência e registra o tempo
de cada etapa.

### Financial

| Passo | Notebook | Verificação |
|---|---|---|
| 3–4 Bronze | `notebooks/bronze/01_ingest_bronze.py` (`source = api` ou `replay`, conforme B2) | resultado por indicador (`succeeded` / `no_data` / `failed`); contagem em `bronze_indicator_observations` |
| 5–6 Silver | `notebooks/silver/01_build_silver.py` | `silver_reconciliation`: equação fechada por execução; `silver_indicator_observations_quarantine` por motivo; `silver_checkpoints` avançou |
| 7–8 Gold | `notebooks/gold/01_build_gold.py` | 6 tabelas `gold_*` com grain único; `gold_brazil_macro_monthly` sem mês duplicado |
| 9 reexecução | as três de novo | Bronze sem versão nova (MERGE insert-only); Silver: checkpoint inalterado, sem alerta repetido; Gold: mesmas contagens |

### Resultado esperado (do ensaio local, não do Databricks)

Ensaio local em 2026-10-12, sem Delta, sobre a mesma amostra de smoke do Retail:
`standardize` + `retail_entity_suite` por entidade + R3, em script fora do repositório.

| Entidade | Bronze | Válidas | Quarentena | Achados (não PASS) |
|---|---|---|---|---|
| categories / suppliers / stores / products / customers | 38 / 20 / 4 / 60 / 120 | iguais | 0 | — |
| orders | 300 | 300 | 0 | `unknown_customer` WARNING 1/300 |
| order_items | 957 | 957 | 0 | `unknown_product` WARNING 1/957 |
| payments | 307 | 307 | 0 | `unknown_order` WARNING 1/307 |
| shipments | 111 | 111 | 0 | — |
| inventory | 150 | 150 | 0 | — |
| inventory_movements | 1077 | 1076 | 1 | — |
| R3 (Gold) | 150 chaves | — | — | `r3__snapshot_matches_movements` ERROR 1/150; sistêmico PASS |

**Nenhum CRITICAL**: a amostra passa os gates da Silver e a R3. No Databricks, espere a Silver e a
Gold como `SUCCEEDED_WITH_ALERTS`. A divergência da R3 é o movimento quarentenado pela anomalia
`invalid_quantity`: a regra aponta exatamente o que deve.

O ensaio **não cobre** o gate completo da Gold (schema, grain, FKs) nem os MERGEs da Silver: os
dois exigem Delta, e o gate da Gold também o ano 9999, que não roda no Windows.

## 5. Ambiente e runtime — PENDENTE

| Campo | Valor |
|---|---|
| Workspace (host, sem token) | |
| Data da execução (UTC) | |
| Runtime serverless / Python / Spark / Delta | |
| Commit executado | |

## 6. Resultados — PENDENTE

| Projeto | Notebook | `run_id` | Início (UTC) | Duração | Status | Linhas (in / out / rejeitadas / duplicadas) | Qualidade (CRITICAL / ERROR / WARNING) |
|---|---|---|---|---|---|---|---|
| Retail | landing | | | | | | |
| Retail | Bronze | | | | | | |
| Retail | Silver | | | | | | |
| Retail | Gold | | | | | | |
| Retail | reexecução | | | | | | |
| Financial | Bronze | | | | | | |
| Financial | Silver | | | | | | |
| Financial | Gold | | | | | | |
| Financial | reexecução | | | | | | |

Erros: —

## 7. Limitações

- **Nada foi executado no Databricks.** Os números da §4 vêm de um ensaio local sem Delta e
  servem só como expectativa.
- **IoT:** fora do escopo deste smoke.
- **Financial:** depende do resultado da rede (B2). Com `replay`, o smoke valida o pipeline, mas
  não a saída para a internet.
