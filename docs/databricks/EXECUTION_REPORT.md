# Execution Report — Databricks Free Edition

Data: 2026-10-08.

## Status geral

| Etapa | Status |
|---|---|
| A — Discovery | ✅ **local** (concluída); consulta ao workspace **PENDING** (sem autorização) |
| B — Preparação | ✅ concluída localmente, com uma lacuna (ensaio do Retail, abaixo) |
| C — Execução no workspace | **PENDING**: não autorizada; **nada foi executado no Databricks** |
| D — Evidências | este documento, [DEPLOYMENT_GUIDE](DEPLOYMENT_GUIDE.md), [COMPATIBILITY_MATRIX](COMPATIBILITY_MATRIX.md), [TROUBLESHOOTING](TROUBLESHOOTING.md) |

**Não há identificadores de execução Databricks, tabelas produzidas no workspace nem tempos de
execução remota.** As tabelas da seção 4 ficam em branco até a execução autorizada.

## 1. Discovery (local, 2026-10-08)

| Item | Resultado |
|---|---|
| Databricks CLI | v1.5.0 instalada |
| Autenticação | perfil `DEFAULT` em `~/.databrickscfg` (host em `cloud.databricks.com`); conteúdo não lido; perfil **não validado** (validar contata o workspace) |
| Variáveis `DATABRICKS_*` | não definidas |
| Configuração Databricks existente | notebooks `.py` por camada (Retail 3, Financial 3, IoT 1); ambiente `[databricks]` nos TOML com catálogos `retail`/`financial` ainda não confirmados; nenhum bundle ou job |
| Compatibilidade com serverless / Spark Connect | código de produção sem `sparkContext`, RDD, `_jvm`, UDF Python, cache; única dependência externa: `pyspark` |
| Sintaxe exclusiva de Python 3.12 | não encontrada nas buscas feitas (PEP 695, f-strings aninhadas); a busca não é exaustiva. O mínimo declarado continua 3.12 e o preflight verifica |
| CDF | sondado no Delta 4.0.1 local: `table_changes` funciona, `MERGE` insert-only gera só `insert`, `delete`/`update` detectáveis, erros explícitos para change data ausente |

## 2. Preparação feita (local)

| Entrega | Arquivos |
|---|---|
| Ambiente `smoke` isolado | seção `[smoke]` em `projects/*/config/{bronze,silver,gold,ingestion,medallion}.toml` |
| Amostra Retail | `config/data_generation_smoke.toml` (anomalias de produção, 300 pedidos) + `notebooks/setup/00_generate_landing.py` |
| Fallback por Volume (Financial) | `financial_lakehouse/clients/replay.py` (gravação/replay por URL, relógio da gravação), `--record-responses` na CLI, widgets `source`/`replay_root` no notebook Bronze |
| Execução ordenada | `notebooks/00_run_pipeline.py` nos dois projetos (tempo por etapa) |
| Preflight | `lakehouse_common/preflight.py` + `shared/notebooks/00_preflight.py` |
| Governança | `tests/contract/test_databricks_readiness.py` (notebooks finos, isolamento do `smoke`, amostra representativa); contrato serverless cobre `shared/notebooks` |

## 3. Testes locais da preparação (2026-10-08)

| Ambiente | Escopo | Resultado |
|---|---|---|
| Windows | unit de todos os projetos, incluindo `test_fin_replay_transport.py` (9), `test_fin_ingestion_cli.py` (`--record-responses`) e `test_preflight.py` (6) | 497 passed |
| Windows | contratos da raiz (inclui `test_databricks_readiness.py`) | 234 passed, 2 skipped |
| Linux (WSL), Delta | `shared/tests/integration/test_preflight_delta.py`: runtime, catálogo, schema, CDF e HTTPS (simulado) OK; Volume reportado como falha (não existe fora do Databricks) | 1 passed |
| Linux (WSL), Delta | `test_smoke_rehearsal.py`: landing de smoke → Bronze → Silver → Gold com gate | **sem resultado**: a 1ª execução passou do limite de 580 s por comando; a 2ª (em segundo plano) foi interrompida pelo Claude Code por falta de memória e não foi reiniciada |

Portanto **não está provado localmente** que a amostra de smoke do Retail passa pela regra de quebra
sistêmica da Silver e pelo gate da Gold. A amostra usa as taxas de anomalia de produção sobre o
tamanho da landing dos testes de integração, mas o teste que comprova isso ainda não terminou.

## 4. Execução no workspace — PENDING

Preencher só com dados de uma execução real.

| Etapa | Run ID / `run_id` | Início (UTC) | Duração | Status | Tabelas / linhas |
|---|---|---|---|---|---|
| Preflight | | | | PENDING | |
| Retail: landing | | | | PENDING | |
| Retail: Bronze | | | | PENDING | |
| Retail: Silver | | | | PENDING | |
| Retail: Gold | | | | PENDING | |
| Financial: Bronze (`api` ou `replay`) | | | | PENDING | |
| Financial: Silver | | | | PENDING | |
| Financial: Gold | | | | PENDING | |
| Financial: reexecução sem commits | | | | PENDING | |

Verificações a registrar:

- resultado de cada check do preflight;
- `pipeline_runs` sem `FAILED`;
- `silver_reconciliation` com `balanced = true`;
- `monthly_sales` igual a `kpi_summary`;
- fonte usada no Financial e a diferença se for replay;
- tempo de cada etapa (tabela do runner).

## 5. Passos restantes

1. **Memória livre:** concluir `test_smoke_rehearsal.py` no Linux (`make test-integration` ou o
   arquivo isolado).
2. **Autorização para o workspace:** validar o perfil (`databricks auth profiles`), criar o Git
   folder (requer o repositório publicado) e rodar a ordem do
   [DEPLOYMENT_GUIDE §4](DEPLOYMENT_GUIDE.md).
3. Se o preflight `outbound_https` falhar: gravar as respostas localmente e copiá-las para o Volume
   (§5 do guia).
4. Preencher a seção 4 com os identificadores e números reais.
