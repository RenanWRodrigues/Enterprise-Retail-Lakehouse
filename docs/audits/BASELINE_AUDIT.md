# Baseline Audit — 2026-10-08

Estado verificado do repositório antes da próxima rodada de evolução. Este documento registra o
que **foi executado nesta data** e separa explicitamente o que vem de evidência anterior. Não
substitui as auditorias de 2026-10-07 ([FINAL_AUDIT.md](../FINAL_AUDIT.md),
[CLAUDE_COMPLIANCE_AUDIT.md](../CLAUDE_COMPLIANCE_AUDIT.md),
[SECURITY_AUDIT.md](../SECURITY_AUDIT.md), [TEST_REPORT.md](../TEST_REPORT.md)): parte delas.

## 1. Git e ambiente

| Item | Estado |
|---|---|
| Commits | **0** (repositório sem histórico) |
| Branch / remoto | `main` / nenhum remoto |
| Staged | nada |
| Arquivos não ignorados (candidatos ao primeiro commit) | 280 |
| Ambiente de execução | Windows 11, `.venv` com Python 3.12; Spark local (PySpark 4.0.4) sem Hadoop; ~0,8 GB de RAM livre no início |
| Linux de referência | WSL Ubuntu com micromamba (Python 3.12 + OpenJDK 21), cópia limpa do repositório, alvos do `Makefile` |

Nenhum arquivo de código ou de dados foi alterado nesta etapa. Mudaram apenas `CLAUDE.md` (nova
seção *Evolution Rules* e documentos de apoio) e os documentos listados em §6.

## 2. Inventário

| Item | Quantidade | Onde |
|---|---|---|
| Projetos | 3 | `projects/{enterprise-retail,brazilian-financial,industrial-iot}-lakehouse` |
| Biblioteca comum | 2 pacotes | `shared/src/lakehouse_common` (qualidade, observabilidade, config, nomes), `shared/src/lakehouse_testing` (só testes) |
| Notebooks Databricks | 7 | Retail bronze/silver/gold; Financial bronze/silver/gold; IoT setup |
| CLIs locais | 3 | `financial_lakehouse.ingestion`, `retail_lakehouse.synthetic`, `iot_lakehouse.synthetic` |
| Arquivos de configuração | 7 TOML | `projects/*/config/` |
| Skills | 10 | `.claude/skills/` |
| Agentes | 0 | não existe `.claude/agents/` |
| ADRs completos | 1 | `docs/ADR/0001-spark-without-cache.md` (+ 61 decisões curtas em `ARCHITECTURE.md` §7) |
| Deploy declarativo no Databricks (bundle, `databricks.yml`, jobs) | **nenhum** | execução só pelos notebooks |
| Testes coletados | **1.212** | unit 469, transformation 163, data_quality 122, contract 395, integration 63 |

Mapa completo de módulos e tabelas: [ARCHITECTURE_OVERVIEW.md](../architecture/ARCHITECTURE_OVERVIEW.md).

## 3. Baseline de testes executado nesta data (Windows)

| Verificação | Comando | Resultado | Duração |
|---|---|---|---|
| Lint | `ruff check .` | sem achados | — |
| Formatação | `ruff format --check .` | 220 arquivos formatados | — |
| Tipos | `mypy` (strict) | 167 arquivos, sem erros | — |
| Unit | `pytest -m unit` | **469 passed** | 5 s |
| Contract | `pytest -m contract` | **341 passed, 54 skipped** | 107 s |
| Data quality | `pytest -m data_quality` | **111 passed, 11 skipped** | 283 s |
| Transformation | `pytest -m transformation`, por área (shared, Retail, Financial, IoT) | **145 passed, 18 skipped** (9 + 45/18 + 81 + 10) | 44 + 266 + 164 + 206 s |
| Integration | — | **não executada nesta data** (ver §4) | — |

Total executado hoje: **1.066 passed, 83 skipped, 0 failed**. Os 83 skips são os testes da Gold
do Retail (datas 9999-12-31 que o PySpark não converte no Windows), com motivo explícito; rodam
no Linux.

A primeira tentativa de `pytest -m transformation` numa única execução estourou o limite de 590 s
por comando no Windows e **não produziu resultado**. A categoria foi executada de novo, dividida
por área. Isso é limitação de tempo do ambiente, não falha de teste.

## 4. O que não foi executado nesta data, e a evidência disponível

| Item | Motivo | Última evidência |
|---|---|---|
| Integração Spark + Delta (63 testes) | exige Linux/Hadoop; as duas últimas execuções completas foram interrompidas pelo Claude Code por falta de memória na máquina | 2026-10-07, Linux, código atual: Financial 3, IoT 4, Retail Bronze 39, Retail Gold + escala 2, Retail Silver 15 — **63 passed** (em duas execuções) |
| Cobertura combinada | depende da integração | 2026-10-07: **95%**, piso medido sem a integração da Silver do Retail |
| Execução no Databricks | sem workspace configurado nesta etapa; deploy remoto fora do escopo | nunca executado |
| Carga de volume completo | proibida nesta etapa (milhões de linhas) | nunca executada pelo pipeline |
| CI do GitHub | sem remoto e sem commits | nunca executada |

## 5. Problemas

### 5.1 Confirmados (com evidência direta)

| ID | Problema | Evidência |
|---|---|---|
| M2 | A Gold do Retail grava sem gate de qualidade em tempo de execução | `retail_lakehouse/gold/build.py::write_gold` chama `createOrReplace` sem `run_suite`; a Gold financeira tem `gold_suite` |
| M6 | A reexecução da Silver financeira alerta de novo sobre linhas já em quarentena | teste de integração Delta (Linux, 2026-10-07): a reexecução termina `SUCCEEDED_WITH_ALERTS`; expectativa registrada em `test_fin_silver_gold_delta.py` |
| M4 | 9 funções com mais de 50 linhas | varredura AST nesta data (maior: `build_readings`, 148; nova: `retail_lakehouse/silver/pipeline.py::run_entity`, 51) |
| M5 | Dependências transitivas sem lock | `pyproject.toml` fixa só as diretas; não há lock file |
| IOT | IoT sem Bronze, Silver e Gold (janelas, MTBF, MTTR, disponibilidade) | `iot_lakehouse/{bronze,silver,gold}` vazios |
| DBX | Nada executado no Databricks; sem deploy declarativo | ausência de bundle/jobs; nenhum registro de execução |
| L2 | CLIs dos geradores sem teste | cobertura de 2026-10-07: `synthetic/__main__.py` do Retail e do IoT em 0% |
| DUP | Nomes de tabela duplicados no Financial | `BRONZE_TABLE` em `storage/bronze.py` e `quality/suites.py`; `SILVER_TABLE` em `silver/pipeline.py` e `quality/suites.py` |
| FIX | Fixture capturada e não usada | `tests/transformation/fixtures/official_sgs_432.json` sem referência em teste |

### 5.2 Ainda não reproduzidos (identificados por leitura de código ou dependentes de ambiente)

| ID | Hipótese | Por que não reproduzido |
|---|---|---|
| M3 | `monthly_sales.sql` descarta fatos com data fora do calendário (INNER JOIN com `dim_date`), divergindo de `kpi_summary.sql` | nenhum teste cobre data fora do calendário; reproduzir exige um teste novo (próxima etapa) |
| M1 | A métrica por execução da Silver financeira não reconcilia `bronze = válidas + quarentena + duplicatas` quando há boletins PTAX intradiários ou versões superadas | os testes de integração não têm boletins intradiários; demonstrar exige teste novo |
| CONC | Escrita concorrente no Bronze durante uma Silver (sem cache no serverless) pode afetar as métricas da execução | requer execução concorrente real |
| SRV | Compatibilidade com o runtime serverless do Free Edition (versão de Spark/Delta, `MERGE` no Unity Catalog, coluna `table` em SQL) | nenhuma execução no Databricks |
| NET | Acesso de saída à internet no Free Edition para as APIs do Financial | idem |
| VOL | Desempenho e memória com o volume completo (1.005.095 linhas Retail; ~32 milhões de leituras IoT, estimativa da configuração) | proibido nesta etapa |
| FLAKE | Falhas de shuffle do Spark no Windows em execuções longas (arquivos temporários removidos) | não reproduziram isoladas ([TEST_REPORT.md §3.7](../TEST_REPORT.md)) |

## 6. Documentos produzidos nesta etapa

- [ARCHITECTURE_OVERVIEW.md](../architecture/ARCHITECTURE_OVERVIEW.md)
- [IMPROVEMENT_PLAN.md](../implementation/IMPROVEMENT_PLAN.md)
- [TEST_MATRIX.md](../implementation/TEST_MATRIX.md)
- [RISK_REGISTER.md](../implementation/RISK_REGISTER.md)
- `CLAUDE.md`: seção *Evolution Rules* e lista de documentos de apoio (contratos de governança
  verificados: 108 passed)
