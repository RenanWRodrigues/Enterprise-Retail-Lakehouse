# Auditoria de conformidade — 2026-10-07

Auditoria independente do código contra as regras do repositório: `CLAUDE.md`, as skills
`.claude/skills/*/SKILL.md` (com foco em testing, pyspark, medallion, security e
dimensional-modeling), `docs/ENGINEERING_STANDARDS.md` e `docs/TESTING_STRATEGY.md`. A premissa
foi **não confiar** que as implementações anteriores seguiram as instruções: cada achado tem
evidência mecânica (varredura AST/regex, relatório de cobertura, plano de execução Spark) ou
leitura de código citada.

| Severidade | Encontrados | Corrigidos nesta auditoria | Abertos |
|---|---:|---:|---:|
| CRITICAL | 0 | — | 0 |
| HIGH | 3 | 3 | 0 |
| MEDIUM | 7 | 0 | 7 |
| LOW | 8 | 0 | 8 |

Problemas de mesma gravidade corrigidos **antes** desta auditoria (mesmo dia, com teste de
regressão) aparecem em §5, para o quadro ficar completo.

## 1. HIGH (corrigidos)

### H1 · Bronze do Retail descartava em silêncio campos novos em JSON

- **Regra:** skill medallion, Bronze: "coluna nova/removida na fonte = mudança de contrato: a carga
  falha (`CRITICAL`), nunca `mergeSchema` silencioso"; "NÃO PODE descartar registros".
- **Evidência:** `retail_lakehouse/ingestion/readers.py` lia JSON Lines com o schema do contrato.
  O leitor JSON do Spark ignora chaves fora do schema, então um campo novo da fonte (por exemplo,
  um cupom no pedido) **sumia do Bronze** sem erro. O CSV já falhava com cabeçalho divergente;
  o JSON não. A `docs/BRONZE.md` registrava isso como limitação, em contradição com a regra.
- **Correção:** `check_json_keys` compara as chaves entregues (`json_object_keys` sobre o texto
  lido com schema declarado) com o contrato antes da leitura. Chave inesperada ou chave ausente em
  todos os registros levanta `SourceContractError`. `null` explícito continua sendo dado.
  O `collect()` é sobre o conjunto de **nomes** de chave (limitado pelo tamanho do contrato).
- **Regressão:** `projects/enterprise-retail-lakehouse/tests/contract/test_bronze_reader.py`
  (`test_json_with_an_unexpected_key_fails_the_load`,
  `test_json_without_a_contract_key_fails_the_load`, `test_json_explicit_null_is_still_data`).
  O teste de JSON malformado ganhou uma primeira linha completa: o comportamento verificado
  (linha malformada falha) não mudou.
- **Custo:** uma passada a mais por arquivo JSON do lote.

### H2 · Dependências não fixadas

- **Regra:** `CLAUDE.md`, *Reproducibility*: "dependências fixadas".
- **Evidência:** o grupo `dev` usava `pytest>=8.3`, `ruff>=0.6`, `mypy>=1.11`, `pre-commit>=4.0`
  e `pyyaml>=6.0` (só PySpark e Delta estavam fixados). A CI instalava a versão que estivesse
  publicada no dia, e o `ruff` do pre-commit (`rev: v0.16.10`) só era igual ao do grupo `dev` por
  um comentário pedindo isso. A job de cobertura instalava `coverage[toml]>=7.6`.
- **Correção:** versões exatas, as mesmas validadas no Windows e no Linux nesta data (pytest
  9.1.1, pytest-cov 7.1.0, coverage 7.16.2, ruff 0.16.10, mypy 2.4.0, pre-commit 4.6.2, pyyaml
  6.0.3). A job de cobertura usa `coverage[toml]==7.16.2`.
- **Regressão:** `tests/contract/test_pinned_dependencies.py`: todo item do grupo `dev` usa `==`,
  todo `pip install` da CI é fixado, e o `rev` do ruff no pre-commit é igual ao do grupo `dev`.

### H3 · README com afirmações sem suporte

- **Regra:** pedido explícito ("README claims não suportados pelo código"; "não inventar
  métricas").
- **Evidência:**
  - o README do Retail dizia "181 testes" e "testes Delta pendentes do container";
  - o do Financial dizia "107 testes com mocks" e "MERGE Delta pendente de execução no
    container";
  - o README principal tinha a seção "Resultados comprovados" vazia (só um comentário HTML de
    marcação, sem conteúdo).

  As contagens coletadas hoje são 434 (Retail) e 257 (Financial), e os testes Delta rodaram no
  Linux (§6).
- **Correção:** os números vêm da execução registrada em §6 e da coleta do pytest. Os READMEs de
  projeto passam a apontar para o resumo do portfólio em vez de repetir contagens que envelhecem.
- **Regressão:** `tests/contract/test_repository_foundation.py::test_readme_has_no_unfilled_placeholders`.

## 2. MEDIUM (abertos)

| # | Achado | Regra | Evidência | Recomendação |
|---|---|---|---|---|
| M1 | A Silver financeira não reconcilia `linhas_bronze = válidas + quarentena + duplicatas` por execução: boletins PTAX que não são de fechamento e versões superadas por revisão saem do lote sem métrica própria | medallion, invariantes | `run_silver` registra `input`, `output`, `rejected`, `duplicate`; a diferença não tem nome | registrar a contagem de excluídos por regra (fechamento, versão superada) em `pipeline_runs`, num campo novo ou num check `INFO` |
| M2 | A Gold do Retail não tem gate CRITICAL em tempo de execução (grain único, FKs válidas) antes de gravar; só testes | dimensional-modeling ("grain testado com uniqueness CRITICAL"); CLAUDE.md ("falhas críticas interrompem") | `retail_lakehouse/gold/build.py::write_gold` grava sem `run_suite`; a Gold financeira tem `gold_suite` | declarar `retail_gold_suite` (schema, grain, FK→dim) e avaliar antes de `write_gold`, como no Financial |
| M3 | `monthly_sales.sql` usa `INNER JOIN dim_date`: um fato com data fora do calendário configurado some da consulta, enquanto `kpi_summary` (sem join) o conta | dimensional-modeling (membro desconhecido; fato nunca descartado) | `_date_key_or_unknown` só converte NULL em -1; datas fora de `calendar_start..end` geram chave inexistente | mapear datas fora do calendário para -1 na construção do fato, ou o gate de M2 com FK→`dim_date` |
| M4 | Funções acima de 50 linhas: `build_readings` (148), `generator._order` (103), `run_checks` (75), `_build_maintenance` (66), `run_silver` (63), `main` da ingestão (56), `_ingest_one` (55), `_run` (51) | CLAUDE.md ("funções pequenas, com uma responsabilidade") | varredura AST | extrair etapas nomeadas (por exemplo, atrasos e reenvios em `build_readings`) quando o código for tocado de novo |
| M5 | Dependências transitivas não travadas (sem lock file) | CLAUDE.md, reprodutibilidade | só dependências diretas fixadas (H2) | adotar `pip-tools`/`uv lock`; exige justificar a ferramenta |
| M6 | A releitura da execução do watermark (correção da corrida, §5) faz a Silver **alertar de novo** sobre linhas já em quarentena | observabilidade (alerta sem ruído) | integração Delta: a reexecução fica `SUCCEEDED_WITH_ALERTS` | watermark pela versão de commit do Delta (Change Data Feed) em vez do `ingestion_timestamp` |
| M7 | Retail Silver e IoT Bronze/Silver/Gold não existem, embora a Fase 4 (Financial) esteja completa | `ROADMAP.md`: "nenhuma fase começa antes de a anterior cumprir a DoD" | estado dos projetos | desvio feito por pedidos explícitos do responsável (precedência do `CLAUDE.md`); manter o estado honesto no README |

## 3. LOW (abertos)

| # | Achado | Evidência |
|---|---|---|
| L1 | Endereços-base das APIs (BCB, IBGE) como constantes no código, não em `config/` | `clients/bcb_sgs.py`, `bcb_ptax.py`, `ibge.py`; versionados com o cliente que os interpreta |
| L2 | CLIs dos geradores sintéticos sem teste (`retail_lakehouse/synthetic/__main__.py`, `iot_lakehouse/synthetic/__main__.py`, 0% de cobertura) | relatório de cobertura |
| L3 | Células de notebook com 19 e 21 linhas de código (diretriz: ~15), embora sejam só orquestração | varredura dos notebooks; nenhuma função ou classe definida neles |
| L4 | Deduplicação do IoT (`keep_first` por `ingestion_timestamp`) sem desempate total | `iot_lakehouse/quality/suites.py`; reenvios são cópias exatas pelo contrato, então o resultado não muda |
| L5 | `pytest.skip` com motivo, mas sem referência a issue (skill testing) | 3 ocorrências; o repositório não tem rastreador de issues |
| L6 | `HttpClient` registra a URL completa | sem credenciais hoje (APIs públicas); ver `SECURITY_AUDIT.md` §5 |
| L7 | A regra `future_reference_date` da Silver usa `to_date(ingestion_timestamp)` no fuso da sessão | correto em UTC (Databricks e testes); folga de até 3 h em relação a Brasília |
| L8 | Desvio documentado da skill medallion: a tabela Bronze financeira guarda só versões novas (deduplica) | justificado: a landing raw guarda cada resposta byte a byte (cópia fiel), pedido explícito de idempotência, ADR 035/052 |

## 4. Verificado e conforme

| Critério | Como foi verificado | Resultado |
|---|---|---|
| Lógica de negócio em notebooks | varredura de `def`/`class` e linhas por célula | nenhuma função ou classe; células só orquestram |
| Duplicação | hash AST de corpos de função com 4+ comandos em `src/` | nenhum corpo duplicado |
| Configuração hard-coded | regex de catálogos qualificados, caminhos absolutos e URLs em `src/` | só as URLs de L1 |
| Testes ausentes / regras sem teste | cobertura por módulo (Linux, 96% total) | todo módulo de negócio de 88% a 100%; só as CLIs de L2 em 0% |
| Transformações Bronze | leitura de `to_bronze` (Retail) e `to_bronze_records` (Financial) | só seleção das colunas do contrato + metadados; sem trim, cast ou filtro |
| Grain da Gold | `TABLE_COMMENTS` iniciados por "Grain:" e cabeçalhos dos SQL (testes de documentação dos dois projetos) | documentado em todas as tabelas e consultas |
| Double counting | leitura das 13 consultas SQL e dos joins dos fatos | joins N:1 comentados; `COUNT(DISTINCT order_id)` no ticket; agregados antes de combinar |
| `collect()` | regex em `src/` | 2, ambos limitados: versões das chaves do lote (Bronze financeiro) e nomes de chaves JSON (H1) |
| Python UDF | regex `udf`/`pandas_udf` | nenhuma |
| `withColumn` em laço | varredura AST | nenhum |
| Relógio ou aleatoriedade em testes | regex `now()`, `today()`, `random` sem semente | nenhum |
| Segredos | scanner `test_no_secrets.py` reforçado | nenhum (`SECURITY_AUDIT.md`) |
| Métricas falsas | cada número de `README.md` e `PORTFOLIO_SUMMARY.md` rastreado a log de teste, manifest ou execução real documentada | sem métrica inventada; estimativas rotuladas como tais |
| Planos de execução | `explain` dos caminhos mais pesados (Gold financeira, geração IoT, fatos do Retail) | joins com intervalo do IoT são `BroadcastHashJoin`; os únicos nested loops são o `crossJoin` intencional da grade IoT e o de uma linha do painel; a Gold mensal lê a Silver 25 vezes por plano (custo aceito do ADR 0001) |

## 5. Corrigidos antes desta auditoria (mesmo dia, com regressão)

| Severidade equivalente | Problema | Teste |
|---|---|---|
| CRITICAL | `run_silver` chamava `.cache()`, que o serverless (único compute do Free Edition) rejeita com exceção | `tests/contract/test_serverless_compatibility.py` |
| HIGH | Revisão A → B → A perdida no Bronze financeiro (a Silver mostrava B) | `test_fin_bronze_versions.py`, `test_fin_bronze_to_silver_revisions.py` |
| HIGH | Corrida do watermark da Silver: linhas gravadas pela mesma execução do Bronze depois de uma leitura da Silver eram puladas para sempre | `test_fin_silver.py::test_rows_of_the_watermark_run_are_read_again` |
| MEDIUM | Backfill no meio do mês tornava o primeiro mês da Gold "completo" com metade dos dias | `test_backfill_must_start_on_the_first_day_of_a_month` |
| MEDIUM | `error_message` persistida sem máscara para `access_token=`, `Bearer`, senha em URL | `test_observability_runs.py::test_error_messages_mask_secret_assignments` |
| MEDIUM | Leitura sem schema (inferência) na contagem da landing IoT | `tests/contract/test_spark_reads.py` |

## 6. Validação após as correções

| Execução (Linux, alvos do `Makefile`) | Escopo | Resultado |
|---|---|---|
| **Final**, com todas as correções (2026-10-07) | lint (ruff, format, mypy strict) | ok |
| | unit | **465 passed** |
| | transformation | **134 passed** |
| | data quality + contract | interrompida aos 57% pelo Claude Code por falta de memória na máquina; **0 falhas** até ali |
| | integration, cobertura | não executadas (mesmo motivo) |
| **Última completa** (2026-10-07, antes das 3 correções HIGH da auditoria de conformidade) | data quality + contract | **508 passed** |
| | integration (Spark + Delta) | 47 passed, 1 failed (expectativa de alerta ajustada depois, ver [TEST_REPORT.md §3.2](TEST_REPORT.md)) |
| | cobertura combinada (unit + transformation + quality + integration) | **96%** (piso: 85%) |

Testes coletados: **1.162** (unit 465, transformation 134, data_quality 121, contract 394,
integration 48). Depois das correções, os testes afetados foram rodados no Windows: os 4 testes
de leitura JSON do Retail e a suíte de contrato da raiz (§6 de
[CLAUDE_COMPLIANCE_AUDIT.md](CLAUDE_COMPLIANCE_AUDIT.md)). A revalidação completa no Linux está
pendente; para fazê-la, rode `make test-quality`, `make test-integration` e `make coverage`.

No Windows, depois das correções:

| Verificação | Resultado |
|---|---|
| `ruff check` | sem achados |
| `ruff format --check` | 210 arquivos formatados |
| `mypy` strict (+ os dois `tests/conftest.py` de projeto) | sem erros (160 + 2 arquivos) |
| unit (toda a suíte) | **465 passed** |
| contratos da raiz: governança, scanner de segredos, CI, dependências fixadas, serverless, leituras com schema, placeholders | **215 passed** |
| leitura JSON do Retail (H1, Spark local) | **4 passed** |

**Não validado depois das correções (aberto):** data quality + contract completos e integration
no Linux, e a cobertura combinada. A execução foi interrompida pelo Claude Code por falta de
memória e não foi reiniciada automaticamente.
