# CI de integração: `Java heap space` — diagnóstico (2026-10-12)

Estado: **causa raiz não comprovada**. Hipótese principal sustentada por evidência indireta; o
log completo da execução não está acessível sem autenticação no GitHub. Nenhuma correção
aplicada (nem em código de produção, nem em testes, nem no workflow).

## 1. Execução analisada

| Campo | Valor |
|---|---|
| Workflow / run | `ci` / `38253484478` (commit `d341bc8`, push em `main`) |
| Job | `integration tests (local Spark + Delta, separate)` — id `114826309359` |
| Runner | `ubuntu-latest` hospedado pelo GitHub (repositório público: 4 vCPU, 16 GB de RAM) |
| Passo que falhou | `make test-integration COV=1` (= `pytest -m integration --cov ...`) |
| Duração do passo | 13:10:00 → 13:47:45 UTC = **37 min 45 s** (limite do job: 45 min) |
| Demais jobs da cadeia | lint, unit, transformation, data quality + contract: **success**; coverage: **skipped** (depende de integration) |

## 2. Evidências disponíveis

Fonte: API pública de anotações do check run (`/check-runs/114826309359/annotations`).

| Nível | Linha do log | Mensagem |
|---|---|---|
| failure | 123 | `Java heap space` |
| failure | 555 | `Java heap space` |
| failure | 826 | `Java heap space` |
| failure | 1017 | `Process completed with exit code 2.` |
| warning | — | Node.js 20 obsoleto (`checkout@v4`, `setup-python@v5`, `setup-java@v4`, `cache@v4`) |
| warning | — | `setup-java v4` obsoleto |

Acesso ao log completo: `GET /actions/jobs/114826309359/logs` → **HTTP 403** (exige token);
página web do log → **404** sem sessão. Não há `gh` instalado nesta máquina. **O trecho
`short test summary info` não pôde ser lido.**

## 3. Classificação da falha

| Tipo | Veredito | Base |
|---|---|---|
| Timeout | **descartado** | 37 min 45 s < 45 min; o job terminou por erro, não foi cancelado |
| OOM da JVM (heap) | **confirmado** | três `Java heap space` em pontos distintos do log (123, 555, 826) |
| OOM do host | improvável | a mensagem é de heap da JVM (limitado por `-Xmx`), não do kernel; 16 GB no runner |
| Falha real de asserção | **não determinável** | o `exit code 2` é do `make` (receita falhou) e não distingue testes com falha de erro interno do pytest |
| Erro de configuração | parcial | não há erro de inicialização (o passo rodou 37 min); mas o heap não é configurado (ver §4) |

## 4. Configuração: local × CI

Fonte: `shared/src/lakehouse_testing/spark.py`, `conftest.py`, `Makefile`,
`.github/actions/setup/action.yml`, `.github/workflows/ci.yml`.

| Item | Windows (local) | Linux/WSL (referência) | CI |
|---|---|---|---|
| Master | `local[2]` | `local[2]` | `local[2]` |
| `spark.sql.shuffle.partitions` / `default.parallelism` | 2 / 2 | 2 / 2 | 2 / 2 |
| `spark.driver.memory` (heap da JVM) | **não definido → 1 GB (padrão)** | idem | idem |
| `spark.ui.enabled` | false | false | false |
| Delta | desabilitado (sem `HADOOP_HOME`): integração pulada | 4.0.1 | 4.0.1 |
| Java | — | OpenJDK 21 | Temurin 21 |
| Sessões Spark | **1 por processo pytest** (fixture `spark`, `scope="session"`) | idem | idem |

Nenhum arquivo do repositório define `spark.driver.memory`, `-Xmx`, `JAVA_OPTS` ou
`PYSPARK_SUBMIT_ARGS`. Nenhum teste cria `SparkSession` própria; a sessão é encerrada no
teardown da fixture (`session.stop()`): **não há sessões órfãs**, mas a mesma JVM atende as 86
integrações.

## 5. Volume de dados e estado acumulado

**Os DataFrames são pequenos.** Retail: no máximo 400 clientes / 2.000 pedidos por fixture
(`tests/conftest.py`), ensaio smoke com 300 pedidos; IoT: 1 equipamento por tipo, 3 dias;
`test_full_scale_generation.py` (100 mil pedidos) é Python puro e não usa a JVM. Há 22 chamadas a
`collect()` nas integrações, todas sobre resultados pequenos, e nenhuma a `toPandas()`. Um único
DataFrame que estoure 1 GB é improvável.

**O estado da sessão cresce a cada teste.** As 86 integrações (ordem de execução: `shared`,
Financial, Retail, IoT) gravam centenas de tabelas Delta na mesma sessão. Cada execução da Silver do
Retail cria 34 tabelas (11 entidades, 11 quarentenas, controle, observabilidade), e cada publicação
da Gold cria 18 (9 de staging e 9 publicadas). Nada é removido até o fim da sessão.

| Arquivo | Testes |
|---|---|
| `retail/test_bronze_ingestion.py` | 39 |
| `retail/test_silver_build.py` | 15 |
| `financial/test_fin_silver_progress_delta.py` | 7 |
| `retail/test_gold_build.py` | 7 |
| `iot/test_iot_landing_write.py` | 4 |
| `iot/test_iot_bronze_delta.py` | 3 |
| demais (6 arquivos) | 11 |
| **total** | **86** |

**Histórico (docs/TEST_REPORT.md, IMPROVEMENT_PLAN.md):**

- 2026-10-07: integração completa **em uma sessão** no Linux com **48** testes, sem nenhum erro de
  heap (47 passed, 1 falha de expectativa corrigida depois);
- 2026-10-08 em diante: os testes novos (gate da Gold M2, progresso da Silver financeira M6) foram
  rodados **"um por execução"**, ou seja, em sessões separadas;
- a suíte atual, com 86 testes, **nunca tinha rodado inteira numa só sessão** antes desta CI.

Observação: as interrupções locais registradas em R02 eram falta de memória **do host** (processo
encerrado pelo Claude Code), não `Java heap space`. São sintomas diferentes e não confirmam esta
hipótese.

## 6. Diagnóstico

### H1 — crescimento acumulado do heap numa JVM de 1 GB (principal; não comprovada)

O heap padrão de 1 GB atende 48 testes, mas não 86 numa única JVM. O que se acumula entre testes:

- **cache de `DeltaLog`/snapshots** de cada tabela Delta já aberta (centenas de tabelas, nenhuma
  removida);
- **status store do Spark**: com a UI desligada, o listener SQL continua retendo até
  `spark.sql.ui.retainedExecutions` (padrão 1000) execuções, com os planos. A suposição de que o
  listener fica ativo com `spark.ui.enabled=false` precisa ser verificada;
- **planos lógicos grandes** e repetidos: a Silver sem cache (ADR 0001) reavalia a mesma cadeia em
  vários `count()` por lote, e a Gold aplica dezenas de checks de FK com joins.

A favor: dados pequenos; 48 testes passaram juntos e 86 não; os testes novos só passaram isolados;
três OOM espalhados pelo log, e não um único.

Contra ou em aberto: o primeiro OOM aparece cedo (linha 123 de cerca de 1.017). Se essa linha
pertencer à saída do pytest, o estouro começaria nos primeiros arquivos (`shared`/Financial), o que
fala mais a favor de H2. Sem o log, não dá para saber qual teste rodava em cada OOM.

### H2 — um teste específico com plano ou estado desproporcional (alternativa)

Um teste isolado consome mais de 1 GB de heap: por exemplo, a análise de planos muito profundos, ou
algum `collect()` maior que o esperado. Seria comprovada se o mesmo teste falhar sozinho numa sessão
nova.

### O que os dados disponíveis não permitem afirmar

- quais testes falharam e se houve também falha de asserção;
- se os três OOM pertencem ao mesmo teste ou a testes diferentes;
- o uso real de heap ao longo da execução.

## 7. Opções de correção (nenhuma aplicada)

| # | Opção | O que muda | Impacto | Trata |
|---|---|---|---|---|
| D | **Diagnóstico primeiro**: `--durations=0 -v` (nome de cada teste no log), `-XX:+HeapDumpOnOutOfMemoryError` com upload do dump, log de GC | só o alvo de teste e o workflow | sem efeito no produto; uma execução extra da CI; o dump pode ter algumas centenas de MB | confirma H1 ou H2 |
| A | **Isolar processos**: uma execução do pytest por projeto (matriz no job ou laço no `Makefile`) | workflow / `Makefile` | uma JVM nova a cada grupo; +1–2 min por grupo; não esconde um teste que estoure sozinho (H2 continua visível); a cobertura passa a ser combinada de vários arquivos | H1 |
| C | **Limitar o estado retido** na sessão de teste: `spark.sql.ui.retainedExecutions`, `spark.ui.retainedJobs`/`retainedStages` baixos e cache de `DeltaLog` menor | `lakehouse_testing/spark.py` | ataca a causa provável sem mais memória; os nomes e efeitos das confs de Delta precisam ser validados na versão 4.0.1 | H1 |
| B | **Fixar o heap** de forma explícita (`spark.driver.memory`, por exemplo 2–3 GB, sobrescrevível por variável de ambiente) | `lakehouse_testing/spark.py` | correção mais simples, cabe no runner de 16 GB; **sozinha só adia o problema** se H1 for verdadeira; maior pressão nas máquinas locais com pouca RAM (R02) | sintoma |
| E | **Limpeza entre módulos**: remover os namespaces de teste do warehouse temporário ao fim de cada módulo | fixtures de teste | reduz o estado acumulado; é um `DROP` em tabelas temporárias de teste e exige autorização explícita (CLAUDE.md, *Evolution Rules*) | H1 |

Não recomendado: marcar testes como `skip`/`xfail`, ou tirar o job de integração da cadeia.

**Sequência sugerida:** D (identificar o teste e confirmar a hipótese) → A + C (isolamento e
retenção menor) → B só se, depois disso, ainda faltar margem, com o valor justificado pelo uso
medido.

## 8. O que falta para fechar o diagnóstico

Do log do job `114826309359` (aba Actions → run `38253484478` → *integration tests*):

1. o bloco **`short test summary info`** inteiro (fim do passo `make test-integration`);
2. as **~30 linhas antes de cada `Java heap space`** (linhas 123, 555 e 826 do log), para saber
   qual teste estava em execução;
3. a última linha de resumo do pytest (`N passed, M failed, K errors in ...`).

## 9. Atualização — 2026-10-12 (Prompts 02 e 03)

### 9.1 Correção do fingerprint Retail: validada e commitada

Commit **`5461e80`** (local, **sem push**): `standardize.fingerprint` (SHA-256 de `concat_ws` com
separador `\u001f` e marcador de NULL `\u0000`) no lugar de `xxhash64` em `_row_hash` e no
hash de conteúdo; teste de regressão; decisão 073 em `docs/ARCHITECTURE.md` §7; linha da
quarentena em `projects/enterprise-retail-lakehouse/docs/SILVER.md`. O relatório desta auditoria
ficou fora do commit (não faz parte da correção).

Execuções (Windows, um processo pytest por vez, `.venv/Scripts/python -m pytest ... -p no:cacheprovider`):

| # | Escopo | Código antes da correção | Exit | Resultado |
|---|---|---|---|---|
| 1 | `test_row_shifted_into_a_null_column_is_not_an_exact_duplicate` | sim (worktree temporário do `HEAD` `d341bc8` + só o teste novo; removido depois) | 1 | **1 failed**: `assert 1 == 0` (a linha sumia como duplicata) |
| 2 | mesmo teste | não | 0 | 1 passed |
| 3 | `test_silver_standardize.py`, `test_silver_customer_versions.py`, `test_incremental_scd2.py`, `unit/test_silver_reference.py` | não | 0 | **40 passed** |
| 4 | `projects/enterprise-retail-lakehouse/tests/data_quality` | não | 0 | **108 passed, 11 skipped** (Gold com 9999-12-31: não roda no Windows, roda na CI) |
| 5 | ruff check, ruff format --check, mypy strict | não | 0 | verdes (220 arquivos) |
| 6 | `tests/contract` (raiz) | não | 0 | 243 passed, 2 skipped |

Não executados: a suíte completa do Retail e a suíte geral depois da correção. A tentativa
anterior foi interrompida pelo Claude Code por falta de memória **do host** (7,7 GB no total;
0,2 GB livres durante o processo 3). Ela não foi contada como aprovada. A cobertura da correção
fica para a CI.

Semântica do `fingerprint`: posicional (a ordem das colunas importa); NULL ≠ `""` (marcador
próprio); o `_row_hash` usa só o texto do Bronze e o `_batch_id` (sem cast, não depende do fuso);
o hash de conteúdo converte os tipos da Silver para texto, e o texto de timestamp depende do fuso
da sessão, o que não afeta o resultado porque a comparação acontece dentro de um lote, numa única
execução, e não é gravada. **Limitação conhecida (teórica, a mesma do IoT):** um valor que contenha
`\u001f`, ou que seja exatamente `\u0000`, pode colidir. O algoritmo não foi alterado por falta de
evidência de falha.

### 9.2 Integração: causa **não confirmada**, nenhuma correção aplicada

O diagnóstico das §6–8 continua aberto. Por que não foi possível confirmar a causa:

- log completo inacessível sem autenticação (API 403, web 404, sem `gh`);
- reprodução local inviável: a integração exige Linux com Delta; o Docker Desktop não está ativo;
  o host tem 7,7 GB e pouca memória livre (R02);
- validar qualquer mudança no workflow exige push, que não foi autorizado.

Aplicar uma das correções da §7 agora seria corrigir um sintoma sem causa comprovada, o que o
Prompt 01 veta. **A fase não está concluída.**

Próximo passo proposto (depende de autorização): um commit **só de diagnóstico**, sem mudar a
lógica nem os asserts (opção D da §7):

- `Makefile`: `test-integration` com `-v --durations=25` (o nome de cada teste no log);
- workflow, job `integration`: `JAVA_TOOL_OPTIONS=-XX:+HeapDumpOnOutOfMemoryError
  -XX:HeapDumpPath=heapdumps/` e upload do diretório quando o job falhar.

Com o log dessa execução, ou com os trechos pedidos na §8, a causa fica comprovada e a correção
(A + C, e B só se ainda faltar margem) pode ser aplicada e validada na CI, incluindo o coverage.
