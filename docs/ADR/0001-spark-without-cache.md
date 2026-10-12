# 0001 — Spark de produção sem APIs de cache; ações repetidas eliminadas na origem

Status: aceito
Data: 2026-10-07

## Contexto

O Databricks Free Edition roda **somente em compute serverless**. A documentação de limitações
do serverless diz: *"Dataframe and SQL cache APIs are not supported on serverless compute. Using
any of these APIs or SQL commands results in an exception."* Isso inclui `df.cache()`,
`df.persist()`, `df.unpersist()`, `df.checkpoint()`, `spark.catalog.cacheTable()` e
`CACHE TABLE`.

A revisão de performance (2026-10-07) encontrou três problemas concretos:

1. **`run_silver` (Financial) chamava `.cache()` três vezes**, além de `unpersist()`. No Spark
   local isso funciona, então todos os testes passavam, mas no Free Edition a execução da Silver
   **falharia na primeira chamada**. Havia um problema secundário: o `unpersist()` só rodava no
   caminho de sucesso, então uma falha de qualidade deixava três DataFrames em cache na sessão.
2. **`run_gold` (Financial) contava as 6 tabelas Gold com `count()`** depois de a suíte de
   qualidade já tê-las contado (`rows_checked`). Cada `count()` reexecuta o plano inteiro da
   tabela: para as 4 tabelas mensais, isso é o painel mensal com 12 leituras da Silver,
   12 agregações e 11 joins.
3. **O gerador IoT contava a landing com `spark.read.json(path).count()` sem schema.** O Spark
   inferia o schema com uma passada completa (`samplingRatio = 1.0`) antes da passada de contagem:
   ~32 milhões de leituras gzip lidas duas vezes. Isso também violava a regra "schemas explícitos
   na leitura" do `CLAUDE.md`.

## Decisão

- **Nenhuma API de cache ou checkpoint em código de produção** (`projects/*/src`, notebooks,
  `shared/src/lakehouse_common`). O contrato `tests/contract/test_serverless_compatibility.py`
  falha se uma delas aparecer. Testes podem usar cache, porque rodam em Spark local.
- **Repetição de ações é resolvida na origem, não com cache:**
  - Silver: as duas contagens do Bronze (`count()` e `dropDuplicates().count()`) viram uma
    única agregação (`count` + `count_distinct(record_key)`). A entrada incremental (só as chaves
    tocadas desde o watermark) é pequena, então recomputá-la nas demais ações é aceitável.
  - Gold: `output_rows` vem do `rows_checked` que a suíte de qualidade já calculou na mesma
    passada, o que elimina 6 reexecuções completas por execução.
- **Leituras de texto sempre com schema explícito** (`tests/contract/test_spark_reads.py`).

## Alternativas

| Alternativa | Por que não |
|---|---|
| Manter `cache()` e tratar a exceção no serverless | o código teria dois caminhos, e o caminho que roda na plataforma não seria testado localmente |
| `localCheckpoint()` / `checkpoint()` | também rejeitados no serverless |
| Materializar intermediários em tabelas Delta temporárias | I/O extra, limpeza e nomes temporários para dados pequenos: complexidade maior que o ganho |
| Cachear as tabelas Gold (testes) | além de não existir no serverless, o Spark 4.0 falhou com `key not found` em `InMemoryRelation` quando vários planos cacheados compartilhavam subplanos (comentário em `tests/conftest.py` do Financial) |
| Ajustar `spark.conf` (shuffle partitions, AQE) | a maioria das propriedades não é configurável no serverless; o AQE já vem ligado |

## Consequências

- A Silver e a Gold do Financial rodam no Free Edition. Antes, a Silver falharia.
- Sem cache, cada ação reexecuta o plano. Isso é aceito onde os dados são pequenos: a entrada
  incremental da Silver e a Gold, construída sobre uma Silver de dezenas de milhares de linhas.
  As ações que restam por execução são as necessárias: qualidade (uma passada por conjunto de
  checks), contagens que ainda não existiam e a gravação.
- Sem um snapshot cacheado, cada ação da Silver lê a versão do Bronze vigente naquele momento.
  Uma escrita concorrente no Bronze durante a execução pode fazer as contagens diferirem do que
  foi gravado. A próxima execução relê a execução do watermark (a releitura é idempotente), então
  o efeito é só nas métricas daquela execução.
- Na geração IoT, a contagem passa a ser uma passada única sobre a landing, sem a passada de
  inferência.
