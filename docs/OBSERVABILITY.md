# Observability — execuções, métricas e alertas

Camada transversal dos três Lakehouses. O código está em `shared/src/lakehouse_common/`:

- `observability/`: registro de execuções, política de alerta e destinos de gravação;
- `quality/`: checks, resultados e decisão.

Cada projeto apenas **declara** o que checar (`src/<pacote>/quality/suites.py`). Qualidade de dados
em detalhe: [DATA_QUALITY.md](DATA_QUALITY.md).

```mermaid
flowchart LR
    P[passo do pipeline] -->|track_run| T[RunTracker]
    T -->|record_counts| R[(pipeline_runs)]
    T -->|record_quality| D{decide}
    D -->|CONTINUE| W[grava a camada]
    D -->|ALERT| A[log lakehouse.alerts<br/>+ grava a camada]
    D -->|FAIL| X[CriticalQualityError<br/>nada é gravado]
    T --> Q[(data_quality_results)]
```

## 1. Conceito: duas tabelas, três destinos

- **`pipeline_runs`**: uma linha por passo executado (uma tabela produzida numa execução).
- **`data_quality_results`**: uma linha por check avaliado nesse passo.

Os dois registros saem de um único mapeamento (`run_record`, `result_record`), e cada projeto
escolhe só **para onde** eles vão:

| Sink | Onde grava | Quando usar |
|---|---|---|
| `DeltaObservabilitySink(spark, namespace)` | `<namespace>.pipeline_runs`, `<namespace>.data_quality_results` (Delta, append-only) | Databricks |
| `JsonLinesObservabilitySink(root)` | `<root>/pipeline_runs.jsonl`, `<root>/data_quality_results.jsonl` (append-only, Python puro) | execução local, inclusive no Windows sem Hadoop; passos que não iniciam Spark |
| `InMemoryObservabilitySink()` | lista em memória | testes |

### `pipeline_runs`: uma linha por passo (`run_id` + `table`)

`pipeline_name, run_id, project, layer, table, started_at, finished_at, duration_seconds, input_rows, output_rows, rejected_rows, status, error_message, duplicate_rows, completeness, uniqueness, validity, referential_integrity, checks_failed`

As 13 primeiras colunas são o registro padrão pedido; as 6 últimas resumem duplicatas e
qualidade do passo.

| Coluna | Significado |
|---|---|
| `pipeline_name` | `retail_bronze`, `financial_silver`, `iot_landing`... (`<projeto>_<camada>`) |
| `run_id` | identificador da execução (instante UTC `AAAAMMDDThhmmssZ`); passos da mesma execução compartilham o id |
| `project` | `retail`, `iot`, `financial` (`PROJECT` no `__init__` de cada pacote) |
| `layer` | `landing`, `bronze`, `silver`, `gold` |
| `table` | tabela (ou caminho) produzida pelo passo; `<namespace>.*` quando o passo produz várias |
| `started_at`, `finished_at` | relógio UTC do início e do fim |
| `duration_seconds` | relógio monotônico (não sofre com ajustes do relógio do sistema) |
| `input_rows` | linhas lidas pelo passo |
| `output_rows` | linhas produzidas ou gravadas (ver §4 por projeto) |
| `rejected_rows` | linhas em quarentena (0 onde a camada nunca rejeita, como Bronze e landing) |
| `status` | `SUCCEEDED`, `SUCCEEDED_WITH_ALERTS`, `FAILED` |
| `error_message` | `Tipo: mensagem`, até 2000 caracteres, com valores que parecem segredos mascarados: atribuições e parâmetros de URL cujo nome contém `token`, `secret`, `password`, `pwd` ou `api_key` (`access_token=***&page=2`), cabeçalhos `Bearer`/`Basic` e a senha de `esquema://usuário:senha@host` |
| `duplicate_rows` | duplicatas removidas antes dos checks |
| `completeness`, `uniqueness`, `validity`, `referential_integrity` | nota da dimensão no passo: `1 − linhas reprovadas / linhas checadas`, somadas nos checks da dimensão; NULL se nenhum check dela rodou |
| `checks_failed` | checks com status diferente de `PASS` |

Contagens não informadas pelo passo ficam NULL. Um passo que falhou antes de contar não inventa
zeros, e um passo sem dados grava zeros explícitos: "nada chegou" (0) é diferente de "não se
sabe" (NULL).

### `data_quality_results`: uma linha por check por execução

Contrato em [DATA_QUALITY.md](DATA_QUALITY.md) §6. Liga-se a `pipeline_runs` por `run_id`;
`table_name` é a tabela checada (pode ser uma das várias produzidas pelo passo), e `details`
explica as falhas estruturais (por exemplo, as colunas que sumiram).

### Os cinco cenários

`shared/tests/unit/test_observability_scenarios.py` verifica cada cenário a partir das linhas
**gravadas** (sink JSON Lines):

| Cenário | `pipeline_runs` | `data_quality_results` |
|---|---|---|
| successful run | `SUCCEEDED`, contagens, duração | checks `PASS` |
| failed run | `FAILED`, `error_message`; contagens não feitas ficam NULL | — |
| zero records | `SUCCEEDED`, `input_rows = output_rows = 0` | nenhuma linha (nenhum check rodou) |
| rejected records | `SUCCEEDED_WITH_ALERTS`, `rejected_rows` > 0 | check `ERROR` com `failed_rows` |
| quality failure | `FAILED` (`CriticalQualityError`), nada gravado na camada | check `CRITICAL` |

## 2. Severidade: quando o pipeline continua, alerta ou falha

| Status do check | Decisão (`decide`) | O que acontece com as linhas | O passo | `pipeline_runs.status` |
|---|---|---|---|---|
| `PASS` (todos) | `CONTINUE` | seguem | grava | `SUCCEEDED` |
| `WARNING` | `ALERT` | **seguem** (nada vai para quarentena) | grava e emite alerta | `SUCCEEDED_WITH_ALERTS` |
| `ERROR` | `ALERT` | reprovadas vão para a **quarentena**, as demais seguem | grava e emite alerta | `SUCCEEDED_WITH_ALERTS` |
| `CRITICAL` | `FAIL` | — | **para antes de gravar** (`CriticalQualityError`) | `FAILED` |
| exceção qualquer | — | — | propaga o erro | `FAILED` (+ `error_message`) |

Como escolher a severidade:

| Severidade | Use quando | Exemplos |
|---|---|---|
| `WARNING` | o dado é plausível, mas merece atenção humana; bloquear seria pior que seguir | FK ainda sem correspondência (ADR 007), lacuna numa série, valor implausível mas possível |
| `ERROR` | a linha está errada, mas o resto do lote é bom | valor negativo, timestamp inválido, valor fisicamente impossível, decimal inválido |
| `CRITICAL` | o lote inteiro não é confiável ou gravá-lo corromperia a camada | schema mudou, chave duplicada depois da deduplicação, chave nula, mais de 5% de FKs órfãs |

**Escalada por limiar**: a mesma regra pode existir em duas severidades. Uma FK órfã é WARNING
(dado atrasado), mas acima de 5% das linhas é CRITICAL (a carga da dimensão provavelmente falhou).
O `threshold` decide o status, não o destino da linha.

**Schema primeiro**: se um check de schema falha, os demais checks daquele conjunto não são
avaliados, porque as colunas que eles leem podem não existir. Uma falha de schema CRITICAL
interrompe a suíte.

## 3. Alertas

`RunTracker.record_quality` gera uma mensagem por check WARNING/ERROR, por exemplo:

```
[WARNING] retail_silver/silver_orders: unknown_customer failed 12/48210 rows
```

- A mensagem vai para o logger **`lakehouse.alerts`** (nível WARNING), que é o ponto de integração
  com o canal de alerta (handler de e-mail, Slack, webhook).
- A mensagem também fica em `PipelineRun.alerts`. Um alerta pode ser montado também sobre a
  tabela: `status = 'SUCCEEDED_WITH_ALERTS'` (alerta) ou `FAILED` (incidente).
- Um job do Databricks falha quando o passo levanta exceção (CRITICAL ou erro), e a notificação
  de falha do job cobre esse caso.
- Nenhuma falha é silenciosa. Se gravar o registro de uma execução bem-sucedida falhar, o erro
  propaga. Se o passo já tinha falhado, o erro original prevalece e a falha de gravação vai para o
  log.

## 4. Uso

```python
from iot_lakehouse import PROJECT
from lakehouse_common.observability import DeltaObservabilitySink, Layer, track_run
from lakehouse_common.quality import keep_first, run_suite

with track_run(
    pipeline_name="iot_silver",
    run_id=run_id,
    project=PROJECT,
    layer=Layer.SILVER,
    table="iot.silver.sensor_readings",
    sink=DeltaObservabilitySink(spark, "iot.silver"),
) as step:
    readings = deduplicate_readings(raw)  # duplicatas fora
    step.record_counts(
        input_rows=raw.count(),
        output_rows=readings.count(),
        duplicate_rows=raw.count() - readings.count(),
    )
    step.record_quality(
        run_suite(iot_readings_suite(readings, sensors), execution_timestamp=started)
    )  # CRITICAL levanta aqui
    write(readings)  # só chega aqui sem CRITICAL
```

### Integração nos três projetos

A observação fica na **orquestração** (a função que executa e grava o passo). As transformações
puras não conhecem observabilidade. O sink é **parâmetro obrigatório** desses pontos de entrada:
quem chama decide o destino, e o mypy acusa quem esquece de passá-lo.

| Projeto | Passo | `pipeline_name` / `layer` | Uma linha por | Contagens | Sink no notebook / CLI |
|---|---|---|---|---|---|
| Retail | `bronze.ingest_all` | `retail_bronze` / bronze | dataset (tabela Bronze) | in = out = linhas gravadas; lote já ingerido = 0/0 | Delta `<bronze ns>` |
| Retail | `gold/pipeline.py::write_gold` (gate) | `retail_gold` / gold | execução (`<gold ns>_staging.*`) | in = linhas candidatas; out = as mesmas, só se o gate passa + checks | Delta `<gold ns>` |
| Retail | `gold/pipeline.py::write_gold` (publicação) | `retail_gold` / gold | tabela Gold | out = linhas da tabela publicada | Delta `<gold ns>` |
| IoT | `synthetic.write_landing` | `iot_landing` / landing | execução (raiz da landing) | out = linhas de todos os arquivos gerados | JSON Lines em `<landing>/_observability` |
| Financial | `ingestion.IncrementalIngestion.run` | `financial_bronze` / bronze | execução | in = observações recebidas, out = versões novas | Delta `financial.bronze` / JSON Lines local |
| Financial | `silver/pipeline.py::run_silver` | `financial_silver` / silver | execução | in = Bronze consumido, out = `valid`, quarentena, duplicatas + checks; destinos de cada linha em `silver_reconciliation` | Delta `<silver ns>` |
| Financial | `gold/pipeline.py::run_gold` | `financial_gold` / gold | execução (`<gold ns>.*`) | in, out + checks | Delta `<gold ns>` |

- **Retail e IoT ainda não têm Silver**: suas suítes de qualidade estão prontas
  (`retail_lakehouse.quality`, `iot_lakehouse.quality`) e entram via `step.record_quality` quando
  as Silvers existirem. Até lá, os passos deles registram execução e contagens, sem checks.
- **Bronze Financial**: um indicador com falha torna a execução `FAILED`, com as contagens
  parciais dos indicadores saudáveis, também com `raise_on_failure=False`. As métricas HTTP
  (requisições, retries) continuam em `_metrics/`, porque não são colunas de `pipeline_runs`.
- **IoT** grava em JSON Lines no próprio Volume da landing, porque o gerador não tem um
  namespace de catálogo configurado.

## 5. Consultas úteis

```sql
-- Passos que falharam ou alertaram nos últimos 7 dias
SELECT project, pipeline_name, `table`, run_id, status, error_message, checks_failed
FROM pipeline_runs
WHERE started_at >= current_timestamp() - INTERVAL 7 DAYS AND status <> 'SUCCEEDED'
ORDER BY started_at DESC;

-- Evolução diária das notas de qualidade de uma tabela
SELECT DATE(started_at) AS day, AVG(completeness) AS completeness, AVG(validity) AS validity
FROM pipeline_runs
WHERE `table` = 'financial.silver.silver_indicator_observations'
GROUP BY DATE(started_at)
ORDER BY day;

-- Passos sem dados (zero records) por pipeline
SELECT pipeline_name, COUNT(*) AS empty_runs
FROM pipeline_runs
WHERE status <> 'FAILED' AND output_rows = 0
GROUP BY pipeline_name;

-- Checks que mais reprovam
SELECT table_name, check_name, severity, COUNT(*) AS failed_runs, SUM(failed_rows) AS failed_rows
FROM data_quality_results
WHERE status <> 'PASS'
GROUP BY table_name, check_name, severity
ORDER BY failed_runs DESC;
```

## 6. Limitações

- **`table` é palavra reservada no ANSI SQL.** O Spark e o Databricks aceitam a coluna sem
  aspas na configuração padrão; nas consultas use crases (`` `table` ``) por segurança.
- **Mudança de schema (2026-10-07):** `table_name`, `start_time` e `end_time` viraram `table`,
  `started_at` e `finished_at`, e entrou `project`. Uma tabela Delta `pipeline_runs` criada antes
  disso não aceita o append: apague-a (são registros de desenvolvimento) e deixe o sink recriá-la.
- O núcleo de observabilidade **importa** o PySpark (o pacote `quality` é baseado em Spark), mas
  nunca inicia uma sessão: `track_run` e o sink JSON Lines funcionam sem JVM.

- Não há envio ativo de alertas (e-mail, Slack): o ponto de integração é o logger
  `lakehouse.alerts` e a própria tabela `pipeline_runs`.
- `output_rows` é contado antes da gravação; linhas efetivamente inseridas por um MERGE estão no
  histórico Delta (`DESCRIBE HISTORY`).
- As contagens custam ações Spark (`count()`). Nos volumes atuais o custo é pequeno; em tabelas
  grandes, prefira as métricas da operação Delta.
- Freshness e volume (dimensões já existentes no framework) não entram nas notas por dimensão do
  `pipeline_runs`, que cobre as quatro dimensões pedidas.
