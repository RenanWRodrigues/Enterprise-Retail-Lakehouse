# Silver — leituras de sensores (Industrial IoT)

Entrada: `bronze_sensor_readings` (texto como chegou) e `bronze_sensors` (dados mestres do
CMMS), [BRONZE.md](BRONZE.md). Saída:

- `silver_sensor_readings`: tipada, uma linha por `reading_id`, unidades canônicas;
- `silver_sensor_readings_quarantine`: linha do Bronze como chegou + motivos;
- `silver_processed_files`: progresso;
- `silver_run_metrics`: métricas por execução.

Código:

- `src/iot_lakehouse/silver/standardize.py`: funções puras;
- `silver/units.py`: conversões de unidade;
- `silver/pipeline.py`: E/S, gate, reconciliação, métricas.

Regras de linha compartilhadas: `quality/suites.py::reading_row_rules`. Notebook:
`notebooks/silver/01_build_silver.py`. Configuração: `config/silver.toml`.

## Etapas (separadas e puras)

| Etapa | Função | O que faz |
|---|---|---|
| Normalização | `normalize` | texto aparado (vazio → NULL), timestamps ISO 8601 → instante UTC (`try_cast`), valor → double; o texto entregue fica em `_src_*`; `_content_hash` = SHA-256 da medição como entregue |
| Dados mestres | `sensor_reference` | unidade e limites físicos do sensor, última entrega por `sensor_id` |
| Unidades | `enrich` + `units.py` | converte para a unidade canônica do sensor quando há conversão exata |
| Contrato e física | `silver_reading_rules` | texto que não converte, regras compartilhadas (timestamp, valor, limites, unidade) → quarentena com todos os motivos |
| Deduplicação | `deduplicate` | primeira chegada vence (ver abaixo) |
| Tempo | `with_time_flags` | atraso e chegada fora de ordem, como marcas |
| Persistência | `pipeline.publish` | MERGEs insert-only, depois o progresso |
| Reconciliação | `pipeline.measure` + `Reconciliation` | `bronze = válidas + quarentena + duplicatas`, CRITICAL antes de gravar |

## Tempo de evento e tempo de chegada

| Coluna | Significado |
|---|---|
| `event_timestamp` | instante da medição (do sensor) |
| `ingestion_timestamp` | chegada ao historian (da fonte) |
| `bronze_ingestion_timestamp` | chegada ao lakehouse |
| `lateness_seconds` | `ingestion_timestamp − event_timestamp` |
| `is_late` | atraso acima de `late_after_minutes` (60, o mesmo limite do gerador) |
| `arrived_out_of_order` | o evento é mais antigo que um já visto para o sensor: antes, na ordem de chegada do lote, ou já na Silver |

**Nenhuma leitura é descartada por chegar atrasada ou fora de ordem.** Não há watermark nem estado
de janela: cada execução processa os arquivos pendentes do Bronze, e o "já visto" vem da própria
Silver (lida numa versão fixa). Não há Structured Streaming. Se for adotado, a política exigida é
a mesma: sem descarte por janela; um evento fora dela é marcado, não perdido.

Timestamps inválidos vão para a quarentena:

- `unparseable_event_timestamp` / `unparseable_ingestion_timestamp`: texto que não é ISO 8601;
- `missing_event_timestamp`;
- `invalid_event_timestamp`: antes de 2000-01-01 (relógio zerado) ou mais de 5 minutos depois da
  chegada (do futuro).

## Deduplicação e conflitos (determinística)

Ordem de chegada total: chegada ao historian → chegada ao Bronze → `_file_id` → `_content_hash`.

| Situação | Destino |
|---|---|
| primeira chegada de um `reading_id` | `valid` |
| reenvio com o **mesmo conteúdo** (`_content_hash`) | `duplicate` (contado, não gravado) |
| reenvio com **outro conteúdo** | quarentena `conflicting_resend` |
| `reading_id` já publicado | o publicado é a primeira chegada: o reenvio igual é `duplicate`, o diferente vai para a quarentena |

Uma leitura publicada **nunca é substituída** (MERGE insert-only por `reading_id`). Escolher a
versão "mais recente" faria o resultado depender de quando cada execução rodou. A primeira
chegada é a medição que o sistema viu primeiro, e a regra vale igual dentro e entre execuções.

## Unidades: conversão válida × erro

A unidade canônica é a do sensor no CMMS (`sensors.unit`).

- Uma unidade **da mesma grandeza** com conversão exata (°F, K → degC; psi, kPa, MPa → bar;
  L/s, m³/s → m³/h; in/s → mm/s; mA, kA → A; kV → V; W, MW → kW; grafias `°C`/`°F`) é
  convertida. O valor e a unidade entregues ficam em `source_value`/`source_unit`, e
  `unit_converted = true`.
- Qualquer outra unidade (outra grandeza, erro de digitação, símbolo desconhecido) fica como
  chegou, e a regra compartilhada `unit_mismatch` põe a leitura na quarentena. Nada é adivinhado:
  `C` não vira Celsius.

Os limites físicos valem sobre o valor **convertido**: 500 °F (260 °C) é impossível para um
sensor de −40 a 200 °C. Um valor exatamente no limite é válido.

## Sensor desconhecido

Leitura de um `sensor_id` ausente dos dados mestres fica na Silver com `unknown_sensor = true`
(dado mestre pode chegar atrasado, ADR 007). Sem limites, não há validação física. Acima de 5% do
lote, `unknown_sensor_systemic` é CRITICAL.

## Incremental, idempotência e retry

- **Unidade de progresso:** o arquivo do Bronze (`_file_id`), como no Bronze. Pendente = ainda
  não está em `silver_processed_files`, e um arquivo tardio numa partição antiga é só mais um
  pendente.
- **Silver fixada:** a Silver é lida `VERSION AS OF` a versão do início. Sem cache no serverless,
  as contagens são recalculadas, e sem a versão fixa as leituras do próprio lote pareceriam
  duplicatas depois do MERGE.
- **Ordem das gravações:** métricas (evidência) → gate → MERGE da Silver → MERGE da quarentena →
  arquivos marcados.
  - Uma falha antes do MERGE não grava nada.
  - Uma falha depois dele é repetida sem efeito: as leituras já publicadas viram `duplicate`, e a
    quarentena e o progresso são insert-only.

## Alertas e métricas

`pipeline_runs` (`iot_silver`):

| Campo | Significado |
|---|---|
| `input_rows` | linhas do Bronze consumidas |
| `output_rows` | válidas novas |
| `rejected_rows` | quarentena |
| `duplicate_rows` | duplicatas |

`data_quality_results`:

| Check | Severidade | Regra |
|---|---|---|
| `duplicate__reading_id` | CRITICAL | `reading_id` único entre as válidas |
| `conflicting_readings` | ERROR | dois ids diferentes no mesmo sensor e instante |
| `unknown_sensor` | WARNING | sensor fora dos dados mestres |
| `unknown_sensor_systemic` | CRITICAL | mais de 5% do lote com sensor desconhecido |
| `quarantined_rows` | ERROR | só para linhas **ainda não** na quarentena: identidade (arquivo, conteúdo), então um retry não realerta |
| `reconciliation` | CRITICAL | a equação acima |

`silver_run_metrics` (uma linha por métrica e execução):

- `bronze_rows`, `valid`, `quarantine`, `duplicate`, `new_quarantine`;
- `late`, `out_of_order`, `unit_converted`, `unknown_sensor`, `max_lateness_seconds`;
- `quarantine__<motivo>`.

As métricas são gravadas antes do gate, então ficam também quando a execução falha.

## Limitações

- Os dados mestres são lidos do Bronze de sensores, sem uma Silver própria de CMMS: uma linha de
  sensor com limite que não converte deixa o sensor sem limite físico.
- Execuções simultâneas da mesma Silver não são suportadas.
- O volume completo (~32 milhões de leituras) não passou pela Silver.

## Testes

| Arquivo | Cobre |
|---|---|
| `tests/transformation/test_iot_silver_standardize.py` | `reading_id` duplicado, reenvio idêntico e conflitante, atraso, fora de ordem, instantes preservados, timestamp que não converte, do futuro e antigo, unidade inválida, conversão válida, impossível depois de converter, valor no limite, valor não numérico, id ausente, sensor desconhecido, lote vazio, reenvio de leitura publicada, cada linha classificada uma vez |
| `tests/integration/test_iot_silver_delta.py` | execuções incrementais sem perda (arquivo tardio em partição antiga, atraso, fora de ordem contra a Silver, reenvio conflitante de leitura publicada), execução sem pendências, métricas; retry depois de falha no meio da publicação sem duplicar nem realertar |
