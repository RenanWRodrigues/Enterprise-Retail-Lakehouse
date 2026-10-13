# Data Quality

## 1. Objetivo

Qualidade de dados é **declarada, testada, medida e persistida** — nunca implícita.
Toda tabela Silver e Gold possui checks explícitos e métricas por execução.

## 2. Dimensões

| Dimensão | Pergunta | Exemplo de regra |
|---|---|---|
| Completude | campos obrigatórios estão preenchidos? | `order_id IS NOT NULL` |
| Unicidade | a chave é única no grain declarado? | 1 linha por `(order_id, line_number)` |
| Validade | valores estão no domínio/formato? | `status IN (...)`, CNPJ com 14 dígitos |
| Consistência | relações entre campos/tabelas fazem sentido? | `delivered_at >= shipped_at` |
| Integridade referencial | chaves estrangeiras existem? | todo `customer_id` existe em `dim_customer` |
| Atualidade (freshness) | o dado é recente o suficiente? | última leitura de sensor < 2h |
| Volume | a quantidade de linhas é plausível? | variação diária dentro de ±50% |

## 3. Severidade, status e ação

Cada check declara uma **severidade** (`WARNING`, `ERROR`, `CRITICAL`; padrão `ERROR`).
Cada execução produz um **status**: `PASS` se a taxa de falha ficou dentro do limiar
(`threshold`, padrão `0.0`); caso contrário, o status é a própria severidade do check.

| Status | Ação | Exemplo |
|---|---|---|
| `PASS` | nenhuma; métrica registrada | — |
| `WARNING` | registra métrica e alerta; registro segue | valor atípico, volume abaixo do esperado, FK sem correspondência na Silver |
| `ERROR` | registro vai para quarentena; pipeline continua | status fora do domínio |
| `CRITICAL` | interrompe o pipeline (falha a execução) e emite alerta CRITICAL | chave primária nula, schema incompatível |

Decisão do passo (`decide(results)`): todos `PASS` → `CONTINUE`; algum `WARNING`/`ERROR` →
`ALERT` (continua, alerta; linhas `ERROR` em quarentena); algum `CRITICAL` → `FAIL` (para antes de
gravar). Quando o pipeline continua, alerta ou falha, e como os alertas saem:
[OBSERVABILITY.md](OBSERVABILITY.md) §2–3.

Limiares (percentuais aceitáveis de falha) são configuração, não constantes no código.
Guia operacional: skill `.claude/skills/data-quality/`.

## 4. Onde as regras atuam

```
Bronze ──▶ [validação de schema: CRITICAL]
Silver ──▶ [completude, validade, unicidade, consistência]
              ├── válidos   ──▶ tabela Silver
              └── inválidos ──▶ <tabela>_quarantine  (+ _dq_failed_checks, _dq_execution_timestamp)
Gold   ──▶ [unicidade do grain, integridade referencial, reconciliação com Silver]
```

- **Bronze** não rejeita registros (preserva o bruto), apenas valida a estrutura.
- **Quarentena** guarda o registro original e o motivo, permitindo reprocessamento.
- **Reconciliação Gold**: totais da Gold batem com a Silver de origem
  (ex.: soma de `gross_amount` por dia), prevenindo double counting.

## 5. Framework (`lakehouse_common.quality`)

Framework interno, sem dependências além do PySpark e sem acoplamento a nenhum domínio.
Os projetos declaram **quais** checks rodar (em `src/<pkg>/quality/` + `config/`); o
framework define **como** eles são avaliados.

### Catálogo de checks

| Check | Classe | Dimensão padrão | Linha falha quando… | NULL |
|---|---|---|---|---|
| `not_null` | `NotNull(column, blank_is_null=False)` | completeness | valor é NULL (ou vazio/espaços, se `blank_is_null`) | **falha** |
| `unique` | `Unique((col, ...))` | uniqueness | a chave aparece em mais de uma linha (**todas** as ocorrências falham) | chave com NULL é ignorada |
| `accepted_values` | `AcceptedValues(column, values)` | validity | valor fora da lista (exato, case-sensitive) | ignorado |
| `range` | `Range(column, min_value=, max_value=, inclusive=True)` | validity | valor fora dos limites (um dos limites pode faltar) | ignorado |
| `regex` | `Regex(column, pattern)` | validity | valor não casa **inteiro** com o regex (sintaxe Java) | ignorado |
| `referential_integrity` | `ReferentialIntegrity(column, reference_df, reference_column)` | referential_integrity | valor não existe na referência | ignorado |
| `row_count` | `RowCount(min_rows=, max_rows=)` | volume | *dataset-level*: total fora dos limites | — |
| `custom_condition` | `CustomCondition(name, condition, column=None)` | validity | a expressão SQL (TRUE = válido) é FALSE | **falha** |
| `schema_matches` | `SchemaMatches(expected_struct, check_nullability=False)` | consistency | *dataset-level*: faltam colunas, sobram colunas ou um tipo mudou (`details` lista quais) | — |
| `sequence_gaps` | `SequenceGaps(partition_by, order_column, max_gap)` | completeness | o passo desde a linha anterior da partição excede `max_gap` (constante ou nome de coluna) | ordinal NULL ignorado |

**Schema primeiro**: `run_checks` avalia `schema_matches` antes de tudo (sem ação Spark); se ele
falhar, os demais checks do conjunto não rodam (suas colunas podem não existir) e só o resultado
de schema é devolvido. `run_suite` (vários `CheckSet`) para no primeiro schema CRITICAL reprovado.

**Duplicatas**: `keep_first(df, chaves, ordem)` remove duplicatas de forma determinística antes dos
checks; `duplicate_rows = entrada − saída`; um `Unique` CRITICAL depois prova que a chave ficou
única.

Opções comuns (keyword): `severity`, `threshold` (taxa tolerada, 0..1), `name` (padrão
`<tipo>__<coluna>`, ex.: `not_null__order_id`), `dimension`.

**Por que NULL é ignorado na maioria dos checks**: um mesmo defeito é contado por um
único check. Ausência de valor é responsabilidade de `not_null`; combine os dois quando a
coluna for obrigatória.

### Resultado de cada check (`CheckResult`)

`check_name`, `check_type`, `table_name`, `column_name` (`None` em `row_count`; colunas
separadas por vírgula em `unique` composto), `dimension`, `status`, `severity`,
`rows_checked`, `failed_rows`, `failure_rate`, `execution_timestamp` (UTC, obrigatório
com timezone), `details` (explicação de falhas estruturais; NULL nos checks de linha).

- Dataset vazio: checks de linha resultam em `PASS` com `failure_rate = 0.0`.
- `row_count` reprovado: `failure_rate = 1.0` e `failed_rows = rows_checked`
  (o dataset inteiro reprova); aprovado: `0.0`.

### Exemplo

```python
from lakehouse_common.quality import (
    AcceptedValues,
    NotNull,
    Range,
    ReferentialIntegrity,
    RowCount,
    Severity,
    Unique,
    raise_on_critical,
    results_to_dataframe,
    run_checks,
    split_by_quality,
)

checks = [
    RowCount(min_rows=1, severity=Severity.CRITICAL),
    NotNull("order_id", severity=Severity.CRITICAL),
    Unique(("order_id", "line_number"), severity=Severity.CRITICAL),
    AcceptedValues("status", ("created", "paid", "shipped")),
    Range("quantity", min_value=1),
    ReferentialIntegrity("customer_id", customers, "customer_id", severity=Severity.WARNING),
]

results = run_checks(
    orders, checks, table_name="retail.silver.orders", execution_timestamp=run_started_at
)  # 1 ação Spark
raise_on_critical(results)  # CriticalQualityError
valid, quarantine = split_by_quality(orders, checks, execution_timestamp=run_started_at)
dq_rows = results_to_dataframe(spark, results, run_id=run_id)  # -> data_quality_results
```

### Desempenho

- `run_checks` executa **uma única ação Spark** para qualquer número de checks: cada check
  de linha adiciona uma coluna-flag (expressões nativas; `unique` via janela;
  `referential_integrity` via left join N:1 com chaves deduplicadas) e uma agregação
  conta linhas e flags. Nenhum `count()` por check, nenhum `collect()` de registros.
- `split_by_quality` reaproveita as mesmas flags; `valid` e `quarantine` são lazy, e cada
  escrita recalcula as flags (serverless não oferece `cache()` confiável).
- `unique` e `referential_integrity` envolvem shuffle; em tabelas grandes, prefira poucos
  checks desses por execução.

### Regras de quarentena

- **O limiar decide o status, não o destino do registro**: linhas reprovadas em checks de
  linha `ERROR`/`CRITICAL` vão para a quarentena mesmo com taxa dentro do limiar.
- Checks `WARNING` e `row_count` nunca colocam linhas em quarentena.
- `_dq_failed_checks` lista os checks reprovados na ordem declarada.

## 6. Métricas persistidas

Tabela `<catalog>.<schema>.data_quality_results` (uma por projeto), uma linha por check por
execução, gravada pela camada de observabilidade junto com `pipeline_runs`
([OBSERVABILITY.md](OBSERVABILITY.md)):

`run_id, table_name, check_name, check_type, column_name, dimension, severity, status, rows_checked, failed_rows, failure_rate, execution_timestamp, details`

(`status` ∈ `PASS`, `WARNING`, `ERROR`, `CRITICAL`; contrato verificado por
`shared/tests/contract/test_quality_results_contract.py`.)

Permite acompanhar a evolução da qualidade ao longo do tempo (observabilidade).

## 7. Testes

- **Framework** (`shared/tests/`): cada tipo de check com zero falhas, algumas falhas,
  100% de falhas, NULL, duplicatas, valores de fronteira e dataset vazio; status e
  validações como `unit`; quarentena e contrato de `data_quality_results`.
- **Checks de um projeto** (`projects/<p>/tests/data_quality/`): a configuração de checks
  de cada tabela pega o defeito que deve pegar e não gera falso positivo nos dados válidos.
- Freshness e reconciliação Gold × Silver entram como checks quando houver o primeiro uso
  real (`CustomCondition` cobre casos pontuais até lá).

## 8. Checks críticos por projeto

Cada projeto declara suas regras em `src/<pacote>/quality/suites.py`; os testes estão em
`projects/<p>/tests/data_quality/`.

### Retail (`retail_lakehouse.quality.retail_silver_suite`, entidades Silver)

| Regra | Check | Severidade |
|---|---|---|
| pedido sem cliente | `missing_customer` (`customer_id` NULL) | ERROR |
| | `unknown_customer` (cliente inexistente) | WARNING (ADR 007) |
| | `unknown_customer_systemic` (> 5% dos pedidos) | CRITICAL |
| produto inexistente | `missing_product`, `unknown_product`, `unknown_product_systemic` | ERROR / WARNING / CRITICAL |
| remessa de pedido inexistente (`shipments.order_id`) | `missing_order`, `unknown_order`, `unknown_order_systemic` | ERROR / WARNING / CRITICAL |
| movimento de estoque de produto ou loja inexistente (`inventory_movements.product_id`, `.store_id`) | `missing_<product\|store>`, `unknown_<…>`, `unknown_<…>_systemic` | ERROR / WARNING / CRITICAL |
| | `inventory_movements.reference_id` é opcional (NULL no saldo de abertura) e aponta para documentos de tipos diferentes: sem checagem de FK | — |
| diagnóstico | `unknown_*` e `unknown_*_systemic` levam em `details` até 5 chaves órfãs distintas, em ordem crescente (`examples: …`) | — |
| valores negativos | `negative__<coluna>` (preços, custo, frete, desconto, pagamento); `non_positive__quantity` | ERROR |
| | `discount_exceeds_line` (desconto maior que a linha) | ERROR |
| duplicados | `duplicate__<entidade>` na chave de negócio (`BUSINESS_KEYS`), após `keep_first` | CRITICAL |
| schema | `schema__<entidade>` contra `SILVER_SCHEMAS` | CRITICAL |
| contrato da fonte (todas as 11 entidades) | `missing__<col>`, `invalid__<col>`, `out_of_domain__<col>` sobre o texto entregue | ERROR (quarentena) |
| outras regras de linha | `future__birth_date`, `delivered_before_shipped`, `zero__quantity`, `non_positive__installments`, `negative__<col>` de frete, estoque e prazo | ERROR (quarentena) |
| conflito no lote | `conflicting_values` (mesma chave, conteúdo diferente) | quarentena |
| reconciliação | `bronze = válidas + quarentena + duplicatas` por lote | CRITICAL |

Implementação e ordem das regras: [Silver do Retail](../projects/enterprise-retail-lakehouse/docs/SILVER.md).

### Retail Gold (`retail_lakehouse.quality.retail_gold_suite`, gate de publicação)

| Regra | Check | Severidade |
|---|---|---|
| schema (colunas e tipos) | `schema__<tabela>` contra `GOLD_SCHEMAS` | CRITICAL |
| grain duplicado | `grain__<tabela>` (`GRAIN`) | CRITICAL |
| chave obrigatória nula | `not_null__<coluna>` (grain e FKs) e `natural_key__<coluna>` (dimensões, exceto o membro -1) | CRITICAL |
| integridade referencial | `fk__<coluna>__<dimensão>` em todas as relações de `FOREIGN_KEYS` | CRITICAL |
| órfão mantido (cliente, produto ou loja desconhecido) | `unknown_member__<coluna>` | WARNING |
| fato com data fora do calendário (chave `-1`, data do evento preservada) | `out_of_calendar__<coluna de data>` | WARNING |
| R3: snapshot de estoque × Σ movimentos até a data do snapshot (check set `inventory_reconciliation`, entrada vinda da Silver, não publicada) | `r3__snapshot_matches_movements` / `r3__snapshot_matches_movements_systemic` (> 5% das chaves) | ERROR / CRITICAL |

Avaliado sobre as tabelas em staging antes de publicar qualquer uma; CRITICAL preserva a Gold
anterior. Detalhes: [DIMENSIONAL_MODEL.md §6](../projects/enterprise-retail-lakehouse/docs/DIMENSIONAL_MODEL.md).

### IoT (`iot_lakehouse.quality.iot_readings_suite`, leituras de sensores)

| Regra | Check | Severidade |
|---|---|---|
| sensor inexistente | `unknown_sensor` / `unknown_sensor_systemic` (> 5%) / `missing_sensor` | WARNING / CRITICAL / ERROR |
| timestamp inválido | `missing_event_timestamp`; `invalid_event_timestamp` (antes de 2000-01-01 ou mais de 5 min depois da ingestão) | ERROR |
| valor fisicamente impossível | `physically_impossible_value` (fora de `[physical_min, physical_max]` do sensor, NaN, infinito); `unit_mismatch` | ERROR |
| duplicados | reenvios removidos por `deduplicate_readings` (primeira chegada); `duplicate__reading_id` | CRITICAL |
| | `conflicting_readings` (ids diferentes, mesmo sensor e instante) | ERROR |
| schema | `schema__sensor_readings` contra o contrato de origem | CRITICAL |

Leitura **atrasada** (evento muito antes da ingestão) é dado válido, não falha de qualidade.

### Financial (`financial_lakehouse.quality`)

| Regra | Check | Severidade | Onde |
|---|---|---|---|
| datas duplicadas | `duplicate_dates` em (`indicator_id`, `reference_date`) | CRITICAL | lote Silver, antes do MERGE |
| nulls | `not_null__value`, `not_null__reference_date` | CRITICAL | lote Silver |
| gaps inesperados | `unexpected_gaps` (`SequenceGaps`): 1 dia (meta Selic, dias corridos), 5 dias (séries de dias úteis, sem calendário de feriados), 1 mês (mensais e trimestre móvel), 3 meses (PIB) | WARNING | tabela Silver completa, após o MERGE |
| valores inválidos | regras de linha da Silver (`invalid_decimal`, `non_positive_value`, `misaligned_reference_date`, `future_reference_date`...) | ERROR (quarentena) | candidatos Silver |
| | `implausible_value` (faixas de `PLAUSIBLE_RANGES`) | WARNING | lote Silver |
| alterações de schema | `schema__bronze`, `schema__silver`, `schema__gold_*` | CRITICAL | Bronze lido, lote Silver, tabelas Gold |
| grain da Gold | `grain__<tabela>` | CRITICAL | antes de substituir qualquer tabela Gold |
