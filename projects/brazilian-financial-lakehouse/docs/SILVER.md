# Silver — séries econômicas padronizadas

Entrada: `bronze_indicator_observations` (texto, todas as versões de cada observação; ver
[INCREMENTAL_STRATEGY.md](INCREMENTAL_STRATEGY.md)). Saída: **um valor validado e tipado por
(indicador, data de referência)**, com as revisões resolvidas e as linhas inválidas em quarentena.

Código: `src/financial_lakehouse/silver/` (`standardize.py` com funções puras
`DataFrame -> DataFrame`; `pipeline.py` com o I/O). Notebook: `notebooks/silver/01_build_silver.py`.

## Tabela `silver_indicator_observations`

Grain: **uma linha por (`indicator_id`, `reference_date`)**, a versão vigente da observação.

| Coluna | Tipo | Regra |
|---|---|---|
| `indicator_id` | STRING | id do catálogo (`catalog.py`) |
| `indicator_name` | STRING | nome oficial (catálogo) |
| `reference_date` | DATE | dia (diárias); 1º dia do mês, do trimestre ou do último mês do trimestre móvel |
| `value` | DECIMAL(24,8) | texto da fonte convertido **sem arredondamento** |
| `unit` | STRING | unidade oficial (`% a.a.`, `% a.d.`, `%`, `R$/US$`, ...) |
| `frequency` | STRING | `daily`, `monthly`, `quarterly`, `rolling_quarter` |
| `source` | STRING | `bcb_sgs`, `bcb_ptax`, `ibge_agregados` |
| `updated_at` | TIMESTAMP | instante (UTC) da requisição que entregou a versão vigente |
| `period_start`, `period_end` | DATE | período que o valor cobre (regra abaixo) |
| `series_id` | STRING | código oficial (SGS, moeda PTAX, `agregado/variavel`) |
| `revision_count` | INT | quantas versões válidas distintas a observação já teve |
| `record_key` | STRING | chave da versão no Bronze (linhagem) |
| `bronze_ingestion_timestamp` | TIMESTAMP | ingestão da versão vigente (base do incremental) |

As oito primeiras colunas são o schema conceitual pedido; as demais tornam explícitos o período
coberto e a linhagem.

## Regras de padronização

### Datas

- A data já chega derivada no Bronze (SGS `dd/MM/aaaa`, PTAX data do boletim, IBGE `AAAAMM` ou
  `AAAATT`). Na Silver ela é **validada contra a frequência**: mensais e trimestres móveis no
  dia 1; trimestrais no dia 1 de janeiro, abril, julho ou outubro.
- Data nula, desalinhada ou **posterior à data da ingestão** vai para a quarentena.

### Decimais

- O texto é aceito só se casar com `^-?[0-9]{1,16}(\.[0-9]{1,8})?$`, o formato exato que cabe
  em DECIMAL(24,8). Assim nenhum valor é arredondado ou truncado em silêncio.
- Vírgula decimal, letras, notação científica ou mais de 8 casas: `invalid_decimal`.
- Espaços nas pontas são removidos. Zeros à direita não mudam o valor (`6.42500` = 6,425).
- Câmbio e saldos (reservas, base monetária, crédito) precisam ser positivos
  (`non_positive_value`). Taxas podem ser negativas (deflação no IGP-M, por exemplo).

### Nulls

- Valor nulo, vazio ou símbolo do IBGE (`-`, `..`, `...`, `X`) gera `missing_value`. Silver não
  imputa valores.
- O `-` do IBGE significa "zero absoluto", mas não é convertido em 0: nenhuma série do catálogo é
  uma contagem, então a ocorrência é tratada como anomalia a investigar.

### Duplicados

- Duplicata exata (mesma `record_key`, por exemplo a mesma resposta relida) entra uma vez.
- Várias versões da mesma chave são revisões (próxima seção).
- A unicidade de (`indicator_id`, `reference_date`) é verificada antes de gravar, como gate CRITICAL.

### Revisões históricas

1. Para cada chave, vence a versão entregue pela **requisição mais recente**
   (`request_timestamp`, ou `ingestion_timestamp` quando ausente).
2. Se a requisição mais recente trouxe **valores diferentes** para a mesma chave, as versões vão
   para a quarentena (`conflicting_values`), sem escolha arbitrária. A linha já gravada na Silver
   permanece até a fonte se resolver.
3. `revision_count` registra quantas versões válidas existem, e o Bronze guarda todas, com o
   arquivo raw de cada uma.

### Frequências diferentes

A Silver **não reamostra**: cada série fica na frequência nativa, com o período explícito.

| Frequência | `period_start` | `period_end` | Exemplo |
|---|---|---|---|
| `daily` | a data | a data | Selic, câmbio, reservas |
| `monthly` | 1º dia do mês | último dia do mês | IPCA, IGP-M, base monetária, crédito |
| `quarterly` | 1º dia do trimestre | último dia do trimestre | PIB |
| `rolling_quarter` | 1º dia de 2 meses antes | último dia do mês de referência | desocupação (PNAD) |

A conversão para mensal acontece só na Gold, com as regras de [GOLD.md](GOLD.md) §2.

### PTAX

O BCB publica vários boletins por dia: abertura, intermediários e fechamento. A taxa oficial do
dia é a do **boletim de fechamento**; os demais são excluídos (não são inválidos, por isso não vão
para a quarentena). Um dia ainda sem fechamento não tem valor na Silver.

## Quarentena: `silver_indicator_observations_quarantine`

Colunas do Bronze mais `_dq_failed_checks` (motivos) e `_dq_execution_timestamp`.
A gravação é insert-only por (`record_key`, motivos), então reprocessar não duplica.

| Motivo | Regra |
|---|---|
| `unknown_indicator` | indicador fora do catálogo |
| `missing_reference_date` | data nula |
| `misaligned_reference_date` | data incompatível com a frequência |
| `future_reference_date` | data posterior à ingestão |
| `missing_value` | nulo, vazio ou símbolo IBGE |
| `invalid_decimal` | texto não é um decimal exato que caiba em DECIMAL(24,8) |
| `non_positive_value` | câmbio ou saldo ≤ 0 |
| `conflicting_values` | valores diferentes na mesma requisição mais recente |

As regras usam o framework compartilhado (`lakehouse_common.quality`: `CustomCondition` e
`split_by_quality`).

## Gate de qualidade e gravação

- Qualidade e observabilidade (`financial_lakehouse.quality`, [DATA_QUALITY.md](../../../docs/DATA_QUALITY.md) §8):
  schema do Bronze e gate da Silver (chave única, nulls, frequência, schema) são CRITICAL e param
  antes de gravar; as regras de linha são ERROR (quarentena e alerta); valores implausíveis e
  lacunas são WARNING. Cada execução grava `pipeline_runs` e `data_quality_results`, também quando
  falha ([OBSERVABILITY.md](../../../docs/OBSERVABILITY.md)).
- **Incremental por versão Delta** ([ADR 0002](../../../docs/ADR/0002-silver-progress-by-delta-version.md)):
  um checkpoint durável (`silver_checkpoints`) guarda a última versão do Bronze publicada e
  reconciliada. A execução captura a última versão do Bronze uma vez, lê as mudanças ainda não
  consumidas pelo Change Data Feed e relê todas as versões das chaves tocadas, para que a revisão
  vencedora e o `revision_count` fiquem exatos. O checkpoint só avança depois dos `MERGE`s e da
  reconciliação. Não depende do horário de ingestão.
- **Alerta uma vez por problema:** regras de linha só sobre linhas do Bronze ainda fora da
  quarentena, plausibilidade só sobre versões ainda fora da Silver, lacunas só ao lado de chaves
  inseridas pela execução. Checks CRITICAL continuam avaliando o lote inteiro.
- `MERGE` por (`indicator_id`, `reference_date`): uma versão nova atualiza a linha, a mesma versão
  é ignorada (idempotente).

## Reconciliação

Toda linha do Bronze consumida por uma execução tem **exatamente um destino** contado:

```
bronze_consumed = valid + quarantine + duplicate + non_closing_bulletin
                  + superseded_revision + redundant_latest_version
```

| Destino | O que é | Passo que decide |
|---|---|---|
| `non_closing_bulletin` | boletim PTAX que não é o de fechamento (abertura, intermediários, ou sem tipo) | 1. observação oficial |
| `quarantine` | falhou uma regra de linha, **ou** é um dos valores conflitantes entregues pela mesma requisição mais recente (`conflicting_values`) | 2. regras de linha; 4. revisões |
| `duplicate` | mesma `record_key` de outra linha válida: reentrega técnica | 3. duplicados |
| `superseded_revision` | versão de uma chave substituída por uma entrega mais recente | 4. revisões |
| `redundant_latest_version` | mesma requisição mais recente e mesmo valor, outra `record_key` (outros atributos): a mesma observação | 4. revisões |
| `valid` | a observação selecionada da chave | — |

- **Exclusividade.** O destino é o do **primeiro** passo que retira a linha, na ordem 1 → 4.
  Uma linha inválida repetida conta como `quarantine` (as regras rodam antes da deduplicação),
  e um boletim de abertura repetido conta como `non_closing_bulletin`.
- **`valid` é lógico, não físico.** São as observações selecionadas nesta execução e oferecidas
  ao `MERGE` (uma por chave), não as linhas que o `MERGE` altera: reprocessar a mesma versão
  conta como `valid` e não muda a tabela. O estado acumulado da Silver não entra na equação.
- **Contagens independentes.** Cada destino é um DataFrame próprio (`SilverResult.silver`,
  `.quarantine` e `.excluded`), contado separadamente (`destination_counts`, uma ação Spark).
  Uma etapa que perca ou invente uma linha quebra a equação.
- **Versão fixa do Bronze.** A execução lê o Bronze `VERSION AS OF` a versão capturada no
  início (`silver.progress`). Sem cache no serverless, cada contagem relê a fonte; com a versão fixa,
  um append concorrente no Bronze não altera as contagens no meio da execução.
- **Falha.** Equação que não fecha é CRITICAL (`reconciliation`, em `data_quality_results` com a
  equação inteira em `details`): a execução fica `FAILED`, emite alerta CRITICAL e para **antes**
  do `MERGE`. Nada é publicado.
- **Evidência.** Antes do gate, a execução grava em `silver_reconciliation` uma linha por destino:
  `run_id`, tabela do Bronze, intervalo de versões consumido (`from_version`, exclusiva;
  `bronze_version`, inclusiva), `run_id`s do Bronze consumidos,
  `bronze_rows`, `rows` e `balanced`. Fica gravada também quando a execução falha; com a versão
  do Bronze, o lote pode ser reclassificado exatamente.
- **`pipeline_runs`.** `input_rows` = Bronze consumido, `output_rows` = `valid`,
  `rejected_rows` = `quarantine`, `duplicate_rows` = `duplicate`. Antes, `duplicate_rows` era
  `linhas − record_keys distintas` sobre todo o Bronze lido, incluindo boletins e linhas
  inválidas repetidas.

Testes: `tests/transformation/test_fin_silver_reconciliation.py` (cada categoria, lote misto,
perda de uma linha, reexecução, lote vazio) e `tests/integration/test_fin_silver_gold_delta.py`
(evidência em Delta, perda de linha no pipeline: `FAILED`, nada publicado).

## Limitações conhecidas

- ~~Revisão que volta ao valor antigo (A → B → A)~~: **corrigida no Bronze** (regra de versão,
  [INCREMENTAL_STRATEGY.md](INCREMENTAL_STRATEGY.md#idempotência)). A volta de A é gravada como
  versão nova e vence por ser a requisição mais recente
  (`tests/transformation/test_fin_bronze_to_silver_revisions.py`).
- Observação que **some** da fonte não é removida da Silver.
- Os testes cobrem a Silver com Spark local; o `MERGE` Delta roda no teste de integração no
  container Linux.
