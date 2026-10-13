# 0004 — Descoberta incremental de lotes do Bronze (Retail e IoT)

Status: aceito (2026-10-13, pelo responsável, conforme a regra de admissão do
`shared/README.md` para código novo em `lakehouse_common`)
Data: 2026-10-12 (proposto) · 2026-10-13 (aceito)
Altera: `pending_batches` (Retail) e `pending_files` (IoT), que liam a tabela Bronze inteira a cada
execução.

## Contexto

A Silver do Retail descobria os lotes pendentes com `SELECT DISTINCT _batch_id,
_ingestion_timestamp` sobre todo o Bronze de cada entidade, menos `silver_processed_batches`. O
IoT fazia o mesmo com `_file_id` e `silver_processed_files`. O custo crescia com o histórico (o
IoT completo é estimado em ~32 milhões de leituras), não com o que chegou.

O que o código garante e é aproveitado aqui:

- Bronze append-only (`delta.appendOnly`), e cada lote (Retail) ou arquivo (IoT) entra num commit
  atômico. As unidades dos commits posteriores a uma versão são exatamente as novas.
- A tabela de controle da Silver registra as unidades processadas, depois das gravações. É ela,
  e não o checkpoint, que garante a correção.

## Decisão

1. **Descoberta pelo Change Data Feed dos commits novos**: `lakehouse_common.incremental.discover`
   lê `table_changes(bronze, checkpoint + 1, latest)` e só as colunas da unidade. A versão `latest`
   é capturada uma vez por execução; commits feitos durante a execução ficam para a próxima.
2. **Checkpoint durável e monotônico**: `silver_discovery_checkpoints` no namespace da Silver, uma
   linha por (pipeline, tabela Bronze), com `discovery_mode`, `run_id` e `committed_at`. É gravado
   **depois** de todas as unidades da execução terem sido escritas e marcadas, por `MERGE` que
   nunca diminui a versão.
3. **Pendente = descoberto − processado (+ reprocessamento explícito)**: um lote de uma execução
   que falhou no meio é relido (o checkpoint não andou) e pulado se já estiver marcado.
4. **Fallback para varredura completa, com motivo registrado**, quando o change data não serve:
   CDF desligado, histórico indisponível (retenção, `VACUUM`, versões anteriores ao CDF) ou
   checkpoint à frente da tabela (recriada ou restaurada). A tabela de controle mantém o resultado
   exato. `update`/`delete` no Bronze interrompe a execução (`BronzeContractError`).
5. **Tabelas Bronze novas nascem com CDF** (`delta.enableChangeDataFeed = true`, além de
   `delta.appendOnly`). Tabelas existentes sem CDF continuam funcionando pelo fallback. Ligar o CDF
   nelas é `ALTER TABLE` e exige autorização.
6. **Operação**: `rediscover=True` força a varredura completa (recuperação); `reprocess=[...]`
   processa de novo unidades já processadas, que precisam existir. Os MERGEs idempotentes mantêm a
   Silver igual.

## Garantias

| Garantia | Como |
|---|---|
| Não perder lotes novos | todo commit após o checkpoint é lido; o checkpoint só avança depois da marcação; o fallback cobre o change data ausente |
| Não reprocessar sem motivo | a tabela de controle filtra o que já foi feito; o reprocessamento só acontece por pedido explícito |
| Retry seguro | falha antes do checkpoint: a próxima execução relê o mesmo intervalo, menos o que foi marcado |
| Concorrência | suportada por idempotência (MERGEs da Silver e da tabela de controle) e checkpoint que nunca recua; conflito de commit do Delta (concorrência otimista) faz uma execução falhar, e o retry é seguro |
| Checkpoint só após persistência | é o último passo da execução, dentro do passo observado |
| Auditoria | `silver_processed_*` (`run_id` por unidade), histórico Delta do checkpoint, `discovery_mode` e motivo do fallback no log |

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| Tabela de controle operacional escrita pelo Bronze (lote, versão, status, tentativas) | dois commits (dados e controle) sem transação entre eles: um crash no meio deixa um lote no Bronze e fora do controle, invisível para a Silver; corrigir isso exige uma varredura de reconciliação, o custo que se quer evitar |
| `DESCRIBE HISTORY` + `userMetadata` com o `_batch_id` no commit | atômico, mas depende de a opção ser aceita pelo `DataFrameWriterV2` no serverless (não verificado) e não cobre os commits antigos |
| Timestamp de ingestão como watermark | perde lotes gravados durante a leitura (o bug corrigido no Financial, ADR 0002) |
| Structured Streaming `availableNow` com checkpoint | muda o modelo de execução da Silver (lotes ordenados, gate por lote) e o checkpoint de streaming é opaco para auditoria |
| Promover `financial_lakehouse/silver/progress.py` | ele lê as **linhas** alteradas com o schema do Financial e para em erro sem fallback; o Retail e o IoT precisam só das unidades e têm a tabela de controle para garantir correção. A consolidação fica como dívida (abaixo) |

## Consequências

- A descoberta custa os commits novos, não a tabela. **Não muda** o custo de ler as linhas de cada
  lote: `_prepare_batch` (Retail) e `_consume` (IoT) ainda filtram o Bronze por `_batch_id` ou
  `_file_id`. Ler as linhas pelo CDF do mesmo intervalo é o próximo passo, se a medição no
  Databricks justificar.
- Primeira execução, rediscovery e fallback custam uma varredura (como antes), registrada.
- Tabelas Bronze locais criadas antes desta mudança não têm CDF e ficam no fallback até um
  `ALTER TABLE` autorizado ou uma recriação.
- **Dívida:** dois mecanismos parecidos convivem (Financial, ADR 0002, e este). Unificar exige
  mudar o Financial, que tem testes e garantias próprios.
- **Verificação:** a lógica de decisão foi testada sem Delta (`shared/tests/unit`). O lado Spark/Delta
  (`shared/tests/integration/test_incremental_delta.py`, Silver do Retail e do IoT) só roda no Linux
  e ainda não rodou: a integração da CI está bloqueada (`docs/audits/ci-integration-memory.md`).
