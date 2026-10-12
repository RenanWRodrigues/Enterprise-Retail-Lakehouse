# 0002 — Progresso da Silver financeira por versão Delta (Change Data Feed)

Status: aceito
Data: 2026-10-08
Substitui: o watermark por `max(bronze_ingestion_timestamp)` com comparação `>=` (M6)

## Contexto

A Silver financeira lia "as chaves com linhas no Bronze a partir do watermark", sendo o watermark
o maior `bronze_ingestion_timestamp` já na Silver. Todas as linhas de uma execução do Bronze têm
o mesmo `ingestion_timestamp` (o início da execução), então:

- com `>`, linhas que uma execução do Bronze gravasse **depois** de uma leitura da Silver eram
  puladas para sempre (bug corrigido na campanha de testes);
- com `>=` (a correção), toda execução relia a última execução do Bronze inteira e reavaliava
  as mesmas linhas: a quarentena não duplicava (insert-only), mas **os alertas se repetiam** (M6).

Havia ainda duas outras fontes de alerta repetido: o check de lacunas avaliava a tabela inteira a
cada execução, e a plausibilidade avaliava vencedores que não tinham mudado.

O progresso não pode depender só do relógio de ingestão: duas escritas na mesma execução do
Bronze são indistinguíveis por ele.

## Decisão

1. **Checkpoint durável por versão Delta.** `silver_checkpoints` guarda, por (pipeline, tabela
   Bronze), a última versão do Bronze cujas mudanças foram publicadas **e** reconciliadas.
2. **Versão final estável.** No início, a execução captura a última versão do Bronze (`end`);
   lê as mudanças de `(checkpoint, end]` pelo Change Data Feed (`table_changes`) e o Bronze
   `VERSION AS OF end`. Commits feitos durante a execução ficam para a próxima.
3. **Entrada.** Todas as versões das chaves tocadas por essas mudanças (para a revisão vencedora
   e o `revision_count`), lidas na versão `end`.
4. **Ordem.** Evidência de reconciliação → gate (CRITICAL para antes de gravar) → `MERGE` da
   Silver → `MERGE` da quarentena → check de lacunas → **checkpoint**. O checkpoint só avança
   depois de publicar e contabilizar.
5. **Contrato do Bronze.** O Bronze é append-only (MERGE insert-only de versões novas), então seu
   feed só tem `insert`. `update`/`delete` no intervalo param a execução (`BronzeContractError`).
6. **Alerta uma vez por problema.** Identidade determinística, lida do estado que o pipeline já
   mantém:

   | Alerta | Identidade | Já registrado quando |
   |---|---|---|
   | regras de linha (ERROR) | (`record_key`, regra) | a `record_key` já está na quarentena |
   | plausibilidade (WARNING) | (`record_key`, regra) | a mesma versão já está na Silver |
   | lacunas (WARNING) | (indicador, datas em volta do buraco) | nenhuma das duas pontas é chave nova |

   A `record_key` já codifica origem, série, referência, valor e atributos: é a versão do
   registro. Checks CRITICAL continuam avaliando o lote inteiro.
7. **Falhas explícitas com procedimento** (a mensagem traz o procedimento):
   - checkpoint depois da última versão (`CheckpointAheadError`: tabela recriada ou restaurada);
   - Bronze sem CDF (`ChangeFeedDisabledError`; habilitar é alteração de tabela, só com
     autorização);
   - change data indisponível por retenção, `VACUUM` ou CDF habilitado depois
     (`ChangesUnavailableError`).

   Recuperação: apagar a linha do checkpoint e reexecutar. Sem checkpoint, a execução lê o Bronze
   inteiro na versão atual; os `MERGE`s idempotentes mantêm Silver e quarentena, e a identidade
   dos alertas evita realertar o que já está registrado.
8. As tabelas Bronze novas são criadas com `delta.enableChangeDataFeed = true`.

## Garantias obtidas (e o que não é garantido)

| Situação | Comportamento | Evidência |
|---|---|---|
| Commit no Bronze durante a Silver | lido na execução seguinte; nenhuma linha perdida | `test_a_bronze_commit_during_the_run_waits_for_the_next_run` |
| Falha antes de publicar | nada gravado; checkpoint parado; o retry processa o mesmo intervalo | `test_a_failure_before_publication_...` |
| Falha depois de publicar e antes do checkpoint | o retry repete o intervalo; `MERGE`s sem efeito; nenhum alerta repetido | `test_a_failure_after_publication_...` |
| Execução sem commits novos | nada lido, `SUCCEEDED`, sem alerta | `test_first_run_then_no_commit_...` |
| Problema novo | alertado uma vez (inclusive lacuna nova) | idem |
| `delete` no Bronze | execução para, checkpoint parado | `test_a_deleted_bronze_row_stops_the_run` |
| Checkpoint impossível / change data ausente | execução para com o procedimento | `test_a_checkpoint_after_...`, `test_versions_without_change_data_...` |

**Efeitos sem duplicação, não exactly-once absoluto.** O que se garante é:

- **at-least-once no processamento** de cada commit do Bronze (um intervalo pode ser repetido);
- **efeitos idempotentes** na Silver (`MERGE` por chave e `record_key`), na quarentena
  (insert-only por `record_key` + motivos) e no checkpoint (`MERGE` que nunca volta atrás).

O resultado observável na Silver é o de processar cada mudança uma vez. Não há transação entre
tabelas no Delta:

- **`pipeline_runs`, `data_quality_results` e `silver_reconciliation` são logs:** cada tentativa
  grava as suas linhas, inclusive a que falhou;
- **um alerta emitido por uma tentativa que falhou antes de publicar é emitido de novo no
  retry,** porque o problema ainda não estava registrado;
- **Silver e quarentena são dois commits:** uma falha entre eles é completada pelo retry;
- **duas execuções simultâneas da mesma Silver não são suportadas:** o checkpoint não é um lock,
  e os dois `MERGE`s concorrentes podem conflitar (o Delta rejeita um deles).

## Alternativas consideradas

- **Manter `>=` e deduplicar só os alertas** (log de alertas por identidade): resolve a
  repetição, mas o progresso continua dependendo do relógio de ingestão e relendo a última
  execução inteira.
- **Progresso por diferença entre versões, sem CDF** (Bronze `VERSION AS OF end` menos
  `VERSION AS OF checkpoint`, anti-join por `record_key`): funciona porque o Bronze é append-only,
  mas lê o Bronze inteiro duas vezes por execução e não detecta `update`/`delete`.
- **Structured Streaming com `readChangeFeed` e checkpoint de streaming**: é a forma nativa, mas o
  checkpoint fica em arquivos fora das tabelas, a execução em lote (`availableNow`) muda o modelo
  de testes, e a lógica de revisões precisa de todas as versões da chave, não só das mudanças.

## Consequências

- O Bronze precisa estar em Delta com CDF: tabelas existentes precisam de
  `ALTER TABLE ... SET TBLPROPERTIES (delta.enableChangeDataFeed = true)` (alteração de tabela, só
  com autorização) e de um reset do checkpoint, porque o feed só existe a partir dessa versão.
- A retenção do change data segue a do Delta Log e o `VACUUM`. Uma Silver parada por mais tempo
  que a retenção precisa do procedimento de recuperação.
- `SilverRunReport.since` virou `after_version` / `bronze_version`, e `silver_reconciliation`
  passou a registrar `from_version` (exclusiva) e `bronze_version` (inclusiva) no lugar de
  `watermark_since`.
- **Verificado só localmente** (Delta 4.0.1 no Linux). O suporte a CDF e `table_changes` no
  serverless do Free Edition está documentado pela Databricks para tabelas gerenciadas do Unity
  Catalog, mas **não foi executado** aqui.
