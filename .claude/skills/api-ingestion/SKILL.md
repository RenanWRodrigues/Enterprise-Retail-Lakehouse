---
name: api-ingestion
description: Use ao ingerir dados de APIs HTTP (ex. Banco Central SGS, CVM dados abertos) para a camada Bronze — timeout, retry, backoff, paginação, rate limits, erros HTTP, validação de schema, ingestão incremental, watermarks e idempotência.
---

# API Ingestion

Pré-requisito: confirmar que o workspace tem acesso de saída ao domínio da API
(skill `databricks`). Sem acesso, a alternativa é pousar arquivos em um Volume.

## Arquitetura

```
cliente HTTP (I/O fino) ──▶ payload bruto + metadados ──▶ Bronze (append, _batch_id)
        ▲                                                       │
   watermark (tabela de controle)  ◀── atualizado após escrita ─┘
```

- Cliente HTTP isolado em um módulo e **injetável**: testes usam um fake, nunca a rede.
- Parsing do payload é função pura (`dict/str -> linhas`), testada como `unit`.
- Bronze guarda o registro como veio + `_ingestion_timestamp`, `_batch_id`, `_source_request`
  (endpoint e parâmetros **sem** credenciais).
- Biblioteca HTTP é dependência: justificar na Fase em que for adicionada.

## Timeout

- Toda requisição tem timeout explícito de conexão e de leitura (configuráveis). Nunca infinito.
- Timeout conta como falha transitória (entra no retry).

## HTTP errors, retry e backoff

| Resposta | Ação |
|---|---|
| 2xx | sucesso; validar payload |
| 429 Too Many Requests | retry respeitando `Retry-After` |
| 500, 502, 503, 504, timeout, conexão recusada | retry com backoff |
| 400, 401, 403, 404, 422 | **não** repetir; falhar com mensagem clara (sem expor credenciais) |

- Backoff exponencial com jitter: `min(base * 2**tentativa, teto) + aleatório`.
- Máximo de tentativas configurável; esgotado → exceção, nada é escrito, watermark não avança.
- Logar tentativa, status e espera — nunca headers, tokens ou URL com query de credencial.
- Em testes, o tempo de espera é injetado (sem `sleep` real).

## Pagination

- Identificar o tipo: offset/limit, número de página, cursor/token, ou janela de datas.
- Condição de parada explícita (página vazia, sem cursor, total atingido).
- Guarda contra loop infinito: limite máximo de páginas configurável + erro se cursor repetir.
- Para séries longas, preferir particionar por janela de datas (requisições menores, retomáveis).

## Rate limits

- Respeitar o limite documentado da API; intervalo mínimo entre chamadas configurável.
- Sem paralelismo agressivo; concorrência só se a API permitir explicitamente.
- Cache de nada que dependa de tempo; cada execução busca só a janela necessária.

## Schema validation

- Envelope da resposta (estrutura, campos obrigatórios, tipo de conteúdo) validado antes de
  escrever Bronze — violação = `CRITICAL` (para tudo, não grava lixo).
- Registros individuais não são rejeitados na Bronze; validação de valores acontece na Silver
  (skill `medallion`, `data-quality`).
- Campos novos inesperados: registrar em log/métrica; não quebrar a ingestão.

## Incremental ingestion e watermarks

- Watermark por fonte (ex.: última data de referência) persistido em tabela de controle Delta,
  não em arquivo local nem variável.
- Requisição cobre `(watermark - lookback, agora]`: o lookback recaptura revisões
  (séries do BCB podem ser revisadas) e atrasos.
- Watermark avança **somente após** a escrita Bronze bem-sucedida.
- Primeira carga (backfill) usa uma data inicial configurada, em janelas.

## Idempotency

- `_batch_id` determinístico: fonte + janela solicitada (não timestamp de execução).
- Reexecutar o mesmo `_batch_id` não duplica a Bronze (checar antes do append ou `MERGE`).
- Sobreposição causada pelo lookback é resolvida na Silver por `MERGE` na chave natural
  (ex.: `series_code + reference_date`), mantendo o valor mais recente.

## Testes mínimos

Fake client cobrindo: sucesso paginado, 429 com `Retry-After`, 5xx que se recupera,
5xx que esgota tentativas, 4xx sem retry, payload com envelope inválido, página vazia,
cursor repetido, watermark que não avança em falha, reexecução sem duplicar.
