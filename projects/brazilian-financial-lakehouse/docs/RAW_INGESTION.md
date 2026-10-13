# Raw Ingestion — Brazilian Financial Data Lakehouse

Primeira etapa do pipeline: buscar os indicadores nas APIs oficiais ([data_sources.md](data_sources.md))
e pousar as respostas **exatamente como recebidas**. A carga incremental no Bronze está em
[INCREMENTAL_STRATEGY.md](INCREMENTAL_STRATEGY.md). Silver e Gold ainda não existem.

## Camadas do código

```
notebooks/bronze/01_ingest_bronze.py   orquestração (widgets → settings → run → display)
        │
src/financial_lakehouse/
├── ingestion/   pipeline: catálogo → clientes → storage; watermark + lookback; relatório
├── clients/     HTTP: HttpClient (timeout, retry, backoff, status, logging) + SGS, PTAX, IBGE
├── transformation/  páginas raw → observações uniformes em TEXTO (funções puras)
├── storage/     landing zone (bytes + checksum), observações JSONL, manifest, watermarks
└── catalog.py   11 indicadores verificados (fonte, série/tabela, unidade, frequência)
```

Regras: notebooks nunca fazem HTTP; clientes não gravam nada; storage não conhece HTTP;
transformação não faz I/O nem converte tipos (isso é Silver).

## Cliente HTTP

| Requisito | Implementação |
|---|---|
| Timeout | `timeout_seconds` em toda requisição (padrão 30 s) |
| Retry | 429, 500, 502, 503, 504, timeouts, erros de conexão e leitura truncada (`IncompleteRead`: o GET é idempotente); até `max_attempts` (padrão 5); esgotado → `RetriesExhaustedError` com `last_status` (None = falha de transporte) |
| Backoff exponencial | `base × 2^(tentativa−1)`, teto `max_delay`, jitter ±25%; `Retry-After` respeitado (com teto) |
| Validação de status | 2xx ok; outros 4xx → `HttpStatusError` imediato (sem retry) |
| Logging | URL com valores de query que parecem segredo mascarados (`mask_secrets`), status, bytes, tentativa; aviso a cada retry; o corpo nunca é registrado |
| Paginação | SGS: janelas ≤ 10 anos · PTAX: janelas anuais + `$top`/`$skip` · IBGE: lotes de períodos explícitos |
| Compressão | `Content-Encoding` gzip (multi-membro)/deflate desfeito no transporte (IBGE envia gzip sem pedido); corrompido ou truncado → `ResponseFormatError`, sem retry |
| Limite de tamanho | `max_response_bytes` (padrão 32 MiB, `config/ingestion.toml`): `Content-Length` acima recusa antes de ler; leitura em blocos de 64 KiB para assim que passa; o corpo descomprimido também é limitado (gzip bomb); o cliente confere de novo qualquer transporte → `ResponseTooLargeError`, sem retry |
| Sem dependências | `urllib` (stdlib); transporte injetável → testes sem rede |

## Landing zone

```
<landing_root>/<fonte>/<indicador>/ingestion_date=<AAAA-MM-DD>/<run_id>-<nnnn>.json        bytes da resposta
<landing_root>/<fonte>/<indicador>/ingestion_date=<AAAA-MM-DD>/<run_id>.observations.jsonl  observações (texto)
<landing_root>/_manifests/<run_id>.json      URLs, SHA-256, bytes, registros, status por indicador
<landing_root>/_state/watermarks.json        última referência pousada por indicador
```

- `run_id` = instante UTC da execução (`AAAAMMDDThhmmssZ`); arquivos nunca são sobrescritos entre execuções.
- Números da PTAX ficam com o texto exato da API (`"6.42500"`, não `6.425`).

## Incremental, revisões e falhas

- Sem watermark: carga desde `backfill_start` (config).
- Com watermark: relê `lookback_days` (90) para fontes diárias/mensais do BCB e `lookback_periods` (6)
  para o IBGE — **revisões** de valores são recapturadas; a Silver resolverá versões por `MERGE`.
- O watermark avança **só depois** que os registros do indicador foram aceitos pelo Bronze.
- Um indicador que falha (HTTP, schema, disco) é registrado no relatório/manifest e **não** interrompe os outros;
  ao final, a execução levanta `IngestionFailedError` (detalhes em INCREMENTAL_STRATEGY.md).

## Como executar

```bash
PYTHONPATH="projects/brazilian-financial-lakehouse/src;shared/src" \
  .venv/Scripts/python -m financial_lakehouse.ingestion --env local [--backfill-start 2026-01-01] [--indicator ipca_monthly]
```

Databricks: `notebooks/bronze/01_ingest_bronze.py` (acesso de saída à internet do Free Edition não confirmado).

## Execução real (validação)

Executada em 2026-10-06 com `--backfill-start 2026-01-01` contra as APIs oficiais:

| Indicador | Observações | Watermark |
|---|---:|---|
| selic_target | 279 | 2026-10-06 |
| selic_daily | 190 | 2026-10-05 |
| ipca_monthly | 8 | 202608 |
| igpm_monthly | 9 | 2026-09-01 |
| gdp_growth_yoy | 2 (1ª execução) | 202602 |
| usd_brl_sell | 190 | 2026-10-05 |
| eur_brl_ptax | 950 (vários boletins/dia) | 2026-10-06 |
| international_reserves | 190 | 2026-10-05 |
| monetary_base | 8 | 2026-08-01 |
| credit_total | 8 | 2026-08-01 |
| unemployment_rate | 8 | 202608 |

A primeira execução revelou que o IBGE responde com gzip sem ser pedido (2 indicadores falharam
com "response is not JSON"); corrigido no transporte, com teste de regressão, e reexecutado com sucesso.

## Testes (sem rede)

| Arquivo | Cobre |
|---|---|
| `tests/unit/test_fin_http_robustness.py` | `IncompleteRead` transitório e persistente, timeouts limitados, 429/500 esgotados com `last_status`, 404 sem retry, corpo vazio, gzip/deflate corrompido e truncado, gzip multi-membro, limites (declarado, em streaming, descomprimido, no cliente), logs sem credenciais nem payload, configuração validada |
| `tests/unit/test_fin_http_client.py` | sucesso, retry por status/timeout/conexão, 4xx sem retry, limite de tentativas, backoff exponencial + jitter, `Retry-After`, timeout repassado, JSON inválido, logging, só https, URL com sintaxe OData/IBGE, gzip/deflate |
| `tests/unit/test_fin_source_clients.py` | URLs documentadas, janelas de 10 anos, 404 "Value(s) not found", paginação PTAX, janelas anuais, períodos IBGE, classificação, lotes, validação de schema |
| `tests/unit/test_fin_raw_pipeline.py` | observações em texto (decimais exatos), landing com checksum, watermarks, execução completa, isolamento de falha, incremental com lookback (BCB e IBGE) |
| `tests/unit/test_fin_catalog.py` | 11 indicadores, cada um documentado com identificador oficial e link; endpoints e data de acesso no documento |

Payloads usados pelos mocks: respostas reais capturadas em 2026-10-05 (`tests/unit/fixtures/`).
