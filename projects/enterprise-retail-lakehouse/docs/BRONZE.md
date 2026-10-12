# Bronze — Enterprise Retail Lakehouse

A Bronze guarda **o dado exatamente como a fonte entregou**, mais metadados de ingestão.
Nenhuma regra de negócio: duplicatas, NULLs, e-mails inválidos, preços negativos, FKs
órfãs e datas impossíveis chegam intactos e são tratados na Silver.

Código: `src/retail_lakehouse/bronze/` (+ `ingestion/readers.py`). Orquestração:
`notebooks/bronze/01_ingest_bronze.py`. Fontes: [SOURCE_SYSTEMS.md](SOURCE_SYSTEMS.md).

## Tabelas

Uma tabela Delta por dataset de origem, em `<namespace>.bronze_<dataset>`:

| Tabela | Fonte | Business key (não garantida na Bronze) |
|---|---|---|
| `bronze_categories` | `erp` CSV | `category_id` |
| `bronze_suppliers` | `erp` CSV | `supplier_id` |
| `bronze_stores` | `erp` CSV | `store_id` |
| `bronze_products` | `erp` CSV | `product_id` |
| `bronze_customers` | `crm` CSV | `customer_id` |
| `bronze_orders` | `ecommerce` JSONL | `order_id` |
| `bronze_order_items` | `ecommerce` JSONL | (`order_id`, `line_number`) |
| `bronze_payments` | `payments` JSONL | `payment_id` |
| `bronze_shipments` | `logistics` CSV | `shipment_id` |
| `bronze_inventory` | `erp` CSV | (`snapshot_date`, `store_id`, `product_id`) |
| `bronze_inventory_movements` | `erp` CSV | `movement_id` |

`namespace` vem de `config/bronze.toml`: `retail_bronze` (local) ou `retail.bronze`
(Unity Catalog; catálogo e Volume confirmados na Fase 1).

## Schema

- **Colunas da fonte**: exatamente as do contrato ([DATA_CONTRACTS.md](DATA_CONTRACTS.md)), na
  mesma ordem, **todas `STRING` e nuláveis** — sem inferência, cast ou trim. A tipagem é
  responsabilidade da Silver, onde valores inválidos vão para a quarentena.
- **Metadados** (não nulos):

| Coluna | Tipo | Conteúdo |
|---|---|---|
| `_ingestion_timestamp` | timestamp | instante da ingestão (UTC), injetado pelo orquestrador |
| `_source_file` | string | caminho completo do arquivo lido (`_metadata.file_path`) |
| `_batch_id` | string | lote da landing zone (`initial`, `batch_002`, …) |
| `_ingestion_date` | date | data UTC de `_ingestion_timestamp` |

## Garantias

| Garantia | Como |
|---|---|
| Preservação | leitura com schema texto explícito; CSV sem trim, `""` = NULL (convenção da fonte); nenhuma linha filtrada |
| Validação estrutural | CSV com `enforceSchema=false` (cabeçalho = contrato) e `FAILFAST`; JSON `FAILFAST` — estrutura errada falha a carga inteira (`CRITICAL`) |
| Idempotência | lote cujo `_batch_id` já existe na tabela é **ignorado** (`SKIPPED`), sem novo commit |
| Atomicidade | um commit Delta por lote; tabela nova via CTAS atômico — falha de leitura não deixa tabela vazia |
| Append-only | tabelas criadas com `delta.appendOnly = true`: `DELETE`/`UPDATE`/overwrite são rejeitados pelo Delta |
| Rastreabilidade | `_source_file` + `_batch_id` em toda linha; linhas escritas lidas das métricas do commit (sem `count()` extra) |

## Como executar

```python
from retail_lakehouse.bronze import ingest_all, load_bronze_settings

settings = load_bronze_settings(Path("config/bronze.toml"), "databricks")
results = ingest_all(
    spark,
    landing_root=settings.landing_root,
    batch_id="initial",
    target=settings.target,
    ingestion_timestamp=datetime.now(UTC),
)
```

No Databricks: notebook `notebooks/bronze/01_ingest_bronze.py` (widgets `environment`,
`batch_id`). Pré-requisito: a landing zone gerada no Volume
(`python -m retail_lakehouse.synthetic --output <volume>/...`, ver [DATA_GENERATION.md](DATA_GENERATION.md)).

## Testes

| Arquivo (`tests/`) | Garante | Onde roda |
|---|---|---|
| `unit/test_bronze_definitions.py` | 11 nomes de tabela, schemas, caminhos, namespace seguro (sem injeção SQL), settings | Windows e Docker |
| `transformation/test_bronze_transformation.py` | valores e duplicatas preservados, metadados, data UTC, schema, validações | Windows e Docker |
| `contract/test_bronze_reader.py` | leitura dos 11 arquivos reais: schema texto, cada linha e valor igual à fonte (anomalias incluídas), `_source_file`; cabeçalho divergente e JSON malformado falham | Windows e Docker |
| `integration/test_bronze_ingestion.py` | tabelas Delta: schema, preservação de linhas, metadados, reexecução sem commit, mesmo lote duas vezes, novo lote anexado, append-only, erro estrutural sem escrita | **Docker** (Delta exige Linux) |

```bash
docker build -f docker/tests.Dockerfile -t lakehouse-tests .
docker run --rm -v "${PWD}:/workspace" lakehouse-tests python -m pytest projects/enterprise-retail-lakehouse
```

## Limitações conhecidas

- Proteção contra reingestão assume **um escritor por tabela** (verificação seguida de
  escrita, não transacional entre jobs concorrentes).
- JSON Lines: o leitor JSON do Spark ignora chaves fora do schema, então `check_json_keys`
  compara as chaves entregues com o contrato antes da leitura. Chave nova ou chave ausente em
  todos os registros é mudança de contrato e falha a carga (`SourceContractError`, CRITICAL),
  como o cabeçalho do CSV. `null` explícito é dado. Isso custa uma passada a mais sobre o
  arquivo (antes da auditoria de conformidade, a chave nova era descartada em silêncio).
- Um arquivo por dataset e lote; ingestão incremental contínua (Auto Loader) depende de
  verificação no Free Edition.
- `_source_file` guarda o caminho absoluto do ambiente (local ou Volume).
