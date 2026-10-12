# Source Systems — Enterprise Retail Lakehouse

Os dados são **sintéticos** (gerador determinístico, ver [DATA_GENERATION.md](DATA_GENERATION.md))
e simulam cinco sistemas de uma rede varejista brasileira fictícia com 29 lojas físicas e
um centro de distribuição de e-commerce. Contratos coluna a coluna:
[DATA_CONTRACTS.md](DATA_CONTRACTS.md). Código dos contratos:
`retail_lakehouse/ingestion/source_contracts.py`.

## Sistemas

| Sistema | Código | Datasets | Formato | Característica simulada |
|---|---|---|---|---|
| ERP | `erp` | `categories`, `suppliers`, `stores`, `products`, `inventory`, `inventory_movements` | CSV | cadastro mestre e estoque; preços com valores negativos por erro de cadastro; quantidades inválidas em movimentos |
| CRM | `crm` | `customers` | CSV | exportação com registros duplicados, e-mails nulos/inválidos, datas de nascimento ausentes ou impossíveis |
| E-commerce / OMS | `ecommerce` | `orders`, `order_items` | JSON Lines | pedidos de todos os canais (loja, site, app); itens com quantidade/preço inválidos e linhas duplicadas; timestamps malformados |
| Gateway de pagamento | `payments` | `payments` | JSON Lines | eventos reenviados (pagamento duplicado), tentativas recusadas legítimas, estornos |
| Logística | `logistics` | `shipments` | CSV | entregas de pedidos online; datas de entrega anteriores ao envio |

## Landing zone

```
<landing_root>/
├── _manifest_<batch_id>.json        configuração, contagens, checksums do lote (sem timestamps)
├── _anomalies_<batch_id>.csv        registro de toda anomalia injetada no lote
├── erp/<dataset>/<dataset>_<batch_id>.csv
├── crm/customers/customers_<batch_id>.csv
├── ecommerce/<dataset>/<dataset>_<batch_id>.jsonl
├── payments/payments/payments_<batch_id>.jsonl
└── logistics/shipments/shipments_<batch_id>.csv
```

- Local: `data/landing/retail/` (ignorado pelo Git). No Databricks: um Volume UC
  (`/Volumes/<catalog>/<schema>/<volume>/landing/`) — definido junto com a Bronze.
- `batch_id` padrão: `initial` (carga histórica completa). Lotes incrementais ficam para
  quando a Bronze incremental for implementada.

## Convenções de serialização (todas as fontes)

| Aspecto | Convenção |
|---|---|
| Encoding | UTF-8, fim de linha `\n` |
| CSV | cabeçalho na primeira linha, separador `,`, aspas quando necessário; vazio = NULL |
| JSON Lines | um objeto por linha; `null` = NULL; **todos os valores serializados como texto** (tipagem acontece na Silver) |
| Decimal | ponto como separador decimal, 2 casas (`1299.90`) |
| Data | ISO 8601 `yyyy-MM-dd` |
| Timestamp | ISO 8601 com offset de Brasília `yyyy-MM-ddTHH:mm:ss-03:00` (sem horário de verão desde 2019) |
| Booleano | `true` / `false` |
| Moeda | BRL |

## O que cada sistema garante (e não garante)

- **Garantido pelo contrato**: nomes e ordem das colunas, formato do arquivo, convenções acima.
- **Não garantido** (a Silver deve tratar): unicidade das chaves, integridade referencial,
  domínio dos valores, coerência entre datas. As violações são intencionais, controladas por
  configuração e registradas em `_anomalies_<batch_id>.csv`.
