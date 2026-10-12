# Data Dictionary (Gold) — Enterprise Retail Lakehouse

Todas as colunas das tabelas Gold. Fonte executável: `src/retail_lakehouse/gold/schemas.py`
(sincronia verificada por `tests/unit/test_gold_documentation.py`). Grain e relações:
[DIMENSIONAL_MODEL.md](DIMENSIONAL_MODEL.md); métricas: [BUSINESS_RULES.md](BUSINESS_RULES.md).

Convenções: `*_sk` = surrogate key BIGINT (`-1` = membro desconhecido); `*_key` de data =
`yyyymmdd` INT (`-1` = desconhecida); valores monetários em BRL, `DECIMAL(12,2)` (linhas
`DECIMAL(14,2)`).

### dim_date

| Coluna | Tipo | Descrição |
|---|---|---|
| `date_key` | int | `yyyymmdd`; PK |
| `date` | date | data do calendário |
| `year` | int | ano |
| `quarter` | int | trimestre (1–4) |
| `month` | int | mês (1–12); NULL no membro desconhecido |
| `year_month` | string | período de relatório `yyyy-MM`; `UNKNOWN_DATE` no membro desconhecido (-1) |
| `month_name` | string | nome do mês em português |
| `day` | int | dia do mês |
| `day_of_week` | int | 1 = segunda … 7 = domingo (ISO) |
| `day_name` | string | nome do dia em português |
| `week_of_year` | int | semana ISO |
| `is_weekend` | boolean | sábado ou domingo |
| `is_black_friday` | boolean | sexta após a 4ª quinta de novembro |

### dim_customer

| Coluna | Tipo | Descrição |
|---|---|---|
| `customer_sk` | bigint | PK; uma por versão (SCD2) |
| `customer_id` | string | natural key (CRM) |
| `segment` | string | `standard` / `premium` (rastreado, SCD2) |
| `city` | string | cidade (rastreado, SCD2) |
| `state` | string | UF (rastreado, SCD2) |
| `region` | string | região do Brasil (rastreado, SCD2) |
| `age_band` | string | faixa etária no início da versão (`18-24` … `65+`, `unknown`) — substitui a data de nascimento (PII) |
| `registered_date` | date | data de cadastro (Brasília) |
| `valid_from` | timestamp | início da versão (inclusivo) |
| `valid_to` | timestamp | fim da versão (exclusivo); `9999-12-31` na vigente |
| `is_current` | boolean | versão vigente |

### dim_product

| Coluna | Tipo | Descrição |
|---|---|---|
| `product_sk` | bigint | PK |
| `product_id` | string | natural key (ERP) |
| `product_name` | string | nome do produto |
| `brand` | string | marca |
| `category_id` | string | subcategoria (folha) |
| `category_name` | string | nome da subcategoria |
| `department_name` | string | departamento (categoria-pai) |
| `supplier_sk` | bigint | FK → `dim_supplier` |
| `supplier_id` | string | natural key do fornecedor |
| `list_price` | decimal(12,2) | preço de lista atual |
| `cost_price` | decimal(12,2) | custo atual |
| `is_active` | boolean | produto ativo |

### dim_store

| Coluna | Tipo | Descrição |
|---|---|---|
| `store_sk` | bigint | PK |
| `store_id` | string | natural key (ERP) |
| `store_name` | string | nome da loja |
| `store_type` | string | `physical` / `online` |
| `city` | string | cidade |
| `state` | string | UF |
| `region` | string | região do Brasil |
| `opened_date` | date | inauguração |

### dim_supplier

| Coluna | Tipo | Descrição |
|---|---|---|
| `supplier_sk` | bigint | PK |
| `supplier_id` | string | natural key (ERP) |
| `supplier_name` | string | razão social (fictícia) |
| `city` | string | cidade |
| `state` | string | UF |
| `lead_time_days` | int | prazo de entrega do fornecedor, em dias |

### fact_sales

Grain: uma linha de pedido (`order_id`, `line_number`), qualquer status.

| Coluna | Tipo | Descrição |
|---|---|---|
| `order_id` | string | dimensão degenerada; parte do grain |
| `line_number` | int | parte do grain |
| `date_key` | int | data do pedido (Brasília); `-1` se fora do calendário ou sem data |
| `order_date` | date | data do pedido (Brasília), preservada mesmo com `date_key = -1`; NULL sem pedido |
| `customer_sk` | bigint | versão do cliente vigente no pedido |
| `product_sk` | bigint | produto |
| `store_sk` | bigint | loja (CD online para `online`/`app`) |
| `supplier_sk` | bigint | fornecedor do produto |
| `channel` | string | `store` / `online` / `app` |
| `order_status` | string | status do pedido (filtrado pelas métricas) |
| `quantity` | int | unidades (aditiva) |
| `unit_price` | decimal(12,2) | preço unitário (não aditiva) |
| `gross_amount` | decimal(14,2) | `quantity × unit_price` (aditiva) |
| `discount_amount` | decimal(12,2) | desconto da linha (aditiva) |
| `net_amount` | decimal(14,2) | `gross_amount − discount_amount` (aditiva) |

### fact_payments

Grain: um evento de pagamento (`payment_id`).

| Coluna | Tipo | Descrição |
|---|---|---|
| `payment_id` | string | grain |
| `order_id` | string | dimensão degenerada |
| `date_key` | int | data do processamento (Brasília); `-1` se fora do calendário |
| `payment_date` | date | data do processamento (Brasília), preservada |
| `customer_sk` | bigint | cliente do pedido (versão vigente no pedido) |
| `store_sk` | bigint | loja do pedido |
| `payment_method` | string | `credit_card`, `debit_card`, `pix`, `boleto`, `cash` |
| `payment_status` | string | `approved`, `declined`, `refunded` |
| `installments` | int | parcelas |
| `amount` | decimal(12,2) | valor do evento (sempre positivo) |
| `signed_amount` | decimal(12,2) | efeito no caixa: +aprovado, −estorno, 0 recusado (aditiva) |

### fact_shipments

Grain: uma remessa (`shipment_id`).

| Coluna | Tipo | Descrição |
|---|---|---|
| `shipment_id` | string | grain |
| `order_id` | string | dimensão degenerada |
| `ship_date_key` | int | data de envio (Brasília); `-1` se não enviado ou fora do calendário |
| `ship_date` | date | data de envio (Brasília), preservada; NULL se não enviado |
| `delivery_date_key` | int | data de entrega; `-1` em trânsito ou fora do calendário |
| `delivery_date` | date | data de entrega (Brasília), preservada; NULL em trânsito |
| `customer_sk` | bigint | cliente do pedido (versão vigente no pedido) |
| `store_sk` | bigint | loja de origem (CD online) |
| `carrier_id` | string | transportadora (fictícia) |
| `shipment_status` | string | `in_transit`, `delivered`, `returned` |
| `freight_amount` | decimal(12,2) | frete cobrado do cliente (aditiva) |
| `delivery_days` | decimal(6,2) | dias entre pedido e entrega (não aditiva; NULL em trânsito) |

### fact_inventory_movements

Grain: um movimento de estoque (`movement_id`).

| Coluna | Tipo | Descrição |
|---|---|---|
| `movement_id` | string | grain |
| `date_key` | int | data do movimento (Brasília); `-1` se fora do calendário |
| `movement_date` | date | data do movimento (Brasília), preservada |
| `store_sk` | bigint | loja |
| `product_sk` | bigint | produto |
| `supplier_sk` | bigint | fornecedor do produto |
| `movement_type` | string | `opening_balance`, `purchase_receipt`, `sale`, `return` |
| `quantity` | int | unidades com sinal (aditiva; saldo derivado é semi-aditivo) |
| `reference_order_id` | string | pedido de origem em `sale`/`return` |
