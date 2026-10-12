# Business Rules — Enterprise Retail Lakehouse

Toda métrica da Gold tem definição aqui **antes** de existir em código ou SQL. Métrica sem
definição neste documento não deve ser publicada. Modelo: [DIMENSIONAL_MODEL.md](DIMENSIONAL_MODEL.md).

## 1. Conceitos

| Conceito | Regra |
|---|---|
| Data de negócio | data local de Brasília (UTC−3) do evento; `date_key = yyyymmdd` |
| Moeda | BRL, 2 casas decimais, aritmética decimal exata (nunca float) |
| Pedido **cancelado** | `order_status = 'cancelled'`: não gera receita, estoque nem pagamento aprovado |
| Pedido **devolvido** | `order_status = 'returned'`: foi entregue e depois estornado integralmente (itens + frete) |
| Pedido **mantido** | status em `created`, `paid`, `shipped`, `delivered` — base das métricas de receita líquida |
| Valor bruto da linha | `gross_amount = quantity × unit_price` |
| Valor líquido da linha | `net_amount = gross_amount − discount_amount` |
| Frete | cobrado por pedido (online/app abaixo de R$ 299); vive em `fact_shipments`, nunca em `fact_sales` |
| Valor de caixa do pagamento | `signed_amount`: `approved` = +`amount`; `refunded` = −`amount`; `declined` = 0 |

## 2. KPIs

| KPI | Definição (fonte) | Escopo | Observações |
|---|---|---|---|
| `gross_revenue` | Σ `fact_sales.gross_amount` | pedidos não cancelados (inclui devolvidos) | receita antes de descontos e devoluções; frete excluído |
| `discount_amount` | Σ `fact_sales.discount_amount` | pedidos não cancelados | descontos promocionais concedidos |
| `returned_amount` | Σ `fact_sales.net_amount` | pedidos devolvidos | componente de `net_revenue` |
| `net_revenue` | `gross_revenue − discount_amount − returned_amount` ≡ Σ `net_amount` dos pedidos mantidos | pedidos mantidos | receita de produtos reconhecida; frete excluído |
| `quantity_sold` | Σ `fact_sales.quantity` | pedidos mantidos | unidades líquidas (sem canceladas e devolvidas) |
| `average_order_value` | `net_revenue / COUNT(DISTINCT order_id)` | pedidos mantidos | ticket médio de produtos, sem frete; calculado no grain do pedido |
| `shipping_cost` | Σ `fact_shipments.freight_amount` | remessas `in_transit` ou `delivered` | frete **cobrado** dos clientes em remessas não devolvidas; o custo real da transportadora não existe nas fontes |
| `delivery_days` | média de `fact_shipments.delivery_days` | remessas `delivered` ou `returned` (ambas foram entregues) | `delivery_days = (delivered_at − order_timestamp)` em dias, 2 casas: prazo percebido pelo cliente |

Regras de cálculo:

- Métricas não aditivas (`average_order_value`, `delivery_days`, preço médio) são **recalculadas** a
  partir dos componentes em cada recorte — nunca média de médias.
- Quebras por atributos de cliente usam `customer_sk` (versão vigente no pedido), nunca
  `customer_id`: juntar por `customer_id` multiplicaria linhas de clientes com várias versões SCD2.
- Fatos de grains diferentes só se combinam **depois** de agregados ao mesmo grain (ex.: pedido).

## 3. Reconciliações (executadas como testes)

### R1 — Gold × Silver (completude)

| Verificação | Igualdade exigida |
|---|---|
| Linhas por fato | `fact_sales` = `order_items`; `fact_payments` = `payments`; `fact_shipments` = `shipments`; `fact_inventory_movements` = `inventory_movements` |
| Totais | Σ `quantity`, `gross_amount`, `discount_amount`, `net_amount` de `fact_sales` = Silver; Σ `amount` de `fact_payments` = Silver; Σ `freight_amount` = Silver; Σ movimentos = Silver |

### R2 — Caixa × receita, por pedido

| Status do pedido | Σ `signed_amount` do pedido deve ser igual a |
|---|---|
| `delivered`, `shipped` | Σ `net_amount` + frete da remessa (0 para pedidos de loja) |
| `paid` | Σ `net_amount` + `orders.freight_amount` (frete cobrado; remessa ainda não emitida) |
| `returned` | 0 (aprovado − estorno integral) |
| `cancelled` | 0 (apenas tentativa recusada) |
| `created` | sem pagamento |

Consolidado:
`Σ signed_amount = net_revenue − net_amount(created) + shipping_cost + frete_pendente(paid)`.

### R3 — Estoque

- Para cada (loja, produto): Σ `fact_inventory_movements.quantity` = `inventory.quantity_on_hand`
  da foto em `end_date` (saldo é semi-aditivo: soma no tempo é proibida, soma de movimentos é a regra).
- −Σ `quantity` dos movimentos `sale` = Σ `fact_sales.quantity` dos pedidos não cancelados.

## 4. Classificações

| Atributo | Regra |
|---|---|
| `age_band` (`dim_customer`) | idade em `valid_from`: `18-24`, `25-34`, `35-44`, `45-54`, `55-64`, `65+`; `unknown` sem data de nascimento ou com idade < 18 (dado inválido) |
| `is_black_friday` (`dim_date`) | sexta-feira após a 4ª quinta-feira de novembro |
| `is_weekend` (`dim_date`) | sábado ou domingo |
