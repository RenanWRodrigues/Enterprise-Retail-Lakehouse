# 0003 — Chave durável do cliente para eventos fora da validade SCD2

Status: aceito
Data: 2026-10-12
Altera: contrato da Gold do Retail (`dim_customer`, `fact_sales`, `fact_payments`, `fact_shipments`)

## Contexto

`dim_customer` é SCD Type 2. A primeira versão de cada cliente começa em `registered_at` (a data de
cadastro, vinda do CRM), e os fatos se ligam à versão vigente no instante do evento
(`valid_from ≤ t < valid_to`). Um pedido com horário **anterior ao cadastro** (relógio do sistema
de origem, compra como convidado antes de criar a conta, cadastro corrigido depois) não encontra
versão e recebe `customer_sk = -1`. Como o fato não guarda nenhuma outra referência ao cliente, a
venda perdia a ligação com ele: não entrava em "clientes distintos" nem no histórico do cliente.

O gerador sintético não produz esse caso; o teste de regressão o reproduz.

## Decisão

1. `dim_customer` ganha `customer_durable_key = xxhash64('customer_identity', customer_id)`: a
   mesma em todas as versões do cliente, `-1` no membro desconhecido.
2. `fact_sales`, `fact_payments` e `fact_shipments` ganham `customer_durable_key`, resolvida por
   `customer_id` (N:1 sobre os pares distintos da dimensão) e **independente do instante do
   evento**. Cliente inexistente: `-1`.
3. `customer_sk` continua point-in-time e não muda. Antes da primeira versão, ele é `-1`
   (versão desconhecida, contado no WARNING `unknown_member__customer_sk`); nenhum evento recebe
   uma versão posterior a ele.
4. O gate da Gold ganha a FK `customer_durable_key → dim_customer.customer_durable_key`
   (CRITICAL, como as demais).

"Cadastro" (`registered_at`, atributo do cliente) e "início da disponibilidade histórica dos
atributos" (`valid_from` da primeira versão) continuam iguais. A diferença é tratada pela chave
durável, não pela alteração do intervalo.

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| Primeira versão desde `1900-01-01` (`valid_from` aberto no passado; prática comum em Kimball) | atribui ao evento atributos (segmento, cidade) registrados **depois** dele: equivale a ligá-lo a uma versão futura e amplia o intervalo sem evidência |
| Ligar o evento à primeira versão quando não há versão vigente | mesmo problema, escondido na regra do join |
| Manter `-1` e só documentar | não preserva a identidade, que era o objetivo |
| `customer_id` (natural key) direto nos fatos | funciona, mas expõe a chave do sistema de origem como coluna do fato; a chave durável segue o padrão de surrogate keys do modelo e tem membro desconhecido |
| Membro especial "antes do cadastro" (`customer_sk = -2`) | outro valor reservado em todos os consumidores, e ainda sem identidade |

## Consequências

- **Mudança de contrato** (colunas novas, nenhuma removida ou renomeada). Consultas com colunas
  explícitas continuam funcionando; `SELECT *` sobre esses fatos passa a trazer mais uma coluna.
- Contagem de clientes distintos e histórico por cliente: usar `customer_durable_key`. Quebras por
  atributo: `customer_sk` (sem mudança). Atributos atuais: join por `customer_durable_key` com
  `is_current`.
- Reconstruir a Gold gera as mesmas chaves (determinísticas); nenhuma migração de dados, porque a
  Gold é reconstruída a cada execução.
- O WARNING `unknown_member__customer_sk` passa a incluir eventos anteriores ao cadastro de
  clientes existentes; para separar os dois casos, filtre `customer_durable_key <> -1`.
