---
name: dimensional-modeling
description: Use ao projetar ou alterar tabelas Gold — fatos, dimensões, agregados, grain, surrogate keys, natural keys, SCD, medidas aditivas/semi-aditivas e prevenção de double counting.
---

# Dimensional Modeling

## Regra inegociável

**Nenhuma fact table pode ser criada sem grain documentado.** O grain é declarado antes
de qualquer coluna, no topo do SQL/módulo e no `COMMENT` da tabela:

```sql
-- Grain: one row per order line (order_id, line_number).
COMMENT ON TABLE retail.gold.fact_sales IS 'Grain: one row per order line (order_id, line_number).';
```

## Definições

| Termo | Definição | Neste projeto |
|---|---|---|
| grain | o que uma linha representa, expresso pelas colunas que a tornam única | testado com regra de uniqueness `CRITICAL` |
| fact | eventos/medições de negócio no grain declarado; FKs para dimensões + medidas | `fact_<processo>` (ex.: `fact_sales`, `fact_sensor_reading`) |
| dimension | contexto descritivo para filtrar/agrupar (quem, o quê, onde, quando) | `dim_<entidade>` (ex.: `dim_customer`, `dim_date`) |
| natural key | identificador do sistema de origem | `<entidade>_id` (ex.: `customer_id`) |
| surrogate key | chave técnica da dimensão, estável e sem significado de negócio | `<entidade>_sk` |
| SCD | estratégia para mudanças em atributos de dimensão | tipo declarado por dimensão |
| additive measure | pode ser somada em todas as dimensões | `quantity`, `gross_amount` |
| semi-additive measure | somável em algumas dimensões, **nunca no tempo** | saldo, estoque, patrimônio de fundo |
| non-additive measure | não somável; recalcular a partir dos componentes | preço unitário, taxa (Selic), percentuais, médias |
| double counting | a mesma medida contada mais de uma vez por join/agregação errada | prevenido por reconciliação `CRITICAL` |

## Surrogate keys

- Determinísticas, para manter idempotência: hash da natural key
  (`F.xxhash64("customer_id")`; em SCD2, hash de `customer_id` + `valid_from`).
- Não usar `monotonically_increasing_id()` nem sequência dependente da ordem de execução.
- Membro "desconhecido" em toda dimensão (sk constante, ex.: `-1`), usado quando a FK do fato
  não encontra correspondência — o fato nunca é descartado por isso.

## SCD

| Tipo | Comportamento | Usar quando |
|---|---|---|
| 0 | nunca muda | atributos fixos (data de nascimento, data de cadastro) |
| 1 | sobrescreve | correções; histórico irrelevante |
| 2 | nova linha por mudança | análise precisa refletir o valor **da época** (segmento, região) |

SCD2: colunas `valid_from`, `valid_to` (`NULL` ou `9999-12-31` para a vigente — escolha uma
convenção por projeto), `is_current`; detecção de mudança com `<=>` (null-safe) sobre os
atributos rastreados; implementado com `MERGE`. Fato liga-se à versão vigente **na data
do evento** (`event_date BETWEEN valid_from AND valid_to`).

## Construindo um fato

1. Declarar o grain e as colunas que o garantem.
2. Listar dimensões e a cardinalidade de cada join (fato N:1 dimensão).
3. Classificar cada medida (aditiva / semi-aditiva / não aditiva).
4. Teste de uniqueness do grain e de FKs (sk desconhecida conta como métrica).
5. Teste de reconciliação: soma das medidas aditivas na Gold = Silver de origem.

## Double counting — causas e prevenção

| Causa | Prevenção |
|---|---|
| Join fato × fato de grains diferentes (pedido × item) | agregar ao grain comum antes do join |
| Join com dimensão não única na chave (SCD2 sem filtro de vigência) | filtrar versão vigente/na data; testar unicidade do lado "1" |
| Somar medida semi-aditiva no tempo | usar valor de fechamento do período (último/médio), nunca `sum` |
| Média de médias / soma de percentuais | recalcular a partir de numerador e denominador |
| Reprocessamento com `append` | `MERGE`/`replaceWhere` (idempotência) |

## Agregados (`agg_`)

Também têm grain declarado (ex.: "uma linha por loja por dia"), derivam de um `fact_`
(nunca da Silver diretamente, para manter uma única definição de métrica) e são
reconciliados com ele.
