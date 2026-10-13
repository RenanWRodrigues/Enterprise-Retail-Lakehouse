# R3 — qualidade de estoque: snapshot × movimentos (2026-10-12)

Decisão: **R3 passa a ser um gate de produção**, avaliado pela Gold do Retail antes da publicação.
`silver_inventory` continua no modelo, como entrada da regra. Nenhuma tabela publicada nova.

## 1. Diagnóstico

### Onde a R3 estava

| Local | O que verificava | Limite |
|---|---|---|
| `projects/enterprise-retail-lakehouse/docs/BUSINESS_RULES.md` §3 | "Para cada (loja, produto): Σ `fact_inventory_movements.quantity` = `inventory.quantity_on_hand` da foto em `end_date`" | só documentação |
| `tests/data_quality/test_gold_reconciliation.py::test_r3_inventory_movements_add_up_to_the_snapshot` | saldo da Gold por `store_sk`/`product_sk` × snapshot, por natural key | dados **sem anomalias**; soma **todos** os movimentos; só Linux |
| `tests/contract/test_referential_expectations.py` | mesma igualdade nas linhas do gerador | gerador limpo |
| `tests/transformation/test_gold_kpis.py::test_inventory_balance_equals_snapshot_total` | total de `inventory_balance_by_store.sql` = total do snapshot | total geral, só Linux |

Nenhum desses testes rodava no pipeline. Uma divergência real seria publicada sem alerta.

### `silver_inventory`

- Fonte contratada: dataset `inventory` do ERP (`ingestion/source_contracts.py`), tão contratado
  quanto `inventory_movements`.
- Grain: uma linha por (`snapshot_date`, `store_id`, `product_id`), a chave de negócio
  (`BUSINESS_KEYS`), única na Silver.
- Semântica: estoque **no fim** de `snapshot_date`. O gerador emite o snapshot depois do último
  movimento da janela, que termina em `end_date 23:59:59` de Brasília. No modo incremental não há
  snapshot, e os lotes trazem movimentos **posteriores** a ele.

### Classificação

A R3 é uma **regra de reconciliação entre duas fontes de entrada** (snapshot × razão de
movimentos), não uma regra do modelo Gold. Mesmo assim, ela pertence ao gate da Gold, porque:

- precisa das duas tabelas da Silver juntas (a Gold pode ler a Silver; a Silver processa uma
  entidade por vez);
- protege uma métrica publicada pela Gold (`sql/gold/inventory_balance_by_store.sql`).

### Por que a formulação antiga não servia como gate

1. **Sem corte por data:** depois de um lote incremental, a soma de todos os movimentos já não é o
   saldo da data do snapshot. A regra antiga falharia em todo lote incremental (falso positivo).
2. **Pelas surrogate keys da Gold:** um movimento de produto quarentenado na Silver fica com
   `product_sk = -1` e perde a ligação com o produto. A regra antiga acusaria divergência que não
   existe. A natural key continua correta na Silver.

### Os dados existem em todos os ambientes?

- `silver_inventory` e `silver_inventory_movements` são criadas pela Silver em todo ambiente
  (`ensure_tables`).
- Snapshot ausente (por exemplo, só lotes incrementais): **não há o que reconciliar**. A regra
  avalia zero linhas e passa. Isso é deliberado: sem foto, nenhuma igualdade é afirmada.

## 2. Regra exata

Para cada `snapshot_date` presente em `silver_inventory` e cada (`store_id`, `product_id`)
presente no snapshot **ou** na Silver de movimentos até aquela data:

```text
coalesce(quantity_on_hand, 0) = coalesce(Σ quantity dos movimentos com
                                         business_date(movement_timestamp) ≤ snapshot_date, 0)
```

- `business_date`: data local de Brasília (`gold.keys.business_date`, UTC−3 fixo), a mesma usada
  nos fatos.
- Chave ausente do snapshot conta como **0 em estoque**: o snapshot cobre o sortimento. Um
  produto que se moveu e voltou a zero pode faltar no snapshot; um com saldo não pode.
- Chave sem movimentos conta como **0 movimentado**.
- Tolerância: **0 unidade**. As quantidades são inteiras, então não há arredondamento a absorver.

Implementação: `retail_lakehouse.gold.inventory.inventory_reconciliation` (função pura
`DataFrame -> DataFrame`, colunas `quantity_on_hand`, `movement_balance`, `difference`) e
`retail_lakehouse.quality.suites.inventory_reconciliation_suite`.

## 3. Severidade e efeito

| Check | Condição | Severidade | Efeito |
|---|---|---|---|
| `r3__snapshot_matches_movements` | alguma chave com `difference <> 0` | **ERROR** | alerta, `SUCCEEDED_WITH_ALERTS`, Gold publicada |
| `r3__snapshot_matches_movements_systemic` | mais de **5%** das chaves divergem | **CRITICAL** | gate falha antes de publicar; a Gold anterior fica intacta; run `FAILED` |

Justificativa:

- **Divergência isolada:** é achado de controle de estoque (erro de contagem, movimento não
  registrado ou quarentenado). Não invalida vendas, pagamentos nem entregas, então bloquear a Gold
  inteira por isso seria desproporcional.
- **Divergência ampla:** indica carga quebrada (razão ou snapshot incompleto). Nesse caso a
  métrica de estoque publicada estaria errada.
- **Limiar de 5%:** é o mesmo usado para órfãos sistêmicos (decisões 047 e 076).
- **No gerador:** a anomalia `invalid_quantity` (0,05% dos movimentos, quantidade zerada e
  quarentenada) produz exatamente o primeiro tipo de divergência.

## 4. Onde roda

`write_gold(..., inventory_reconciliation=...)` → `validate_gold` → `retail_gold_suite(staged,
inventory_reconciliation=...)`: o check set `inventory_reconciliation` entra depois das tabelas,
no mesmo passo observado do gate. Os resultados vão para `data_quality_results`; um CRITICAL
impede a publicação, como os demais checks do gate.

- A reconciliação **não** é publicada nem entra nas contagens de linhas da Gold
  (`_record_gate` soma só as tabelas de `GOLD_SCHEMAS`).
- O notebook de produção (`notebooks/gold/01_build_gold.py`) passa a reconciliação. O teste
  `tests/unit/test_gold_r3_wiring.py` falha se ele deixar de passar. O parâmetro é opcional em
  `write_gold` porque os testes de integração constroem a Gold sem a Silver de estoque.

## 5. Evidências (Windows, 2026-10-12)

| Escopo | Exit | Resultado |
|---|---|---|
| `tests/data_quality/test_inventory_r3.py` antes da implementação | 2 | erro de coleta: `retail_lakehouse.gold.inventory` não existia |
| mesmo arquivo depois (9 testes: válido, violação, snapshot sem movimentos, sem snapshot, estoque fora do snapshot, corte por data e reprocessamento com lotes posteriores, falha antes da publicação, inclusão no gate, dados do gerador) | 0 | 9 passed |
| `tests/unit/test_gold_r3_wiring.py` + `tests/contract` (raiz) | 0 | 244 passed, 2 skipped |
| ruff, format, mypy strict | 0 | verdes |

| `test_retail_gold_suite.py` + `test_gold_calendar_coverage.py` (gate com a assinatura nova) | 0 | 73 passed |
| `tests/unit` do Retail + `tests/contract` (contrato de documentação incluído) | 0 | 394 passed, 2 skipped |

## 6. Limitações

- **Publicação real:** a falha antes da publicação foi provada no nível do gate (`CriticalQualityError`
  no passo observado). A publicação de ponta a ponta com Delta (`write_gold`) só roda na
  integração, que está bloqueada na CI (`docs/audits/ci-integration-memory.md`).
- **Teste do notebook:** o teste de ligação foi escrito depois da mudança no notebook. Ele protege
  contra regressão, mas não foi visto falhar antes.
- **Testes antigos:** os três testes existentes da R3 continuam como estão (dados limpos). Eles não
  contradizem a regra nova, porque nos dados limpos não há movimentos posteriores ao snapshot.
