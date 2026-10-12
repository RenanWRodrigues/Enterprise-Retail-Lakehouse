---
name: medallion
description: Use ao criar ou modificar qualquer tabela ou transformação Bronze, Silver ou Gold, para decidir em que camada uma lógica pertence e o que é permitido ou proibido em cada uma.
---

# Medallion

Contrato resumido: `docs/ARCHITECTURE.md` §2. Esta skill define os limites operacionais.

## Bronze — raw + metadata

**Propósito:** cópia fiel da fonte, auditável e reprocessável.

PODE:
- ler com schema explícito (campos sujos como string);
- adicionar colunas técnicas: `_ingestion_timestamp` (UTC), `_source_file` ou `_source_request`,
  `_batch_id`, `_ingestion_date`;
- validar a **estrutura** (arquivo legível, colunas esperadas) — falha = `CRITICAL`;
- `append` idempotente (não reingerir o mesmo `_batch_id`/arquivo);
- tratar coluna nova/removida na fonte como **mudança de contrato**: a carga falha
  (`CRITICAL`) até o contrato ser atualizado — nunca `mergeSchema` silencioso.

NÃO PODE:
- filtrar, corrigir, deduplicar ou descartar registros;
- renomear colunas de negócio ou aplicar regra de negócio;
- fazer join com outras tabelas;
- sobrescrever (`overwrite`) histórico.

## Silver — validated + standardized

**Propósito:** entidades limpas, confiáveis, uma linha por chave natural (ou por evento).

PODE:
- tipar (cast explícito), padronizar nomes `snake_case`, normalizar texto/datas/timezone (UTC);
- deduplicar com critério determinístico;
- aplicar regras de qualidade e mover inválidos para `<tabela>_quarantine`;
- manter histórico SCD2 de entidades, quando o requisito pedir;
- joins de **enriquecimento técnico** N:1 (ex.: lookup de código → descrição);
- `MERGE` por chave natural; ler incrementalmente da Bronze (watermark/CDF).

NÃO PODE:
- agregar para métricas de negócio ou mudar o grain da entidade;
- descartar registros inválidos silenciosamente (sempre quarentena + métrica);
- conter lógica específica de um relatório/consumidor;
- `append` cego (reexecução duplicaria dados).

## Gold — business-ready

**Propósito:** modelos de consumo: `fact_`, `dim_`, `agg_`.

PODE:
- modelagem dimensional, agregações, KPIs, surrogate keys;
- joins entre entidades Silver com cardinalidade declarada;
- `MERGE` ou `replaceWhere` por intervalo/partição.

NÃO PODE:
- existir sem **grain documentado** (comentário no SQL/módulo + `COMMENT` da tabela);
- ler Bronze;
- usar `SELECT *`;
- produzir double counting (reconciliar totais com Silver);
- conter dados pessoais sem mascaramento (skill `security`).

## Decisão rápida: onde fica esta lógica?

| Lógica | Camada |
|---|---|
| Guardar a resposta da API como veio | Bronze |
| Converter `"1.234,56"` em decimal | Silver |
| Remover pedido duplicado | Silver |
| `status` fora do domínio | Silver (quarentena) |
| Receita líquida por loja/dia | Gold |
| Classificação de cliente (regra de negócio para consumo) | Gold |
| Dias úteis brasileiros como dimensão | Gold (`dim_date`) a partir de referência Silver |

## Invariantes entre camadas

- Fluxo só para frente: Bronze → Silver → Gold.
- Toda linha Silver é rastreável até Bronze (`_batch_id`/`_source_file` preservados).
- `linhas_bronze_lote = linhas_silver_validas + linhas_quarentena + duplicatas_removidas`
  (métrica de reconciliação registrada por execução).
- Reexecutar qualquer camada com a mesma entrada gera o mesmo estado (idempotência).
