---
name: architecture
description: Use ao criar módulos, pacotes, notebooks ou configuração, ao decidir onde um código deve morar (shared vs projects, src vs notebooks), ao mudar fronteiras entre componentes ou ao tomar uma decisão arquitetural que pode exigir ADR.
---

# Architecture

Referência completa: `docs/ARCHITECTURE.md` (layout, nomes, decisões).

## Onde cada coisa mora

| Tipo de código | Local | Teste |
|---|---|---|
`<pkg>` = `retail_lakehouse` | `iot_lakehouse` | `financial_lakehouse`.

| Tipo de código | Local | Teste (`projects/<p>/tests/…`) |
|---|---|---|
| Ler fonte (arquivo, HTTP) → DataFrame | `src/<pkg>/ingestion/` (I/O fino) | `unit` (parsing) / `integration` |
| Transformação `DataFrame -> DataFrame` | `src/<pkg>/{bronze,silver,gold}/` | `transformation` |
| Escrita Delta/MERGE da camada | writer fino no pacote da camada | `integration` |
| Regra de qualidade do domínio | `src/<pkg>/quality/` | `data_quality` |
| Helper sem regra de negócio, só deste projeto | `src/<pkg>/utils/` (não é depósito) | `unit` |
| Reutilizado por 2+ projetos | `shared/src/lakehouse_common/<área>/` | `shared/tests/<categoria>/` |
| Orquestração (parâmetros → chamadas → exibição) | `notebooks/{bronze,silver,gold}/*.py` | não testado (por isso sem lógica) |
| Valores por ambiente (catalog, paths, limiares) | `config/<env>.yml` | `unit` (loader) |
| DDL e consultas Gold | `sql/` | `contract` / `integration` |

Regra de promoção para `shared/`: só quando o **segundo** projeto precisar
(`shared/README.md`). Não antecipar. Subpacote ganha `__init__.py` com o primeiro módulo.
Imports sempre absolutos: `from retail_lakehouse.silver.orders import ...`.

## Boundaries (dependências permitidas)

```
notebooks ──▶ src (projeto) ──▶ shared
                  │
                  └──▶ conf (via loader tipado)
```

- `shared` nunca importa de `projects`.
- Um projeto nunca importa de outro projeto.
- Silver lê Bronze; Gold lê Silver (e `agg_` lê `fact_`). Nenhuma camada pula outra
  (Gold nunca lê Bronze) nem lê "para trás".
- Transformações não fazem I/O, não leem configuração global e não criam `SparkSession`.
- I/O não contém regra de negócio: recebe DataFrame pronto e escreve.

## src vs notebooks

Notebook correto (≈ 10–30 linhas no total):

```python
config = load_config(env=dbutils.widgets.get("env"))
raw = read_orders_source(spark, config.sources.orders)
silver, quarantine = build_silver_orders(raw, config.quality)
write_silver_orders(silver, quarantine, config.tables)
display(silver.limit(20))
```

Sinal de alerta → mover para `src/`: `def` no notebook, `withColumn` encadeado,
regra `F.when`, join, janela, ou célula com mais de ~15 linhas de transformação.

## Configuration

- Dataclass `frozen=True` tipada, carregada **uma vez** no entrypoint e passada por parâmetro.
- Proibido no código: nome de catalog/schema, caminho absoluto, limiar de qualidade,
  data fixa de corte. Tudo isso é configuração.
- Secrets não são configuração: vêm de Databricks Secrets / env vars (skill `security`).
- Configuração inválida falha cedo, no load, com mensagem clara.

## Architecture decisions

Toda decisão que muda layout, fronteira, padrão de escrita, tecnologia ou convenção
é registrada **antes** de implementada (nunca silenciosamente).

| Situação | Registro |
|---|---|
| Decisão pequena, reversível, sem alternativas reais | linha na tabela de `docs/ARCHITECTURE.md` §7 |
| Alternativas reais com trade-off, custo alto de reverter, nova dependência de runtime, mudança de contrato de camada ou de tecnologia (ex.: adotar Lakeflow/DLT) | ADR completo em `docs/ADR/NNNN-titulo.md` + linha na tabela com link |
| Ainda não sabemos (falta verificar ambiente) | "Questões em aberto" em `docs/ROADMAP.md` |

Formato do ADR completo (curto):

```
# NNNN — Título
Status: proposto | aceito | substituído por NNNN
Data: AAAA-MM-DD
## Contexto      (o problema e as restrições)
## Decisão       (o que faremos)
## Alternativas  (o que foi rejeitado e por quê)
## Consequências (o que fica mais fácil / mais difícil)
```

ADR aceito não é editado; uma mudança gera um novo ADR que o substitui.

## Checklist antes de criar um arquivo

- [ ] A fase atual (`docs/ROADMAP.md`) inclui isso?
- [ ] O local segue a tabela acima?
- [ ] Alguma fronteira foi violada?
- [ ] Precisa de registro de decisão?
