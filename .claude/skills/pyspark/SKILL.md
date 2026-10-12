---
name: pyspark
description: Use ao escrever ou revisar transformações PySpark — funções puras DataFrame -> DataFrame, schemas StructType, joins, janelas, deduplicação, tratamento de null, particionamento e performance básica.
---

# PySpark

Anti-padrões e tabela de alternativas: `docs/ENGINEERING_STANDARDS.md` §2.

## Transformação pura (forma obrigatória)

```python
def deduplicate_orders(orders: DataFrame) -> DataFrame:
    """Keep the latest version of each order_id (ties broken by _ingestion_timestamp)."""
    window = Window.partitionBy("order_id").orderBy(
        F.col("updated_at").desc(), F.col("_ingestion_timestamp").desc()
    )
    return (
        orders.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )
```

- Entrada e saída `DataFrame`; parâmetros extras são valores/config, nunca `spark` global.
- Sem leitura, escrita, `display`, `count()` ou log de dados dentro da transformação.
- Compor pipelines com `df.transform(f)`; cada passo é testável isoladamente.
- Terminar Silver/Gold com `select` de colunas explícitas (contrato de saída).

## Spark functions

- `pyspark.sql.functions as F`; referências por `F.col("x")`.
- Condicional: `F.when(...).otherwise(...)`. Texto: `F.trim`, `F.lower`, `F.regexp_replace`.
- Datas: `F.to_date(col, "yyyy-MM-dd")` / `F.to_timestamp` com formato explícito; timestamps em UTC.
- Várias colunas derivadas: um `select`/`withColumns({...})`, não `withColumn` em loop.
- **UDF Python proibida** por padrão. Se nenhuma função nativa resolver: justificar no PR,
  preferir Pandas UDF, e cobrir com teste.

## Schemas

- Leitura de fonte sempre com `StructType` declarado em módulo `schemas.py`; `inferSchema` proibido.
- Bronze lê tipos "seguros" (strings para campos sujos) + `_rescued_data`/coluna de corrupção
  quando o leitor suportar; a tipagem forte acontece na Silver.
- Cast explícito com verificação: valor não nulo que vira null após `cast` é erro de qualidade
  (não silenciar).

## Null handling

- Comparações com null retornam null: use `isNull`/`isNotNull`, `eqNullSafe` (`<=>`) em joins
  e comparações de mudança (SCD).
- `coalesce` só com default de negócio documentado; nunca para "esconder" ausência.
- String vazia/espaços → null na padronização Silver (`F.when(F.trim(c) == "", None)`).
- Agregações ignoram null (`sum`, `avg`); `count("*")` ≠ `count(col)` — escolha consciente.
- Chaves de join nulas não casam: trate antes (quarentena ou membro "desconhecido" na Gold).

## Joins

- Antes de escrever o join, declarar a cardinalidade em comentário (`# N:1 orders -> customers`).
- Validar a premissa: o lado "1" é único na chave (teste `contract`/`data_quality`).
- Join que multiplica linhas sem intenção = double counting (skill `dimensional-modeling`).
- `how` sempre explícito; `left_anti` para "registros sem correspondência"; evitar `cross`.
- Colunas homônimas: renomear/selecionar antes do join, nunca depender de `df["col"]` ambíguo.
- `F.broadcast(dim)` só para dimensões comprovadamente pequenas.

## Partitions

- Particionar tabela só por coluna de baixa cardinalidade usada em filtro (ex.: `event_date`),
  e só com volume que justifique; neste portfólio, o padrão é **não particionar**.
- Nunca particionar por id ou timestamp completo.
- `replaceWhere` combina com a coluna de partição/data para reprocessar intervalos.
- `repartition`/`coalesce` só com motivo medido; evitar `coalesce(1)`.

## Proibidos em pipeline

`collect()`, `toPandas()` sobre dados não limitados, loops Python sobre linhas,
`count()` repetido para log, `dropDuplicates()` sem critério de ordem, `inferSchema`.
Para inspecionar: `display(df.limit(n))` no notebook.

## Performance básica

- Filtrar e selecionar colunas cedo (pushdown).
- Agregar antes de juntar quando o grain permitir.
- Uma ação por etapa; métricas (linhas escritas) vêm de `operationMetrics` do Delta.
- Em serverless, não contar com `cache()` nem tuning de `spark.conf` (skill `databricks`).
