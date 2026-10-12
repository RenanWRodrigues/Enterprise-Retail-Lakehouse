# Engineering Standards

Complementa a seção *Coding Rules* do `CLAUDE.md` com exemplos concretos.
As regras mecânicas são aplicadas por `ruff` e `mypy` (configurados em `pyproject.toml`).

## 1. Python

| Regra | Aplicação automática |
|---|---|
| `snake_case` | ruff `N` |
| type hints | ruff `ANN` + mypy `strict` |
| `pathlib` | ruff `PTH` |
| sem `print` | ruff `T20` |
| sem números mágicos | ruff `PLR2004` |
| datetimes com timezone | ruff `DTZ` |
| segurança básica | ruff `S` (bandit) |

Estrutura esperada de um módulo de transformação:

```python
"""Silver transformations for orders."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VALID_ORDER_STATUSES = ("created", "paid", "shipped", "delivered", "cancelled")


def standardize_order_status(orders: DataFrame) -> DataFrame:
    """Lower-case and trim ``status``; unknown values become NULL for quarantine."""
    normalized = F.lower(F.trim(F.col("status")))
    return orders.withColumn(
        "status",
        F.when(normalized.isin(*VALID_ORDER_STATUSES), normalized),
    )
```

- Sem I/O dentro de transformações; sem `spark` global — quando necessário, recebido por parâmetro.
- Configuração por dataclasses `frozen=True`, carregadas uma vez pelo notebook/entrypoint.
- Logging: `logger = logging.getLogger(__name__)` por módulo; nunca logar valores sensíveis.

## 2. PySpark

**Fazer**

- Schemas explícitos (`StructType`) para toda leitura de fonte.
- `F.col(...)` e funções nativas; `F.when` em vez de UDF condicional.
- `select` com lista de colunas explícita ao final de cada transformação Silver/Gold.
- Deduplicação determinística: `row_number()` sobre janela com ordenação total
  (incluir chave de desempate).
- Escritas idempotentes: `MERGE INTO` por chave natural ou `replaceWhere`.

**Evitar**

| Anti-padrão | Alternativa |
|---|---|
| Python UDF | funções nativas; Pandas UDF se inevitável (justificar) |
| `collect()` / `toPandas()` em dados grandes | agregações no Spark; `limit` + `display` para inspeção |
| `withColumn` em loop | um único `select` com expressões geradas |
| `inferSchema=True` | `StructType` explícito |
| `count()` repetido para logging | contar uma vez e reutilizar; ou métricas do Delta (`operationMetrics`) |
| `dropDuplicates()` sem critério | janela com ordenação explícita |

## 3. SQL

```sql
-- Grain: one row per order line (order_id, line_number).
-- Joins: order_lines N:1 orders, orders N:1 dim_customer (current version).
WITH orders_paid AS (
    SELECT order_id, customer_id, order_date
    FROM retail.silver.orders
    WHERE status = 'paid'
),

lines_enriched AS (
    SELECT
        l.order_id,
        l.line_number,
        o.customer_id,
        o.order_date,
        l.quantity * l.unit_price AS gross_amount
    FROM retail.silver.order_lines AS l
    INNER JOIN orders_paid AS o
        ON l.order_id = o.order_id  -- N:1
)

SELECT order_id, line_number, customer_id, order_date, gross_amount
FROM lines_enriched;
```

- Keywords em MAIÚSCULAS; identificadores em `snake_case`.
- Toda tabela Gold declara o grain no topo do arquivo e no `COMMENT` da tabela.
- **Double counting**: nunca juntar dois fatos de grains diferentes diretamente —
  agregar cada um ao grain comum antes do join.
- Todo join tem comentário de cardinalidade; joins 1:N em Gold exigem justificativa.

## 4. Notebooks

- Formato fonte Databricks (`# Databricks notebook source`), versionados em `projects/<p>/notebooks/{bronze,silver,gold}/`.
- Estrutura padrão:
  1. Parâmetros (widgets) e carregamento de configuração.
  2. Imports dos módulos do projeto.
  3. Execução do pipeline (uma chamada por etapa).
  4. Exibição de resultados/métricas.
- Nenhuma função de negócio definida no notebook.

## 5. Configuração

- Arquivos por ambiente em `projects/<p>/config/` (ex.: `local.yml`, `databricks.yml`).
- Nada de caminhos absolutos ou nomes de catalog hard-coded no código.
- Secrets nunca em arquivos de configuração (ver `SECURITY.md`).

## 6. Dependências

Toda nova dependência exige, no PR/relato: **o quê**, **por quê**, **alternativa considerada**.
Dependências de runtime devem ser compatíveis com o runtime serverless do Databricks.
