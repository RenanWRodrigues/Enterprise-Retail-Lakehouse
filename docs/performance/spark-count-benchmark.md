# Benchmark: as quatro contagens por lote da Silver do Retail

Data: 2026-10-12. **Medição local** (Windows, Spark 4.0.4 clássico, `local[2]`, Java 21).
**Medição no Databricks: PENDENTE** (o smoke não foi executado; ver
[smoke-test-report.md](../databricks/smoke-test-report.md)).

## 1. Onde estão e o que executam

`retail_lakehouse/silver/pipeline.py::_prepare_batch`, uma vez por lote e por entidade, antes do
gate e dos MERGEs:

| Contagem | DataFrame | O que executa |
|---|---|---|
| `input_rows` | Bronze filtrado por `_batch_id` | leitura do Bronze |
| `output_rows` | `batch.valid` | todo o `standardize`: leitura, tipagem, `row_number` sobre o fingerprint (shuffle), checks de linha, `collect_set` por chave (shuffle), segundo `row_number` (shuffle) |
| `rejected_rows` | `batch.quarantine` | o mesmo plano até a separação, mais a união com os conflitos |
| `duplicate_rows` | `batch.duplicates` | o mesmo plano, ramo das duplicatas exatas e por chave |

Sem cache (o serverless rejeita, ADR 0001), **cada ação recalcula o `standardize` desde a leitura
do Bronze**. Depois das contagens, o mesmo lote ainda é recalculado pelo gate (`run_suite`) e pelas
duas fontes dos MERGEs (válidas e quarentena).

As contagens precisam ser **independentes**: o `standardize.py` produz cada saída como um DataFrame
próprio para que `entrada = válidas + quarentena + duplicatas` seja uma verificação real (CRITICAL),
e não verdadeira por construção.

## 2. Método

Script do apêndice, fora do repositório: o layout não prevê pasta de benchmarks.

- **Dados:** gerador do Retail (configuração de smoke com `orders` ampliado), entidade `order_items`
  (a maior, com janelas), com as anomalias de produção.
- **Bronze simulado:** cópia em JSON Lines lida pelo Spark (varredura de arquivo). No Windows o Spark
  não grava arquivos, por isso Parquet e Delta ficaram de fora.
- **O que se mede por ação:** tempo de parede (`perf_counter`) e número de jobs Spark
  (`setJobGroup` + `statusTracker`).
- **Execução:** um aquecimento descartado, depois as repetições.
- **Fontes dos MERGEs:** aproximadas por escrita `noop`, que executa o plano inteiro sem gravar.
- **Gate:** o real (`retail_entity_suite`). As referências (produtos, pedidos) são DataFrames em
  memória, não tabelas Silver.
- **Comando:** `python bench_counts.py <repo> <orders> <repetições>`.

## 3. Resultados

### Escala 1: 2.000 pedidos → 6.535 linhas no Bronze (1 repetição; validação do script)

| Ação | Tempo | Jobs |
|---|---|---|
| 4 `count()` separados | 7,83 s (0,45 + 2,05 + 3,14 + 2,19) | 16 |
| ação única (as 4 contagens numa consulta) | 4,90 s | 10 |
| gate | 25,84 s | 21 |
| fontes dos MERGEs | 4,99 s | 7 |

### Escala 2: 10.000 pedidos → 32.561 linhas no Bronze (3 repetições)

| Ação | Rep. 1 | Rep. 2 | Rep. 3 | Jobs |
|---|---|---|---|---|
| `count` Bronze | 0,58 s | 0,35 s | 0,42 s | 2 |
| `count` válidas (32.468) | 3,23 s | 2,29 s | 1,63 s | 4 |
| `count` quarentena (62) | 4,93 s | 3,13 s | 2,05 s | 5 |
| `count` duplicatas (31) | 2,96 s | 2,18 s | 1,82 s | 5 |
| **soma dos 4 `count()`** | **11,69 s** | **7,95 s** | **5,91 s** | **16** |
| **ação única** | **6,49 s** | **4,51 s** | **4,37 s** | **10** |
| gate (`rows_checked` = 32.468) | 23,54 s | 17,49 s | 18,23 s | 20 |
| fonte do MERGE: válidas | 3,38 s | 1,96 s | 2,17 s | 3 |
| fonte do MERGE: quarentena | 3,48 s | 2,66 s | 2,84 s | 4 |

- As contagens são idênticas nos dois métodos em todas as repetições.
- O plano final da ação única tem **2 `ReusedExchange`**: o Spark reaproveitou shuffles comuns entre
  os ramos, sem cache.
- Ação única × quatro `count()`: **26% a 44% menos tempo** e **6 jobs a menos** por lote.
- O lote inteiro na rep. 2 (contagens + gate + fontes dos MERGEs) levou cerca de 30 s. As contagens
  eram 26% disso; a ação única economiza cerca de 11% do lote.
- **O gate é o maior custo:** de 2 a 3 vezes as quatro contagens juntas (20 jobs).

## 4. Alternativas

| Alternativa | Semântica | Memória / materialização | Evidência | Decisão |
|---|---|---|---|---|
| **Ação única** (4 contagens unidas, cada uma do seu DataFrame) | preservada: quatro planos independentes | nenhuma (4 linhas no driver) | −26 a −44%, 16 → 10 jobs | **aplicada** |
| Contagens derivadas do processamento (uma coluna de destino, `groupBy`) | **muda**: a reconciliação ficaria verdadeira por construção | nenhuma | não medida | rejeitada |
| Persistência temporária (`cache`/`persist`/`localCheckpoint`) | preservada | ocupa memória ou disco do cluster | não medida | **proibida no serverless** (ADR 0001) |
| Materializar o lote padronizado numa tabela temporária | preservada | uma escrita e uma leitura por lote | exige Delta (não roda no Windows) | pendente: medir no Databricks, junto com o custo do gate |
| Métricas já calculadas: `rows_checked` do gate = válidas | preservada (mesmas linhas) | nenhuma | o gate devolveu 32.468 = `count` válidas | não aplicada: tira só uma contagem, que a ação única já compartilha, e inverteria a ordem entre a reconciliação (CRITICAL) e o gate |

Nenhum `count()` foi acrescentado para medir: o benchmark usa o `statusTracker` do Spark.

## 5. Decisão

**Aplicada:** `batch_counts(bronze, batch)` em `silver/pipeline.py`. As quatro contagens rodam numa
só ação, cada uma do seu DataFrame. O comportamento observável não muda (mesmos números, mesma
reconciliação CRITICAL antes de qualquer gravação). Não há cache.

Testes (`tests/transformation/test_silver_batch_counts.py`):

- RED: erro de coleta (`batch_counts` inexistente).
- GREEN: 2 passed. As contagens são iguais às dos quatro `count()`, reconciliam, e um lote vazio
  conta zero.
- Regressão: `-m unit` 629 passed, `tests/contract` 246 passed, ruff, format e mypy verdes.

**Não aplicado:** nenhuma das outras alternativas.

**Próximo passo, com mais retorno:** o **gate** custa de 2 a 3 vezes as contagens. Medir no
Databricks e investigar os 20 jobs (joins de integridade referencial, janela do `Unique`) antes de
mexer nas contagens restantes.

## 6. Limitações

- **Local ≠ Databricks:** o serverless roda Spark Connect com Photon. O reuso de exchange e os
  tempos podem diferir. No pior caso a ação única custa o mesmo que os quatro `count()`, porque
  executa os mesmos quatro planos.
- **Escala:** 32 mil linhas; o comportamento com milhões não foi medido (sem autorização para volume
  completo; risco R11).
- **Pressão de memória no Windows:** os tempos variam entre repetições (a rep. 1 é sempre a mais
  lenta). As conclusões valem para as três repetições.
- **Gate:** medido com referências em memória. Com tabelas Silver, os joins leem dados de verdade.

## Apêndice: script do benchmark

```python
"""Local benchmark of the four per-batch counts of the Retail Silver (no Delta, classic Spark).

Usage: python bench_counts.py <repo_root> <orders> <repetitions>
Bronze is simulated by a JSON Lines copy of the generated batch, read by Spark (a file scan).
Run with PYTHONPATH="shared/src;projects/enterprise-retail-lakehouse/src" from the repo root.
"""

import json
import sys
import tempfile
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_common.quality import run_suite
from lakehouse_testing.spark import build_local_spark_session
from retail_lakehouse.bronze import bronze_schema
from retail_lakehouse.ingestion.source_contracts import RETAIL_SOURCE_CONTRACTS
from retail_lakehouse.quality.suites import retail_entity_suite
from retail_lakehouse.silver.schemas import SILVER_SCHEMAS
from retail_lakehouse.silver.standardize import standardize
from retail_lakehouse.synthetic import MemorySink, generate_retail_dataset, load_config

repo, orders, repetitions = Path(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
AT = datetime(2026, 10, 12, 12, tzinfo=UTC)
ENTITY = "order_items"  # the largest entity; window functions in standardize

config = load_config(
    repo / "projects/enterprise-retail-lakehouse/config/data_generation_smoke.toml"
)
config = replace(config, orders=orders, customers=max(120, orders // 3))
sink = MemorySink()
generate_retail_dataset(config, sink)

spark = build_local_spark_session(Path(tempfile.mkdtemp()), enable_delta=False)
sc = spark.sparkContext
contract = RETAIL_SOURCE_CONTRACTS[ENTITY]
rows = [
    (*(r.get(c) for c in contract.column_names), AT, f"landing/{ENTITY}", "b1", AT.date())
    for r in sink.rows[ENTITY]
]
path = Path(tempfile.mkdtemp()) / "bronze.jsonl"  # Spark cannot write files on Windows
names = bronze_schema(contract).fieldNames()
with path.open("w", encoding="utf-8") as stream:
    for row in rows:
        stream.write(json.dumps(dict(zip(names, row, strict=True)), default=str) + chr(10))
bronze = (
    spark.read.schema(bronze_schema(contract)).json(str(path)).where(F.col("_batch_id") == "b1")
)
batch = standardize(bronze, ENTITY, execution_timestamp=AT)
contracted = batch.valid.select(*SILVER_SCHEMAS[ENTITY].fieldNames())
references = {
    "products": spark.createDataFrame(
        [(r["product_id"],) for r in sink.rows["products"]], "product_id string"
    ),
    "orders": spark.createDataFrame(
        [(r["order_id"],) for r in sink.rows["orders"]], "order_id string"
    ),
}
gate = retail_entity_suite(ENTITY, contracted, references)

counter = iter(range(10_000))


def timed(label, action):
    group = f"{label}-{next(counter)}"
    sc.setJobGroup(group, label)
    start = time.perf_counter()
    value = action()
    elapsed = time.perf_counter() - start
    jobs = len(sc.statusTracker().getJobIdsForGroup(group))
    return value, round(elapsed, 3), jobs


def single_frame() -> DataFrame:
    """The four counts as one query; each still computed from its own DataFrame."""
    tagged = None
    for name, frame in (
        ("input", bronze),
        ("valid", batch.valid),
        ("quarantine", batch.quarantine),
        ("duplicates", batch.duplicates),
    ):
        part = frame.agg(F.count(F.lit(1)).alias("n")).select(F.lit(name).alias("k"), "n")
        tagged = part if tagged is None else tagged.unionByName(part)
    return tagged


def single_action() -> dict[str, int]:
    return {r["k"]: int(r["n"]) for r in single_frame().collect()}


timed("warmup", lambda: (bronze.count(), batch.valid.count()))
results = []
for _ in range(repetitions):
    run = {}
    for name, frame in (
        ("input", bronze),
        ("valid", batch.valid),
        ("quarantine", batch.quarantine),
        ("duplicates", batch.duplicates),
    ):
        value, seconds, jobs = timed(f"count_{name}", frame.count)
        run[f"count_{name}"] = {"rows": value, "s": seconds, "jobs": jobs}
    value, seconds, jobs = timed("single_action", single_action)
    run["single_action"] = {"rows": value, "s": seconds, "jobs": jobs}
    value, seconds, jobs = timed("gate", lambda: run_suite(gate, execution_timestamp=AT))
    run["gate"] = {"rows_checked": value[0].rows_checked, "s": seconds, "jobs": jobs}
    for name, frame in (
        ("merge_source_valid", batch.valid),
        ("merge_source_quarantine", batch.quarantine),
    ):
        _, seconds, jobs = timed(
            name, lambda f=frame: f.write.format("noop").mode("overwrite").save()
        )
        run[name] = {"s": seconds, "jobs": jobs}
    results.append(run)

executed = single_frame()
executed.collect()  # AQE decides exchange reuse at run time: inspect the final plan
physical = executed._jdf.queryExecution().executedPlan().toString()  # local diagnostics only
print(
    json.dumps(
        {
            "orders": orders,
            "entity": ENTITY,
            "bronze_rows": len(rows),
            "spark": spark.version,
            "repetitions": results,
            "reused_exchange_in_union": physical.count("ReusedExchange"),
        },
        indent=1,
    )
)
spark.stop()
```
