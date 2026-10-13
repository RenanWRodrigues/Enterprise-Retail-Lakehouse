"""The four batch counts in one Spark action, each still taken from its own DataFrame.

The reconciliation ``input = valid + quarantine + duplicates`` must stay a real check: the counts
come from four independent plans (not from one classified frame), only run as one query.
docs/performance/spark-count-benchmark.md.
"""

from datetime import UTC, datetime

from pyspark.sql import SparkSession

from retail_lakehouse.bronze import bronze_schema
from retail_lakehouse.ingestion.source_contracts import RETAIL_SOURCE_CONTRACTS
from retail_lakehouse.silver.pipeline import batch_counts
from retail_lakehouse.silver.standardize import BatchCounts, standardize

AT = datetime(2026, 10, 12, 12, tzinfo=UTC)


def _line(order: str, line: str, quantity: str = "1") -> tuple[object, ...]:
    return (
        order,
        line,
        "P1",
        quantity,
        "10.00",
        "0.00",
        AT,
        "landing/order_items",
        "b1",
        AT.date(),
    )


def test_counts_equal_the_separate_counts_and_reconcile(spark: SparkSession) -> None:
    schema = bronze_schema(RETAIL_SOURCE_CONTRACTS["order_items"])
    bronze = spark.createDataFrame(
        [
            _line("O1", "1"),
            _line("O1", "1"),  # exact duplicate
            _line("O2", "1"),
            _line("O3", "1", quantity="0"),  # quarantined: quantity below 1
        ],
        schema,
    )
    batch = standardize(bronze, "order_items", execution_timestamp=AT)

    counts = batch_counts(bronze, batch)

    assert counts == BatchCounts(
        input_rows=bronze.count(),
        output_rows=batch.valid.count(),
        rejected_rows=batch.quarantine.count(),
        duplicate_rows=batch.duplicates.count(),
    )
    assert counts == BatchCounts(input_rows=4, output_rows=2, rejected_rows=1, duplicate_rows=1)
    counts.reconcile()


def test_an_empty_batch_counts_zero(spark: SparkSession) -> None:
    schema = bronze_schema(RETAIL_SOURCE_CONTRACTS["order_items"])
    bronze = spark.createDataFrame([], schema)

    counts = batch_counts(bronze, standardize(bronze, "order_items", execution_timestamp=AT))

    assert counts == BatchCounts(input_rows=0, output_rows=0, rejected_rows=0, duplicate_rows=0)
