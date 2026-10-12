"""The preflight against local Spark + Delta: what works here and what only Databricks has.

Needs Delta (test container, Linux). Locally ``CREATE VOLUME`` and ``/Volumes`` do not exist,
so that check must be reported as failed, not raised.
"""

from pyspark.sql import SparkSession

from lakehouse_common.preflight import run_preflight

NAMESPACE = "spark_catalog.preflight_it"


def test_the_preflight_reports_each_capability(delta_spark: SparkSession) -> None:
    results = {
        r.check: r
        for r in run_preflight(
            delta_spark, namespace=NAMESPACE, volume="probe", opener=lambda url, timeout: 200
        )
    }

    assert list(results) == [
        "runtime",
        "catalog",
        "create_schema",
        "volume_files",
        "delta_change_feed",
        "outbound_https",
    ]
    ok = {name for name, result in results.items() if result.ok}
    assert ok == {"runtime", "catalog", "create_schema", "delta_change_feed", "outbound_https"}
    assert not results["volume_files"].ok  # Unity Catalog Volumes exist only on Databricks
