"""Guard the schema/DataFrame assertions every transformation test relies on.

The project uses PySpark's native ``pyspark.testing`` helpers instead of custom ones;
these tests document (and protect) the behaviour we depend on.
"""

import pytest
from pyspark.errors import PySparkAssertionError
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

ORDER_SCHEMA = StructType(
    [StructField("order_id", StringType(), False), StructField("quantity", LongType(), True)]
)


def test_schema_assertion_detects_type_mismatch() -> None:
    other = StructType(
        [StructField("order_id", StringType(), False), StructField("quantity", IntegerType(), True)]
    )
    with pytest.raises(PySparkAssertionError):
        assertSchemaEqual(other, ORDER_SCHEMA)


def test_schema_assertion_detects_missing_column() -> None:
    other = StructType([StructField("order_id", StringType(), False)])
    with pytest.raises(PySparkAssertionError):
        assertSchemaEqual(other, ORDER_SCHEMA)


def test_dataframe_assertion_ignores_row_order(spark: SparkSession) -> None:
    actual = spark.createDataFrame([("o-2", 1), ("o-1", 3)], schema=ORDER_SCHEMA)
    expected = spark.createDataFrame([("o-1", 3), ("o-2", 1)], schema=ORDER_SCHEMA)
    assertDataFrameEqual(actual, expected)


def test_dataframe_assertion_detects_different_values(spark: SparkSession) -> None:
    actual = spark.createDataFrame([("o-1", 3)], schema=ORDER_SCHEMA)
    expected = spark.createDataFrame([("o-1", 4)], schema=ORDER_SCHEMA)
    with pytest.raises(PySparkAssertionError):
        assertDataFrameEqual(actual, expected)


def test_dataframe_assertion_treats_nulls_as_equal(spark: SparkSession) -> None:
    actual = spark.createDataFrame([("o-1", None)], schema=ORDER_SCHEMA)
    expected = spark.createDataFrame([("o-1", None)], schema=ORDER_SCHEMA)
    assertDataFrameEqual(actual, expected)
