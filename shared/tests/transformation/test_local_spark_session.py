"""The shared ``spark`` fixture is configured for fast, deterministic tests."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual


def test_session_uses_utc_timezone(spark: SparkSession) -> None:
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_session_uses_small_local_parallelism(spark: SparkSession) -> None:
    assert spark.sparkContext.master == "local[2]"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "2"


def test_session_is_shared_across_tests(spark: SparkSession) -> None:
    assert SparkSession.getActiveSession() is spark


def test_native_transformation_on_small_dataframe(spark: SparkSession) -> None:
    # Arrange
    schema = StructType(
        [StructField("store_id", StringType(), False), StructField("units", LongType(), False)]
    )
    sales = spark.createDataFrame([("s-1", 2), ("s-1", 3), ("s-2", 5)], schema=schema)

    # Act
    result = sales.groupBy("store_id").agg(F.sum("units").alias("units"))

    # Assert
    expected = spark.createDataFrame([("s-1", 5), ("s-2", 5)], schema=schema)
    assertDataFrameEqual(result, expected)
