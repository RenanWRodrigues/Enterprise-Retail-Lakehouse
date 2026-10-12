"""Deterministic de-duplication by business key (feeds the ``duplicate_rows`` run metric).

Duplicates are removed *before* the quality checks run; a ``Unique`` CRITICAL check on the
result then proves the key really is unique. Counting ``input_rows - output_rows`` gives
``duplicate_rows`` without an extra pass over the data.
"""

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

_RANK = "_dq_dedup_rank"


def keep_first(df: DataFrame, key_columns: Sequence[str], order_by: Sequence[Column]) -> DataFrame:
    """One row per key: the first by ``order_by`` (make it total, e.g. end with a unique column)."""
    if not key_columns or not order_by:
        raise ValueError("keep_first needs key columns and an ordering")
    window = Window.partitionBy(*key_columns).orderBy(*order_by)
    return df.withColumn(_RANK, F.row_number().over(window)).where(F.col(_RANK) == 1).drop(_RANK)
