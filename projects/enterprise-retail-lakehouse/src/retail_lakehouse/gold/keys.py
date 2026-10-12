"""Surrogate keys and business-date keys (both deterministic and timezone-independent)."""

from datetime import date
from typing import Final

from pyspark.sql import Column
from pyspark.sql import functions as F

UNKNOWN_KEY: Final = -1
BRASILIA_OFFSET_SECONDS: Final = -3 * 3600  # fixed UTC-3: no daylight saving since 2019
SECONDS_PER_DAY: Final = 86_400
_EPOCH: Final = date(1970, 1, 1)
_YEAR_FACTOR, _MONTH_FACTOR = 10_000, 100


def surrogate_key(dimension: str, *natural_key: Column) -> Column:
    """``xxhash64(dimension, natural key...)``: same input, same key, on every rebuild."""
    return F.xxhash64(F.lit(dimension), *natural_key)


def business_date(timestamp: Column) -> Column:
    """Local date in Brasília, from epoch arithmetic (independent of the session timezone)."""
    local_seconds = F.unix_timestamp(timestamp) + F.lit(BRASILIA_OFFSET_SECONDS)
    return F.date_add(F.lit(_EPOCH), F.floor(local_seconds / SECONDS_PER_DAY).cast("int"))


def date_key_of(day: Column) -> Column:
    """``yyyymmdd`` as an integer."""
    return (F.year(day) * _YEAR_FACTOR + F.month(day) * _MONTH_FACTOR + F.dayofmonth(day)).cast(
        "int"
    )


def calendar_key(day: date) -> int:
    """``yyyymmdd`` of a Python date (the literal twin of ``date_key_of``)."""
    return day.year * _YEAR_FACTOR + day.month * _MONTH_FACTOR + day.day


def within_calendar(date_key: Column, start: date, end: date) -> Column:
    """``date_key`` when dim_date has it (``[start, end]``), else the unknown member -1.

    dim_date holds every day of the range, so this is "has a calendar row" without a join.
    NULL and -1 stay -1; a date outside the range never becomes a dangling key.
    """
    inside = date_key.between(calendar_key(start), calendar_key(end))
    return F.when(inside, date_key).otherwise(F.lit(UNKNOWN_KEY))


def business_date_key(timestamp: Column) -> Column:
    """``yyyymmdd`` of the Brasília business date; NULL when the timestamp is NULL."""
    return date_key_of(business_date(timestamp))
