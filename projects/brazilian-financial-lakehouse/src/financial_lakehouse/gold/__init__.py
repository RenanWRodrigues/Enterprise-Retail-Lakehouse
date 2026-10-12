"""Gold: analytical tables by theme and the monthly macro panel (docs/GOLD.md)."""

from financial_lakehouse.gold.build import (
    MedallionSettings,
    build_gold,
    load_medallion_settings,
    write_gold,
)
from financial_lakehouse.gold.schemas import GOLD_SCHEMAS, TABLE_COMMENTS

__all__ = [
    "GOLD_SCHEMAS",
    "TABLE_COMMENTS",
    "MedallionSettings",
    "build_gold",
    "load_medallion_settings",
    "write_gold",
]
