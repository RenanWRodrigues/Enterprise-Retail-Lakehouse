"""IoT Gold contract: grain, keys and comments of every table (docs/GOLD.md).

There is no ``fact_sensor_readings``: one row per reading is exactly
``silver_sensor_readings`` (typed, deduplicated, traceable), so a Gold copy would be redundant.
Reading-level analysis reads the Silver; the Gold aggregates it by hour and day.
"""

from typing import Final

# dim_equipment member for readings and events of no known equipment
UNKNOWN_EQUIPMENT: Final = "UNKNOWN"
UNKNOWN_DATE_KEY: Final = -1  # dim_date member for events outside the observation period

GRAIN: Final[dict[str, tuple[str, ...]]] = {
    "dim_equipment": ("equipment_id",),
    "dim_date": ("date_key",),
    "fact_equipment_failures": ("failure_id",),
    "fact_maintenance": ("maintenance_order_id",),
    "agg_equipment_hourly": ("equipment_id", "hour_start", "sensor_type"),
    "agg_equipment_daily": ("equipment_id", "date_key"),
    "agg_equipment_reliability": ("equipment_id", "period_start"),
}
TABLE_ORDER: Final = tuple(GRAIN)  # dimensions first: a fact never references a missing member

# Foreign keys: (table, column) -> dimension table and its key.
FOREIGN_KEYS: Final[dict[str, tuple[tuple[str, str], ...]]] = {
    "fact_equipment_failures": (("equipment_id", "dim_equipment"), ("date_key", "dim_date")),
    "fact_maintenance": (("equipment_id", "dim_equipment"), ("date_key", "dim_date")),
    "agg_equipment_hourly": (("equipment_id", "dim_equipment"), ("date_key", "dim_date")),
    "agg_equipment_daily": (("equipment_id", "dim_equipment"), ("date_key", "dim_date")),
    "agg_equipment_reliability": (("equipment_id", "dim_equipment"),),
}

TABLE_COMMENTS: Final[dict[str, str]] = {
    "dim_equipment": "Grain: one equipment (CMMS); UNKNOWN = no known equipment.",
    "dim_date": "Grain: one UTC day of the observation period; date_key -1 = outside it.",
    "fact_equipment_failures": "Grain: one failure (failure_id) with its repair and status.",
    "fact_maintenance": "Grain: one maintenance order (corrective or preventive).",
    "agg_equipment_hourly": "Grain: one equipment, UTC hour and sensor type (valid readings).",
    "agg_equipment_daily": "Grain: one equipment and UTC day: stops, reliability, reading quality.",
    "agg_equipment_reliability": "Grain: one equipment over the observation period.",
}
