"""Deterministic synthetic IoT sources: CMMS master data (Python) + sensor readings (Spark).

See docs/DATA_GENERATION.md. Readings are never materialised on the driver.
"""

from iot_lakehouse.synthetic.config import IotGenerationConfig, estimated_readings, load_config
from iot_lakehouse.synthetic.master_data import MasterData, generate_master_data
from iot_lakehouse.synthetic.readings import (
    INTERNAL_COLUMNS,
    LABEL_COLUMNS,
    READING_COLUMNS,
    build_readings,
    readings_checksum,
)
from iot_lakehouse.synthetic.writer import write_landing

__all__ = [
    "INTERNAL_COLUMNS",
    "LABEL_COLUMNS",
    "READING_COLUMNS",
    "IotGenerationConfig",
    "MasterData",
    "build_readings",
    "estimated_readings",
    "generate_master_data",
    "load_config",
    "readings_checksum",
    "write_landing",
]
