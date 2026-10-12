"""Contracts of the IoT source systems (CMMS and historian), before Bronze.

Documented in docs/DATA_CONTRACTS.md (kept in sync by tests).
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Final


class SourceSystem(StrEnum):
    CMMS = "cmms"
    HISTORIAN = "historian"


class FileFormat(StrEnum):
    CSV = "csv"
    JSONL_GZIP = "json.gz"


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    logical_type: str  # string | integer | double | date | timestamp
    nullable: bool = False


@dataclass(frozen=True, slots=True)
class DatasetContract:
    name: str
    source_system: SourceSystem
    file_format: FileFormat
    business_key: tuple[str, ...]
    columns: tuple[Column, ...]
    foreign_keys: tuple[tuple[str, str, str], ...] = ()  # (column, dataset, column)

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)


def _c(name: str, logical_type: str = "string", *, nullable: bool = False) -> Column:
    return Column(name, logical_type, nullable)


IOT_SOURCE_CONTRACTS: Final[dict[str, DatasetContract]] = {
    contract.name: contract
    for contract in (
        DatasetContract(
            "equipment",
            SourceSystem.CMMS,
            FileFormat.CSV,
            ("equipment_id",),
            (
                _c("equipment_id"),
                _c("equipment_type"),
                _c("model"),
                _c("manufacturer"),
                _c("site_id"),
                _c("site_name"),
                _c("state"),
                _c("installed_at", "date"),
                _c("rated_power_kw", "double"),
            ),
        ),
        DatasetContract(
            "sensors",
            SourceSystem.CMMS,
            FileFormat.CSV,
            ("sensor_id",),
            (
                _c("sensor_id"),
                _c("equipment_id"),
                _c("sensor_type"),
                _c("unit"),
                _c("physical_min", "double"),
                _c("physical_max", "double"),
                _c("sampling_interval_seconds", "integer"),
            ),
            (("equipment_id", "equipment", "equipment_id"),),
        ),
        DatasetContract(
            "sensor_readings",
            SourceSystem.HISTORIAN,
            FileFormat.JSONL_GZIP,
            ("reading_id",),
            (
                _c("reading_id"),
                _c("sensor_id"),
                _c("event_timestamp", "timestamp"),
                _c("ingestion_timestamp", "timestamp"),
                _c("reading_value", "double"),
                _c("unit"),
            ),
            (("sensor_id", "sensors", "sensor_id"),),
        ),
        DatasetContract(
            "failure_events",
            SourceSystem.CMMS,
            FileFormat.CSV,
            ("failure_id",),
            (
                _c("failure_id"),
                _c("equipment_id"),
                _c("failure_mode"),
                _c("failure_timestamp", "timestamp"),
                _c("severity"),
            ),
            (("equipment_id", "equipment", "equipment_id"),),
        ),
        DatasetContract(
            "maintenance_orders",
            SourceSystem.CMMS,
            FileFormat.CSV,
            ("maintenance_order_id",),
            (
                _c("maintenance_order_id"),
                _c("equipment_id"),
                _c("order_type"),
                _c("related_failure_id", nullable=True),
                _c("opened_at", "timestamp"),
                _c("started_at", "timestamp"),
                _c("completed_at", "timestamp", nullable=True),
                _c("status"),
            ),
            (
                ("equipment_id", "equipment", "equipment_id"),
                ("related_failure_id", "failure_events", "failure_id"),
            ),
        ),
    )
}
