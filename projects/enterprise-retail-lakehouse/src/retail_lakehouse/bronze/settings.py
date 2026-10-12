"""Bronze settings per environment (``config/bronze.toml``)."""

from dataclasses import dataclass
from pathlib import Path

from lakehouse_common.config import load_environment
from retail_lakehouse.bronze.ingest import BronzeTarget


@dataclass(frozen=True, slots=True)
class BronzeSettings:
    landing_root: str
    target: BronzeTarget


def load_bronze_settings(path: Path, environment: str) -> BronzeSettings:
    """Landing root and Bronze namespace of ``environment``."""
    section = load_environment(path, environment)
    return BronzeSettings(
        landing_root=str(section["landing_root"]),
        target=BronzeTarget(str(section["target_namespace"])),
    )
