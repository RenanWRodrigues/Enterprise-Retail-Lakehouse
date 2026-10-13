"""Characterization of the IoT master data: a refactor must not change any generated record.

Recorded before ``_build_maintenance`` was split (2026-10-13): a digest of every equipment,
sensor, failure, maintenance order, episode and offline period from the tiny test configuration.
"""

import hashlib
import json
from dataclasses import asdict, fields

from iot_lakehouse.synthetic.master_data import MasterData

GOLDEN = "37010ab8a1ec8fb0da09dd3c1879e0ec0219e447e427bd4fd2b3e575b8ac865e"


def _digest(master: MasterData) -> str:
    digest = hashlib.sha256()
    for field in fields(master):
        for record in getattr(master, field.name):
            digest.update(
                json.dumps([field.name, asdict(record)], sort_keys=True, default=str).encode()
            )
    return digest.hexdigest()


def test_the_master_data_is_identical(iot_master: MasterData) -> None:
    assert _digest(iot_master) == GOLDEN
