"""Silver reference data and the reconciliation invariant (pure Python)."""

import pytest

from lakehouse_common.quality import ReconciliationError
from retail_lakehouse.ingestion.source_contracts import REGIONS
from retail_lakehouse.silver import STATE_REGIONS
from retail_lakehouse.silver.standardize import BatchCounts
from retail_lakehouse.synthetic.reference_data import CITIES

BRAZILIAN_STATES = 27  # 26 states + Distrito Federal


def test_every_brazilian_state_maps_to_a_contract_region() -> None:
    assert len(STATE_REGIONS) == BRAZILIAN_STATES
    assert set(STATE_REGIONS.values()) == set(REGIONS)


def test_the_generator_cities_agree_with_the_reference() -> None:
    assert all(STATE_REGIONS[city.state] == city.region for city in CITIES)


def test_counts_reconcile() -> None:
    BatchCounts(input_rows=10, output_rows=6, rejected_rows=3, duplicate_rows=1).reconcile()


def test_a_lost_row_fails_the_reconciliation() -> None:
    with pytest.raises(ReconciliationError, match="10 Bronze rows != 6 valid"):
        BatchCounts(input_rows=10, output_rows=6, rejected_rows=2, duplicate_rows=1).reconcile()
