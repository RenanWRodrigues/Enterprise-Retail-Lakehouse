"""Reference data used to standardize Silver (no business rules of a specific report).

``STATE_REGIONS``: the five IBGE regions of Brazil, by state (UF). The CRM delivers the customer's
state but not the region, which the Gold dimension tracks (SCD Type 2); the region is derived here
as a technical N:1 enrichment, so every customer version carries it.
"""

from typing import Final

_REGIONS: Final[dict[str, tuple[str, ...]]] = {
    "Norte": ("AC", "AM", "AP", "PA", "RO", "RR", "TO"),
    "Nordeste": ("AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"),
    "Centro-Oeste": ("DF", "GO", "MS", "MT"),
    "Sudeste": ("ES", "MG", "RJ", "SP"),
    "Sul": ("PR", "RS", "SC"),
}

STATE_REGIONS: Final[dict[str, str]] = {
    state: region for region, states in _REGIONS.items() for state in states
}
