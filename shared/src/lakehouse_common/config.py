"""Per-environment configuration files: one TOML table per environment (``[local]``, ...)."""

import tomllib
from pathlib import Path
from typing import Any


def load_environment(path: Path, environment: str) -> dict[str, Any]:
    """The ``[environment]`` table of ``path``; ``ValueError`` naming the valid ones if absent."""
    with path.open("rb") as handle:
        document = tomllib.load(handle)
    section = document.get(environment)
    if not isinstance(section, dict):
        raise ValueError(f"unknown environment {environment!r}; expected one of {sorted(document)}")
    return dict(section)
