"""Atomic file writes: readers see the old file or the complete new one, never half of it."""

from pathlib import Path


def atomic_write(path: Path, data: bytes) -> None:
    """Write to a sibling temporary file, then rename over ``path`` (same directory)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(data)
    temporary.replace(path)


def atomic_write_text(path: Path, text: str) -> None:
    atomic_write(path, text.encode("utf-8"))
