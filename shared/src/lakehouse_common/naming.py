"""Safe identifiers: table names and namespaces are interpolated into SQL, so only plain
identifiers are accepted (no quoting, spaces or statement separators)."""

import re
from typing import Final

_IDENTIFIER: Final = r"[A-Za-z_][A-Za-z0-9_]*"
_TABLE_NAME: Final = re.compile(rf"^{_IDENTIFIER}(\.{_IDENTIFIER}){{0,2}}$")
_NAMESPACE: Final = re.compile(rf"^{_IDENTIFIER}(\.{_IDENTIFIER})?$")


def validate_table_name(name: str) -> str:
    """Return ``name`` if it is ``[catalog.][schema.]table`` (or a namespace); raise otherwise."""
    if not _TABLE_NAME.match(name):
        raise ValueError(f"invalid table name: {name!r}")
    return name


def validate_namespace(namespace: str) -> str:
    """Return ``namespace`` if it is ``schema`` or ``catalog.schema``; raise otherwise."""
    if not _NAMESPACE.match(namespace):
        raise ValueError(f"invalid namespace: {namespace!r}")
    return namespace
