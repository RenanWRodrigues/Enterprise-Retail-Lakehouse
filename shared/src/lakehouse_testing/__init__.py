"""Test-only infrastructure shared by every test suite in the monorepo.

Not a runtime library: nothing in ``projects/*/src`` or ``lakehouse_common`` may import it.

- ``markers`` — test category derived from the directory layout
- ``spark`` — local SparkSession tuned for small, deterministic tests
"""
