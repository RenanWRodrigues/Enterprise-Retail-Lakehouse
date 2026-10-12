"""Enterprise Retail Lakehouse.

Subpackages (each gains an ``__init__.py`` with its first module):

- ``ingestion`` — read sources into DataFrames (thin I/O)
- ``bronze`` / ``silver`` / ``gold`` — pure ``DataFrame -> DataFrame`` transformations
  plus thin writers per layer
- ``quality`` — retail-specific data quality rules
- ``utils`` — project-specific helpers without business rules
"""

from typing import Final

PROJECT: Final = "retail"  # `project` column of pipeline_runs (docs/OBSERVABILITY.md)
