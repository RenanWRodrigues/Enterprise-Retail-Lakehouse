"""Brazilian Financial Data Lakehouse.

Subpackages (each gains an ``__init__.py`` with its first module):

- ``ingestion`` — fetch public financial data (BCB, CVM) into DataFrames (thin I/O)
- ``bronze`` / ``silver`` / ``gold`` — pure ``DataFrame -> DataFrame`` transformations
  plus thin writers per layer
- ``quality`` — financial-data-specific quality rules
- ``utils`` — project-specific helpers without business rules
"""

from typing import Final

PROJECT: Final = "financial"  # `project` column of pipeline_runs (docs/OBSERVABILITY.md)
