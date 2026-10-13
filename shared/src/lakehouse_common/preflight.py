"""Databricks workspace preflight: detect what the pipelines need instead of presuming it.

Each check returns a ``PreflightResult`` (never raises), so one run lists every limitation:
Python/Spark versions, the catalog, creating a schema and a Volume, writing files to the Volume,
Delta with Change Data Feed (``table_changes``, ``DESCRIBE HISTORY``, ``VERSION AS OF``) and
outbound HTTPS to the official APIs.

Effects: creates (``IF NOT EXISTS``) the given schema and Volume, a probe file in the Volume and
the probe table ``_preflight_cdf`` in that schema (replaced on every run). Nothing else is
written; nothing is deleted. Run it only in the smoke schema, with authorization.
"""

import os
import sys
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Protocol

import pyspark
from pyspark.sql import SparkSession

from lakehouse_common.naming import validate_namespace
from lakehouse_common.observability.runs import safe_error_message

MINIMUM_PYTHON: Final = (3, 12)
PROBE_TABLE: Final = "_preflight_cdf"
PROBE_FILE: Final = "_preflight_probe.txt"
HTTPS_TIMEOUT_SECONDS: Final = 15.0
OFFICIAL_APIS: Final = (
    "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/1?formato=json",
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/Moedas?$top=1&$format=json",
    "https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos",
)


@dataclass(frozen=True, slots=True)
class PreflightResult:
    check: str
    ok: bool
    detail: str


class Opener(Protocol):
    def __call__(self, url: str, timeout: float) -> int:
        """HTTP status of a GET to ``url``."""


def _attempt(check: str, probe: Callable[[], str]) -> PreflightResult:
    try:
        return PreflightResult(check, True, probe())
    except Exception as error:
        return PreflightResult(check, False, safe_error_message(error).splitlines()[0][:300])


def check_runtime(
    spark: SparkSession,
    version: tuple[int, int] | None = None,
    *,
    client_version: str | None = None,
    environment: Mapping[str, str] | None = None,
) -> PreflightResult:
    """Python >= 3.12 (the code's minimum) and the three versions that differ on serverless.

    Serverless runs Spark Connect: ``spark.version`` is the **server** Spark, ``pyspark`` is the
    **client** library the runtime ships, and ``DATABRICKS_RUNTIME_VERSION`` names the runtime
    (``local`` outside Databricks). docs/databricks/compatibility.md.
    """
    major, minor = version or sys.version_info[:2]
    client = client_version or pyspark.__version__
    runtime = (os.environ if environment is None else environment).get(
        "DATABRICKS_RUNTIME_VERSION", "local"
    )

    def probe() -> str:
        if (major, minor) < MINIMUM_PYTHON:
            raise RuntimeError(f"Python {major}.{minor} < required {MINIMUM_PYTHON}")
        return (
            f"Python {major}.{minor}; Spark {spark.version} (server); "
            f"PySpark {client} (client); runtime: {runtime}"
        )

    return _attempt("runtime", probe)


def check_catalog(spark: SparkSession, catalog: str) -> PreflightResult:
    def probe() -> str:
        catalogs = sorted(row[0] for row in spark.sql("SHOW CATALOGS").collect())  # a few rows
        if catalog not in catalogs:
            raise LookupError(f"catalog {catalog!r} not found; visible: {catalogs}")
        return f"catalog {catalog} found"

    return _attempt("catalog", probe)


def check_schema(spark: SparkSession, namespace: str) -> PreflightResult:
    def probe() -> str:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {validate_namespace(namespace)}")
        return f"schema {namespace} ready"

    return _attempt("create_schema", probe)


def check_volume(spark: SparkSession, namespace: str, volume: str) -> PreflightResult:
    """Create the Volume and write/read a file through its POSIX path (``/Volumes/...``)."""

    def probe() -> str:
        catalog, schema = validate_namespace(namespace).split(".")
        if not volume.isidentifier():
            raise ValueError(f"invalid volume name {volume!r}")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {namespace}.{volume}")
        path = Path("/Volumes") / catalog / schema / volume / PROBE_FILE
        path.write_text("preflight", encoding="utf-8")
        return f"{path.parent} writable: {path.read_text(encoding='utf-8') == 'preflight'}"

    return _attempt("volume_files", probe)


def check_delta_change_feed(spark: SparkSession, namespace: str) -> PreflightResult:
    """Delta with CDF: insert-only MERGE, ``table_changes``, history and time travel."""

    def probe() -> str:
        table = f"{validate_namespace(namespace)}.{PROBE_TABLE}"
        spark.sql(
            f"CREATE OR REPLACE TABLE {table} (k STRING) USING DELTA "
            "TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
        spark.sql(f"INSERT INTO {table} VALUES ('a')")  # noqa: S608 - validated name
        spark.sql(
            f"MERGE INTO {table} t USING (SELECT 'b' AS k) s ON t.k = s.k "
            "WHEN NOT MATCHED THEN INSERT *"
        )
        history = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").select("version").first()
        latest = int(history["version"]) if history else -1
        # table validated, versions are ints -> no injection surface
        kinds = spark.sql(f"SELECT _change_type FROM table_changes('{table}', 1, {latest})")  # noqa: S608
        changes = sorted(r[0] for r in kinds.collect())  # 2 rows
        before = spark.sql(f"SELECT * FROM {table} VERSION AS OF {latest - 1}").count()  # noqa: S608
        if changes != ["insert", "insert"] or before != 1:
            raise RuntimeError(f"unexpected change feed {changes} or time travel {before}")
        return f"CDF, history (v{latest}) and time travel work"

    return _attempt("delta_change_feed", probe)


def urllib_status(url: str, timeout: float) -> int:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})  # noqa: S310
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - https only
        return int(response.status)


def check_https(urls: Sequence[str], opener: Opener = urllib_status) -> PreflightResult:
    """One GET per official API (no retries): is outbound internet available?"""

    def probe() -> str:
        statuses = []
        for url in urls:
            if not url.startswith("https://"):
                raise ValueError(f"only https URLs are checked: {url}")
            statuses.append(f"{url.split('/')[2]}={opener(url, HTTPS_TIMEOUT_SECONDS)}")
        return ", ".join(statuses)

    return _attempt("outbound_https", probe)


def run_preflight(
    spark: SparkSession,
    *,
    namespace: str,
    volume: str,
    https_urls: Sequence[str] = OFFICIAL_APIS,
    opener: Opener = urllib_status,
) -> list[PreflightResult]:
    """Every check, in dependency order; a failed check does not stop the next ones."""
    catalog = validate_namespace(namespace).split(".")[0]
    return [
        check_runtime(spark),
        check_catalog(spark, catalog),
        check_schema(spark, namespace),
        check_volume(spark, namespace, volume),
        check_delta_change_feed(spark, namespace),
        check_https(https_urls, opener),
    ]
