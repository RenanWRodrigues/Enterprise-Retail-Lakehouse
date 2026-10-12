# Linux test environment for the whole monorepo.
# Why: Delta Lake (and any local Spark file write) needs Hadoop native helpers that Windows
# lacks (winutils.exe). Linux also matches Databricks and future CI.
#
#   docker build -f docker/tests.Dockerfile -t lakehouse-tests .
#   docker run --rm -v "${PWD}:/workspace" lakehouse-tests                   # PowerShell
#   docker run --rm -v "$(pwd -W):/workspace" lakehouse-tests                # Git Bash
#   docker run --rm -v "${PWD}:/workspace" lakehouse-tests python -m pytest -m integration
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /workspace

# Only pyproject.toml is copied: the repository is mounted at run time.
COPY pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --group dev

# Resolve the Delta JARs at build time (Ivy cache), so test runs need no network.
RUN python -c "from delta import configure_spark_with_delta_pip as delta; \
from pyspark.sql import SparkSession; \
delta(SparkSession.builder.master('local[1]')).getOrCreate().stop()"

CMD ["python", "-m", "pytest", "-p", "no:cacheprovider"]
