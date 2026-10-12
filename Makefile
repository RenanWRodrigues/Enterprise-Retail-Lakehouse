# Developer and CI commands (GNU make). CI calls these same targets (.github/workflows/ci.yml),
# so a target that works here works there. Every test target runs pytest by marker
# (docs/TESTING_STRATEGY.md): unit | transformation | data_quality | contract | integration.
#
#   make install           .venv + dev dependencies (pyproject [dependency-groups] dev)
#   make lint              ruff check, ruff format --check, mypy strict
#   make test              every test except integration (what the CI chain runs)
#   make test-unit         pure Python, no Spark, no network
#   make test-transformation   PySpark transformations (needs Java 17/21)
#   make test-quality      data quality + contract tests (Spark)
#   make test-integration  local Spark + Delta end to end (Linux/Docker; skipped on Windows)
#   make coverage          unit + transformation + quality with coverage, fail_under 85
#
# COV=1 makes a test target write .coverage.<suite> (CI combines them with coverage-report).
# No target talks to Databricks or to a public API: HTTP is faked, data is local.

ifeq ($(OS),Windows_NT)
VENV_PYTHON := .venv/Scripts/python.exe
else
VENV_PYTHON := .venv/bin/python
endif

SYSTEM_PYTHON ?= python
PYTHON ?= $(VENV_PYTHON)
PYTEST := $(PYTHON) -m pytest -p no:cacheprovider
COV ?= 0

ifeq ($(COV),1)
COV_ARGS := --cov --cov-report= --cov-fail-under=0
endif

.PHONY: help install lint test test-unit test-transformation test-quality test-integration \
	coverage coverage-report

help:  # default target: the command list from the header above
	@$(SYSTEM_PYTHON) -c "import pathlib; print(chr(10).join(l[4:] for l in pathlib.Path('Makefile').read_text().splitlines() if l.startswith('#   make')))"

install:
	$(SYSTEM_PYTHON) -m venv .venv
	$(PYTHON) -m pip install --upgrade "pip>=25.1"
	$(PYTHON) -m pip install --group dev

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .
	$(PYTHON) -m mypy
	$(PYTHON) -m mypy projects/industrial-iot-lakehouse/tests/conftest.py
	$(PYTHON) -m mypy projects/brazilian-financial-lakehouse/tests/conftest.py

test:
	$(PYTEST) -m "not integration" $(COV_ARGS)

test-unit: export COVERAGE_FILE := .coverage.unit
test-unit:
	$(PYTEST) -m unit $(COV_ARGS)

test-transformation: export COVERAGE_FILE := .coverage.transformation
test-transformation:
	$(PYTEST) -m transformation $(COV_ARGS)

test-quality: export COVERAGE_FILE := .coverage.quality
test-quality:
	$(PYTEST) -m "data_quality or contract" $(COV_ARGS)

test-integration: export COVERAGE_FILE := .coverage.integration
test-integration:
	$(PYTEST) -m integration $(COV_ARGS)

coverage:
	$(PYTHON) -m coverage erase
	$(MAKE) test-unit COV=1
	$(MAKE) test-transformation COV=1
	$(MAKE) test-quality COV=1
	$(MAKE) coverage-report

coverage-report:  # combines every .coverage.<suite> in the repository root
	$(PYTHON) -m coverage combine
	$(PYTHON) -m coverage report
