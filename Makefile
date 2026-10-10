# Developer entry points. `make` alone prints the help below, which is the
# documentation of every target; keep it in step with the recipes.
#
# Settings come from .env (copy .env.example), exported to every recipe.
#
# Every target runs from Git Bash and PowerShell on Windows and from bash on
# Linux. Recipes that would need shell features (deleting trees, background
# processes) are Python scripts called on a single line instead.

-include .env
export

PYTHON ?= python
COMPOSE ?= docker compose
ifeq ($(OS),Windows_NT)
# GNU Make on Windows runs a recipe line without a shell when it has no shell
# metacharacters, and needs the leading ./ to find a relative program path.
# Keep $(VENV_PY) recipe lines free of quotes and metacharacters: with them,
# Make hands the line to cmd.exe, which cannot run a ./ path.
VENV_PY := ./.venv/Scripts/python.exe
else
VENV_PY := .venv/bin/python
endif

# Printed with $(info), so the text never passes through a shell and reads
# the same everywhere.
define HELP
Usage: make <target>    (make alone prints this help)

stack
  make up                     start the full stack (core plus ThingsBoard), create the Kafka topics, set
                              Schema Registry compatibility, import the ThingsBoard rule chains
  make up-core                start the core stack only (Mosquitto, ZooKeeper, Kafka, Schema Registry), provision
  make up-pipeline            make up, then the bridge, the alarm consumer and the ThingsBoard sink in
                              containers (images built from docker/); do not also run them on the host
  make up-pipeline-core       make up-core, then the bridge and the alarm consumer in containers
  make down                   stop and remove every container of every profile (ThingsBoard and the
                              pipeline containers included); named volumes are kept, never -v
  make logs                   follow the container logs (all profiles)

quality
  make lint                   ruff (rules E, F) on Python; node --check on every tracked .js
  make test                   unit tests, Python and Node.js, with coverage (lines and branches >= 80 per cent each)
  make coverage               as make test, also writing coverage.xml and coverage/lcov.info
  make test-integration       integration tests on the core stack (starts and stops it; run with no stack up)
  make test-integration-full  integration tests including ThingsBoard (same rule)
  make smoke                  the Phase 1 smoke run, 200 messages end to end (stack up, telemetry.raw empty)
  make smoke-containers       the same 200-message run against the pipeline in containers (after make
                              up-pipeline-core or make up-pipeline, telemetry.raw empty)
  make check-secrets          scan the staged files for secrets; the pre-commit hook runs it

tools
  make install                create .venv, install the Python and npm dependencies, enable the
                              versioned git hooks (core.hooksPath scripts/hooks)
  make clean                  remove test, coverage and smoke output and __pycache__; never Docker volumes or .env
  make help                   this text
endef

.DEFAULT_GOAL := help

.PHONY: help install up up-core up-pipeline up-pipeline-core down logs check-secrets lint test coverage test-integration test-integration-full smoke smoke-containers clean

# The recipe line is only there so Make does not report "Nothing to be done";
# `exit 0` is a builtin of both sh and cmd.exe.
help:
	$(info $(HELP))
	@exit 0

install:
	$(PYTHON) -m venv .venv
	$(VENV_PY) -m pip install -r requirements.txt -r requirements-dev.txt
	npm ci --no-fund --no-audit
	git config core.hooksPath scripts/hooks

# --force-recreate: containers are always created fresh. Phase 0 reproduced a
# ThingsBoard container that, restarted after an unclean stop, came up without
# its port bindings. ThingsBoard data lives in a named volume, and Kafka and
# ZooKeeper start empty by design, so recreating loses nothing.
up:
	$(COMPOSE) --profile full up -d --wait --wait-timeout 600 --force-recreate
	$(VENV_PY) -m broker.kafka.provision
	$(VENV_PY) scripts/thingsboard/import_rule_chains.py

up-core:
	$(COMPOSE) up -d --wait --wait-timeout 600 --force-recreate
	$(VENV_PY) -m broker.kafka.provision

# The pipeline in containers (Phase 3, Task 3.3, owner decisions E1, D-B and
# D-C): the stack first, provisioned (and, with ThingsBoard, the rule chains
# imported), because the bridge and the alarm consumer refuse to start
# without their topics and the sink needs the devices the import creates.
# Then the images are built and only the pipeline containers are started:
# --no-deps, so the running stack is not recreated (that would empty Kafka).
up-pipeline: up
	$(COMPOSE) --profile pipeline build
	$(COMPOSE) --profile full --profile pipeline --profile pipeline-tb up -d --wait --wait-timeout 120 --no-deps --force-recreate bridge alarm-consumer thingsboard-sink

up-pipeline-core: up-core
	$(COMPOSE) --profile pipeline build
	$(COMPOSE) --profile pipeline up -d --wait --wait-timeout 120 --no-deps --force-recreate bridge alarm-consumer

# Every profile, so nothing the project started is left behind. Verified on 2026-10-01 (Compose v2.40.3): a plain
# `docker compose down` removes only the services of the active profiles, so
# after `make up` it stopped the four core services, left ThingsBoard running,
# and could not remove the network ("Resource is still in use"). With
# --profile full every service of the project is stopped and removed, and
# with no ThingsBoard container it behaves exactly like a plain down. The
# pipeline profiles (Task 3.3) are listed for the same reason.
ALL_PROFILES := --profile full --profile pipeline --profile pipeline-tb --profile simulator

down:
	$(COMPOSE) $(ALL_PROFILES) down

logs:
	$(COMPOSE) $(ALL_PROFILES) logs -f --tail=200

check-secrets:
	$(VENV_PY) scripts/check_secrets.py

lint:
	$(VENV_PY) -m ruff check .
	node scripts/check_js_syntax.js

# Thresholds live in pyproject.toml (fail_under, and check_branch_coverage for
# branches alone) and jest.config.js (coverageThreshold); package.json is the only entry point for Jest.
test:
	$(VENV_PY) -m pytest --cov
	$(VENV_PY) scripts/check_branch_coverage.py
	npm test

coverage:
	$(VENV_PY) -m pytest --cov --cov-report=term-missing --cov-report=xml
	$(VENV_PY) scripts/check_branch_coverage.py
	npm run test:coverage

test-integration:
	$(VENV_PY) -m pytest tests/integration --stack-profile=core

test-integration-full:
	$(VENV_PY) -m pytest tests/integration --stack-profile=full

smoke:
	$(VENV_PY) scripts/smoke/run_smoke.py

smoke-containers:
	$(VENV_PY) scripts/smoke/run_smoke_containers.py

clean:
	$(VENV_PY) scripts/clean.py --pycache .coverage coverage.xml coverage.json coverage .pytest_cache .ruff_cache smoke-logs smoke-logs-containers
