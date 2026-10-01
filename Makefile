# Developer entry points.
#
#   make install                create .venv, install requirements.txt and
#                               requirements-dev.txt, and the npm dependencies
#                               from package-lock.json (npm ci)
#   make up                     start the full stack (profile "full": core plus
#                               ThingsBoard), create Kafka topics, set Schema
#                               Registry compatibility, import the rule chains
#   make up-core                start the core stack only (no ThingsBoard), create
#                               topics, set compatibility
#   make down                   stop the stack (keeps the ThingsBoard volume; never -v)
#   make logs                   follow the container logs
#   make test                   unit tests, Python and Node.js, with coverage
#   make test-integration       integration tests against the core stack
#   make test-integration-full  integration tests including ThingsBoard
#   make check-secrets          scan staged files; runs before every commit
#
# Settings come from .env (copy .env.example), exported to every recipe.
# The integration tests start and stop the stack themselves; run them with no
# stack up.

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
JEST := node node_modules/jest/bin/jest.js

.PHONY: install up up-core down logs check-secrets test test-integration test-integration-full

install:
	$(PYTHON) -m venv .venv
	$(VENV_PY) -m pip install -r requirements.txt -r requirements-dev.txt
	npm ci --no-fund --no-audit

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

# Always with --profile full. Verified on 2026-10-01 (Compose v2.40.3): a plain
# `docker compose down` removes only the services of the active profiles, so
# after `make up` it stopped the four core services, left ThingsBoard running,
# and could not remove the network ("Resource is still in use"). With
# --profile full every service of the project is stopped and removed, and
# with no ThingsBoard container it behaves exactly like a plain down.
down:
	$(COMPOSE) --profile full down

logs:
	$(COMPOSE) --profile full logs -f --tail=200

check-secrets:
	$(PYTHON) scripts/check_secrets.py

test:
	$(VENV_PY) -m pytest --cov
	$(JEST) --coverage

test-integration:
	$(VENV_PY) -m pytest tests/integration --stack-profile=core

test-integration-full:
	$(VENV_PY) -m pytest tests/integration --stack-profile=full
