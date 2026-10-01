# Developer entry points.
#
#   make install        create .venv, install requirements.txt, and install the
#                       npm dependencies from package-lock.json (npm ci)
#   make up             start the stack, create Kafka topics, set Schema Registry
#                       compatibility, import the ThingsBoard rule chains
#   make down           stop the stack (keeps the ThingsBoard volume; never -v)
#   make logs           follow the container logs
#   make check-secrets  scan staged files; runs before every commit in Phase 1
#
# Settings come from .env (copy .env.example), exported to every recipe.

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

.PHONY: install up down logs check-secrets test

install:
	$(PYTHON) -m venv .venv
	$(VENV_PY) -m pip install -r requirements.txt
	npm ci --no-fund --no-audit

# --force-recreate: containers are always created fresh. Phase 0 reproduced a
# ThingsBoard container that, restarted after an unclean stop, came up without
# its port bindings. ThingsBoard data lives in a named volume, and Kafka and
# ZooKeeper start empty by design, so recreating loses nothing.
up:
	$(COMPOSE) up -d --wait --wait-timeout 600 --force-recreate
	$(VENV_PY) -m broker.kafka.provision
	$(VENV_PY) scripts/thingsboard/import_rule_chains.py

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=200

check-secrets:
	$(PYTHON) scripts/check_secrets.py

test:
	@echo "no tests yet - Phase 2"
	@exit 1
