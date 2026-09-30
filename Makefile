# Developer entry points. The stack definition (docker-compose.yml) and the
# ThingsBoard import step behind `up` arrive in Phase 1 Task 1.7.
#
# `make check-secrets` runs before every commit in this phase. A non-zero exit
# blocks the commit.

PYTHON ?= python
COMPOSE ?= docker compose

.PHONY: up down logs check-secrets test

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=200

check-secrets:
	$(PYTHON) scripts/check_secrets.py

test:
	@echo "no tests yet - Phase 2"
	@exit 1
