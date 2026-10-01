# Phase 2 Briefing — Engineering Quality

**Project:** industrial-telemetry-streaming-pipeline  
**Status:** FASE 1 closed and approved 2026-10-01; FASE 2 ready to begin  
**Owner:** Anthony Davila Carrillo  
**Briefing date:** 2026-10-01  

---

## Overview

Phase 1 built a solid, reproducible pipeline: simulator → MQTT → Kafka with Avro, end-to-end verified with 200/200 messages. Phase 2 elevates it to production-grade:

1. **Alarm consumer** — production logic (thresholds, persistence, hysteresis) on Kafka → topics
2. **Dead-letter handling** — invalid messages routed, not dropped
3. **Structured logging** — JSON across all components
4. **Tests** — unit + integration with compose
5. **CI pipeline** — GitHub Actions, reproducible on every commit
6. **Telemetry into ThingsBoard** — wire Kafka alarms to visualization

At the end of Phase 2, `make up && make test` on a fresh clone must pass, CI must run, and the stack must flow telemetry end-to-end into ThingsBoard. The README stays a stub until Phase 3.

---

## Scope and Decisions

### S1: Alarm Consumer Language and Location

**Decision:** Node.js, same repo as simulator and bridge.

**Rationale:**
- Brings diversity to the stack: Python (simulator, bridge) + Node.js (consumer, matches course pattern)
- Shares `config/alarms.yaml` seamlessly through common loader
- Demonstrates cross-language Kafka integration (production-grade)
- Node.js ecosystem: `kafkajs` + `winston` for structured logging

**Design Sketch (refined during work):**
```
Kafka.telemetry.raw --consume--> AlarmConsumer
  ├─ reads config/alarms.yaml (asset thresholds, hysteresis, persistence window)
  ├─ tracks asset state per alarm rule (triggered, cleared, debounce)
  ├─ outputs (asset_id, rule_id, state_change, ts) to telemetry.alarms
  └─ logs structured JSON (asset, rule, trigger_value, transition)

Invalid messages (schema mismatch, missing fields) → telemetry.dlq + log + metric
```

### S2: Dead-Letter Topic

**Specification:**
- Topic: `telemetry.dlq` (1 partition, retention 7 days)
- Message format: `{ original_bytes: "<base64>", error: "<reason>", bridge_ts: <ms> }`
- Trigger: bridge validation failure (required fields, type mismatch, NaN, bounds)
- Logging: structured JSON, level WARN, includes original key/value (redacted if secrets suspected)

**No consumer** in Phase 2; Phase 3 may add a monitor.

### S3: Structured Logging

**Standard:** JSON-only on stderr, no printf.

**Implementation:**
- Consolidate `edge/simulator/jsonlog.py` + `bridge/mqtt_to_kafka/jsonlog.py` → shared package `common/jsonlog.py`
- AlarmConsumer uses the same package
- Fields: `@timestamp` (ISO 8601 UTC), `level`, `component`, `msg`, contextual fields (asset_id, seq, rule_id, etc.)

**Verification in tests:** parse stderr lines as JSON, no non-JSON lines present.

### S4: Test Strategy

**Scope:** Unit + Integration

**Unit Tests (Python unittest or pytest):**
- Simulator: fault injection, duration bounds, reproducibility, edge cases (empty config, bad seed)
- Bridge validation: malformed messages, schema errors, round-trip Avro
- AlarmConsumer logic: threshold transitions, hysteresis, persistence window, state cleanup

**Integration Tests (Docker Compose + Testcontainers or compose fixture):**
- Fresh stack (5 containers): bring up, verify health
- Simulator → Mosquitto → Bridge → Kafka: 50 messages, verify all stages, decoded against schema
- Bridge + Consumer: 50 messages in, verify alarm triggers match config, output to dlq for intentional faults
- Idempotent import of rule chains
- Teardown: no dangling volumes, clean exit codes

**Test Execution:**
```
make test
```
Runs unit tests (pytest in .venv) + integration tests (compose + tools), reports coverage, exits non-zero on any failure.

**CI (GitHub Actions):**
- Trigger: push to main, all pull requests
- Matrix: run on ubuntu-latest (pull base image, Docker available)
- Steps: lint, unit test, integration test, coverage report
- Artifacts: test report, coverage badge

### S5: Telemetry into ThingsBoard

**Bridge:** already sends telemetry to Kafka (Phase 1). No change needed.

**Consumer:** sends alarms to Kafka topic `telemetry.alarms`.

**Wiring (new in Phase 2):**
- Root Rule Chain node: "AlarmToThingsBoard" (existing REST node, renamed)
- Input: subscribe to `telemetry.alarms` (Kafka Connector in ThingsBoard)
- Output: Store the alarm in device telemetry attributes (asset_id, rule_id, state, ts)
- Node: "TelemetryLogger" → existing, log alarms

**Verification:**
- ThingsBoard device dashboard shows latest alarm state per rule
- Alarm timeline in the UI matches Kafka offset
- No loss of events in the round-trip

**Note:** ThingsBoard is not in the Critical Path for Phase 2 completion; if wiring is complex, it moves to Phase 3 refinement. Focus is alarm consumer + tests.

---

## Detailed Work Plan

### Task 2.1: Refactor Logging → Shared Package (Python + Node.js)

**Deliverable:** 
- Python: `common/jsonlog.py` (1 function, 1 class) — consolidate `edge/simulator/jsonlog.py` + `bridge/mqtt_to_kafka/jsonlog.py`
- Node.js: `common/jsonlog.js` (1 class) — shared JSON logger for alarm consumer (mirrors Python behavior)

**Owner decisions recorded 2026-10-01 (after Step 1 findings):**
- Module name is `common/jsonlog.py`, not `common/logging.py`: a module shadowing the standard library is avoided, not guarded by a test. Node counterpart is `common/jsonlog.js`.
- Output format: compact JSON in both languages (no spaces after separators, non-ASCII not escaped). The Node logger writes Python level names (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`). Field names unchanged: `ts`, `level`, `logger`, `event`, extras flattened, `exception` when present.
- `scripts/smoke/avro_consume.py` stays out of the refactor; its plain-text stdout report is read by a human.
- `broker/kafka/provision.py` and `scripts/thingsboard/import_rule_chains.py` migrate to the shared logger (structured logging is a cross-cutting requirement).
- Node.js consumer supersedes Phase 0 decision 14 and `phase2_backlog.md` §8.3 item 1; record a dated owner note in the backlog, as was done for Firebase. `common/` is approved as a new top-level directory.

**Steps:**
1. Read `edge/simulator/jsonlog.py` and `bridge/mqtt_to_kafka/jsonlog.py`; confirm they are identical. (Done: identical, SHA-256 5c3b3cc086b4bf5a…, 40 lines.)
2. Create `common/jsonlog.py` with the shared code, switched to compact JSON.
3. Update imports in `edge/simulator/__main__.py`, `bridge/mqtt_to_kafka/__main__.py`, `broker/kafka/provision.py`, `scripts/thingsboard/import_rule_chains.py`; delete the two original copies.
4. Create `common/jsonlog.js` producing byte-identical lines for the same record.
5. Add a small equivalence test: same event, same extras, through both loggers, lines compared as strings (excluding `ts`).
6. Run the Phase 1 smoke test (closure report section 3) with the stack brought up by `make up` and down by `make down` (no `-v`); verify 200/200 at every stage, stderr JSON-only, 0 non-JSON lines, exit 0.
7. Commit on `feature/phase2-shared-logging`, Conventional Commits, merge `--no-ff`.

**DETENTE 2.1.a:** Smoke test passes, stderr JSON-clean. Confirm.

---

### Task 2.2: Build Alarm Consumer (Node.js)

**Location:** `processing/alarm_consumer/` (new)

**Files:**
- `index.js` — CLI entry point, `--bootstrap-servers`, `--config`, exit on Kafka unavailable
- `consumer.js` — Kafka consumer loop (kafkajs), message parsing, state machine
- `state_machine.js` — Asset alarm state, threshold logic, hysteresis, persistence window, debounce
- `config_loader.js` — Load `config/alarms.yaml` (reuse Python loader or implement in Node)

**Dependencies:**
- `kafkajs` (already in package.json from Phase 1)
- `winston` for structured JSON logging (or simple `console.log` with structured output if preferred)
- `yaml` (to parse config/alarms.yaml)

**Design Notes:**
- Read `config/alarms.yaml` on startup; fail fast if missing or invalid
- For each asset, maintain: Map of active_alarms (rule_id → state)
- On each `telemetry.raw` message:
  1. Parse and validate against schema (reuse bridge validation helper)
  2. Extract numeric values (temperature, vibration, etc.)
  3. For each rule in `config/alarms.yaml` matching this asset:
     - Evaluate threshold (e.g., `temperature > 80`)
     - Apply hysteresis (don't re-trigger within N seconds)
     - Track state in Map (triggered_at, debounce_until)
     - If state changed: produce to `telemetry.alarms` with `{ asset_id, rule_id, state, ts, value }`
  4. Invalid messages: produce to `telemetry.dlq`, log WARN
- Periodic cleanup: remove alarms older than persistence window (default 1 hour) with setInterval
- On SIGTERM: flush pending alarms, close Kafka, exit 0

**Verification (manual before tests):**
- Run consumer against Kafka (from Phase 1 stack, no simulator)
- Inject 5 test messages by hand (Avro, key by asset_id) using `scripts/smoke/avro_produce.js` (new)
- Verify 5 alarms appear in `telemetry.alarms` and dlq (if any invalid)
- Check stderr for JSON lines with rule_id, state, asset_id

**DETENTE 2.2.a:** Consumer runs, consumes from Kafka, produces to alarms topic with correct schema. Log the consumer startup message. Confirm.

**DETENTE 2.2.b:** Config loader rejects bad YAML. Config with missing asset/rule is caught. Confirm.

---

### Task 2.3: Implement Dead-Letter Topic

**Steps:**
1. Ensure `docker-compose.yml` topic provisioning creates `telemetry.dlq` (1 partition, 1 min retention for now, changed in Phase 3)
2. Bridge validation: on error, produce to DLQ with `{ original_bytes, error, bridge_ts }` (base64 the bytes)
3. Consumer validation: on error, produce to DLQ, same format
4. Logging: every DLQ produce includes WARN-level structured log with component, error reason, original key

**Verification:**
- Smoke test with 1 intentional malformed message
- Verify 1 message in DLQ, readable (base64 decode matches original), logging shows WARN
- Verify valid messages are **not** in DLQ

**DETENTE 2.3.a:** DLQ topic exists, receives malformed messages, zero valid messages in it. Confirm.

---

### Task 2.4: Unit Tests

**Framework:** 
- Python: pytest (already in `requirements.txt` after Phase 1)
- Node.js: Jest or Mocha + Chai (add to `package.json`)

**Test Files:**
- `tests/unit/test_simulator.py` — fault injection, duration bounds, seed reproducibility
- `tests/unit/test_bridge_validation.py` — malformed messages, schema errors, round-trip Avro
- `tests/unit/test_config_loader.py` — parse `config/alarms.yaml`, reject bad YAML, missing asset (Python)
- `tests/unit/alarm_consumer.test.js` — threshold logic, hysteresis, state transitions, persistence cleanup (Node.js)
- `tests/unit/config_loader.test.js` — parse YAML in Node context (Node.js)

**Coverage Target:** 80% of application code (exclude CLI argument parsing, Kafka broker connection errors)

**DETENTE 2.4.a:** `pytest tests/unit/` + `npm test` run, both pass, combined coverage ≥ 80%. Output to stdout.

---

### Task 2.5: Integration Tests

**Framework:** Docker Compose + pytest fixture or testcontainers (Python)

**Test File:** `tests/integration/test_e2e_stack.py`

**Tests:**
1. **Stack health:** `make up`, wait 10 s, `docker ps -f label=com.docker.compose.project` shows 5 healthy containers
2. **Simulator → Kafka:** 50 messages from simulator, all decoded in Kafka with correct schema id
3. **Bridge + Consumer:** 50 messages in, verify alarm count matches thresholds in config, 0 lost, 0 duplicates
4. **DLQ routing:** inject 5 intentional faults, verify 5 in DLQ, 45 valid in `telemetry.alarms`
5. **Rule-chain import:** `make up` runs import, query ThingsBoard REST API, verify 4 chains present and linked

**Teardown:** `make down` (no `-v`), named volumes kept for next run, tmpfs volumes cleaned

**DETENTE 2.5.a:** Integration tests pass. Report container health, message counts (simulator in, Kafka out, alarms, DLQ), and rule-chain import status. Confirm.

---

### Task 2.6: GitHub Actions CI

**File:** `.github/workflows/ci.yml`

**Pipeline:**
1. **Trigger:** push to main, pull requests to main
2. **Matrix:** ubuntu-latest (Docker available), Python 3.13
3. **Steps:**
   ```
   - Checkout
   - Set up Python 3.13
   - Install dependencies: pip install -r requirements.txt
   - Lint: flake8 (or similar; optional in Phase 2, but advisable)
   - Unit tests: pytest tests/unit/ --cov --cov-report=xml
   - Integration tests: pytest tests/integration/ (requires docker-in-docker or service containers)
   - Upload coverage: codecov (optional; phase 3)
   - Artifact: test report
   ```
4. **Failure:** exits non-zero, blocks merge (if protected branch rule set)

**Notes:**
- Integration tests in CI may require `docker run ... -v /var/run/docker.sock` or a service container
- Phase 2: focus on getting unit tests to CI first; if integration tests are complex, move to Phase 3 refinement
- Owner will approve whether to run integration tests on every commit (cost vs. confidence trade-off)

**DETENTE 2.6.a:** CI workflow file exists, runs on a test push, unit tests pass. Report workflow URL and pass/fail status. Confirm.

---

### Task 2.7: Makefile Updates

**New targets:**
- `make test` — runs `pytest tests/unit/ && pytest tests/integration/`
- `make lint` (optional Phase 2, advised) — `flake8 src/ tests/`
- `make coverage` — pytest with coverage report, exits if < 80%

**Update existing:**
- `make down` — document that named volumes are kept (add to help)

**Verification:** `make test` on a fresh clone (after `make install && make up`) passes in < 2 min.

**DETENTE 2.7.a:** `make test` passes, `make lint` (if added) passes. Confirm.

---

### Task 2.8: Consolidate and Verify

**Steps:**
1. Run full `make install && make up && make test` on a fresh directory (python tests + npm tests)
2. Check: 0 secrets in git (`.gitignore` + `.gitattributes` updated if needed for line endings)
3. Check: CI workflow runs successfully (manual push to a feature branch, check Actions tab)
4. Verify: simulator + bridge + consumer produce correct message counts to all three topics (telemetry.raw, telemetry.alarms, telemetry.dlq)
5. Verify: stderr of all components is JSON-only (grep for non-JSON, expect 0 lines from Python + Node.js)
6. Verify: GitHub checkout with no `.env` defaults correctly (must error, not silently fail)
7. Verify: alarm consumer runs in Node.js, consumes from Kafka, produces alarms with correct schema

**DETENTE 2.8.a:** Full stack runs end-to-end (simulator → alarms → dlq), CI passes, secrets clean, all component stderr is JSON. Confirm.

---

## Definition of Done (Phase 2)

| Criterion | Status |
|---|---|
| `make test` passes (unit + integration) on a fresh clone after `make install && make up` | Pending |
| Alarm consumer produces to `telemetry.alarms` with correct schema and counts | Pending |
| Invalid messages routed to `telemetry.dlq` with base64 payload and error reason | Pending |
| All component output (simulator, bridge, consumer) is JSON-only, structured logging | Pending |
| GitHub Actions CI runs on push/PR, unit tests green, integration tests optional Phase 3 | Pending |
| Zero secrets in tree, `.gitignore` clean | Pending |
| `Makefile` has `test`, `lint`, `coverage` targets | Pending |
| README still a stub, no unmeasured claims | Pending |

---

## Decisions Logged for This Phase

1. **Alarm consumer in Python, same repo** (S1)
2. **DLQ format with base64 payload** (S2)
3. **Shared logging package** (S3)
4. **80% coverage target** (S4)
5. **ThingsBoard wiring optional for Phase 2 completion** (S5)
6. **CI on GitHub Actions, unit tests required, integration tests owner-decision** (S6)

---

## Outstanding from Phase 1 (Still Open)

- Firebase credential rotation (owner decision)
- Twelve Airflow/Hadoop containers (owner decision on restart)
- ThingsBoard first-install healthcheck timing (Phase 2 or 3)
- `KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE` inert variable cleanup (Phase 2 advisory)
- Line ending consistency for `.gitattributes`, `.gitignore`, `LICENSE` (Phase 2 advisory)

---

## Dependency and Risk Summary

### Dependencies
- Phase 1 stack must be running (make up from Phase 1 directory works)
- `config/alarms.yaml` from Phase 1 is the source of truth
- Schema Registry must have `telemetry.raw` and `alarm_v1` schemas (verified in Phase 1)

### Risks
1. **Kafka consumer group management:** if consumer crashes and restarts, it will re-consume from offset. Idempotency in alarms (same rule_id should not trigger twice for same event) depends on state deduplication. **Mitigation:** state is in-memory; restart clears it. Long-term: track last-seen offset per rule.
2. **ThingsBoard wiring complexity:** if the Kafka Connector in ThingsBoard is not straightforward, Phase 2 can close without it and move to Phase 3. **Decision point: DETENTE 2.5.a**.
3. **CI with Docker-in-Docker:** GitHub Actions ubuntu-latest has Docker, but integration tests may need special setup. **Mitigation:** Phase 2 focuses unit tests in CI; integration tests can be optional or moved to local runs only.

---

## Checkpoints (DETENTE)

| DETENTE | Blocker? | What | Owner approval? |
|---|---|---|---|
| 2.1.a | No | Smoke test with shared logging passes, stderr JSON-clean | Yes |
| 2.2.a | Yes | Alarm consumer consumes, produces alarms | Yes |
| 2.2.b | Yes | Config loader rejects bad YAML | Yes |
| 2.3.a | Yes | DLQ topic receives malformed, zero valid messages | Yes |
| 2.4.a | Yes | Unit tests pass, coverage ≥ 80% | Yes |
| 2.5.a | No (advisory) | Integration tests pass, report message counts and chain status | Yes |
| 2.6.a | Yes | CI workflow runs, unit tests green on test push | Yes |
| 2.7.a | Yes | `make test` and `make lint` pass | Yes |
| 2.8.a | Yes | Full end-to-end: simulator → alarms → dlq, CI passes, secrets clean | Yes |

Each DETENTE is a pause point where Claude Code reports findings and waits for owner confirmation before continuing.

---

## Next Steps (After Phase 2 Close)

**Phase 3 — Evidence and Documentation:**
- Benchmark: throughput (msg/s) and latency (p50/p95) at scale (1000 msg/s, 60 s run)
- ThingsBoard dashboard export (if not completed in Phase 2)
- Architecture diagram with data flows and message schemas
- Final README with Problem-Action-Result structure, measured numbers only
- Draft cover letter for hiring managers (optional, separate from repo)

---

**Start now. Report at each DETENTE. Owner must approve every checkpoint before continuation.**

