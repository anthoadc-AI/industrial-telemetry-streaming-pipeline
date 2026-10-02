# Phase 2 Closure Report — Engineering Quality

| Field | Value |
|---|---|
| Project | industrial-telemetry-streaming-pipeline |
| Phase | 2 of 3, Engineering quality |
| Briefing | `docs/internal/claude_code_briefings/phase2_briefing_engineering_quality.md`, as last revised in `86a17bd` (2026-10-02) |
| Work dates | 2026-10-01 and 2026-10-02 |
| Machine | Windows 11 Home, Docker Engine 29.0.1, Compose v2.40.3, Python 3.13.4, Node.js 24.16.0, GNU Make 3.81 (GnuWin32) |
| CI | GitHub Actions, `ubuntu-24.04`, GNU Make 4.3 |
| Status | **Closed, owner approval 2026-10-02.** Tag `v2-engineering-quality`. |

All data produced in this phase is synthetic.

---

## 1. Summary

Phase 2 turned the Phase 1 pipeline into a tested, continuously integrated system with a production-style alarm stage:

```
edge/simulator --MQTT--> Mosquitto --bridge--> Kafka telemetry.raw --alarm consumer (Node.js)--> telemetry.alarms
                                        |                                   |
                                        +----------- telemetry.dlq <--------+
```

The pipeline of this phase ends at `telemetry.alarms`. Wiring alarms into ThingsBoard (briefing section S5) was not built and moved to Phase 3 by owner decision of 2026-10-02 (section 7.3).

- **Alarm consumer** in Node.js: ISA-18.2 style HI/HIHI level alarms with persistence and hysteresis from `config/alarms.yaml`, at-least-once processing, state rebuilt at start-up from the compacted `telemetry.alarms`.
- **Dead-letter topic** `telemetry.dlq` (Avro `dlq_v1`), fed by the bridge and the consumer with the original bytes and a machine-readable reason.
- **One structured JSON logger** per language, byte-identical between Python and Node.js except for the timestamp.
- **Tests:** 203 Python and 101 Node.js unit tests; 11 integration tests against the real stack. Lines and branches are each enforced at 80 % per language; at closure lines are 98.54 % (Python) and 100 % (Node.js), branches 95.28 % and 98.57 %.
- **CI** on every push: lint, unit tests with coverage gates, and the integration tests on the core stack.
- **Makefile** as the single documented entry point, the same on Git Bash, PowerShell and Linux bash, with `make smoke` reproducing the Phase 1 smoke run (200/200 at every stage).

The test suite found three defects in this phase, one of them a test that had never tested anything (section 7.1).

The final verification on a fresh clone, in PowerShell, ran `make install`, `make up`, `make test`, `make smoke`, `make down`, `make test-integration-full` and `make down`, all exit 0, Docker volumes 89 before and after, 0 secrets (section 3.2).

---

## 2. What was built

### 2.1 Repository tree at closure

```
.env.example  .gitattributes  .gitignore  .nvmrc  .python-version  LICENSE  Makefile  README.md (stub)
docker-compose.yml  requirements.txt  requirements-dev.txt  package.json  package-lock.json
pyproject.toml  jest.config.js
.github/workflows/ci.yml
common/        jsonlog.py  jsonlog.js
config/        telemetry.yaml  alarms.yaml  topics.yaml  loader.py
schemas/       telemetry_v1.avsc  alarm_v1.avsc  dlq_v1.avsc  README.md
edge/          simulator/ (model.py, __main__.py)
bridge/        mqtt_to_kafka/ (__main__.py, validation.py)
broker/        mosquitto/mosquitto.conf   kafka/provision.py
processing/    alarm_consumer/ (index.js, consumer.js, state_machine.js, config_loader.js)
               thingsboard/rule-chains/ (4 parameterised JSON)
scripts/       check_secrets.py  check_branch_coverage.py  check_js_syntax.js  clean.py
               hooks/pre-commit   thingsboard/import_rule_chains.py
               smoke/ (run_smoke.py, avro_consume.py, alarms_dump.py, dlq_dump.py, pahopublish.py, pahosubscribe.py)
tests/unit/          19 files (pytest and Jest), conftest.py
tests/integration/   5 test files, conftest.py, integration_support.py
docs/internal/ Phase 0 to 2 briefings, Phase 0 report and review, Phase 1 report, Phase 2 backlog, this report
```

93 tracked files at `1b8875a`, before this report. Removed in this phase: `edge/simulator/jsonlog.py` and `bridge/mqtt_to_kafka/jsonlog.py` (merged into `common/`), and `edge/mqtt-temperature/publisher.py` (the coursework publisher; the original remains in tag `v0-coursework` as `Project_24_MQTT/paho-mqtt/TBpublish.py`).

### 2.2 By task

| Task | Result |
|---|---|
| 2.1 | `common/jsonlog.py` and `common/jsonlog.js`: compact JSON, Python level names in both languages, identical lines for the same record (equivalence test). Provisioning and the rule-chain import moved to the shared logger. |
| 2.2 | Node.js alarm consumer (`processing/alarm_consumer/`), root `package.json` with exact pins and lockfile, `telemetry.alarms` compacted and keyed by `alarm_id`, `alarms_dump.py`, cross-language config-loader equivalence test. |
| 2.3 | `dlq_v1` schema; `telemetry.dlq` (1 partition, delete, 7 days); dead letters from the bridge (six reasons, including the new `non_finite_value`) and the consumer (`undecodable`, `missing_field`); transient Schema Registry failures stop the consumer uncommitted, never dead-lettered; `dlq_dump.py`. |
| 2.4 | pytest, pytest-cov and Jest tooling; 177 Python and 101 Node.js unit tests at the time; coverage gates; two defects found and fixed (section 7.1). |
| 2.5 | Core and full compose profiles; 11 integration tests against the real stack (section 4.3); `make test-integration` and `make test-integration-full`. |
| 2.6 | ruff (rules E, F) and `node --check` behind `make lint`; GitHub Actions workflow with two jobs, actions pinned by SHA, runner pinned to `ubuntu-24.04`. |
| 2.7 | Makefile consolidation: `make help` as default target, `make coverage`, `make clean`, `make smoke`; versioned pre-commit hook for the secret scan; single entry point for Jest. |
| 2.8 | Coursework publisher removed; branch coverage enforced at 80 % in both languages; the vacuous time-constant test fixed; double `make up` checked; two Phase 1 leftovers closed (inert Kafka variable, LF rules); final fresh-clone verification; this report. |

### 2.3 Git history

Conventional Commits with narrative bodies; 41 commits and 9 `--no-ff` merges since the Phase 1 closure commit `ddb0d9b`, every branch deleted after merge. One commit went to `main` directly, `86a17bd`, a briefing revision made as housekeeping at the start of Task 2.7. Owner decisions are recorded in the commit bodies that implement them and, when they span tasks, in `docs/internal/phase2_backlog.md`.

The secret scan runs on every commit through `scripts/hooks/pre-commit` since `f6d8c39` (Task 2.7); before that it was run by hand before each commit, as in Phase 1. At closure, `scripts/check_secrets.py --tracked` scans 93 tracked files: 0 hits, 17 allowed hits, the same 17 as at Phase 1 closure, all under `docs/internal/` and covered by owner-approved allow-list entries.

---

## 3. Commands

### 3.1 Everyday entry points

`make` alone prints the help. The targets used in this report:

```
make install                 .venv, Python and npm dependencies, git hooks (core.hooksPath scripts/hooks)
make up | make up-core       full stack (with ThingsBoard) | core stack; topics, registry compatibility, rule chains (full)
make lint                    ruff (E, F); node --check on every tracked .js
make test                    unit tests of both languages with coverage; lines and branches >= 80 % each
make test-integration        integration tests, core stack (start and stop their own stack)
make test-integration-full   integration tests including ThingsBoard
make smoke                   the Phase 1 smoke run: 200 messages end to end, every stage checked
make down                    stop and remove every container, always --profile full; named volumes kept
```

`make smoke` (`scripts/smoke/run_smoke.py`) runs exactly the Phase 1 section 3 sequence: the bridge with `--count 200 --duration 180` (waits for `mqtt_subscribed`), the Mosquitto probe, the simulator with `--count 200 --seed 42 --fault pump-01:imbalance:20`, then `avro_consume.py --count 200` with the bridge log. It refuses to start when the stack is not reachable or `telemetry.raw` already holds records.

### 3.2 Final verification on a fresh clone (Task 2.8)

Windows PowerShell, `git clone` of the branch at `8eaa413` into a temporary directory, `.env` copied from `.env.example` with the tenant password set to the ThingsBoard CE public default, as in Phase 1:

| Step | Exit | Time |
|---|---|---|
| `make install` | 0 | 50 s |
| `make up` | 0 | 56 s |
| `make test` | 0 | 35 s |
| `make smoke` | 0 | 60 s |
| `make down` | 0 | 3 s |
| `make test-integration-full` | 0 | 155 s |
| `make down` | 0 | 0 s |

Docker volumes 89 before and 89 after; 0 containers left; `check_secrets.py --tracked` 0 hits; `git status` clean.

The briefing's order (`make up`, `make test`, `make test-integration-full`, `make smoke`, `make down`) cannot run as written: `make test-integration-full` refuses to start while a stack is up, and stops its own stack at the end, which would leave `make smoke` without one. The order above runs every step once.

---

## 4. Measured results

### 4.1 Tests and coverage per language, at closure

| | Python (pytest) | Node.js (Jest) |
|---|---|---|
| Unit tests | **203** passed on Linux (CI); 202 passed, 1 skipped on Windows (the symbolic-link test of `clean.py`: this account cannot create links) | **101** passed, 5 suites |
| Lines | **1215 / 1233, 98.54 %** | **1015 / 1015, 100 %** |
| Branches | **303 / 318, 95.28 %** | **277 / 281, 98.57 %** |
| Gates (enforced in `make test` and CI) | lines (coverage.py total) >= 80 %; branches alone >= 80 % (`scripts/check_branch_coverage.py`) | lines >= 80 %, branches >= 80 % (`coverageThreshold`) |
| Duration | about 15 s (Windows), 7 s (CI) | about 2 s |
| Measured scope | `common/`, `config/`, `edge/simulator/`, `bridge/`, `broker/`, `scripts/check_secrets.py`, `scripts/check_branch_coverage.py`, `scripts/clean.py`, `scripts/thingsboard/` | `processing/alarm_consumer/`, `common/` |

coverage.py's own total, statements and branches together, is 97.87 %. Excluded from coverage, by owner decision and each with a comment: only the `main()` orchestration entry points of the simulator, the bridge, provisioning, the rule-chain import and the alarm consumer. They are exercised by the integration tests.

Integration: **11 tests.** Full profile on Windows: 10 passed, 1 skipped (SIGTERM, POSIX only). Core profile on Linux CI: 9 passed, 2 skipped (the two ThingsBoard tests, full profile only); the SIGTERM test runs and passes there.

### 4.2 Smoke run counts (`make smoke`)

Run three times in this phase (in Task 2.7 from PowerShell on the full stack and from Git Bash on the core stack; in Task 2.8 on the fresh clone, section 3.2), identical every time, and identical to Phase 1 section 4.1:

| Stage | Value |
|---|---|
| Simulator sent / acknowledged | **200 / 200**, exit 0 |
| Mosquitto probe received | **200**, 40 on each of the 5 asset topics |
| Bridge received / invalid / produced / delivered / failed | **200 / 0 / 200 / 200 / 0**, exit 0 |
| Kafka records decoded and checked | **200 / 200**, schema id 1, 40 per asset, `seq` contiguous, key = `asset_id` |
| Kafka per partition | 0: 120, 1: 80, 2: 0 (keyed partitioning, as in Phase 1) |
| Records with `injected_fault` | 20 (`imbalance`, pump-01) |
| Non-JSON stderr lines | simulator 0, bridge 0 |

### 4.3 Integration tests, per test (Task 2.5)

Full profile, Windows, 2026-10-02, on `main` after the Task 2.8 merge: 10 passed, 1 skipped, 155 s; stack up 59.0 s, ready with the rule-chain import 65.9 s, down 3.3 s; Docker volumes 89 before and after. Every test reads only what was written after its own offset mark, and each alarm consumer uses its own consumer group.

| Test | What it verifies against the real stack | Profile | Result | Time (call) |
|---|---|---|---|---|
| `test_stack_provisioned` | Topics with `config/`'s partitions (3, 3, 1) and the broker's real settings: `telemetry.alarms` compacted, `telemetry.dlq` delete with 7-day retention; registry compatibility BACKWARD | core | passed | 0.1 s |
| `test_pipeline_end_to_end` | Simulator → Mosquitto (QoS 1 acknowledged by the broker) → bridge → Kafka: 50 sent, 50 acknowledged, 50 received, 0 invalid, 50 delivered; all 50 decoded with the registry's schema id; key = `asset_id`; `seq` 0–9 for each of 5 assets; latency fields on every delivery; 0 non-JSON lines | core | passed | 3.2 s |
| `test_dead_letters_real` | Five MQTT faults (`invalid_json`, `invalid_ts`, `schema_mismatch`, `topic_mismatch`, `non_finite_value`) and two Kafka ones (`undecodable`, `missing_field`) reach `telemetry.dlq` as `dlq_v1`: 7 letters, each with its reason, original bytes, key and source; the bridge put nothing on `telemetry.raw` | core | passed | 2.9 s |
| `test_provision_is_idempotent` | A second provisioning creates nothing: 3 × `topic_exists` | core | passed | 0.4 s |
| `test_offset_not_committed_when_handler_throws` | `telemetry.dlq` deleted after start-up makes the consumer's dead-letter send fail for real: exit 1, group offset unchanged; with the topic back, a new consumer processes and commits the same message | core | passed | 9.2 s |
| `test_bridge_dead_letter_send_failure_real` | The same failure in the bridge: `dlq_delivery_failed` at ERROR with the original payload in base64, `dead_letters_failed` 1 in the run summary, exit 1 | core | passed | 31.6 s |
| `test_schema_registry_down` | Registry stopped: the consumer stops without committing or dead-lettering; after the restart the message is processed | core | passed | 12.8 s |
| `test_restore_after_real_compaction` | Topic settings tightened so the log cleaner compacts during the test; the consumer restores exactly the active alarms, emits nothing while values stay high, and clears them with the same `alarm_id`s | core | passed | 16.8 s |
| `test_sigterm_flushes_simulator_and_bridge` | SIGTERM to the simulator and the bridge flushes and exits cleanly | core | skipped on Windows (cannot send SIGTERM to a native process); **passed on Linux CI** | — |
| `test_rule_chains_imported_and_linked` | Through the ThingsBoard REST API: the 4 project chains present, Root Rule Chain set as root, every rule-chain input node resolving to its intended chain | full | passed | 0.3 s |
| `test_import_is_idempotent` | A second import keeps the same four chain ids and creates nothing | full | passed | 2.0 s |

The session fixture's stack start-up is counted in the setup of the first test (68.7 s here, for the full profile).

### 4.4 CI runs

| Run | Commit | Checks job | Integration job (core) |
|---|---|---|---|
| 37039834987 | `f6d8c39` (Task 2.7 branch) | success: 194 Python, 101 Node.js | success: 9 passed, 2 skipped |
| 37040543132 | `feec2f9` (Task 2.7 merge on `main`) | success | success |
| 37043901981 | `4c423df` (Task 2.8 branch) | success | success |
| 37044447526 | `1b8875a` (Task 2.8 merge on `main`) | success: 203 Python, 101 Node.js | success: 9 passed, 2 skipped, 139 s; stack up 33.4 s |

Earlier runs of Tasks 2.6 and 2.7 are in the Actions history; the first, 36954037397, prompted the runner pin (`a37f9f2`).

### 4.5 Timings

| Measurement | Value |
|---|---|
| `make test`, fresh clone after `make install` (limit 2 minutes) | 35 s Git Bash, 21 s PowerShell (Task 2.7); 35 s PowerShell (Task 2.8, with the branch gate) |
| `make up`, from a stopped stack | 53 s, 56 s, 59 s in three runs |
| `make up`, run again on the running stack | 60 s |
| `make up-core` | 29 s |
| `make install`, fresh clone | 49–50 s |
| `make smoke` | 58.5–60 s |
| `make test-integration` (core), Windows / CI | 134 s / 139 s |
| `make test-integration-full`, Windows | 155–170 s |

---

## 5. What was verified, and how

### 5.1 Alarm consumer

Unit tests cover every transition rule of the level alarms and replay the Task 2.2 verification (F1–F3) in memory with the real rules: F1 raises pump-01 WARNING and compressor-01 WARNING and CRITICAL at `seq` 4; an engine restored from the latest event per `alarm_id` emits nothing while values stay high (F2); F3 clears all three with the same `alarm_id`s. Against the stack: restore from a log that the broker really compacted, at-least-once behaviour when a send fails, and the stop on a registry outage (section 4.3). pump-01 under `imbalance` reaches WARNING but not CRITICAL (about 6.5 mm/s against 7.1 mm/s); intended, owner decision recorded in `config/telemetry.yaml` (`05f2729`).

### 5.2 Dead-letter topic

Every reason the bridge and the consumer can be made to produce from outside reaches `telemetry.dlq` intact (section 4.3). `serialization_error` and the failed dead-letter serialisation cannot be provoked from outside and are covered by unit tests with injected faults. Valid messages never reach the DLQ: checked in the Task 2.3 run (50 valid, 0 in the DLQ) and in `test_dead_letters_real`.

### 5.3 Structured logging

`tests/unit/test_jsonlog_equivalence.py` sends the same records through both loggers and compares the lines as strings without `ts`: identical, including non-ASCII, quotes, control characters and every level. Every component run in the integration tests and the smoke run is checked for non-JSON stderr lines: 0 in every case.

### 5.4 Makefile, on three shells

Every target was run from Git Bash and from PowerShell on Windows (Task 2.7 audit), and `install`, `lint`, `test` and `test-integration` run on Linux bash in CI on every push. The only platform branch is the pre-existing `VENV_PY` path; `make help` is printed with `$(info)` so it never passes through a shell. Recipes that would need shell features call Python scripts.

### 5.5 `make up` twice in a row (Task 2.8)

State captured after each run through the Kafka AdminClient, the registry REST API, the ThingsBoard REST API and `docker volume ls`. Identical: the three topics with their partitions and non-default settings, registry compatibility BACKWARD with no subjects, the four project rule chains with **the same chain ids**, node and connection counts and root flag, 89 volumes. Two qualifications:

- **Rule-chain node ids change on every import.** ThingsBoard assigns new node ids when the import saves a chain's metadata. Nothing in the project refers to node ids.
- **The topics are created again on each `make up`** (`topic_created` both times): `--force-recreate` starts Kafka empty by design (tmpfs). Equal state there comes from recreation, not from idempotent provisioning; idempotent provisioning on a running broker is what `test_provision_is_idempotent` checks.

### 5.6 `make down` with profiles

Verified on 2026-10-01 with Compose v2.40.3 (`9fc73de`): after `make up` (profile `full`), a plain `docker compose down` removes only the services of the active profiles, so it stopped the four core services, **left ThingsBoard running**, and could not remove the network ("Resource is still in use"). `make down` therefore always passes `--profile full`: every service of the project is stopped and removed, and with no ThingsBoard container it behaves exactly like a plain `down`. Never `-v`: named volumes are kept. The integration fixture stops its stack with the same command and fails the session if the Docker volume count changes; it did not change in any run of this phase (89 locally, 0 on CI runners).

### 5.7 Secrets and the pre-commit hook

`scripts/hooks/pre-commit`, enabled by `make install`, blocked a commit with a staged test password assignment (`FAIL ... [password-assignment]`, exit 1, HEAD unchanged), and refused a commit in a clone without `.venv` ("run make install first"). Every commit since Task 2.7 went through it.

### 5.8 ThingsBoard

The rule chains are imported and checked through the REST API (section 4.3). No telemetry or alarm data reaches ThingsBoard in this phase (section 7.3, item 1).

---

## 6. Definition of done

| Criterion (briefing) | Status | Evidence |
|---|---|---|
| `make test` passes (unit + integration) on a fresh clone after `make install && make up` | **Met, with the agreed split** | `make test` is unit-only by owner decision (`9fc73de`, `86a17bd`); on a fresh clone `make test` (35 s) and `make test-integration-full` both exit 0 (section 3.2) |
| Alarm consumer produces to `telemetry.alarms` with correct schema and counts | **Met for schema, identity and restore; no count test** | `alarm_v1` through the registry; raise, clear and restore with deterministic `alarm_id`s (sections 4.3, 5.1). The briefing's "50 messages, alarm count matches thresholds" integration test was not built (section 7.3) |
| Invalid messages routed to `telemetry.dlq` with base64 payload and error reason | **Met; payload stored as bytes** | 7 reasons end to end (section 4.3). The record keeps the payload as Avro `bytes`; base64 appears in logs and in `dlq_dump.py` (`e05521c`) |
| All component output (simulator, bridge, consumer) is JSON-only | **Met** | 0 non-JSON lines in every integration test and smoke run; logger equivalence test |
| GitHub Actions CI runs on push/PR, unit tests green, integration tests optional | **Met, and more** | Integration tests run on every push too (`c58d914`); runs in section 4.4 |
| Zero secrets in tree, `.gitignore` clean | **Met** | 93 tracked files, 0 hits; pre-commit hook active |
| `Makefile` has `test`, `lint`, `coverage` targets | **Met** | Section 3.1 |
| README still a stub, no unmeasured claims | **Met** | `README.md` unchanged since the skeleton commit `35477ca` |

---

## 7. Deviations, corrections and findings

### 7.1 Defects found by the test suite

| Found | Defect | Fix |
|---|---|---|
| Task 2.4, Python unit tests | `config/loader.py` read four optional keys as `value or default` (`degradation`, `fault_schedule`, `asset_overrides`, a topic's `configs`). An empty container of the wrong type is falsy, so `fault_schedule: {}` or `configs: []` silently became the default and skipped the type check. For `asset_overrides: []` the two loaders disagreed: Node.js rejected it, Python accepted it. | `7a29126`: `_absent_as()` returns the default only for `None`. The cross-language test gained the case; it fails without the fix. |
| Task 2.4, Node.js unit tests | `decodeTelemetry` built its transient-error message from `e.message`; a registry client rejecting with a non-Error value (null, a string) raised a `TypeError` there. The consumer still stopped uncommitted, but without the `schema_registry_unavailable` line and with a misleading error. | `8913426`: `String(e)` for non-Error values. The test fails without the fix. |
| Task 2.8, while making the test read its constants from config | **`test_temperature_is_a_first_order_lag` tested nothing.** `AssetModel` starts at the steady state of its own load; the test built it already at full load, so the start temperature was the target (65.0 = 25 + 40) and the gap to close was zero. It passed for any time constant and any lag formula. The Task 2.6 note in the backlog said "the test is still correct"; that note was wrong and is corrected there. | `8eaa413`: cold start at ambient; time constant, rise and ambient read from `config/telemetry.yaml`; a guard asserts the gap is real. With temporary mutations the old test passed and the new one failed when the model ignored the configured time constant (40.74 vs 50.28) and when the lag ran at twice the rate (59.59 vs 50.28). |

### 7.2 Other defects found and fixed in this phase

| Found | Fix |
|---|---|
| Python's `json` accepts `NaN` and `Infinity` and an Avro double carries them, so such a payload passed validation and reached `telemetry.raw` as valid data | New `non_finite_value` rejection in the bridge (`e85a404`) |
| With Schema Registry stopped, its HTTP client reported "status 400": treating any 4xx as permanent would have dead-lettered valid telemetry and committed its offset | Only an explicit 404 counts as a permanent decode failure; everything else stops the consumer uncommitted (`0a35326`, `73208f6`) |
| A plain `docker compose down` after `make up` left ThingsBoard running and the network in use | `make down` always passes `--profile full` (section 5.6) |
| `make` with no target ran `install` (the first target), recreating `.venv` and reinstalling everything | `make help` is the default goal (`f6d8c39`) |
| `KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE` was not recognised by cp-server 6.1.1 (Phase 1 open item) | Removed (`02564b5`); the broker still logs `confluent.telemetry.enabled = false` |
| `.gitattributes` and `.gitignore` checked out with CRLF on Windows (Phase 1 open item) | `text eol=lf` for those two and `LICENSE` (`f60672d`); every tracked text file now has an LF rule |

### 7.3 Deviations from the briefing, with the decision for each

Compared section by section at Task 2.8. "Owner decision" items were decided during the phase and recorded in the commit cited; "documented" items are accepted as they are by owner decision of 2026-10-02 (DETENTE 2.8.a) and not corrected.

| # | Briefing says | Implemented | Decision |
|---|---|---|---|
| 1 | S5 and Overview: alarms wired into ThingsBoard (an "AlarmToThingsBoard" node, Kafka connector on `telemetry.alarms`, alarm attributes on the device); the stack flows end to end into ThingsBoard | Not built. The root chain's REST node is still "Firebase"; no ThingsBoard component reads `telemetry.alarms` | **Moved to Phase 3**, owner 2026-10-02. The Phase 2 pipeline ends at `telemetry.alarms` (backlog note) |
| 2 | Task 2.8 step 6: a checkout with no `.env` must error | `-include .env` ignores a missing file; compose has defaults for its three substitutions; only the ThingsBoard import stops (`settings_missing`) | **Current behaviour is correct**, owner 2026-10-02; not changed |
| 3 | `make test` runs unit and integration tests (S4, DoD) | Unit only; integration in `make test-integration[-full]` | Owner decision, `9fc73de`; 2-minute limit applies to `make test` (`86a17bd`) |
| 4 | Fresh stack of 5 containers | Core profile of 4; ThingsBoard in profile `full` | Owner decision, `9fc73de` |
| 5 | CI: ubuntu-latest; push to main and all PRs; `pip install -r requirements.txt`; `pytest --cov-report=xml`; integration optional | `ubuntu-24.04`; push to main and `feature/**`, PRs to main; `make install` and `make test` (no XML); integration on every push | Owner decisions, `c58d914`, `a37f9f2`; CI running `make test` is correct (owner, Task 2.7) |
| 6 | CI artifacts: test report, coverage badge | Only the integration logs, uploaded on failure | Documented |
| 7 | Bridge reason `out_of_range` | Not implemented: `config/telemetry.yaml` gives units but no physical limits | Owner decision, `e85a404` |
| 8 | `dlq_v1` fields | Also `source_key` | Owner decision, `e05521c` |
| 9 | S2: `alarms_dump.py` extended to dump the DLQ | Separate `dlq_dump.py` | Allowed by Task 2.3 step 5; `b146422` |
| 10 | DLQ "with base64 payload" (DoD, Decisions) | Payload as Avro `bytes`; base64 only in logs and the dump tool | Owner decision, `e05521c` |
| 11 | "Debounce" and "state cleanup" in S1, S4, Task 2.2, Task 2.4 | No time-based debounce, no periodic clean-up (as the briefing's own Task 2.2 design notes say) | Owner decision, `08f94d9` |
| 12 | S1: config shared "through common loader" | Two loaders, Python and Node.js, kept equal by a cross-language test | Allowed by Task 2.2 ("implement in Node"); documented |
| 13 | S1: invalid messages → DLQ + log + metric | No metrics system; a `messages_dead_lettered` counter in the run summary log | Documented |
| 14 | S1: consumer logs `asset, rule, trigger_value, transition` | Events `alarm_raised` / `alarm_cleared` with `alarm_id`, `asset_id`, `channel`, `severity`, `value`, `threshold` and source coordinates | Documented |
| 15 | Task 2.2: CLI flag `--config` | No `--config`; the consumer reads the repository's `config/` | Documented |
| 16 | Task 2.2: `scripts/smoke/avro_produce.js` | Not created; the manual verification used other tools | Documented |
| 17 | Task 2.4: file names `test_bridge_validation.py`, `alarm_consumer.test.js` | `test_validation.py` and `test_bridge.py`; `state_machine.test.js` and `consumer.test.js` | Documented |
| 18 | S4: simulator unit tests for "empty config, bad seed" | No tests named for those cases | Documented |
| 19 | Task 2.4: exclude CLI argument parsing and broker connection errors from coverage; "combined coverage" | Only `main()` entry points excluded; argument parsing is unit-tested; thresholds per language, not combined | Owner decision, `e008ee4` |
| 20 | Task 2.4: pytest "already in `requirements.txt`" | pytest in `requirements-dev.txt` | Owner decision, `e008ee4` |
| 21 | Task 2.5: one file `test_e2e_stack.py`; Testcontainers or a compose fixture | Five test files and a compose fixture | Allowed by the briefing; documented |
| 22 | Task 2.5 test 1: `make up`, wait 10 s, 5 healthy containers | No container-count test; the fixture uses `docker compose up --wait`; `test_stack_provisioned` checks topics and registry instead | Documented |
| 23 | Task 2.5 test 3: 50 messages, alarm count matches thresholds, 0 lost, 0 duplicates | No such integration test; alarm behaviour against the stack is tested through restore and failure paths, the threshold replay in unit tests | Documented |
| 24 | Task 2.5 test 4: 5 faults → 5 in DLQ, 45 valid in `telemetry.alarms` | 7 faults → 7 dead letters; no valid-message count on `telemetry.alarms` (valid telemetry does not map 1:1 to alarms) | Documented |
| 25 | Task 2.5: rule-chain test after `make up`; teardown "tmpfs volumes cleaned" | Full profile only, outside CI; the fixture runs the import itself; tmpfs mounts are not volumes, volume count checked instead | Owner decisions, `9fc73de`, `c58d914`; wording documented |
| 26 | Checkpoints table, row 2.7.a: `make test` and `make lint` pass | The rewritten Task 2.7 DETENTE asks for the audit, help output, timing and smoke | Documented (row not updated with the rewrite) |
| 27 | Decisions Logged item 6 cites section "S6" | No S6 section; CI is in S4 | Documented |
| 28 | Dependencies: `alarm_v1` "verified in Phase 1" in Schema Registry | `alarm_v1` was first registered by the consumer at start-up in Phase 2 (`264eb6a`) | Documented (factual error in the briefing) |
| 29 | Risk 1: `rule_id`; state in memory, cleared on restart | No `rule_id`; state rebuilt at start-up from the compacted `telemetry.alarms` | Owner decisions, `95976cd`, `264eb6a`; text stale, documented |
| 30 | Header status "FASE 2 ready to begin"; every DoD row "Pending" | — | Documented (the briefing was not kept as a status record) |
| 31 | Task 2.8 steps (briefing) | Replaced by the owner's closed list of seven items at DETENTE 2.7.a; briefing steps 4 (counts on all three topics with the consumer running) and 6 (missing `.env`, item 2 above) not run as written | Owner decision, 2026-10-02 |
| 32 | Final verification order `up`, `test`, `test-integration-full`, `smoke`, `down` | Run as `up`, `test`, `smoke`, `down`, `test-integration-full`, `down` (section 3.2) | Required by the integration fixture; documented |

### 7.4 Corrections to earlier notes

- **Phase 2 backlog, Task 2.6 note on the tau test:** "the test is still correct" was wrong (section 7.1); corrected in the backlog on 2026-10-02 (`4c423df`).
- **Phase 1 report, section 2.1:** listed `edge/mqtt-temperature/publisher.py` as kept; removed in this phase (`242d762`).

### 7.5 Owner decisions taken during the phase

Node.js consumer superseding Phase 0 decision 14; `common/` as a top-level directory; compact JSON with Python level names in both languages; no `rule_id` (a rule is `(asset_id, channel)`); `telemetry.alarms` compacted and keyed by `alarm_id`; state restore at start-up; root `package.json` with exact pins and Node.js 24; kafkajs maintenance risk accepted; Avro `dlq_v1` with 7-day retention and the reason set; asymmetric dead-letter send failure (bridge carries on, consumer stops); no `out_of_range`; pump-01 imbalance stays below CRITICAL; coverage tooling and exclusions; core and full profiles; integration tests in CI on the core profile only; ruff E and F at line length 130; runner pin; the Task 2.7 Makefile, hook, clean and smoke decisions; branch coverage at 80 %; S5 to Phase 3; missing `.env` behaviour kept. Each is recorded in the commit that implements it or in `docs/internal/phase2_backlog.md`.

---

## 8. Deliberately not done, and the backlog

### 8.1 Phase 3

- **ThingsBoard wiring of alarms and telemetry** (briefing S5; owner, 2026-10-02), with the course alarm logic in `CreateAndClearAlarms`, which still filters on the course payload's `temperature` field.
- ThingsBoard dashboards for the industrial simulator (Phase 1 decision).
- Latency and throughput benchmark; README and architecture documentation (the README stays a stub until then).
- Coverage upload (codecov) and a CI test report (briefing Task 2.6 and S4).

### 8.2 Phase 1 backlog items, status at Phase 2 closure

| Item (`phase2_backlog.md`) | Status |
|---|---|
| Shared JSON-log package | **Done**, Task 2.1 |
| Firebase-disabled import | **Not done.** The import logs `firebase_leg_disabled` (3 REST nodes unresolved) but leaves the nodes in the chain |
| Alarm consumer | **Done**, Task 2.2 (Node.js, owner note) |
| Dead-letter topic | **Done**, Task 2.3 |
| Tests and CI | **Done**, Tasks 2.4 to 2.6 |
| Firebase authentication | **Not done**; the sink stays disabled |
| Telemetry into ThingsBoard | **Phase 3**, owner 2026-10-02 |
| Course alarm logic in ThingsBoard | **Not done**; goes with the Phase 3 ThingsBoard work |
| `ts_to_ack_ms` measurement bias | **Not done**: the bridge still waits up to 100 ms on its inbox between delivery-report polls; needed before the Phase 3 benchmark |
| Bridge delivery semantics (manual MQTT acknowledgement) | **Not done** |
| First-install healthcheck on an empty volume | **Not done**; needs owner authorisation for `down -v` |
| `KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE` | **Closed**, `02564b5` |
| Line endings of `.gitattributes`, `.gitignore`, `LICENSE` | **Closed**, `f60672d` |

### 8.3 Recorded during Phase 2

- **kafkajs maintenance risk:** no release since 2023-02-27; the alternative is `@confluentinc/kafka-javascript`. Accepted by the owner.
- **kafkajs on Node.js 24:** one `TimeoutNegativeWarning` per consumer start, clamped to 1 ms, logged as JSON; no effect observed.

---

## 9. State at closure

- `main` at `1b8875a`, CI green on both jobs (run 37044447526); no feature branch left locally or on GitHub. This report was merged into `main` after owner approval.
- Phase 2 tagged `v2-engineering-quality` (annotated) on the merge of this report into `main`.
- Stack stopped with `make down` (no `-v`) after the last integration run. The named ThingsBoard volumes `industrial-telemetry_tb-data` and `industrial-telemetry_tb-logs` are kept; Kafka and ZooKeeper data was in tmpfs and is gone by design. Docker volume count 89, unchanged through the whole phase.
- Temporary clones used for the fresh-clone checks were created under the user's temporary directory and deleted afterwards.
- The repository is on GitHub, private, since 2026-10-02 02:04Z (Task 2.6). The branches of Tasks 2.1 to 2.5 were merged locally before it existed and were never run by CI; from Task 2.6 on, every branch was pushed, run green in CI before merging, and deleted locally and on GitHub after merge.
