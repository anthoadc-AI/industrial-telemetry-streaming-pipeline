# Claude Code Briefing — PHASE 1: Solid Base

**Project:** industrial-telemetry-streaming-pipeline
**Phase:** 1 of 3 — Solid base (repository, v0 snapshot, simulator, bridge, schemas, single compose)
**Owner / approver:** Anthony Davila Carrillo. Phase closure is reserved for the owner.
**Prerequisite:** Phase 0 closed by owner on 2026-09-26. Read `docs/internal/phase0_diagnostic_report.md` in full before starting; it is the source of every fact below.
**Briefing version:** 1.0 — 2026-09-26

---

## 1. Purpose

Turn the course folders into a public-grade repository with a working, reproducible core pipeline:

```
edge/simulator --MQTT--> Mosquitto --bridge--> Kafka (Avro, Schema Registry)
```

plus ThingsBoard running from the same `docker-compose.yml` with the four rule chains imported from parameterised JSON. The alarm consumer, dead-letter topic, tests and CI belong to Phase 2; this phase leaves the repository publishable on its own, with `make up` bringing the whole stack up on a clean machine.

## 2. Owner decisions in force (do not reopen)

From the project instructions and the Phase 0 closure:

- Single repository, layered structure (target tree in §7.2 of the Phase 0 report, as approved).
- Single story: condition monitoring of rotating industrial assets. The course's vehicle-GPS producer and Express UI are v0 legacy, removed in cleanup.
- Data is synthetic and labelled as such everywhere.
- Firebase is an optional sink, disabled by default, documented as course heritage.
- `v0-coursework` is a faithful snapshot at original paths (§7.1 rules: exclude only binaries and secrets; `node_modules/` excluded; `mosquitto.log` included; sibling folders outside).
- Author identity for all commits: `Anthony Davila Carrillo <anthoadc-AI@users.noreply.github.com>`.
- Conventional Commits with narrative bodies that record design decisions; feature branches merged with `--no-ff`; annotated tags at milestones.
- English in every public artifact. Spanish only under `docs/internal/`.
- `config/` is the single source of truth for thresholds, constants and topic names, created before any file references them.
- No decorative content anywhere public: no emojis, no filler.
- No figure in any public document that has not been measured. The README stays a stub until Phase 3.

Decisions taken in the design chat for this phase (owner-approved unless the owner says otherwise before launch):

- Confluent stack trimmed to `zookeeper`, `broker`, `schema-registry`. Evidence: Phase 0 §5.2, seven of nine services carried no traffic and cost 3.6 GiB.
- Kafka client for new Python code is `confluent-kafka` (Avro serializer, Schema Registry client, librdkafka). `kafka-python` stays only in v0 legacy.
- `paho-mqtt` 2.x with `CallbackAPIVersion.VERSION2`, pinned in `requirements.txt`. The course's accidental 1.x dependency (Phase 0 risk 4) is fixed by migration, not by pinning an old version.
- ThingsBoard runs on a **named Docker volume** in the unified compose, never on `~/.mytb-data`. The course volume is a Phase 0 artifact and stays untouched.
- Every publisher and producer accepts `--count` and `--duration`, handles `SIGINT` and `SIGTERM`, and flushes on exit (Phase 0 risk 3, three reproduced instances).
- Repository stays **private on GitHub until Phase 3 closure**.
- Phase 0 §8.2 open questions 2–5 are closed by the decisions above: (2) Confluent trimmed to three services; (3) Airflow/Hadoop stopped as a standing precondition (rule 6); (4) `ksqldb-server` is dropped, and the surviving `broker` gets `KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE=false` so the stack does not phone home; (5) bounded runs and flush-on-exit are requirements of Tasks 1.5 and 1.6. Record these four closures in §8.2 of the Phase 0 report as the first action of Task 1.0, dated, and change nothing else in that report.

## 3. Hard rules for this phase

1. The three course folders remain read-only. All work happens inside the repository directory. Copying from the course folders is allowed; writing into them is not.
2. **Never run `docker compose down -v` or `docker volume rm`** against the course stacks. Against the new unified stack, `down -v` is allowed only when the owner authorises it explicitly at a checkpoint.
3. `its-pipeline-backup/` never enters git, is never referenced by a relative path from the repo, and its contents are never printed. Read the four rule-chain JSON files from it only to produce the parameterised copies.
4. Before any `git commit`, run the secret scan defined in Task 1.1. A failing scan blocks the commit.
5. Never `git add -A` or `git add .`. Stage by explicit path.
6. Stop Airflow and Hadoop containers (`docker stop`, never `down -v`) before bringing any stack up. Verify `docker info` reports at least 12 GB.
7. Each `DETENTE` is a full stop: report, then wait. Do not batch two checkpoints.
8. All output is in English, except files under `docs/internal/`.
9. When something in this briefing contradicts what you find on disk, stop and report; do not improvise around it.

## 4. Environment facts (from Phase 0; do not rediscover)

- Windows 11, PowerShell is the shell Claude Code runs in. Python is native `python.exe` 3.13, no venv. Node v24.16.0. Docker engine 29.0.1, WSL cap 12 GB.
- Image pins: `thingsboard/tb-postgres:4.2.1.1`, `eclipse-mosquitto:2.1.2`. Confluent Platform version: take it from the course compose tags recorded in Phase 0 §3.
- Ports: Mosquitto 1883, ThingsBoard MQTT 9883, ThingsBoard HTTP 8080 (conflicts with Airflow; make it configurable via `.env` with 8080 default).
- MSYS `timeout` does not deliver signals to native Windows Python. Bounding must be internal to the scripts.
- ThingsBoard: 5 rule chains exist; 4 are project logic (`TempToFirebase` 1/0, `AlarmToFirebase` 1/0, `CreateAndClearAlarms` 5/4, `Root Rule Chain` 9/8); `Thermostat` is stock demo. Three of the four carry a `TbRestApiCallNode` with the Firebase database URL.
- Firebase Realtime Database rules expired on 2026-09-25; the database is locked. The Firebase sink cannot work until authenticated. It is out of scope for this phase beyond being structurally isolated and disabled.
- Windows line endings: add `.gitattributes` forcing LF on `*.sh`, `*.yml`, `*.yaml`, `*.py`, `*.json`, `*.md`, `Makefile`, `.env.example`.

## 5. Tasks

### Task 1.0 — Repository directory

- Rename `its-pipeline-staging/` to `industrial-telemetry-streaming-pipeline/`. Everything below happens inside it.
- Confirm `docs/internal/` already holds the Phase 0 report and review files. Copy this briefing into `docs/internal/claude_code_briefings/`.

### Task 1.1 — Skeleton

Create, in this order, and commit as `chore: initialise repository skeleton`:

- `git init` with default branch `main`. Configure user name and email as in §2, repository-local.
- `.gitignore`: `node_modules/`, `__pycache__/`, `*.pyc`, `.venv/`, `*.log`, `.env`, `.mytb-*`, `*/data/`, `.pytest_cache/`, `.idea/`, `.vscode/`, `*.docx`, `*.pdf`, `*.zip`, `*.png`, `*.jpg`.
- `.gitattributes` per §4.
- `LICENSE` — MIT, copyright Anthony Davila Carrillo 2026. (Owner may override at DETENTE 1.)
- `README.md` stub: title, one sentence saying the repository is under construction, a line stating all data is synthetic, and nothing else. No architecture claims yet.
- `.env.example` with every variable this phase will use, public defaults only. Variables include at least: `MQTT_HOST`, `MQTT_PORT`, `TB_HTTP_PORT`, `TB_MQTT_PORT`, `TB_ACCESS_TOKEN`, `KAFKA_BOOTSTRAP`, `SCHEMA_REGISTRY_URL`, `TELEMETRY_TOPIC`, `FIREBASE_ENABLED=false`, `FIREBASE_DB_URL=`, `FIREBASE_DB_SECRET=`.
- `scripts/check_secrets.py`: scans the staged files for patterns (`firebaseio.com`, `firebasedatabase.app`, `?auth=`, `-----BEGIN`, `password=`, `secret=`, `token=` followed by 20+ chars, and the owner's Windows username in absolute paths). Exit non-zero on any hit. Wire it into `Makefile` as `make check-secrets` and document that it runs before every commit in this phase.
- `Makefile` with targets `up`, `down`, `logs`, `check-secrets`, `test` (placeholder that fails with "no tests yet — Phase 2").

**DETENTE 1.** Report: `git log --oneline`, `git status`, the tree, and the exact `.gitignore`. Owner confirms LICENSE choice and the `.env.example` variable list.

### Task 1.2 — `v0-coursework` snapshot

Apply §7.1 of the Phase 0 report exactly:

- Copy the three course folders into the repository root at their original names and paths (`Project_24_Docker/`, `Project_24_MQTT/`, `Project24_2_Kafka/`), excluding only: `node_modules/`, `*.docx`, `*.pdf`, `*.zip`, screenshots, and anything under `.gitignore`. `mosquitto.log`, the `.bak` file, dead config files and the stale README all stay.
- Run `make check-secrets` on the staged set. The course default ThingsBoard token in `TBpublish.py` is expected and allowed in v0 (Phase 0 §7.1); document that exception inside the check script as a named allow-list entry, not a pattern weakening.
- Single commit: `chore: import MIT xPRO Module 24 coursework as v0 baseline`. The body states what the three folders are, which categories were excluded and why, and that the code is byte-identical to the course working copies except for those exclusions.
- Annotated tag `v0-coursework` with a message pointing to the Phase 0 report for the inventory.

**DETENTE 2.** Report: `git show --stat HEAD`, `git tag -n`, file count vs. the Phase 0 inventory, and the secret-scan output. Owner verifies against the Phase 0 tree before anything is restructured.

### Task 1.3 — Restructure and clean

Branch `feat/layered-structure`. Two or more commits, in this order:

1. `refactor: move coursework into layered structure` — `git mv` every mapped file per §7.3 into the target tree (edge/, broker/, processing/, visualization/, deploy targets become the compose inputs for Task 1.7). Use `git mv` so history follows each file. Files that have no place in the target tree (Express UI, GPS producer, kafkajs consumer) go to their nearest layer for now; they are removed in the next commit, not deleted here.
2. `chore: remove dead coursework artifacts` — remove `.bak`, `librdkafka.config`, `config.js`, stale `README.md`, `mosquitto.log`, Express UI, GPS producer, kafkajs consumer. The commit body lists each file with the one-line reason from Phase 0 §7.3, and records that the vehicle-GPS pipeline is preserved under tag `v0-coursework`.
3. `refactor: parameterise ThingsBoard access token` — `TBpublish.py` reads `TB_ACCESS_TOKEN` from the environment; remove the default token; update `.env.example`.
4. `docs: translate and fix coursework comments` — fix the stale `node-rdkafka` header comment if the file survives; translate any Spanish in surviving public files.

Merge into `main` with `--no-ff`.

**DETENTE 3.** Report: `git log --graph --oneline`, the resulting tree, `git diff v0-coursework..main --stat`. Owner reviews the diff narrative.

### Task 1.4 — `config/` and `schemas/`

Branch `feat/config-and-schemas`. Nothing else may reference a threshold or topic name until this is merged.

- `config/telemetry.yaml`: asset definitions (a small fleet: e.g. 2 motors, 2 pumps, 1 compressor), sensor channels per asset (bearing temperature, casing vibration RMS, motor current, speed), sample interval, unit conventions.
- `config/alarms.yaml`: per-channel thresholds (warning, critical), persistence (samples), hysteresis (units), with a comment stating the values are illustrative defaults for synthetic data.
- `config/topics.yaml`: `telemetry.raw`, `telemetry.alarms`, `telemetry.dlq` (the last two are Phase 2 consumers of this file, defined now).
- `config/loader.py`: a single function that loads and validates these files (pydantic or plain dataclasses), used by every Python component.
- `schemas/telemetry_v1.avsc`, `schemas/alarm_v1.avsc`, with a `schemas/README.md` explaining versioning policy (backward-compatible evolution, `_v2` on breaking change).

Merge `--no-ff`. No checkpoint unless something is ambiguous.

### Task 1.5 — Industrial telemetry simulator

Branch `feat/edge-simulator`. `edge/simulator/`:

- Physics that a plant engineer would accept as plausible, and nothing more elaborate than needed: bearing temperature as a first-order response toward a load-dependent steady state with ambient offset; vibration RMS as a baseline plus load term plus Gaussian noise; motor current proportional to load; slow degradation drift on one asset; injectable fault modes (bearing overheating, imbalance step in vibration) triggered by CLI flag or schedule. Each channel's model is described in a docstring with its parameters.
- Reads assets and channels from `config/`. Publishes JSON to Mosquitto on `plant/<asset_id>/telemetry` with a `synthetic: true` field in every message and a `ts` in ISO 8601 UTC.
- `--count`, `--duration`, `--rate`, `--fault <asset>:<mode>:<start_s>` CLI flags. Clean shutdown on `SIGINT`/`SIGTERM`, flushes before exit, logs the number of messages sent.
- Structured logging (JSON lines) via the standard `logging` module; no print statements.
- `requirements.txt` pinned.

Commit with a body that explains the model choices and states that the data is synthetic. Merge `--no-ff`.

### Task 1.6 — MQTT → Kafka bridge

Branch `feat/mqtt-kafka-bridge`. `bridge/mqtt_to_kafka/`:

- Subscribes to `plant/+/telemetry`, validates each JSON message against the telemetry schema, serialises to Avro with the Schema Registry serializer, produces to `telemetry.raw` keyed by `asset_id`.
- Invalid messages are logged with the reason and dropped for now; the dead-letter topic is Phase 2. Leave a clearly marked `TODO(phase-2)` at that point.
- Delivery reports handled; producer flushed on shutdown. Same CLI and signal conventions as the simulator.
- Structured logging with per-message latency from `ts` to produce time (this is the seed of the Phase 3 benchmark; keep the field names stable).

Merge `--no-ff`.

### Task 1.7 — Unified `docker-compose.yml`, rule-chain import, `make up`

Branch `feat/unified-compose`.

- One `docker-compose.yml` at the root: `mosquitto`, `zookeeper`, `broker`, `schema-registry`, `thingsboard` (named volume), all with pinned images, healthchecks, and `depends_on` conditions. Profile `firebase` for anything Firebase-specific (nothing to add yet; the profile exists so the sink is structurally optional). No `restart: always`.
- `broker/mosquitto/mosquitto.conf` from the course, reviewed: anonymous access is acceptable for a local demo and must be stated in a comment.
- `processing/thingsboard/rule-chains/`: the four JSON files copied from `its-pipeline-backup/`, with every Firebase URL replaced by the literal placeholder `${FIREBASE_DB_URL}`. Run `make check-secrets` before staging them. Record in the commit body that three of four chains carry REST nodes and which ones.
- `scripts/thingsboard/import_rule_chains.py`: logs in to ThingsBoard via REST with tenant credentials from `.env`, renders the placeholders from `.env`, imports the four chains, sets `Root Rule Chain` as root, and creates the demo device with `TB_ACCESS_TOKEN`. When `FIREBASE_ENABLED=false`, the REST nodes are imported as-is with the placeholder unresolved and the script logs that the Firebase leg is disabled. Idempotent: re-running updates instead of duplicating.
- `make up` = `docker compose up -d` + wait for ThingsBoard health + run the import script. `make down` = `docker compose down` (no `-v`). `make logs`.
- Bring it up. Stop at the checkpoint before running any producer.

**DETENTE 4a.** Report: `docker ps`, health status, `docker stats --no-stream`, import script output, ThingsBoard rule chain list via REST. **Owner then exports the ThingsBoard dashboards** from the UI into `its-pipeline-backup/` (this was the gap flagged in Phase 0 §7.3) and confirms. Wait for that confirmation.

### Task 1.8 — End-to-end smoke run

After DETENTE 4a confirmation:

- Run the simulator for `--count 200` and the bridge concurrently. Verify with a throwaway Avro consumer script under `scripts/smoke/` that 200 messages arrived on `telemetry.raw`, deserialise correctly against the registered schema, and report min/max/mean of the bridge latency field. Verify Mosquitto side with `scripts/smoke/pahosubscribe.py` (the course probe, translated).
- Confirm ThingsBoard shows telemetry for the demo device only if the simulator is also pointed at ThingsBoard; if not, state plainly that the ThingsBoard leg in this phase is import-only and that wiring telemetry into ThingsBoard from Kafka is Phase 2 scope. Do not claim what was not run.
- Write `docs/internal/phase1_closure_report.md` (English is fine here; Spanish allowed): what was built, exact commands run, measured counts and latencies, what is deliberately not done, and the Phase 2 backlog (alarm consumer, dead-letter, tests, CI, Firebase authentication, dashboard parameterisation).
- `make down`. Do not push to GitHub in this phase unless the owner asks.

**DETENTE 4b — Phase 1 report.** Deliver the closure report and `git log --graph --oneline --all`. Await owner review. Phase closure is the owner's decision.

## 6. Definition of done

- `git clone` + `make up` on this machine brings up five containers healthy and imports four rule chains without manual steps beyond `.env`.
- Simulator → Mosquitto → bridge → Kafka verified with a measured count, zero secrets in the tree (`make check-secrets` clean at HEAD), `v0-coursework` tag present and faithful, history readable as a narrative.
- Three course folders unchanged (owner verifies by hash).
- README still a stub. No unmeasured claims anywhere.
