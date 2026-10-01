# Phase 1 Closure Report — Solid Base

| Field | Value |
|---|---|
| Project | industrial-telemetry-streaming-pipeline |
| Phase | 1 of 3, Solid base |
| Briefing | `docs/internal/claude_code_briefings/phase1_briefing_solid_base.md`, v1.0 |
| Work dates | 2026-09-30 local (UTC-4); container runs logged in UTC on 2026-10-01 |
| Machine | Windows 11 Home, Docker Engine 29.0.1, WSL 2 cap 12 GB (12,544,364,544 B reported) |
| Status | **Phase 1 CLOSED by owner, 2026-10-01.** Report reviewed and accepted. |

All data produced in this phase is synthetic.

---

## 1. Summary

The repository exists, is private and local only (nothing pushed), and carries the coursework as a faithful `v0-coursework` snapshot followed by a readable history of restructuring and new work. The core pipeline runs end to end on this machine:

```
edge/simulator --MQTT--> Mosquitto --bridge--> Kafka (Avro, Schema Registry)
```

A 200-message smoke run delivered 200 of 200 messages at every stage, with zero invalid, zero lost and zero undelivered, and every Kafka record decoded against the registered schema. ThingsBoard runs from the same `docker-compose.yml` and receives the four project rule chains from parameterised JSON. In this phase the ThingsBoard leg is **import-only**: no telemetry was sent to ThingsBoard (section 5.4).

`make up` brings up five healthy containers, creates the Kafka topics, sets Schema Registry compatibility and imports the rule chains, from a fresh `git clone` with only `.env` added (section 6).

---

## 2. What was built

### 2.1 Repository tree at closure

```
.env.example  .gitattributes  .gitignore  LICENSE  Makefile  README.md (stub)
docker-compose.yml  requirements.txt
config/        telemetry.yaml  alarms.yaml  topics.yaml  loader.py
schemas/       telemetry_v1.avsc  alarm_v1.avsc  README.md
edge/          simulator/ (model.py, __main__.py, jsonlog.py)
               mqtt-temperature/publisher.py (coursework publisher, token parameterised)
bridge/        mqtt_to_kafka/ (__main__.py, validation.py, jsonlog.py)
broker/        mosquitto/mosquitto.conf   kafka/provision.py
processing/    thingsboard/rule-chains/ (4 parameterised JSON)
scripts/       check_secrets.py  thingsboard/import_rule_chains.py
               smoke/ (pahopublish.py, pahosubscribe.py, avro_consume.py)
docs/internal/ Phase 0 report and review, Phase 0 and Phase 1 briefings, this report
```

`tests/` and `.github/workflows/` are not created yet; they are Phase 2 scope (section 8).

### 2.2 By task

| Task | Result |
|---|---|
| 1.0 | Staging directory renamed; Phase 0 report section 8.2 questions 2–5 recorded as closed, dated 2026-09-30. |
| 1.1 | Skeleton: `.gitignore`, `.gitattributes`, MIT licence, README stub, `.env.example`, secret scan, Makefile. |
| 1.2 | `v0-coursework`: 15 files from the three course folders, faithful except `node_modules/` and the starter `.zip`; annotated tag. Four CRLF files stored as LF, as documented in the commit. |
| 1.3 | Layered restructure with `git mv`; dead artifacts and the vehicle-GPS pipeline removed; ThingsBoard token parameterised. |
| 1.4 | `config/` (5 assets, 4 channels, 2 fault modes, 15 alarm rules, topic names) with a strict loader; Avro schemas `telemetry_v1`, `alarm_v1`, and a versioning policy. |
| 1.5 | Industrial telemetry simulator: documented physical models, fault injection, `--count`/`--duration`/`--rate`/`--seed`, clean shutdown with acknowledgement flush. |
| 1.6 | MQTT to Kafka bridge: schema validation, Avro through Schema Registry, keyed by `asset_id`, per-message latency fields. |
| 1.7 | Unified `docker-compose.yml` (5 services), Kafka provisioning, parameterised rule chains with idempotent import, `make install` / `make up`, translated MQTT smoke probes. |
| 1.8 | End-to-end smoke run, Avro verification script, this report. |

### 2.3 Git history

Conventional Commits with narrative bodies; every feature branch merged with `--no-ff` and deleted after merge; tag `v0-coursework` on the coursework import. The secret scan (`make check-secrets`) ran before every commit; at the last commit before this report it scanned 43 tracked files with 0 hits and 17 allowed hits, all under `docs/internal/` and all covered by named allow-list entries the owner approved.

---

## 3. Exact commands run for the smoke test

From the repository root, with `.env` copied from `.env.example` and the tenant password set to the ThingsBoard CE public default:

```
make up
python -m bridge.mqtt_to_kafka --count 200 --duration 180          # started first, waits for mqtt_subscribed
python scripts/smoke/pahosubscribe.py --count 200 --duration 180 --quiet   # Mosquitto-side probe
python -m edge.simulator --count 200 --seed 42 --fault pump-01:imbalance:20
python scripts/smoke/avro_consume.py --count 200 --bridge-log <bridge stderr file>
```

`python` is the repository virtual environment (`./.venv/Scripts/python.exe`). Stderr of the simulator and the bridge was written to files and checked for non-JSON lines. The orchestration itself was a scratch shell script, not committed.

The run was executed twice: once at 01:55Z and, after a compose change (section 7.3), again at 02:03Z on the final configuration. Both passed with identical counts. The figures below are from the final run unless marked.

---

## 4. Measured results

### 4.1 Counts

| Stage | Measurement | Value |
|---|---|---|
| Simulator | messages sent / broker-acknowledged | **200 / 200** in 40 cycles, 39.057 s, exit 0 |
| Simulator | fault | `imbalance` on pump-01 active from simulated t = 20.0 s |
| Mosquitto (probe) | messages received | **200**, 40 on each of the 5 asset topics |
| Bridge | received / invalid / produced / delivered / failed | **200 / 0 / 200 / 200 / 0**, unprocessed 0, undelivered at exit 0, exit 0 |
| Kafka | records decoded by `avro_consume.py` | **200 / 200**, all with schema id 1 |
| Kafka | per asset | 40 each; `seq` contiguous from 0 for every asset, no duplicates |
| Kafka | per partition | partition 0: 120, partition 1: 80, partition 2: 0 |
| Kafka | records carrying `injected_fault` | 20 (pump-01, `imbalance`) |
| Kafka | key equals `asset_id`, `synthetic` true | all 200 |
| Schema Registry | subjects | `telemetry.raw-value`, version 1; global compatibility BACKWARD |
| Logs | non-JSON lines on stderr | simulator 0, bridge 0 |

Partition 2 received nothing because five keys hash to only two of the three partitions; this is expected keyed partitioning, not a fault. Ordering per asset is guaranteed by the key, whatever the spread.

### 4.2 Bridge latency fields

Milliseconds from each message's own `ts` (the simulator's clock; simulator and bridge run on the same host), over the 200 delivered messages:

| Field | min | median | mean | p95 | max |
|---|---|---|---|---|---|
| `ts_to_receive_ms` | 1.417 | 3.018 | 3.267 | 5.757 | 7.679 |
| `ts_to_produce_ms` | 1.645 | 3.261 | 5.102 | 7.326 | 67.528 |
| `ts_to_ack_ms` | 10.673 | 109.344 | 109.454 | 123.463 | 169.698 |

The first run gave the same picture (`ts_to_produce_ms` mean 5.444, max 81.098; `ts_to_ack_ms` mean 109.156, max 192.502).

How to read these, without over-reading them:

- **The five highest `ts_to_produce_ms` values are the five `seq 0` messages** (66.7–67.5 ms), the first cycle after start-up. Excluding the first cycle, the field stays in single-digit milliseconds.
- **`ts_to_ack_ms` is dominated by the bridge's own polling, not by Kafka.** The gap between produce and acknowledgement has median 106.1 ms (min 6.9, max 120.0). The bridge serves delivery reports only when it polls the producer, and between messages its main loop waits up to 100 ms on its inbox. The field therefore measures "acknowledgement observed by the bridge", which includes up to about 100 ms of waiting. It must not be quoted as broker latency. Fixing this is in the Phase 2 backlog (section 8.3).
- These are single runs at 5 messages per second on one machine. They are not a benchmark and are not to be used in public documents; the Phase 3 benchmark is the place for that.

### 4.3 Resources and timings

| Measurement | Value |
|---|---|
| Memory after the final smoke run (`docker stats --no-stream`) | ThingsBoard 1.443 GiB, broker 522.7 MiB, schema-registry 235.6 MiB, zookeeper 87.3 MiB, mosquitto 2.7 MiB; about 2.27 GiB in total |
| Kafka services only | about 846 MiB (Phase 0 measured 4.38 GiB for the nine-service Confluent stack) |
| `make up`, stack running, containers recreated | 70 s |
| `make up`, from a stopped stack | 56 s |
| `make install` + `make up` from a fresh clone | 27 s + 57 s |

The first-ever `make up`, which installs the ThingsBoard database into an empty volume, was not timed end to end (section 7.2).

---

## 5. What was verified, and how

### 5.1 Simulator

Against a throwaway pure-Python MQTT broker before Mosquitto existed, and then against Mosquitto in the smoke run: count and duration bounds, CTRL_BREAK shutdown with acknowledgement flush, fault injection, reproducibility with `--seed`, start-up errors. Offline, over one simulated hour, normal operation of every asset stays below every warning threshold in `config/alarms.yaml`, and every fault mode crosses at least a warning. SIGTERM delivery was not exercised, since Windows cannot send it to a native process; the handler shares the SIGBREAK code path that was.

### 5.2 Bridge

Ten malformed-message cases each rejected with the expected reason; Avro round trip against a mock Schema Registry; fail-fast start-up without Kafka. In the smoke run: 200 of 200 produced and delivered to real Kafka, with stderr pure JSON.

### 5.3 Stack and import

Five services healthy. Topics `telemetry.raw` (3 partitions), `telemetry.alarms` (3), `telemetry.dlq` (1), automatic topic creation off; Schema Registry compatibility BACKWARD. Rule chains read back through the ThingsBoard REST API: TempToFirebase 1 node, AlarmToFirebase 1, CreateAndClearAlarms 5 nodes and 4 connections, Root Rule Chain 9 and 8, set as root, every input node resolving to the intended chain. A second import changed the same four chain ids and created nothing. Broker telemetry to Confluent confirmed off (`confluent.telemetry.enabled = false`, no metric reporters).

### 5.4 ThingsBoard leg — not run

The simulator publishes to Mosquitto only. No telemetry reached ThingsBoard in this phase, so ThingsBoard shows no data for the demo device from Phase 1. **The ThingsBoard leg in Phase 1 is import-only.** Wiring telemetry into ThingsBoard from Kafka is Phase 2 scope.

---

## 6. Definition of done

| Criterion (briefing section 6) | Status | Evidence |
|---|---|---|
| `git clone` + `make up` brings up five healthy containers and imports four rule chains without manual steps beyond `.env` | **Met**, with `make install` as the one setup step | Fresh clone into a temporary directory, `.env` copied, `make install` (27 s), `make up` (57 s), exit 0, five healthy, four chains imported |
| Simulator to Kafka verified with a measured count | **Met** | 200 / 200 at every stage, section 4.1 |
| Zero secrets in the tree (`make check-secrets` clean at HEAD) | **Met** | 0 hits; 17 allowed hits under `docs/internal/` only, by named owner-approved entries |
| `v0-coursework` tag present and faithful | **Met** | Annotated tag; owner verified against the Phase 0 tree at DETENTE 2 |
| History readable as a narrative | For owner review | Conventional Commits with narrative bodies, `--no-ff` merges |
| Three course folders unchanged | **Met by my check; owner verifies by hash** | SHA-256 of all 16 files unchanged since before the copy; 0 files newer than 2026-09-01; `mosquitto.log` 505 B, mtime 2026-08-26 21:19 |
| README still a stub, no unmeasured claims | **Met** | README unchanged since the skeleton commit |

---

## 7. Deviations, corrections and findings

### 7.1 Corrections to the Phase 0 report (recorded here; the Phase 0 report is unchanged)

- **Section 5.1b, Firebase write semantics.** The PUT-overwrite behaviour belongs to the Root Rule Chain's own REST node, named "Firebase", which PUTs to `/temperature.json` on every telemetry message. `TempToFirebase` uses **POST** and is reached only through `CreateAndClearAlarms` when its `MaxTemp` filter is true. `AlarmToFirebase` POSTs to `/alarm.json`.
- **No Firebase node used an `auth=` parameter or stored credentials.** The database host was the only sensitive value in the exports, consistent with the database having run in open test mode. `FIREBASE_DB_SECRET` has nothing to replace yet.
- **Mosquitto image tag.** The briefing's pin `eclipse-mosquitto:2.1.2` does not exist on Docker Hub; the 2.1 line is published only as `-alpine`. The stack uses `eclipse-mosquitto:2.1.2-alpine`, which reports version 2.1.2.

### 7.2 Owner decisions taken during the phase

MIT licence; `.env.example` variable list; docs/internal redaction and allow-list; LF normalisation in v0; all five Task 1.3 departures; smoke scripts as the first Task 1.7 commit; ThingsBoard dashboard export closed with nothing to export (section 8.1).

### 7.3 Defects found and fixed in this phase

| Found | Fix |
|---|---|
| fastavro validation errors are objects, not strings; formatting them raised `TypeError` (found in pre-commit testing) | Converted with `str()` before joining |
| librdkafka writes its own non-JSON lines to stderr | Routed through a Python logger |
| A third-party deprecation warning at Schema Registry import broke JSON-only stderr | Captured by `logging.captureWarnings` in the components; suppressed for that one import in the smoke consumer |
| GNU Make on Windows could not resolve `.venv/Scripts/python.exe` | `./.venv/Scripts/python.exe`, tested from Git Bash and PowerShell |
| ThingsBoard's first-start installer serves `/login`, so the healthcheck passed too early | Healthcheck also requires `/data/.firstlaunch`, written only after a successful install. Verified on the existing volume only: proving it on a first install needs `down -v`, which was not authorised |
| Images declare VOLUME paths the compose file did not mount; each `make down` + `make up` left five anonymous volumes | All declared paths mounted (named volumes for ThingsBoard, tmpfs elsewhere); volume count stable at 99 across up, down, up |
| The smoke probes block-buffered stdout when redirected | Line-buffered |

### 7.4 Housekeeping

- **Ten orphaned anonymous Docker volumes** were left by this phase before the fix above, five created at 2026-10-01T00:09:24Z and five at 01:45:08Z (2026-09-30 20:09 and 21:45 local), all anonymous and attached to no container, holding nothing of value (secrets directories with no secrets, empty Mosquitto data and log). **Removed on owner instruction at the end of the 2026-09-30 session**, by id, one by one, never with `prune`; each was re-checked for creation time, anonymous label and no attached container before removal. Docker volume count: **99 before, 89 after**; still 89 when re-checked on 2026-10-01, when all ten ids were confirmed absent. No other volume was touched: the project's named ThingsBoard volumes and the 62 dangling volumes of other projects and earlier months remain.
- **Twelve Airflow and Hadoop containers remain stopped**, as they were at the start of the phase (Phase 0 outstanding item).

### 7.5 Smoke-run attempts that did not count

Two earlier attempts at the smoke orchestration failed in my scratch script, not in the pipeline: a backgrounded `&&` chain lost its variables, then a wait helper returned the wrong status, and a redirected probe buffered its output. In those attempts the bridge started, subscribed and exited at its bound having received nothing; the simulator never ran, and nothing was produced to Kafka (offsets confirmed at 0 before the counted runs).

---

## 8. Deliberately not done, and the backlog

### 8.1 Phase 3

- **ThingsBoard dashboards for the industrial simulator: Phase 3 will build and export them.** (Owner, 2026-09-30.) The Phase 0 gap is closed with nothing to export: the coursework instance holds only four stock dashboards ("Rule Engine Statistics", "Firmware", "Thermostats", "Software"), all created 2026-08-26 10:27:35, the same moment as the install.
- Latency benchmark, seeded by the bridge's latency fields.
- Public README and architecture documentation (the README stays a stub until then).

### 8.2 Phase 2, owner-directed

- **Shared JSON-log package.** `edge/simulator/jsonlog.py` and `bridge/mqtt_to_kafka/jsonlog.py` are identical copies; they become one shared package when the alarm consumer would make a third.
- **Firebase-disabled import.** When `FIREBASE_ENABLED=false`, the import script must detach or skip the Root Rule Chain REST node instead of leaving it failing on every telemetry message.

### 8.3 Phase 2, from the briefing and from this phase

- **Alarm consumer** in Python on `telemetry.raw`: thresholds, persistence and hysteresis from `config/alarms.yaml`, producing `alarm_v1` events to `telemetry.alarms`.
- **Dead-letter topic.** The bridge marks the spot with `TODO(phase-2)`; invalid messages are currently logged and dropped.
- **Tests and CI** (`tests/`, `.github/workflows/`).
- **Firebase authentication** and auth-based database rules; the database has been locked since 2026-09-25, and the sink stays disabled until then.
- **Telemetry into ThingsBoard** from Kafka, so the ThingsBoard leg carries the simulator's data.
- **Course alarm logic in ThingsBoard.** `CreateAndClearAlarms` still filters on the course payload's `temperature` field (TBEL `msg.temperature < 80`), which the simulator does not send. `config/alarms.yaml` is now the source of thresholds.
- **`ts_to_ack_ms` measurement bias** (section 4.2): serve delivery reports promptly, by polling more often or from a dedicated thread, before the Phase 3 benchmark uses the field. The field names stay as they are.
- **Bridge delivery semantics.** MQTT QoS 1 messages are acknowledged on receipt, before Kafka confirms them; a crash can lose messages queued in the bridge. paho-mqtt 2.x manual acknowledgement after Kafka delivery would close this.
- **First-install healthcheck.** Verify the ThingsBoard healthcheck on an empty volume (needs owner authorisation for `down -v` on the new stack) and time the first-ever `make up`.
- **`KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE`** is set as the briefing required but is not recognised by cp-server 6.1.1; telemetry is off through `confluent.telemetry.enabled = false`. Remove or replace the inert variable.
- **Line endings.** `.gitattributes`, `.gitignore` and `LICENSE` are not in the LF list and check out with CRLF on Windows. Harmless; add them for consistency.

---

## 9. State at closure

- Stack stopped with `make down` (no `-v`) after the final run. The ThingsBoard named volumes `industrial-telemetry_tb-data` and `industrial-telemetry_tb-logs` are kept; Kafka and ZooKeeper data was in tmpfs and is gone by design.
- The coursework ThingsBoard data `~/.mytb-data` was used only to inspect dashboards at DETENTE 4a, through its own course compose stack, stopped afterwards with `docker compose down`, no `-v`. It measured 150,037,650 B before and after that stop; the growth from 149,645,786 B at Phase 0 closure is ThingsBoard's own writes while it ran.
- The raw rule-chain exports remain only in `its-pipeline-backup/`, outside the repository; it was read to produce the parameterised copies, never written, and is never referenced by a relative path from the repository.
- **Firebase credential rotation: closed as moot (owner, 2026-10-01).** Phase 0 decision 5 planned to rotate the Firebase secret at Phase 1 closure. No Firebase REST node used an `auth=` parameter or stored credentials (section 7.1), so there is no secret to rotate. The database itself has been locked since its test-mode rules expired on 2026-09-25; authenticating it is Phase 2 backlog (section 8.3).
- Nothing was pushed to GitHub.
