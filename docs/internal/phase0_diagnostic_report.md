# Phase 0 Diagnostic Report — industrial-telemetry-streaming-pipeline

| Field | Value |
|---|---|
| Date | 2026-09-13 |
| Machine | Windows 11 Home 10.0.26200, x64 |
| Course root | the course folder on the owner's machine (local path omitted) |
| Python (coursework interpreter) | 3.13.4 — `%USERPROFILE%\AppData\Local\Programs\Python\Python313\python.exe` (system install; **no venv exists**) |
| Python packages | paho-mqtt 1.6.1, kafka-python 3.0.11, numpy 2.4.2 |
| Node.js / npm | v24.16.0 / 11.13.0 |
| Docker / Compose | 29.0.1 (build eedd969) / v2.40.3-desktop.1 |
| WSL 2 | kernel 6.6.87.2-microsoft-standard-WSL2; distro `docker-desktop` (**state: Stopped**) |
| WSL 2 resource cap | `~/.wslconfig`: memory=8GB, processors=4 |
| Briefing | v1.0 — 2026-09-13. Section 2 path placeholder was unfilled; paths resolved from the working directory. |

> **Scope note.** The briefing named three folders. The course root contains **five additional sibling folders and four loose files** that are part of the same coursework and contain duplicates of the graded code. They are inventoried in §2.4 because they affect what enters `v0-coursework`, but nothing in them was modified either.

---

## 1. Executive summary

| Folder | Status | Basis |
|---|---|---|
| `Project_24_Docker` | 🟢 **Green** | 3 files, all byte-identical to the course starter. No secrets. Only issues: untagged image + a committed runtime log. |
| `Project_24_MQTT` | 🟡 **Yellow** | **MQTT → ThingsBoard verified end-to-end 2026-09-23** (§5.1b): 46 messages published → 46 stored, zero loss, regular 3.0 s cadence. Code is clean and secret-free, but the folder still holds **only the publisher** — the four rule chains now exist as files only in `its-pipeline-backup/`, outside the repo, and carry a live credential. The publisher cannot be bounded from outside on Windows (§5.1b). |
| `Project24_2_Kafka` | 🟢 **Green** (was Yellow) | **Verified end-to-end 2026-09-13** (§5.2): 19 messages produced → consumed, offsets committed, lag 0. Both known deviations confirmed in code. Remaining issues are cosmetic or cleanup: 58 MB of `node_modules`, three dead course files, a stale `node-rdkafka` header comment, and a producer that loses buffered messages on abrupt stop. |

**✅ Previously risk 1 — now mitigated (2026-09-26).** "The processing layer is unversioned and one command from destruction" no longer holds. All **four** rule chains were exported to `its-pipeline-backup/` on 2026-09-23 (`temptofirebase` 1/0, `alarmtofirebase` 1/0, `createandclearalarms` 5/4, `root_rule_chain` 9/8 — matching the live counts), and `.mytb-data` was copied clean on 2026-09-26 to `its-pipeline-backup/mytb-data_2026-09-23_clean` (144 MB, taken after the clean `down` at STOP 2), alongside the earlier `mytb-data_2026-09-22` (143 MB). The logic now has three independent copies. **Residual risk:** `its-pipeline-backup/` is not a git repo by decision (§8.1 #4) and is not backed up off this machine, so a disk loss still ends it.

**Top risks for going public (updated 2026-09-26)**

1. **The Firebase sink is not reproducible, and cannot be re-enabled as built.** The Realtime Database ran in **test mode — open read and write to anyone — until 2026-09-25**, and is now **expired and locked**: reads and writes are denied. So the sink worked for the life of the coursework only because the database was world-writable, and the credential in the rule nodes was never the only exposure. Phase 1 must **authenticate the sink and replace test mode with auth-based rules**; until then any demo of the Firebase leg fails, and the pipeline must not be presented as working end to end without that caveat (§5.1b).
2. **The live credential is now materialised on disk, in four files.** What was previously a latent risk has happened: three of the four exports (`root_rule_chain.json`, `temptofirebase.json`, `alarmtofirebase.json`) each contain a `TbRestApiCallNode` config holding the live Firebase database URL. They are currently outside the repo in `its-pipeline-backup/`, which must never become a git repo. **They must be parameterised before they enter the repository** (§7.3), and the credential is to be rotated at Phase 1 closure (§8.1 #5). Note that `Root Rule Chain` is secret-bearing too — three REST call nodes need parameterising, not two.
3. **Neither publisher can be bounded, and this has now failed three times.** `TBpublish.py` and `PublishVehicleCoordinates.py` both run unbounded `while True` loops and catch only `KeyboardInterrupt`. Consequences observed, not theorised: the Kafka producer **silently lost a buffered message** on `timeout`'s SIGTERM (§5.2), and `TBpublish.py` **ignored an external 60-second bound entirely and ran 137.6 s, publishing 46 messages instead of ~20** — a 2.3× overrun of an explicit authorisation, because an MSYS signal never reaches a native Windows `python.exe` (§5.1b). On Windows the only external bound that works is `taskkill /F`, which is precisely the abrupt stop that loses buffered messages. **Phase 1 must give both publishers a duration/iteration limit as a parameter and handle `SIGTERM`.** A public repo whose demo scripts cannot be stopped cleanly is a defect reviewers will find immediately.
4. **Nothing is reproducible from the repository as it stands.** No `requirements.txt`, two images untagged (`eclipse-mosquitto`, `thingsboard/tb-postgres` — both resolve to a moving `:latest`; the running versions are now pinned as **Mosquitto 2.1.2** and **`thingsboard/tb-postgres:4.2.1.1`**), and `paho-mqtt` is pinned by accident rather than intent: `TBpublish.py` uses the 1.x `mqtt.Client()` signature and breaks on paho-mqtt 2.x, which is what a fresh `pip install paho-mqtt` gives today.
5. **The stack cannot survive an ungraceful stop.** Two distinct latent faults, both now reproduced: ZooKeeper's stale `/brokers/ids/1` znode blocked broker startup (§5.2), and the ThingsBoard container came up twice with `HostConfig.PortBindings` set but `NetworkSettings.Ports` empty — no host mapping, recoverable only by `down` + `up` (§5.1). Both stem from stacks that are stopped by an engine kill rather than `docker compose down`.

---

## 2. Folder inventory

Total across the three folders: **16 tracked files** (excluding `node_modules`), **~59 MB on disk**, of which 58 MB is `node_modules`. **No file inside the three folders exceeds 5 MB.**

Classification legend — `STARTER` = byte-identical to the official course starter archive; `MODIFIED` = owner-authored change. This was established by extracting the two starter `.zip` archives to a scratch directory and diffing, not by inspection. The starters contain literal `????` placeholders, so the distinction is exact.

### 2.1 `Project_24_Docker` — 4 files, 11 KB

| File | Size | Class | Evidence / note |
|---|---|---|---|
| `docker-compose_step1.yml` | 289 B | Compose, **STARTER** | identical to starter zip |
| `mosquitto/config/mosquitto.conf` | 134 B | Config, **STARTER** | identical to starter zip |
| `mosquitto/log/mosquitto.log` | 505 B | **Runtime artifact** | Mosquitto 2.1.2 start/stop trace, last entry 2026-08-26 21:19. Exclude from repo. |
| `Required_Project_24.1_(Part1)-…_starter.zip` | 1585 B | **Binary**, course starter | The only binary in the three folders. Contains the 4 pristine starter files. |
| `mosquitto/data/` | — | empty dir | `persistence false`, so nothing is written here. |

### 2.2 `Project_24_MQTT` — 2 files, 5 KB

| File | Size | Class | Evidence / note |
|---|---|---|---|
| `ThingsBoard/docker-compose_step5.yml` | 295 B | Compose, **STARTER** | identical to starter zip |
| `paho-mqtt/TBpublish.py` | 978 B | Python, **MODIFIED** | Owner added the `humidity` field (starter published temperature only). Diff vs starter is exactly 2 hunks. |

**Exported ThingsBoard artifacts: none.** No rule chain JSON, no dashboard export, no device export exists in this folder or anywhere under the course root. A tree-wide search for `*rule*chain*`, `*firebase*`, `*serviceaccount*`, `*adminsdk*` returned zero results.

### 2.3 `Project24_2_Kafka` — 10 files + `node_modules`, 59 MB

| File | Size | Class | Evidence / note |
|---|---|---|---|
| `kafka-docker/docker-compose.yml` | 6720 B | Compose, **STARTER** | 9 services, Confluent Platform 6.1.1 |
| `kafka-docker/config.js` | 3009 B | JS, **STARTER**, **dead** | Confluent Cloud CLI helper. Requires `minimist`, which is not declared anywhere, and sits in a directory with no `package.json`. Never imported by any file. |
| `kafka-docker/librdkafka.config` | 32 B | Config, **STARTER**, **dead** | `bootstrap.servers=localhost:9092`. A `node-rdkafka` artifact; `kafkajs` never reads it. |
| `pythonProducer/PublishVehicleCoordinates.py` | 1032 B | Python, **MODIFIED** | Deviation 1 applied (see §6) |
| `pythonProducer/PublishVehicleCoordinates.py.bak` | 1308 B | Python, **STARTER** | **Not an owner leftover** — this file ships inside the official starter zip, byte-identical. It is the pre-fix reference version using the removed `KafkaClient` API. |
| `NodeJSConsumer/myconsumer.js` | 1902 B | JS, **MODIFIED** | Deviation 2 applied (see §6) |
| `NodeJSConsumer/server.js` | 751 B | JS, **MODIFIED** | Owner filled the two `????` placeholders: topic `vehicle-coordinates`, button label |
| `NodeJSConsumer/package.json` | 277 B | Config, **MODIFIED** | `node-rdkafka` dependency replaced by `kafkajs` |
| `NodeJSConsumer/package-lock.json` | 240 KB | Lockfile, **MODIFIED** | Regenerated; starter lockfile was 400 KB |
| `NodeJSConsumer/README.md` | 505 B | Docs, **STARTER** | Describes `restProducer.js` / `myproducer.js`, **files that do not exist in this project**. Stale. |
| `NodeJSConsumer/node_modules/` | 58 MB, 8551 files | **Vendored dependencies** | Exclude from repo. |

### 2.4 Duplicates, leftovers and course material outside the three folders

These sit alongside the three folders in the course root. None were modified.

| Path | Size | Assessment |
|---|---|---|
| `Mosquitto/` | 2 KB | **Superseded duplicate.** Extraction of `DockerMosquittoInstall.zip` (dated 2022), an earlier standalone Mosquitto stack replaced by `Project_24_Docker`. Contains its own stale `mosquitto.log`. |
| `Nueva carpeta/` | 0 | **Empty leftover.** Safe to ignore. |
| `Project_24.1/` | 1.7 MB | **Duplicate working copy.** Holds a second `TBpublish.py` (929 B — the *unmodified* starter), plus copies of both compose files and `mosquitto.conf`, plus two submission `.docx`, one nested in `archivo/`. Superseded by `Project_24_Docker` + `Project_24_MQTT`. |
| `Project_24.1_part2/` | 3.7 MB | Submission `.docx` ×3 + **2 course video transcript PDFs** (third-party copyrighted material — must never be published). |
| `Project_24.2/` | 2.0 MB | Submission `.docx` ×2 + the 24.2 starter zip (111 KB). |
| `pahopublish.py`, `pahosubscribe.py` | 905 B | **Owner scratch scripts**, not graded deliverables. Plain Mosquitto pub/sub on port 1883, topic `sensors/temperatures`, Spanish console output. Genuinely useful as a broker smoke test — recommend keeping, translated. |
| `DockerMosquittoInstall.zip` | 1317 B | Source archive for `Mosquitto/`. Redundant. |
| `Minilección 24.2.docx`, `Minilección 24.3 …docx` | 1.5 MB | Spanish course notes. `docs/internal/` candidates at most. |

**Files over 5 MB:** none anywhere under the course root. Largest single file is `Project_24_1_Part2.docx` at 2.7 MB.

---

## 3. Dependency inventory

### 3.1 Python

No `requirements.txt` exists in any of the three folders, or anywhere under the course root. **All dependencies are installed globally into the system Python 3.13.4** — there is no virtual environment.

| Script | Import | Resolves to | Installed | In requirements? |
|---|---|---|---|---|
| `TBpublish.py` | `paho.mqtt.client` | paho-mqtt | **1.6.1** | ❌ no file |
| | `time`, `random`, `json` | stdlib | — | n/a |
| `PublishVehicleCoordinates.py` | `kafka` | kafka-python | **3.0.11** | ❌ no file |
| | `numpy` | numpy | **2.4.2** | ❌ no file |
| | `json`, `from time import sleep` | stdlib | — | n/a |
| | `time`, `random` | stdlib, **unused** | — | dead imports |
| `PublishVehicleCoordinates.py.bak` | `kafka.KafkaClient` | **API removed in kafka-python ≥2.0** | n/a | reference only |

Findings:

- **`requirements.txt` missing entirely.** Phase 1 must create one.
- **`paho-mqtt` must be pinned `<2.0`.** `TBpublish.py` calls `mqtt.Client()` with no argument. paho-mqtt 2.x requires `mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)` and raises on the bare call. A fresh `pip install paho-mqtt` today installs 2.x and **the coursework breaks immediately**. The current 1.6.1 is a pin by accident.
- **Dead code in the producer:** `acked()` is defined but never referenced — it is a `confluent-kafka` delivery callback that `kafka-python`'s `producer.send()` does not accept. Inherited from the starter.
- `numpy` is used for exactly two `randint` calls and could be dropped, removing a heavy dependency from the edge layer. *(Proposal, not an owner decision.)*

### 3.2 Node.js

`NodeJSConsumer/package-lock.json`, `lockfileVersion: 3`.

| Package | Declared (`package.json`) | Locked (`package-lock.json`) | Note |
|---|---|---|---|
| `kafkajs` | `^2.2.4` | **2.2.4** | consistent |
| `express` | `^4.17.3` | **4.22.2** | caret drift, in-range |
| `jest` | `^26.0.1` | **26.6.3** | declared as a **runtime** dependency; no test files exist |
| `minimist` | — | — | **required by `kafka-docker/config.js` but never declared** (dead file) |

**`node-rdkafka` confirmed absent** from `package.json` and from `package-lock.json` — zero entries in the lockfile. The single remaining occurrence anywhere in the three folders is a **stale comment**, `myconsumer.js:18`: *"Using the node-rdkafka client for Apache Kafka"*, left inside the retained Confluent Apache-2.0 header while the body below it uses `kafkajs`. Cosmetic, but it is the first thing a reader of a public repo sees, and it contradicts the code. Flag for Phase 1.

### 3.3 Docker images

| Compose file | Service | Image | Tag | Pinned? |
|---|---|---|---|---|
| `Project_24_Docker/docker-compose_step1.yml` | `broker` | `eclipse-mosquitto` | **none → `:latest`** | ❌ |
| `Project_24_MQTT/ThingsBoard/docker-compose_step5.yml` | `mytb` | `thingsboard/tb-postgres` | **none → `:latest`** | ❌ |
| `Project24_2_Kafka/kafka-docker/docker-compose.yml` | `zookeeper` | `confluentinc/cp-zookeeper` | 6.1.1 | ✅ |
| | `broker` | `confluentinc/cp-server` | 6.1.1 | ✅ |
| | `schema-registry` | `confluentinc/cp-schema-registry` | 6.1.1 | ✅ |
| | `connect` | `cnfldemos/cp-server-connect-datagen` | 0.4.0-6.1.0 | ✅ |
| | `control-center` | `confluentinc/cp-enterprise-control-center` | 6.1.1 | ✅ |
| | `ksqldb-server` | `confluentinc/cp-ksqldb-server` | 6.1.1 | ✅ |
| | `ksqldb-cli` | `confluentinc/cp-ksqldb-cli` | 6.1.1 | ✅ |
| | `ksql-datagen` | `confluentinc/ksqldb-examples` | 6.1.1 | ✅ |
| | `rest-proxy` | `confluentinc/cp-kafka-rest` | 6.1.1 | ✅ |

- **Confluent Platform version: 6.1.1** (released 2021; bundles Apache Kafka 2.7.x). The `connect` image is the only one off-version, at `6.1.0`.
- The two untagged images are the pinning work for Phase 1. The Mosquitto log records that the image actually ran as **2.1.2**, which is a defensible pin. The ThingsBoard version that built the current Postgres volume is **NOT VERIFIED** — it must be read from the running container before that volume is ever recreated, because a newer `tb-postgres` may refuse or migrate it.
- All three compose files declare a top-level `version:` key (`"3"`, `"2.2"`, `"3"`), obsolete in Compose v2.40 and emitted as a warning on every invocation.

---

## 4. Secrets and sensitive data

Two methods were used: a case-insensitive pattern grep across all three folders for `password|secret|token|apikey|api_key|-----BEGIN|firebaseio|firebasedatabase|?auth=|sasl|credential|@gmail|@mit.edu|C:\Users|tenant@thingsboard`, and a manual read of every config-type and source file (all 16 are small enough to read in full; all 16 were read).

**VALUES REDACTED throughout.**

| File | Line | Type | Live / dead | Proposed remediation |
|---|---|---|---|---|
| `Project_24_MQTT/paho-mqtt/TBpublish.py` | 7 | ThingsBoard device access token — the **public course default**, not a private credential | **Live** (authenticates the MQTT session) | `TB_ACCESS_TOKEN` env var; `.env.example` carries the public default |
| `Project24_2_Kafka/NodeJSConsumer/package.json` | 9 | Third-party personal email (course author, `…@mit.edu`) — **not the owner's** | Dead (metadata) | Replace `author` with the owner's chosen public identity |
| `Project_24.1/TBpublish.py` | 7 | Same course-default token, duplicate copy | Dead (superseded file) | File excluded from repo |

**Negative results — confirmed absent from all three folders:**

- No Firebase database URL, no `*.firebaseio.com` / `*.firebasedatabase.app` host, no `?auth=` parameter, no service-account JSON.
- No JWT, no tenant credentials, no hardcoded admin password. `tenant@thingsboard.org` / `tenant` appears **nowhere in any file** — those defaults are typed into the ThingsBoard UI at login and live in the Postgres volume. Reported here per the briefing: the defaults are unchanged, but they are not exposed by any file.
- No Kafka/Confluent SASL credentials or API keys. The Confluent stack runs `PLAINTEXT` with no auth; `CONFLUENT_SUPPORT_CUSTOMER_ID` is the literal `anonymous`.
- No private keys (`-----BEGIN`).
- No absolute Windows paths containing the owner's username. Both ThingsBoard volumes use `~/.mytb-data` and `~/.mytb-logs`, which are portable.

### 4.1 The material finding

**The Firebase credential is real, is in use, and is not in any file.** The `TempToFirebase` and `AlarmToFirebase` rule chains reach Firebase through a ThingsBoard REST API Call node whose URL — including the database host and, per the briefing's description of the write path, its auth parameter — is stored as node configuration **inside the ThingsBoard PostgreSQL database at `%USERPROFILE%\.mytb-data\db`**. Consequences:

1. There is nothing to redact from the repository today, because the credential has never been in a file.
2. There is also **no backup of it, or of the rule chains, anywhere**. That 144 MB directory is the single copy.
3. Phase 1 must export the three rule chains to JSON. **That export will contain the live Firebase URL and secret in cleartext.** It must be treated as a secret from the moment it is written — staged outside the repo, parameterised (`FIREBASE_DB_URL`, `FIREBASE_DB_SECRET`), and only the parameterised version committed. If a raw export is ever committed and pushed, rotating the Firebase secret becomes mandatory.

### 4.2 Not scanned

The submission `.docx` files contain **14, 16 and 18 embedded screenshots** respectively (plus 6 in the Spanish mini-lesson notes). These are images of the ThingsBoard and Firebase consoles and were **NOT VERIFIED — no OCR performed**. They plausibly display the Firebase database URL, device tokens, and the ThingsBoard UI in a logged-in state. They are excluded from the repository by the §7 mapping, so this is not a blocker, but **no screenshot from these documents should be reused in public README material without being reviewed and redacted first.**

---

## 5. Functional verification

**5.1 Pipeline 24.1 (MQTT → ThingsBoard → Firebase) — ✅ MQTT→ThingsBoard VERIFIED 2026-09-23; Firebase leg NOT VERIFIED (owner checks console). ⚠️ The 60-second bound failed — the run published for 137.6 s / 46 messages; see §5.1b.**
**5.2 Pipeline 24.2 (Python → Kafka → Node.js) — ✅ VERIFIED END-TO-END, 2026-09-13 23:06–23:13.**

### 5.0 Preconditions (owner-mandated, verified before any container started)

| # | Precondition | First check | After owner fix |
|---|---|---|---|
| 1 | Docker running, `docker info` responds | ❌ `docker-desktop` distro `Stopped`; engine pipe unreachable | ✅ engine 29.0.1, distro `Running` |
| 2 | Total Memory ≥ 12 GB | ❌ **8,329,089,024 B (7.76 GiB)**; `.wslconfig` still `memory=8GB`, mtime 17 days old | ✅ **12,544,364,544 B (12.54 GB / 11.68 GiB)**; `.wslconfig` `memory=12GB` |

Phase 0 stopped and reported on the first failure rather than working around it, per instruction. Note for the record: `memory=12GB` in `.wslconfig` is parsed as 12 GiB and yields 11.68 GiB usable after VM overhead — it clears 12 GB decimal, and sits marginally under 12 GiB binary.

### 5.2 Pipeline 24.2 — Python → Kafka → Node.js

**Startup.** `docker compose up -d` returned in **3 s** (all nine images already cached locally; no pull). Containers were *restarted*, not recreated — this stack declares no volumes, so each container's August state persisted in its writable layer.

**Failure and recovery — the broker did not survive first start.** Eight of nine containers came up; `broker` exited (1) within ~20 s, taking `ksql-datagen`, `schema-registry`, `connect` and `rest-proxy` down with it as a cascade. Root cause:

```
org.apache.zookeeper.KeeperException$NodeExistsException: KeeperErrorCode = NodeExists
	at kafka.zk.KafkaZkClient.registerBroker(KafkaZkClient.scala:98)
	at kafka.server.KafkaServer.startup(KafkaServer.scala:524)
```

ZooKeeper reloaded a persisted `/brokers/ids/1` ephemeral znode from the August session off its own writable layer, and the broker refused to register over a node that already existed. This is **stale-state, not a configuration fault** — it is a latent reliability defect of a volume-less stack that is stopped without `down`. Resolved non-destructively by waiting out the ZK session expiry and re-issuing `docker compose up -d broker`; the broker then logged `[KafkaServer id=1] started` at 03:08:31, and a second `up -d` restored the four cascaded services. **Time to all nine healthy: ~3 min including the recovery** (~45 s of it actual startup). Nothing was deleted and no `-v` was used.

**Topic.** `vehicle-coordinates` **already existed** from the August run — not auto-created during this run.

| Property | Value |
|---|---|
| Topic | `vehicle-coordinates` |
| PartitionCount | **1** |
| ReplicationFactor | **1** |
| `min.insync.replicas` | 1 |
| `retention.ms` | 604800000 (7 days) |
| `cleanup.policy` | delete |
| Non-internal topics present | `vehicle-coordinates`, `default_ksql_processing_log`, `docker-connect-{configs,offsets,status}` |
| Internal topics | 59 |

**End-to-end run.** Consumer started first (`node server.js`, then `GET /consumer` → `Success- Consumed: vehicle-coordinates`), producer second, bounded by `timeout 59` against the script's 3 s loop.

| Measurement | Value |
|---|---|
| Baseline offset before run | **920** |
| Offset after run | **939** |
| Messages produced | **19** (not 20 — see below) |
| Messages consumed | **19**, offsets 920–938, contiguous, zero gaps |
| Consumer group | `node-example-group-1` |
| Client id / member | `node-example` / `node-example-7970dd4a-…` |
| Partition assignment | `vehicle-coordinates:[0]`, RoundRobinAssigner, consumer was group leader |
| Offsets committed? | **Yes** — CURRENT-OFFSET 939, LOG-END-OFFSET 939, **LAG 0** |

Sample consumed payload: `{"longitude": -75, "latitude": -82} of partition 0 @ offset 938`. Coordinates fall in valid geographic ranges, confirming Deviation 3 at runtime.

**19 messages, not the 20 requested — and the reason is a real defect.** The script has no `producer.flush()` and no `producer.close()`, and its `except KeyboardInterrupt` does not catch the SIGTERM that `timeout` delivers. The final message was still in the client's send buffer when the process was killed. **Any abrupt stop of this producer silently loses buffered messages.** Added to the Phase 1 fix list. The count is reported as observed rather than rounded to the requested 20.

**Prior-state evidence for offset committing.** Before this run the group already showed CURRENT-OFFSET 920 / LAG 0 with no active members — i.e. the August session had committed cleanly. `kafkajs` auto-commit is on by default and the code does not override it. Because a committed offset existed, `fromBeginning: true` was correctly ignored and the consumer resumed at 920, consuming only the 19 new records rather than replaying 920 historical ones.

**Confluent services actually used.** Confirmed at runtime, matching the static finding: **only `zookeeper` and `broker`**. The producer and consumer both address `localhost:9092` and nothing else. `schema-registry`, `connect`, `rest-proxy`, `ksqldb-server`, `ksqldb-cli` and `ksql-datagen` carried no coursework traffic. `control-center` consumes broker metrics but serves no pipeline function. **Seven of nine services are ceremony** — candidates for removal from the Phase 1 unified compose, which would cut the stack's ~4.38 GiB footprint to roughly 0.8 GiB.

**Memory.** `docker stats --no-stream`, stack stable:

| Scope | Usage |
|---|---|
| **Confluent stack (9 containers)** | **≈ 4.38 GiB** |
| — `connect` | 2.361 GiB (54% of the stack, and entirely unused) |
| — `broker` | 632.8 MiB |
| — `control-center` | 573.3 MiB |
| — `ksqldb-server` | 368.6 MiB |
| — `schema-registry` / `rest-proxy` / `zookeeper` | 220.8 / 148.6 / 125.0 MiB |
| — `ksql-datagen` / `ksqldb-cli` | 2.2 / 1.1 MiB |
| **Unrelated stacks running concurrently** | ≈ 4.65 GiB |
| — Airflow (7 containers, other coursework) | ≈ 3.07 GiB |
| — Hadoop (5 containers, other coursework) | ≈ 1.59 GiB |
| **Total in use / available** | **9.03 GiB / 11.68 GiB** |

⚠️ **Two unrelated multi-container stacks from other course modules were already running** and were left untouched. They consume more memory than the Confluent stack itself. The 12 GB ceiling is therefore shared three ways, leaving ~2.6 GiB headroom — this is the real constraint on Task 0.4, not the Confluent stack in isolation.

**Log warnings (`docker compose logs --tail=200`, all nine).** No warning indicated a coursework fault.

| Container | WARN/ERROR in last 200 | Assessment |
|---|---|---|
| `ksqldb-cli` | 0 | clean |
| `broker` | 1 | benign Cruise Control goal-violation skip |
| `ksqldb-server` | 6 | **phones home to `version-check.confluent.io`** (failed, no DNS) — telemetry, note for a public project |
| `schema-registry` | 7 | benign Jersey provider registration warnings |
| `rest-proxy` | 7 | broker-down cascade |
| `zookeeper` | 16 | standalone-mode notice + Jetty contextPath |
| `ksql-datagen` | 32 | broker-down cascade: `Expected 1 brokers but found only 0` |
| `control-center` | 43 | benign Jersey provider warnings |
| `connect` | 56 | benign Jersey provider warnings |

Every compose invocation also emits `the attribute 'version' is obsolete` (§3.3).

**Shutdown.** `docker compose down` (**no `-v`**) at 23:13:02, completed 23:13:16 — 14 s. All nine containers and `kafka-docker_default` removed; zero containers remain. No named volume was created or destroyed.

⚠️ **Consequence worth stating plainly:** because this stack declares no volumes, `down` destroyed the broker's writable layer, and with it the `vehicle-coordinates` topic and all 939 messages. That is acceptable here — Kafka holds nothing irreplaceable — but it is the exact opposite of the ThingsBoard situation in Task 0.4, where the equivalent data *is* the project. It also means the `NodeExists` startup failure will not recur on the next `up`, since ZooKeeper's state is gone too.

### What a live run would touch — for the STOP 1 decision

This is the point the owner needs before authorising:

- **`TBpublish.py` does not contain any Firebase code.** It publishes a JSON payload to `v1/devices/me/telemetry` over MQTT to `localhost:9883` at QoS 1, on a **3-second loop with no iteration limit** — it runs until interrupted.
- **The Firebase write is nevertheless automatic.** Every telemetry message that arrives is processed by the `TempToFirebase` rule chain, which PUTs to `/temperature.json`. So **running `TBpublish.py` for any duration at all writes to the live Firebase Realtime Database**, once per message, at 20 writes/minute. There is no way to exercise the ThingsBoard leg without the Firebase leg unless the rule chain is disabled first.
- A **connect-only probe is safe** and writes nothing: a Paho `connect()`/`disconnect()` with no `publish()`, or `mosquitto_sub` run inside the broker container. This verifies both brokers' port mappings with zero cloud side effects, and I recommend it as the first step whatever the Firebase decision is.
- **Pipeline 24.2 has no cloud side effects whatsoever.** Kafka is entirely local, PLAINTEXT, no external sink. Task 0.5 can be authorised independently of, and with a lower bar than, Task 0.4.

### 5.1 Pipeline 24.1 — stack up, probes PASSED, halted at STOP 1b

Authorised 2026-09-13. Steps 1–2 executed 23:17–23:28; step 3 onward awaits owner confirmation that the rule-chain backup exists.

#### Rule-1 conflict resolved without breaching read-only

`Project_24_Docker/docker-compose_step1.yml` bind-mounts `./mosquitto/log` into the container and `mosquitto.conf` sets `log_dest file /mosquitto/log/mosquitto.log`. **Running that stack in place would have written to a file inside a course folder**, breaching hard rule 1 and the "zero modifications, verified by file-hash" definition of done. Resolved by running Mosquitto from a **byte-identical copy** of `docker-compose_step1.yml` and `mosquitto.conf` staged in the session scratchpad (both verified identical by `diff`). Broker behaviour is unchanged; the container's log writes landed on the scratch copy (1114 B) while `Project_24_Docker/mosquitto/log/mosquitto.log` remains **505 B, mtime 2026-08-26 21:19**. ThingsBoard was run in place — its compose mounts only `~/.mytb-data` and `~/.mytb-logs`, nothing under the course tree.

#### Two environment obstacles, both external to the coursework

1. **Port 8080 was held by `airflow-docker-airflow-webserver-1`** (another course module). ThingsBoard maps `8080:9090` and could not bind. Per owner decision, the whole Airflow stack was stopped with `docker stop` — **7 containers stopped, not removed, fully reversible with `docker start`**. Restore list recorded: `flower-1`, `airflow-webserver-1`, `airflow-scheduler-1`, `airflow-worker-1`, `airflow-triggerer-1`, `postgres-1`, `redis-1` (all were `Up 21 minutes (healthy)`). ⚠️ **These must be restarted when Phase 0 finishes.**
2. **A stale container from the failed create kept its port config but never established the bindings** — `HostConfig.PortBindings` was correct while `NetworkSettings.Ports` was empty, so no host mapping existed. Cleared with `docker compose down` (**no `-v`**) followed by `up -d`. `~/.mytb-data` measured **145 MB before and 145 MB after** — the bind mount is untouched by container replacement.

#### Environment facts confirmed

| Item | Value |
|---|---|
| Mosquitto container / image | `mos-broker-1` / `eclipse-mosquitto` → **2.1.2** (Phase 1 pin candidate) |
| ThingsBoard container / image | `thingsboard-mytb-1` / `thingsboard/tb-postgres`, image created 2025-12-23 |
| **ThingsBoard version (resolves Q6)** | **v4.2.1.1**, Java 17.0.17, Hibernate ORM 6.6.29, **Postgres DB version 12.0** → Phase 1 pin `thingsboard/tb-postgres:4.2.1.1` |
| ThingsBoard port mappings | `9883→1883`, `8080→9090`, `5683/udp→5683` |
| Time to HTTP ready | **~20 s** from container start |

#### Connectivity probes — all PASSED, zero telemetry published

| Probe | Method | Result |
|---|---|---|
| Mosquitto pub/sub | `mosquitto_sub`/`mosquitto_pub` inside container, topic `probe/test` | ✅ `RECEIVED: phase0-probe-ok` |
| Mosquitto host port | TCP connect to `localhost:1883` | ✅ OPEN |
| **ThingsBoard MQTT 9883** | Paho **connect-only** script in scratchpad — `connect()` + `disconnect()`, **no `publish()`** | ✅ **rc=0, Connection Accepted** — port works *and* the `DHT11_DEMO_TOKEN` device exists and authenticates |
| ThingsBoard HTTP UI | `GET /login` | ✅ **HTTP 200** |

#### Rule chain inventory (read-only REST, `GET /api/ruleChains`)

Authenticated with the public default tenant credentials. **JWT acquired and redacted; no node configuration was read or printed, because those configs hold the live Firebase credential (§4.1).**

| Rule chain | Nodes | Connections | Root | Note |
|---|---|---|---|---|
| `Root Rule Chain` | **9** | 8 | ✅ | Contains `TbRestApiCallNode` + `TbRuleChainInputNode` |
| `CreateAndClearAlarms` | **5** | 4 | — | `TbJsFilterNode`, `TbCreateAlarmNode`, `TbClearAlarmNode`, 2× `TbRuleChainInputNode` |
| `TempToFirebase` | **1** | 0 | — | single `TbRestApiCallNode` |
| `AlarmToFirebase` | **1** | 0 | — | single `TbRestApiCallNode` |
| `Thermostat` | **7** | 6 | — | **Not mentioned in the briefing** — stock ThingsBoard demo chain, matches the default device-profile chain; appears unused by the coursework |

All three briefed chains **exist and are intact**. Two findings worth carrying into Phase 1:

- ⚠️ **The secret surface is larger than assumed.** `Root Rule Chain` contains a **third** `TbRestApiCallNode` alongside the two Firebase chains. Phase 1 must parameterise **three** REST call nodes, not two, and the export of `Root Rule Chain` must be treated as secret-bearing too.
- A fifth chain, `Thermostat`, exists and is almost certainly stock ThingsBoard demo content. It should be confirmed as out of scope rather than exported as project logic.

#### Session 2 — STOP 1b re-established after an unclean shutdown (2026-09-23)

The 2026-09-13 session ended at STOP 1b without executing its closing steps. Between the two sessions the Docker engine went down while the stack was still running: `thingsboard-mytb-1` and `mos-broker-1` were both found `Exited (255)`, the signature of an engine-level kill rather than a clean `down`.

**Preconditions, re-verified before anything was started.**

| # | Precondition | Result |
|---|---|---|
| 1 | Docker reachable, Total Memory ≥ 12 GB | ✅ **12,544,364,544 B**, engine 29.0.1. (First attempt this session: ❌ engine unreachable, `docker-desktop` distro `Stopped` — reported, nothing started.) |
| 2 | No Airflow containers running | ✅ after remediation. **Initially ❌** — all 7 Airflow containers had come back up automatically when Docker Desktop started, and `airflow-webserver-1` again held port 8080. |

⚠️ **The August `docker stop` does not survive an engine restart.** The Airflow containers carry a restart policy and return on every Docker Desktop start. Per owner decision the 7 Airflow **and** 5 Hadoop containers were stopped with `docker stop` — **12 containers stopped, not removed, fully reversible with `docker start`**: `airflow-docker-{flower,airflow-webserver,airflow-scheduler,airflow-worker,airflow-triggerer,postgres,redis}-1`, and `namenode`, `datanode`, `resourcemanager`, `nodemanager`, `historyserver`. ⚠️ **All 12 must be restarted when Phase 0 finishes.** With both stacks down the full 11.68 GiB was available to ThingsBoard, removing the ~2.6 GiB headroom constraint noted in §5.2.

**The stale port-binding defect recurred — and is now confirmed reproducible.** On first `up -d` the existing container was *started*, not recreated, and came up with `HostConfig.PortBindings` correct (`9883→1883`, `8080→9090`, `5683/udp`) but `NetworkSettings.Ports` empty — no host mapping at all, exactly as in the August session. Resolved the same non-destructive way: `docker compose down` (**no `-v`**) then `up -d`, which recreated the container with all three bindings live. **`~/.mytb-data` measured 148,883,259 B before and 149,031,079 B after** — the ~148 KB delta is the running server's own writes, not loss from the recreation; the bind mount survives container replacement. The Docker volume list was byte-identical before and after: **no named volume was created or destroyed**.

This is no longer a one-off. **Any ungraceful stop of this stack leaves a container that starts without its port bindings**, and the only recovery is `down` + `up`. It belongs on the Phase 1 fix list beside the ZooKeeper `NodeExists` defect (§5.2) — both are the same class of latent fault in a stack that is never shut down cleanly.

**Time to HTTP ready: 62 s**, against ~20 s in August. The extra ~40 s is Postgres recovering from the unclean shutdown. Mosquitto again ran from a **byte-identical scratchpad copy** (both files verified by `diff` and SHA-256: compose `36dce0bc…`, conf `72a6e386…`); the container's log writes landed on the scratch copy (1,616 B) and `Project_24_Docker/mosquitto/log/mosquitto.log` remains **505 B, mtime 2026-08-26 21:19**.

**Connectivity probes re-run — all four PASSED, zero telemetry published.**

| Probe | Result |
|---|---|
| Mosquitto pub/sub inside container (`probe/test`) | ✅ `RECEIVED: phase0-probe-ok` — Mosquitto **2.1.2** |
| Mosquitto host port `localhost:1883` | ✅ OPEN |
| ThingsBoard MQTT 9883, Paho **connect-only**, no `publish()` | ✅ **rc=0, Connection Accepted** — `DHT11_DEMO_TOKEN` still authenticates |
| ThingsBoard HTTP `GET /login` | ✅ **HTTP 200** |

**Rule chains survived the unclean shutdown intact.** Re-read with the same read-only REST calls; **no node configuration was read or printed**, because those configs hold the live Firebase credential (§4.1).

| Rule chain | Nodes | Conns | Root | vs STOP 1b |
|---|---|---|---|---|
| `TempToFirebase` | 1 | 0 | — | ✅ match |
| `AlarmToFirebase` | 1 | 0 | — | ✅ match |
| `CreateAndClearAlarms` | 5 | 4 | — | ✅ match |
| `Root Rule Chain` | 9 | 8 | ✅ | ✅ match |
| `Thermostat` (stock demo) | 7 | 6 | — | unchanged |

All four briefed chains match the **1 / 1 / 5 / 9** node counts recorded at STOP 1b. Total chains: 5. ThingsBoard version re-confirmed from `/api/system/info`: **4.2.1.1 CE** — the Phase 1 pin `thingsboard/tb-postgres:4.2.1.1` stands.

**Memory, stack idle:** `thingsboard-mytb-1` **1.542 GiB**, `mos-broker-1` **5.883 MiB**, of 11.68 GiB. **Log warnings:** 5 in ThingsBoard's last 200 lines — a Leshan CoAP PskStore notice, a system-tenant lifecycle message and an OWASP validator directive notice, all benign and none coursework-related; 0 in Mosquitto.

#### 🛑 STOP 1b — awaiting owner

Stack is **up and idle**. No telemetry has been published; **nothing has reached Firebase**. The backup gate **stands**: `its-pipeline-backup/` does not exist. Awaiting owner export of the four rule chains (`TempToFirebase`, `AlarmToFirebase`, `CreateAndClearAlarms`, `Root Rule Chain`) and confirmation before `TBpublish.py` runs.

**Remaining steps, still NOT VERIFIED:** the 60-second bounded `TBpublish.py` run; telemetry arrival confirmed via ThingsBoard REST; the Firebase record (**to be marked NOT VERIFIED** — the owner checks the console); shutdown with `docker compose down` (no `-v`); the today's-date integrity re-check; restart of the **12** stopped Airflow and Hadoop containers.

#### Gate cleared — rule-chain exports verified (2026-09-23 11:14)

Owner exported four chains to `its-pipeline-backup/`. Verified by parsing each file for name and node/connection counts only — **node configurations were not read or printed**, as they carry the live Firebase credential (§4.1):

| File | Chain | Nodes | Conns | Size |
|---|---|---|---|---|
| `temptofirebase.json` | `TempToFirebase` | 1 | 0 | 1,654 B |
| `alarmtofirebase.json` | `AlarmToFirebase` | 1 | 0 | 1,650 B |
| `createandclearalarms.json` | `CreateAndClearAlarms` | 5 | 4 | 5,233 B |
| `root_rule_chain.json` | `Root Rule Chain` | 9 | 8 | 7,016 B |

All four match the live **1 / 1 / 5 / 9** counts. ⚠️ **These files are secret-bearing** — `root_rule_chain.json`, `temptofirebase.json` and `alarmtofirebase.json` each contain a `TbRestApiCallNode` config with the Firebase URL. `its-pipeline-backup/` must never become a git repo (decision 4) and the credential is to be rotated at Phase 1 closure (decision 5). The directory also holds `mytb-data_2026-09-22/`.

### 5.1b Live run — telemetry VERIFIED, but the 60-second bound FAILED

**⚠️ The run exceeded its authorised bound. It published for 137.6 s, not 60 s — 46 messages instead of the ~20 the owner authorised, so roughly 26 more Firebase writes than sanctioned.** Reported as it happened rather than presented as a clean 60-second run.

**What went wrong.** The run was bounded with `timeout --signal=INT 60`, the same mechanism that worked for the Kafka producer in §5.2. It does not work here: `timeout` is an **MSYS/Git Bash** utility, while the interpreter is a **native Windows** `python.exe`. A POSIX `SIGINT` raised by MSYS is not translated into a Windows console `CTRL_C_EVENT` for a non-MSYS child, so the signal was delivered to nothing the interpreter could see. `TBpublish.py`'s `except KeyboardInterrupt` never fired and the `while True` loop ran on. The process was ended with `taskkill /F` (PID 23440) as soon as the overrun was detected — about 78 s past the bound.

| Measurement | Value |
|---|---|
| Publisher start → kill | **11:21:36 → 11:23:54 = 137.6 s** (authorised: 60 s) |
| Messages published | **46** (expected at 60 s: ~20) |
| Excess Firebase writes | **≈ 26** |
| How it was stopped | `taskkill /F` — not a graceful shutdown |

**This is a third instance of the same underlying defect**, and the most consequential: §5.2 recorded that `TBpublish.py`'s sibling producer loses buffered messages because it catches only `KeyboardInterrupt`; here the same narrow exception handling, combined with an uninterruptible loop and no iteration limit, meant **the script could not be bounded from outside at all** on this platform. **Phase 1 must give both publishers an explicit iteration/duration limit as a parameter** rather than relying on an external `timeout`. On Windows the only reliable external bound is `taskkill /F`, which is precisely the abrupt stop that loses buffered messages.

**Telemetry — VERIFIED via ThingsBoard REST.** Read with `GET /api/plugins/telemetry/DEVICE/{id}/values/timeseries`, device `DHT11 Demo Device` (`47d472f0-…`).

| Measurement | Value |
|---|---|
| Baseline before run | `temperature` 81, `humidity` 77 @ **2026-08-26** — 27.6 days stale |
| Points after run | **46 `temperature` + 46 `humidity`** |
| First → last | 11:21:37 → 11:23:52, span 135.0 s |
| Inter-arrival gaps | **min 3.0 s, max 3.0 s** — perfectly regular, zero gaps |
| Latest value | `temperature` 76, `humidity` 69 @ 11:23:52 |
| Messages published vs stored | **46 published, 46 stored — zero loss** |

Every message published reached ThingsBoard and was persisted. The MQTT → ThingsBoard leg is **fully verified**. Unlike the Kafka producer, no message was lost to the abrupt stop: QoS 1 with `loop_start()` had flushed each publish before the kill landed between iterations.

**🔸 Firebase leg — NOT VERIFIED.** The `TempToFirebase` chain fired once per message, so **46 writes to `/temperature.json` are expected**. No REST GET against the Firebase database was made — the URL sits inside a rule-node config that was deliberately never read (§4.1), and the owner verifies the console directly. **No rule chain was viewed, edited, disabled or otherwise touched at any point.**

**Shutdown.** `docker compose down` (**no `-v`**) on both stacks at 11:24:43, complete 11:24:47 — 4 s. Both containers and both networks removed; **0 containers running**. Docker volume count **87 before and 87 after**; `~/.mytb-data` **149,645,786 B before and after, byte-identical** — the ThingsBoard database, including the 46 new telemetry points, survives intact.

#### ✅ Firebase leg — VERIFIED by owner in the console (2026-09-26)

Verified by the owner directly in the Firebase console, not by me. This supersedes the NOT VERIFIED status above for the fact of arrival, but **not** for the write count.

- **`/temperature` shows the last value of the 2026-09-23 run — `76` / `69`**, changed from the August baseline of `81` / `77`. The Firebase leg therefore works end to end: `TempToFirebase` fired and reached the live database.
- **The write count cannot be verified.** `TempToFirebase` uses **PUT**, so each message overwrites the node and **only the last value persists**. The ~46 writes the overrun produced (§ above) leave no countable trace at `/temperature`; the owner's console check can confirm arrival and the final value, but not how many writes occurred. The 46 figure stands on the ThingsBoard-side evidence alone.
- **The two sinks use different write semantics.** `/alarm` **accumulates** records, while `/temperature` is overwritten in place. Phase 1 must not assume one shape for both: a per-message history exists on the alarm path and does not on the temperature path. If retained temperature history is wanted, that node needs POST or a timestamped child key instead of PUT.

#### 🔒 Firebase database rules — were open, now expired (owner, 2026-09-26)

This closes open question 5 from §8.1, and it is worse than "rotatable".

- The database was running in **test mode: open read and write to anyone, until 2026-09-25**. That window has now **expired, and the database is currently locked** — reads and writes are denied.
- So for the whole life of the coursework, including the 2026-09-23 run, **the Realtime Database was world-readable and world-writable**. The credential in the rule-node configs was never the only exposure; the data was reachable without it.
- ⚠️ **Consequence for Phase 1:** the optional Firebase sink must be **authenticated**, with **auth-based security rules** replacing test mode — not merely a rotated URL or secret. Until that is done the sink cannot be re-enabled, and any Phase 1 demo of it will fail against the now-locked database. The lock also means **the Firebase leg is not currently reproducible**, which must be stated wherever the pipeline is presented as working end to end.

#### 🛑 STOP 2 — Task 0.4 complete

Integrity re-verified after the live run: **0 files in `Project_24_Docker`, `Project_24_MQTT` or `Project24_2_Kafka` carry today's date**, no `__pycache__` or `.pyc` was produced, and the latest mtimes are unchanged at 2026-08-26 21:19 / 2026-08-26 10:44 / 2026-08-27 12:43. `Project_24_Docker/mosquitto/log/mosquitto.log` remains **505 B, mtime 2026-08-26 21:19**.

⚠️ **Outstanding owner action: restart the 12 stopped containers** — `airflow-docker-{flower,airflow-webserver,airflow-scheduler,airflow-worker,airflow-triggerer,postgres,redis}-1` and `namenode`, `datanode`, `resourcemanager`, `nodemanager`, `historyserver`. All were stopped with `docker stop`, never removed; `docker start` restores them.

---

## 6. Deviations from course material found in code

Both known deviations are **confirmed in code**, and the starter archives make the before/after exact.

**Deviation 1 — `KafkaClient` removed in kafka-python 3.x.** Confirmed. The starter (preserved byte-identical as `PublishVehicleCoordinates.py.bak`) contains `client = kafka.KafkaClient(...)`, a `consumer.topics()` existence check, and `client.ensure_topic_exists(topic_name)`. The live file deletes that block entirely, including the now-unnecessary `KafkaConsumer`. NumPy integers are wrapped: `sensor_data['longitude'] = int(longitude)`, same for latitude — required because `json.dumps` cannot serialise `numpy.int64`. Installed kafka-python is **3.0.11**, consistent.

**Deviation 2 — `node-rdkafka` incompatible with Node.js v24.** Confirmed. `myconsumer.js` now imports `{ Kafka }` from `kafkajs`, and **the exported function signature is preserved** — `myconsumer(topic)` taking `{topic: …}` and returning `""` — so `server.js`'s call site is unchanged. `package.json` declares `kafkajs ^2.2.4`; the lockfile contains no `node-rdkafka` entry.

**Additional deviations and defects found, not previously documented:**

3. **The producer switched from `random` to `numpy.random`, and the coordinate ranges changed.** The starter's placeholders were filled with `np.random.randint(-180, 180)` / `(-90, 90)` — correct geographic longitude/latitude ranges. The `.bak` reference used `random.randint(0, 100)` / `(50, 100)`, which are not coordinates. This is a genuine improvement over the course material and worth stating in the public README rather than leaving as an unexplained difference.
4. **The starter's `lattitude` misspelling was corrected to `latitude`** throughout the live producer.
5. **`TBpublish.py` was extended with a `humidity` field** not present in the starter, making the payload `{temperature, humidity}`.
6. **`topic_name` changed from `mytest` to `vehicle-coordinates`** and matched to `server.js` — a placeholder the course left for the student.
7. **Stale `node-rdkafka` attribution comment** survives at `myconsumer.js:18` inside the retained Confluent header, directly contradicting Deviation 2. Must be corrected in Phase 1 — a reviewer reading top-to-bottom hits the wrong claim before the right code.
8. **`NodeJSConsumer/README.md` is stale course text**, describing `restConsumer.js`, `restProducer.js` and `myproducer.js` — none of which exist in this project. It must not survive into a public repo unedited.
9. **`jest` is declared as a production dependency with no tests present.**

### 6.1 Disposition (owner decision, 2026-09-13)

| Deviations | Classification | Action |
|---|---|---|
| **1–2** | Documented course-compatibility fixes | Already recorded in course submissions; explain in the public README |
| **3–6** | **Owner improvements** — correct geographic ranges, `lattitude`→`latitude` spelling fix, added `humidity` field, topic naming | **To be claimed in the final README** as deliberate improvements over the course material, not silent drift |
| **7–9** | Defects inherited or introduced | **Phase 1 fix list** — stale `node-rdkafka` comment, stale `README.md`, `jest` misclassified as a runtime dependency |

---

## 7. Proposed repository structure

> **The structure below is a proposal.** Owner decisions, followed as given: single repo; layered by pipeline stage rather than by course sub-project; course code enters as `v0-coursework`; Firebase isolated as an optional, config-disabled sink; `docs/internal/` for Spanish working documents; `config/` as the single source of truth for thresholds and constants. Every other choice — directory names, file names, the placement of the Node consumer and web server — is mine to be overruled.

### 7.1 `v0-coursework` — faithful snapshot (owner decision, 2026-09-13)

**`v0-coursework` preserves original paths and file names.** The layered structure of §7.2 is applied in later Phase 1 commits, so the restructuring is legible as a diff. Only **two categories are excluded even from v0: secrets and binaries** (`.docx`, screenshots, `.zip` archives).

Under that rule, v0 **keeps** everything previously proposed for exclusion on quality grounds — the dead files stay and are cleaned in a later commit:

| Included in v0 at original path | Was previously proposed for exclusion — reason it now stays |
|---|---|
| `Project24_2_Kafka/kafka-docker/config.js` | Dead code, not a secret or binary → cleaned in a Phase 1 commit |
| `Project24_2_Kafka/kafka-docker/librdkafka.config` | Dead `node-rdkafka` artifact → cleaned later |
| `Project24_2_Kafka/pythonProducer/…py.bak` | Course-provided starter; it is the evidence for Deviation 1 → cleaned later |
| `Project24_2_Kafka/NodeJSConsumer/README.md` | Stale course text → rewritten later |
| `Project_24_Docker/mosquitto/log/mosquitto.log` | Runtime artifact, plain text → see interpretation note below |

**Excluded from v0 — binaries:** all `*.docx` (course submissions and mini-lessons, per Q10), `Project_24.1_part2/*.pdf` (third-party copyrighted transcripts), all `.zip` starter archives, and any screenshot.

**Excluded from v0 — secrets:** none exist as files. No file in the three folders contains a live credential (§4). The ThingsBoard token in `TBpublish.py:7` is the **public course default**, not a secret, and is retained in v0 so the snapshot stays faithful; it is parameterised to `TB_ACCESS_TOKEN` in a later Phase 1 commit.

⚠️ **Three interpretation points I am flagging rather than deciding** — the rule as stated does not settle them:

1. **`node_modules/` (58 MB, 8551 files).** Not a `.docx`, screenshot or `.zip`, but it contains compiled binaries and is fully reproducible from `package-lock.json`. I have assumed **excluded**. Committing it would put 58 MB of vendored third-party code into the first commit of a public portfolio repo.
2. **`mosquitto.log`.** Plain text, so a strict reading includes it. It is a runtime artifact of no value. I have assumed **included in v0** (faithful) and removed in the cleanup commit, which is the more literal reading of your rule.
3. **The sibling folders** (`Project_24.1/`, `Mosquitto/`, `Nueva carpeta/`, loose scratch scripts). The briefing scoped v0 to "which files from the three folders go into that commit", so the duplicates are outside v0 by scope, not by exclusion rule. `pahopublish.py` / `pahosubscribe.py` are approved for `scripts/smoke/` (Q7) — I have assumed they enter at a **later** Phase 1 commit in English, not in v0, since v0 is a snapshot of the three folders. Confirm if you want them in v0 at their original root paths instead.

### 7.2 Phase 1 target structure — owner-specified (2026-09-26)

**This tree is the owner's, given verbatim.** It supersedes the layered proposal previously in this section (decision 11, §8.1). The governing change is architectural, not cosmetic: **the course's two pipelines are v0 legacy, and Phase 1 replaces them with a single industrial condition-monitoring pipeline.** The vehicle-GPS producer and the temperature publisher both give way to one sensor simulator, and the two stacks become one MQTT → bridge → Kafka → alarm-consumer path.

```
industrial-telemetry-streaming-pipeline/
├── README.md
├── Makefile                       # make up / make test
├── docker-compose.yml             # single file, profiles (firebase optional)
├── .env.example
├── .gitignore
├── config/                        # thresholds, constants, topic names — single source of truth
├── schemas/                       # versioned Avro schemas
├── edge/
│   └── simulator/                 # industrial sensor simulator; replaces both course producers
├── bridge/
│   └── mqtt_to_kafka/
├── broker/
│   ├── mosquitto/
│   └── kafka/                     # topics, Schema Registry config
├── processing/
│   ├── alarm_consumer/            # threshold + persistence + hysteresis, dead-letter topic
│   └── thingsboard/rule-chains/   # 4 JSON with placeholders + import script
├── sinks/
│   └── firebase/                  # optional, disabled by default
├── visualization/
│   └── thingsboard/dashboards/
├── scripts/                       # smoke tests, probes, rule-chain import
├── tests/                         # reserved for Phase 2
├── .github/workflows/             # reserved for Phase 2
└── docs/
    ├── architecture/
    └── internal/
```

**One-line justification per top-level entry**

| Entry | Why it exists |
|---|---|
| `Makefile` | `make up` / `make test` is the reproducibility promise the current coursework cannot make — today every run is retyped by hand. |
| `docker-compose.yml` | One file at the root, not a `deploy/` directory. Compose **profiles** make the Firebase sink optional structurally, which is what "disabled by default" has to mean in practice. |
| `config/` | Owner constraint, unchanged: thresholds, constants and topic names in one place. Alarm limits currently live inside rule-chain nodes; this is where they move. |
| `schemas/` | **New.** Versioned Avro schemas — the one Confluent service the coursework never used (`schema-registry`) becomes load-bearing instead of ceremony (§5.2). |
| `edge/simulator/` | **One producer replaces two.** The temperature publisher and the vehicle-GPS producer were two unrelated demos; a single industrial sensor simulator is what makes this one pipeline rather than two coursework leftovers. It is also where the bounding defect gets fixed (§5.1b). |
| `bridge/mqtt_to_kafka/` | **New, and the architectural keystone.** The coursework had MQTT and Kafka as two disconnected pipelines. The bridge is what joins them into one path, and it is the single largest piece of Phase 1 work with no course antecedent. |
| `broker/` | Mosquitto and Kafka play the identical architectural role; co-locating them is the clearest argument the repo makes. Kafka now also owns topic definitions and Schema Registry config, absorbing the old `streaming/`. |
| `processing/alarm_consumer/` | **Python, not Node** (decision 14). Threshold evaluation with **persistence and hysteresis** plus a **dead-letter topic** — real condition-monitoring behaviour, where the course consumer only printed messages to a web page. |
| `processing/thingsboard/rule-chains/` | **4 JSON with placeholders**, never the raw exports — the placeholder requirement is the §7.3 parameterisation gate made structural. The **import script** is what turns the exports from a backup into something deployable, which has never been tested (§7.3). |
| `sinks/firebase/` | Owner constraint: optional and disabled by default. Now doubly necessary — the database is locked as of 2026-09-25, so an enabled-by-default sink would fail every fresh clone (§5.1b). |
| `visualization/thingsboard/dashboards/` | Dashboards only. The Express UI does **not** survive here (decision 15) — it belonged to the vehicle demo that Phase 1 retires. Note the dashboards still have **no source file**; they need the same export treatment as the rule chains (§7.3). |
| `scripts/` | Smoke tests, probes and the rule-chain import, so verification is repeatable rather than retyped. The two paho scripts land here in English (decision 12c). |
| `tests/`, `.github/workflows/` | **Reserved for Phase 2**, deliberately empty in Phase 1. Naming them now fixes where CI and tests will go instead of leaving it to be argued later. |
| `docs/` | Public English under `architecture/`, Spanish working material under `internal/` — owner constraint, unchanged. |

⚠️ **Consequence for §7.3.** Most of the mapping table below now targets paths that no longer exist in this tree: `streaming/` is absorbed into `broker/kafka/`, `deploy/compose/*` collapses into the root `docker-compose.yml`, `edge/mqtt-temperature/` and `edge/kafka-vehicle-gps/` are replaced by `edge/simulator/`, `processing/kafka-consumer/` becomes `processing/alarm_consumer/` in Python, and `visualization/web/server.js` has no Phase 1 destination at all. **§7.3 remains approved as the v0 → cleanup record** (decision 13), but it describes the legacy path, not the Phase 1 target. The two are reconciled by the v0 rule: the course code enters faithfully, then is replaced rather than moved.

### 7.3 Mapping table — course path → Phase 1 layered path

> This is the **Phase 1 target**, reached in commits *after* `v0-coursework`. v0 itself keeps original paths and names (§7.1).

**Mapped into the layered structure:**

| Course path | Proposed repo path |
|---|---|
| `Project_24_MQTT/paho-mqtt/TBpublish.py` | `edge/mqtt-temperature/publisher.py` |
| `Project_24_Docker/mosquitto/config/mosquitto.conf` | `broker/mosquitto/mosquitto.conf` |
| `Project_24_Docker/docker-compose_step1.yml` | `deploy/compose/mosquitto.yml` |
| `Project_24_MQTT/ThingsBoard/docker-compose_step5.yml` | `deploy/compose/thingsboard.yml` |
| `Project24_2_Kafka/pythonProducer/PublishVehicleCoordinates.py` | `edge/kafka-vehicle-gps/producer.py` |
| `Project24_2_Kafka/NodeJSConsumer/myconsumer.js` | `processing/kafka-consumer/consumer.js` |
| `Project24_2_Kafka/NodeJSConsumer/server.js` | `visualization/web/server.js` |
| `Project24_2_Kafka/NodeJSConsumer/package.json` | `processing/kafka-consumer/package.json` |
| `Project24_2_Kafka/NodeJSConsumer/package-lock.json` | `processing/kafka-consumer/package-lock.json` |
| `Project24_2_Kafka/kafka-docker/docker-compose.yml` | `deploy/compose/kafka.yml` |
| `pahopublish.py`, `pahosubscribe.py` *(root, proposal)* | `scripts/smoke/` — genuinely useful broker probes; translate to English |

**Excluded from the Phase 1 repository — with reason.** Rows marked ◆ are *not* excluded from `v0-coursework` under the owner's faithful-snapshot rule (§7.1); they enter v0 at their original path and are removed in a later cleanup commit.

| Course path | Reason |
|---|---|
| `Project24_2_Kafka/NodeJSConsumer/node_modules/` | 58 MB / 8551 vendored files; reproducible from the lockfile |
| ◆ `Project24_2_Kafka/pythonProducer/…py.bak` | Dead pre-fix starter using a removed API; the deviation is documented in §6 and belongs in prose, not in a `.bak` |
| ◆ `Project24_2_Kafka/kafka-docker/librdkafka.config` | Dead `node-rdkafka` artifact, unreachable after Deviation 2 |
| ◆ `Project24_2_Kafka/kafka-docker/config.js` | Dead Confluent Cloud CLI helper; undeclared `minimist` dependency, never imported |
| ◆ `Project24_2_Kafka/NodeJSConsumer/README.md` | Stale course text describing files that do not exist; rewrite rather than import |
| ◆ `Project_24_Docker/mosquitto/log/mosquitto.log`, `Mosquitto/mosquitto/log/mosquitto.log` | Runtime artifacts |
| `Project_24_Docker/…starter.zip`, `Project_24.2/…starter.zip`, `DockerMosquittoInstall.zip` | Binaries; course-owned starter material |
| `Project_24.1/` (whole folder) | Duplicate working copy of files already mapped above |
| `Mosquitto/`, `Nueva carpeta/` | Superseded duplicate; empty leftover |
| All `*.docx` submissions | Binaries containing unreviewed screenshots (§4.2); Spanish; graded coursework |
| `Project_24.1_part2/*.pdf` (video transcripts) | **Third-party copyrighted course material — must never be published** |
| `Minilección 24.2.docx`, `Minilección 24.3 …docx` | Spanish course notes → `docs/internal/` only if the owner wants them, otherwise excluded |
| `%USERPROFILE%\.mytb-data`, `.mytb-logs` | Docker volume data, 144 MB, contains live credentials |

**✅ Rule chains — no longer blocked (2026-09-26).** What §7.3 previously listed as "not yet mappable" now has source files. The owner exported **four** chains on 2026-09-23 to `its-pipeline-backup/`, verified against the live instance at 1 / 1 / 5 / 9 nodes. Note the count: **four chains, not the three the briefing named** — `Root Rule Chain` is project logic, not stock content, because it holds the third `TbRestApiCallNode`.

| Source file (in `its-pipeline-backup/`) | Chain | Nodes / conns | Proposed repo path |
|---|---|---|---|
| `temptofirebase.json` | `TempToFirebase` | 1 / 0 | `processing/thingsboard/rule-chains/temp-to-firebase.json` |
| `alarmtofirebase.json` | `AlarmToFirebase` | 1 / 0 | `processing/thingsboard/rule-chains/alarm-to-firebase.json` |
| `createandclearalarms.json` | `CreateAndClearAlarms` | 5 / 4 | `processing/thingsboard/rule-chains/create-and-clear-alarms.json` |
| `root_rule_chain.json` | `Root Rule Chain` | 9 / 8 | `processing/thingsboard/rule-chains/root-rule-chain.json` |

⚠️ **None of these may enter the repository as exported.** Three of the four — `root_rule_chain.json`, `temptofirebase.json` and `alarmtofirebase.json` — each contain a `TbRestApiCallNode` whose config holds the **live Firebase database URL**. That is the credential described in §4.1, and exporting has now materialised it into plaintext files on disk. Before any of these is staged for commit:

- **Parameterise all three REST call nodes** — the URL becomes an environment variable or a `config/` value, never a literal. `Root Rule Chain` is included; it is not a pass-through.
- **Keep `its-pipeline-backup/` out of git permanently** (§8.1 #4). It is the unparameterised copy and must stay that way to remain a usable restore point.
- **Rotate the credential at Phase 1 closure** (§8.1 #5) — and note that rotation alone is insufficient, since the database rules were open to anyone until 2026-09-25 (§5.1b, risk 1 in §1).
- **Verify the parameterised exports re-import cleanly** into a fresh ThingsBoard before the repo claims they are deployable. This has not been tested; the exports have been read for node counts only, never re-imported.

`Thermostat` (7 / 6) is **excluded** — stock ThingsBoard demo content, confirmed unused by the coursework (§5.1), so it is not project logic and was correctly not exported.

**Still not mappable:** the ThingsBoard **dashboards** under `visualization/`. No dashboard export exists, and none was made in Phase 0. If the dashboards are part of what the repo presents, they need the same export-and-parameterise treatment as the rule chains.

---

## 8. Owner decisions and remaining open questions

### 8.1 Decisions recorded (Anthony Dávila Carrillo, 2026-09-13)

| # | Question | Decision |
|---|---|---|
| 1 | Authorise live runs / Firebase writes? | **Authorised.** `TBpublish.py` bounded to 60 s. Rule chains must **not** be disabled, edited or touched; the Firebase writes in that window are accepted. Gated behind STOP 1b. |
| 2 | Run Task 0.5 separately? | **Yes — 0.5 first, then 0.4**, because 24.2 has no cloud side effects. |
| 3 | Docker memory | **Resolved.** WSL cap raised to 12 GB by the owner; verified in effect (§5.0). |
| 4 | Where do rule-chain exports stage? | **`its-pipeline-backup/`**, a sibling of the staging directory. It holds the rule-chain JSON and the `.mytb-data` backup. **It will never become a git repo.** |
| 5 | Is the Firebase secret rotatable? | **Yes — it will be rotated at Phase 1 closure.** The owner will also check whether the Firebase database rules are left open. |
| 6 | ThingsBoard build version | **Read it from the running container at STOP 1b.** |
| 7 | Keep `pahopublish.py` / `pahosubscribe.py`? | **Approved** — they stay, as `scripts/smoke/`, in English. |
| 8 | Author identity | **`Anthony Davila Carrillo <anthoadc-AI@users.noreply.github.com>`.** No personal email anywhere in the repo. |
| 9 | Is `v0-coursework` faithful or cleaned? | **Faithful snapshot**, cleaned in later commits. Original paths and names in v0; only secrets and binaries (`.docx`, screenshots, `.zip`) excluded even from v0. §7 mapping updated accordingly. |
| 10 | Course `.docx` files | **Out of the repository entirely.** |

> **Owner note, 2026-10-01, on decision 5:** closed as moot. The Phase 1 parameterisation of the rule-chain exports found that no Firebase REST node used an `auth=` parameter or stored credentials; the database host was the only sensitive value. There is no secret to rotate. See `docs/internal/phase1_closure_report.md`, sections 7.1 and 9.

Deviation disposition also decided: **3–6 are owner improvements to be claimed in the final README; 7–9 go to the Phase 1 fix list** (§6.1).

### 8.1b Decisions recorded at Phase 0 closure (Anthony Dávila Carrillo, 2026-09-26)

| # | Question | Decision |
|---|---|---|
| 11 | §7.2 target structure | **Replaced by the owner's tree** (§7.2, given verbatim). The governing decision behind it: **the course's two pipelines are v0 legacy, and Phase 1 replaces them with a single industrial condition-monitoring pipeline.** Adds `Makefile`, root `docker-compose.yml` with profiles, `schemas/` (Avro), `bridge/mqtt_to_kafka/`, and `tests/` + `.github/workflows/` reserved for Phase 2. Drops `streaming/` and `deploy/`. |
| 12 | The three §7.1 interpretation points | **All three confirmed as assumed.** (a) `node_modules/` **excluded** from v0 as a binary. (b) `mosquitto.log` **enters v0** and is removed in the cleanup commit. (c) The sibling folders are **outside v0**; `pahopublish.py` / `pahosubscribe.py` enter **later**, in English, under `scripts/smoke/` — not in v0 at their original root paths. This closes open question 1 of §8.2. |
| 13 | §7.3 mapping table | **Approved** — as the v0 → cleanup record. It describes the legacy path, not the Phase 1 target; see the consequence note at the end of §7.2. |
| 14 | Alarm consumer language | **Python**, replacing the course's `kafkajs` consumer. `processing/alarm_consumer/` carries threshold evaluation with persistence and hysteresis plus a dead-letter topic. |
| 15 | The Express web UI | **Enters v0, removed in cleanup.** It has no place in the Phase 1 tree — it belonged to the vehicle demo Phase 1 retires. `visualization/` holds ThingsBoard dashboards only. |

⚠️ **Two items these decisions leave outstanding**, neither of which they resolve:

- **The four rule-chain exports have never been re-imported.** §7.2 now requires "4 JSON with placeholders + import script", so the parameterise-and-re-import round trip is Phase 1 work that has no evidence behind it yet (§7.3).
- **The dashboards have no source file.** `visualization/thingsboard/dashboards/` is specified but nothing was ever exported to fill it. It needs the same export-and-parameterise treatment as the rule chains.

### 8.2 Still open — carried into STOP 2

1. ~~**The three §7.1 interpretation points**, which the faithful-snapshot rule does not settle: whether `node_modules/` is excluded as a "binary" (I assume yes), whether `mosquitto.log` enters v0 (I assume yes, literal reading), and whether the two scratch scripts enter v0 at their original root paths or only later in English (I assume later).~~ → **Closed 2026-09-26 by decision 12** (§8.1b): all three confirmed as assumed.
2. **Do the seven unused Confluent services survive Phase 1?** Runtime evidence (§5.2) shows only `zookeeper` and `broker` carry coursework traffic. Dropping the rest cuts ~4.38 GiB to ~0.8 GiB, at the cost of losing Control Center as a teaching visual. This is the single biggest simplification available in Phase 1.
3. **Is the concurrent Airflow + Hadoop load acceptable during Task 0.4?** They consume ≈4.65 GiB of the 12 GB, more than the Confluent stack itself, leaving ~2.6 GiB headroom for ThingsBoard. I have not touched them — they belong to other modules. Say the word if they should be stopped before 0.4.
4. **`ksqldb-server` phones home to `version-check.confluent.io`.** Harmless here (DNS failed), but a public repo shipping a stack with telemetry enabled deserves either a disabling env var or a note in the README.
5. **Should the Phase 1 producer gain a `flush()`/`close()` and a bounded message count?** The current script loses buffered messages on any abrupt stop (§5.2) and cannot be run for a fixed count without editing. Both are small fixes that make the demo reproducible.

**Closures recorded 2026-09-30, at the start of Phase 1 Task 1.0 (Phase 1 briefing v1.0, §2):**

- **Q2 → closed.** Confluent stack trimmed to `zookeeper`, `broker`, `schema-registry`.
- **Q3 → closed.** Airflow and Hadoop containers are stopped (`docker stop`, never `down -v`) as a standing precondition before any stack is brought up (Phase 1 hard rule 6).
- **Q4 → closed.** `ksqldb-server` is dropped; the surviving `broker` gets `KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE=false` so the stack does not phone home.
- **Q5 → closed.** Bounded runs (`--count`, `--duration`), `SIGINT`/`SIGTERM` handling and flush-on-exit are requirements of Phase 1 Tasks 1.5 and 1.6.

---

*Phase 0 CLOSED by owner, 2026-09-26.*

*Closing state: Tasks 0.1–0.6 complete. Task 0.4 closed at STOP 2 on 2026-09-23 with one deviation — the `TBpublish.py` run overran its 60-second authorisation and published for 137.6 s (§5.1b). The Firebase leg was verified by the owner in the console on 2026-09-26, with the write count unverifiable because `TempToFirebase` uses PUT; the database is now locked, its test-mode rules having expired on 2026-09-25. §7 decisions recorded at §8.1b. **Two outstanding items carry into Phase 1: the 12 Airflow/Hadoop containers remain stopped and await `docker start`, and open questions 2–5 of §8.2 were never answered.***

*Integrity statement, re-verified at STOP 2 on 2026-09-23 after the live `TBpublish.py` run: **zero files inside `Project_24_Docker`, `Project_24_MQTT` or `Project24_2_Kafka` were created, modified, renamed or deleted** — 0 files carry today's date, no `__pycache__` or `.pyc` was produced, and the latest mtimes remain 2026-08-26 21:19 / 2026-08-26 10:44 / 2026-08-27 12:43 as recorded before any container started. `Project_24_Docker/mosquitto/log/mosquitto.log` is still 505 B; Mosquitto ran from a byte-identical scratchpad copy so its log writes landed outside the course tree. No git repository was initialised. No `pip install`, `npm install`, formatter or linter was run. Both the Confluent and the ThingsBoard/Mosquitto stacks were stopped with `docker compose down` (**no `-v`**); the Docker volume count was 87 before and after, no named volume was created or destroyed. `~/.mytb-data` was written only by ThingsBoard itself — 149,645,786 B before and after shutdown, byte-identical, with the 46 new telemetry points intact. **No rule chain was viewed, edited, disabled or touched.** The only file created in the course tree is this report, under `its-pipeline-staging/`; the rule-chain exports are the owner's, in `its-pipeline-backup/` outside the staging tree. Other writes went to the session scratchpad, outside the course tree.*
