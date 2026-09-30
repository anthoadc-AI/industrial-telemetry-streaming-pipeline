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

### 7.2 Phase 1 target structure (layered)

```
industrial-telemetry-streaming-pipeline/
├── README.md                     # public entry point, English, architecture diagram
├── .gitignore                    # node_modules, __pycache__, *.log, .env, mosquitto/data, .mytb-*
├── .env.example                  # every variable, public defaults only, no live values
├── config/                       # OWNER CONSTRAINT: single source of truth for thresholds/constants
├── edge/                         # data producers — simulated sensors and vehicles
│   ├── mqtt-temperature/         #   Paho publisher → Mosquitto/ThingsBoard
│   └── kafka-vehicle-gps/        #   Python producer → Kafka
├── broker/                       # message transport: Mosquitto and Kafka configuration
│   ├── mosquitto/
│   └── kafka/
├── streaming/                    # Kafka topic definitions and consumer group settings
├── processing/                   # where telemetry becomes decisions
│   ├── thingsboard/rule-chains/  #   the three exported chains, PARAMETERISED
│   └── kafka-consumer/           #   kafkajs consumer
├── visualization/                # ThingsBoard dashboards + the Express web UI
├── sinks/
│   └── firebase/                 # OWNER CONSTRAINT: optional, disabled by config
├── deploy/                       # the unified docker-compose + per-stack overrides
├── scripts/                      # smoke tests and probes
└── docs/
    ├── architecture/             # public, English
    └── internal/                 # OWNER CONSTRAINT: Spanish working docs + briefings
```

**One-line justification per top-level directory**

| Directory | Why it exists |
|---|---|
| `config/` | Owner constraint. Thresholds (alarm limits) currently live inside rule chain nodes; this is where they move. |
| `edge/` | Both pipelines begin with a simulated sensor; grouping them makes the shared pattern visible and the two course projects stop looking unrelated. |
| `broker/` | Mosquitto and Kafka play the identical architectural role; co-locating them is the clearest argument the repo makes. |
| `streaming/` | Topic and consumer-group configuration is neither broker infrastructure nor application logic. |
| `processing/` | The layer that carries the actual engineering — rule chains and the consumer. Today it is the layer with no files at all. |
| `visualization/` | Dashboards and the Express UI are the presentation tier; separating them keeps `processing/` honest. |
| `sinks/` | Owner constraint: Firebase must be isolable. A sink directory makes "optional" structural rather than a comment. |
| `deploy/` | One compose file per the Phase 1 goal, kept away from source. |
| `scripts/` | The connect-only probes and smoke tests, so verification is repeatable rather than retyped. |
| `docs/` | Public English under `architecture/`, Spanish working material under `internal/`. |

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
