# Phase 2 Backlog

Source: `docs/internal/phase1_closure_report.md`, sections 8.2 and 8.3, copied verbatim at Phase 1 closure (owner, 2026-10-01). The section reference inside the items ("section 4.2") points to that report. Phase 3 items are in its section 8.1.

## Owner-directed (closure report section 8.2)

- **Shared JSON-log package.** `edge/simulator/jsonlog.py` and `bridge/mqtt_to_kafka/jsonlog.py` are identical copies; they become one shared package when the alarm consumer would make a third.
- **Firebase-disabled import.** When `FIREBASE_ENABLED=false`, the import script must detach or skip the Root Rule Chain REST node instead of leaving it failing on every telemetry message.

## From the Phase 1 briefing and from Phase 1 work (closure report section 8.3)

- **Alarm consumer** in Python on `telemetry.raw`: thresholds, persistence and hysteresis from `config/alarms.yaml`, producing `alarm_v1` events to `telemetry.alarms`.

  > **Owner note, 2026-10-01:** the alarm consumer is built in **Node.js** (kafkajs), not Python. This supersedes Phase 0 decision 14 and this item's language. The rest of the item stands: thresholds, persistence and hysteresis from `config/alarms.yaml`, `alarm_v1` events to `telemetry.alarms`. `common/` is approved as a new top-level directory for the shared JSON loggers (`common/jsonlog.py`, `common/jsonlog.js`). Source: `docs/internal/claude_code_briefings/phase2_briefing_engineering_quality.md`, Task 2.1.

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

## Recorded during Phase 2

- **kafkajs maintenance risk** (owner, 2026-10-01). The alarm consumer uses `kafkajs` 2.2.4, the latest release, published 2023-02-27; the project has published nothing since. It is the owner's choice and does not block Phase 2. The maintained alternative, if kafkajs becomes a problem (for example on a future Node.js major), is `@confluentinc/kafka-javascript`, Confluent's client built on librdkafka, the library the Python bridge already uses.
- **kafkajs on Node.js 24: `TimeoutNegativeWarning` at start-up** (found in Task 2.2, 2026-10-01). Every start of the alarm consumer emits one Node.js warning, "-<epoch ms> is a negative number. Timeout duration was set to 1.", from a timeout kafkajs 2.2.4 computes internally. Node.js clamps it to 1 ms; no effect was observed. The consumer routes process warnings through the JSON logger, so stderr stays JSON-only. Another sign of the kafkajs maintenance risk above.
