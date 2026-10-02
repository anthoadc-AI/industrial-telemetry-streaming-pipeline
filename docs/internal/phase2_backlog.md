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

  > **Owner note, 2026-10-02:** moved to **Phase 3**, together with the rest of the Phase 2 briefing's section S5 (alarms from `telemetry.alarms` into ThingsBoard: the "AlarmToThingsBoard" node, a Kafka connector, alarm attributes on the device). The Phase 2 pipeline ends at `telemetry.alarms`. Nothing of S5 was built in Phase 2. Source: DETENTE 2.8.a.
- **Course alarm logic in ThingsBoard.** `CreateAndClearAlarms` still filters on the course payload's `temperature` field (TBEL `msg.temperature < 80`), which the simulator does not send. `config/alarms.yaml` is now the source of thresholds.
- **`ts_to_ack_ms` measurement bias** (section 4.2): serve delivery reports promptly, by polling more often or from a dedicated thread, before the Phase 3 benchmark uses the field. The field names stay as they are.
- **Bridge delivery semantics.** MQTT QoS 1 messages are acknowledged on receipt, before Kafka confirms them; a crash can lose messages queued in the bridge. paho-mqtt 2.x manual acknowledgement after Kafka delivery would close this.
- **First-install healthcheck.** Verify the ThingsBoard healthcheck on an empty volume (needs owner authorisation for `down -v` on the new stack) and time the first-ever `make up`.
- **`KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE`** is set as the briefing required but is not recognised by cp-server 6.1.1; telemetry is off through `confluent.telemetry.enabled = false`. Remove or replace the inert variable.

  > **Closed, 2026-10-02** (owner decision, Task 2.8): removed in `02564b5`; the broker still logs `confluent.telemetry.enabled = false` at start-up.
- **Line endings.** `.gitattributes`, `.gitignore` and `LICENSE` are not in the LF list and check out with CRLF on Windows. Harmless; add them for consistency.

  > **Closed, 2026-10-02** (owner decision, Task 2.8): added to the LF list in `f60672d`; every tracked file now has `text eol=lf`.

## Recorded during Phase 2

- **kafkajs maintenance risk** (owner, 2026-10-01). The alarm consumer uses `kafkajs` 2.2.4, the latest release, published 2023-02-27; the project has published nothing since. It is the owner's choice and does not block Phase 2. The maintained alternative, if kafkajs becomes a problem (for example on a future Node.js major), is `@confluentinc/kafka-javascript`, Confluent's client built on librdkafka, the library the Python bridge already uses.
- **kafkajs on Node.js 24: `TimeoutNegativeWarning` at start-up** (found in Task 2.2, 2026-10-01). Every start of the alarm consumer emits one Node.js warning, "-<epoch ms> is a negative number. Timeout duration was set to 1.", from a timeout kafkajs 2.2.4 computes internally. Node.js clamps it to 1 ms; no effect was observed. The consumer routes process warnings through the JSON logger, so stderr stays JSON-only. Another sign of the kafkajs maintenance risk above.

## Notes for the Phase 2 closure report

- **The 41 first-run ruff findings were style only** (Task 2.6, commit `7802211`, assessed at owner request, DETENTE 2.6.a). None hid a defect:
  - 37 × E501 (line over 130 characters). One is in application code, `config/loader.py` line 283, the allowed-keys call `_keys(...)` in `_parse_telemetry`; the fix only wraps the argument list, and the key sets are unchanged. The other 36 are test tables and asserts.
  - 2 × E741 (a variable named `l`), in tests; renamed to `d` and `line`.
  - 1 × F401: `import subprocess` in `tests/unit/test_check_secrets.py` was dead; the tests patch `cs.subprocess.run` through the module under test, so they never needed their own import.
  - 1 × F841: `tau = 600.0` in `test_temperature_is_a_first_order_lag` was assigned and never read. The F841 itself was harmless, but the test around it was vacuous.

    > **Correction, 2026-10-02** (Task 2.8, commit `8eaa413`). This note said "the test is still correct". It was not: the test checked nothing. `AssetModel` starts at the steady state of its own load, and the test built it already at full load, so the start temperature was the target (65.0 = 25 + 40) and the gap was zero; the assertion held for any time constant and any lag formula. Fixed in `8eaa413`: cold start at ambient, time constant, rise and ambient read from `config/telemetry.yaml`. With temporary mutations, the old test passed and the new one failed when the model ignored the configured time constant and when the lag ran at twice the rate. Goes into the closure report as a defect found by the test suite.
- Already agreed for this report, recorded here so they are not lost: the two defects the unit tests found in Task 2.4 (`7a29126` fix(config), `8913426` fix(processing)); the first part of the Task 2.5 report; the `make down` behaviour with profiles (a plain `down` leaves ThingsBoard running, Compose v2.40.3).
- **Missing `.env`** (owner, 2026-10-02, DETENTE 2.8.a): the Phase 2 briefing, Task 2.8 step 6, asks a checkout without `.env` to error. It does not, and that is the intended behaviour: the Makefile reads `.env` with `-include`, compose has a default for every setting, and the only step that needs a value with no default, the ThingsBoard import (tenant credentials), stops with `settings_missing`. Not changed; recorded as a deviation in the closure report.
