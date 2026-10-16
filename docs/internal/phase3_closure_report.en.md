# Phase 3 Closure Report — Quantified Evidence

| Field | Value |
|---|---|
| Project | industrial-telemetry-streaming-pipeline |
| Phase | 3, the last of four (0 to 3), Quantified evidence |
| Briefings | `docs/internal/claude_code_briefings/phase3_briefing_quantified_evidence.md` (phase) and the task briefings in the same folder (3.3 STEP 0 to 2, 3.4, 3.5, the H3 fix, 3.6) |
| Work dates | 2026-10-02 to 2026-10-15 |
| Machine | Windows 11 Home 10.0.26200, Docker Desktop (Engine 29.0.1, WSL 2, 8 vCPUs and 12 GB for the VM), Python 3.13.4, Node.js 24.16.0, GNU Make 3.81 (GnuWin32) |
| CI | GitHub Actions, `ubuntu-24.04`, four jobs |
| Status | **Ready for review (DETENTE 3.6.b).** Branch `chore/phase3-publication`, not merged, not tagged. The repository is still private. |

The Spanish version of this report is `docs/internal/phase3_closure_report.md`; the two say the same, with identical figures, hashes and paths. All data produced in this phase is synthetic.

---

## 1. Summary

Phase 3 turned a tested but invisible pipeline into evidence a reviewer can check. At closure:

- **Operations layer.** A ThingsBoard sink forwards telemetry and alarm events to ThingsBoard through its gateway MQTT API; an operations dashboard, rule chains and devices are created by `make up` with no manual step. ThingsBoard evaluates no threshold: the Node.js alarm consumer is the only alarm engine.
- **Benchmark.** A harness under `bench/` with its configuration in `config/benchmark.yaml`; every run is kept in `bench/results/` and `make bench-report` regenerates every published figure. Sustained throughput 2000 msg/s, confirmed 5 of 5; latencies in section 4.
- **Documentation.** `docs/ARCHITECTURE.md` (14 sections, three Mermaid diagrams, kept equal to `config/` and compose by a unit test), `README.md` and `docs/CASE_STUDY.md`, whose figures, links and line citations are checked by unit tests.
- **Defects found and fixed.** The benchmark and the reviews found product defects that tests had not: Mosquitto dropping acknowledged messages, a healthcheck loading the broker, Docker Desktop killing containers after 1 s, and two alarm-replay defects. Each was fixed with a test that fails on the old code (section 3).
- **Publication readiness.** A history-wide scan found no secret; local paths and other projects' names were removed from the tree; `make install` and `make test` pass on a fresh clone.

The phase did not measure the path into ThingsBoard (F5, F6, F7) and did not lock the Python dependency tree; both are in the backlog (section 6).

---

## 2. What was delivered, by task

| Task | Delivered | Merge on `main` |
|---|---|---|
| 3.0 | Read-only state verification of the phase (DETENTE 3.0.a), with an erratum to the 3.1-B paragraph of the briefing | — |
| 3.1-A | The bridge's acknowledgement time is taken on a dedicated delivery-report thread, not when the main loop polls; `ts_to_ack_ms` removed and replaced by four per-stage fields (`event_to_receive_ms`, `receive_to_produce_ms`, `produce_to_ack_ms`, `event_to_ack_ms`) and `receive_ts_ms`; the alarm consumer logs `consume_to_alarm_ack_ms` | `9b6df92` |
| 3.1-B | `prune_firebase()`: with Firebase disabled the import removes the three REST nodes, the two chains left empty and the input nodes that reached them, idempotent in both directions | `cc575b5` |
| (3.1) | A deterministic compaction test and per-test process isolation in the integration suite | `f28516f` |
| 3.2 | ThingsBoard sink (Python): gateway MQTT API, one device per asset, alarm events mapped by a rule chain (AlarmMapping) keyed by `alarm_id`, dashboard as code, alarm reconciliation at `make up`; then the PipelineSequential queue, SQL batch delays of 25 ms and a sink limit of 10 msg/s per asset after two red runs on `main` | `47f0a78`, `7c690e8`, `832ab50` |
| 3.3 | The pipeline in containers (`make up-pipeline`, `make smoke-containers`, a CI job of its own); the benchmark harness, its runs and `make bench-report`; three product fixes found by the benchmark | `53a53a0` |
| 3.4 | `docs/ARCHITECTURE.md` and `tests/unit/test_architecture_doc.py`; the review found the H3 defect | `a69859b` |
| H3 fix | A replay guard by the timestamp of each alarm's latest event; fixes H3 and the phantom alarm found with it | `c576f2b` |
| 3.5 | `README.md`, `docs/CASE_STUDY.md` and `tests/unit/test_public_docs.py` | `f21c941` |
| (3.5) | A race between an alarm and its `alarm_last` in the full-profile tests, after a red run on `main` | `0604a08` |
| 3.6 | Audit of STEP 0; local paths and names removed; a `drive-path` rule in `scripts/check_secrets.py`; line citations checked by content; the workflow sentence in the README and the case study; this report | branch `chore/phase3-publication` |

Git history: 140 commits on `main` since `v2-engineering-quality` (2026-10-02), 14 of them `--no-ff` merges, none committed to `main` directly; 8 more on the Task 3.6 branch, this report included. The pre-commit hook runs the secret scan on every commit.

---

## 3. Findings (problem, action, result)

**The acknowledgement time included the polling delay (3.1-A).**
- *Problem.* `ts_to_ack_ms` was taken in the delivery callback, which the bridge served only when its main loop polled the producer, after up to 100 ms of waiting on its inbox. At the smoke rate, ack minus produce had a median of 106 ms.
- *Action.* `Message.latency()` was measured and rejected: in confluent-kafka 2.15.1 it reported 60.4, 100.5 and 300.4 ms with `poll()` delayed by 0, 100 and 300 ms. A dedicated thread blocked in `poll()` was chosen (`fe37d8f`).
- *Result.* With the main thread busy 0, 100 and 300 ms, p50 63.7, 56.2 and 57.5 ms. A unit test fails on the Phase 2 design (110.8 ms against a 20 ms fake broker).

**Rule chains with an unresolved Firebase URL (3.1-B).**
- *Problem.* With `FIREBASE_ENABLED=false` the import left three REST nodes whose URL was the `${FIREBASE_DB_URL}` placeholder; every message would have run a failing REST call.
- *Action.* `prune_firebase()` (`609cf0b`).
- *Result.* On the committed exports: 3 REST nodes and 2 input nodes removed, `TempToFirebase` and `AlarmToFirebase` not imported; the full-profile tests assert the pruned state.

**A compaction test that depended on chance.**
- *Problem.* `test_restore_after_real_compaction` failed in CI run 37070295558: the broker evaluates `segment.ms` only when it appends to a partition, and the cleaner never compacts the active segment.
- *Action.* `roll_every_partition()` writes one tombstone with a reserved key to every partition; every process a test starts is stopped after the test (`5b28fd1`).
- *Result.* The compaction is still the broker's own and the test is deterministic; the behaviour is documented in `docs/ARCHITECTURE.md`, section 5.

**ThingsBoard dropped and reordered alarm events (3.2).**
- *Problem.* With the default partitioner an asset's alarms spread over partitions: 19 of 48 alarms kept after a replay. ThingsBoard's SequentialByOriginator queue (2 s per batch, 3 retries) dropped alarm events (run 37227357007: 43 of 48 alarms, 108 messages dropped). Devices created by a client other than the sink went to the parallel Main queue (41 of 200 alarms applied out of order, run 37215619234).
- *Action.* Alarm events partitioned by asset (`AssetPartitioner`); the PipelineSequential queue, SQL batch delays of 25 ms and a sink limit of 10 msg/s per asset; the import creates each asset's device with its type.
- *Result.* A backlog of 3,200 messages of one asset arrives with no drop (320 s); a burst of 400 alarm events straight to the gateway, all applied; the full session fails if ThingsBoard dropped any message.

**Mosquitto dropped messages it had acknowledged (3.3).**
- *Problem.* Mosquitto's queue per client held 1040 messages, measured; a bridge that fell behind for under a second lost messages while the publisher got its PUBACK. Diagnostic at 3000 msg/s: 61,140 dropped, exactly the number missing from `telemetry.raw`.
- *Action.* A queue of 60,000 messages and 25 MiB (`baef69b`), a drop monitor in the bridge (`5c225e0`), two integration tests (`97ecefe`), and a run with a Mosquitto drop is invalid (`61c009f`).
- *Result.* A paused bridge received 1,040 of 5,000 messages before and 5,000 of 5,000 after; no run of the final series had a drop.

**A healthcheck loaded the broker it was checking (3.3).**
- *Problem.* `cub kafka-ready` and `cub zk-ready` started a JVM every ~11 s on the broker's pinned vCPUs.
- *Action.* Healthchecks without a JVM (`cd711fc`); the cp-server telemetry reporter and Self-Balancing off (`7825d3e`).
- *Result.* Idle broker 18.0 % to 5.9 % of a core on average; samples at or above 50 %, 17 to 1.

**Docker Desktop stopped containers after 1 s (3.3).**
- *Problem.* Docker Desktop's API proxy created every container with a `StopTimeout` of 1 s; the alarm consumer needs 1.5 to 3.6 s to stop.
- *Action.* An explicit `stop_grace_period` for every service (`7386596`, `da7a728`).
- *Result.* Every service exits 0 or 143, none by SIGKILL, except ThingsBoard, whose start script ignores SIGTERM (backlog).

**A restart could clear an alarm still active, or raise a phantom one (3.4, H3 fix).**
- *Problem.* After a stop between an alarm send and its offset commit, replayed samples cleared an alarm restored as active (H3), or raised an already-cleared alarm again under a new id.
- *Action.* A replay guard: each alarm ignores samples stamped at or before its latest event; the restore returns the latest event per alarm type, CLEARED included (`4b87a8d`).
- *Result.* `test_replay_before_the_raise_does_not_clear_the_restored_alarm`, `test_replay_of_a_clear_does_not_raise_a_phantom_alarm` and `test_after_the_replay_thingsboard_keeps_the_alarm_active_until_the_real_clear` fail on the code before the fix and pass after it.

**A full-profile test read `alarm_last` too early (after 3.5).**
- *Problem.* CI run 38486097882 on `f21c941`: AlarmMapping creates the alarm before it saves `alarm_last`, and a test read it once; its alarm then failed the next test.
- *Action.* `alarm_last_when` polls with a timeout; a failed test clears the alarms it raised (`06ec32c`).
- *Result.* Run 38494698202 on the branch and run 38496923949 on `main` (`0604a08`) passed all four jobs.

**Local paths in tracked files (3.6).**
- *Problem.* The audit found the working tree's absolute path in `bench/results/series-20261012T153319Z.log` (lines 73 and 77) and the course folder's in `docs/internal/phase0_diagnostic_report.md` (line 7). `check_secrets.py` caught only paths into a user profile.
- *Action.* Both replaced on their own lines (`4fe3d42`); a `drive-path` rule (`f1ae378`).
- *Result.* The rule flags both earlier versions and no tracked file: 957 files scanned, 0 hits, 17 allowed. The earlier versions remain in the history, which is not rewritten.

---

## 4. Final figures and their source

Benchmark conditions for every figure in this table: one laptop, Docker Desktop on WSL 2 with 8 vCPUs, services pinned to fixed vCPUs, one Kafka broker with replication factor 1 on tmpfs, synthetic messages of about 218 B, 300 s windows, five runs per row; median and, in brackets, the range across runs, whole milliseconds.

| Figure | Value | Source |
|---|---|---|
| F1, sample to record read from Kafka, 50 msg/s | p50 7 (7-8), p95 11 (11-12), p99 13 (12-13) ms; 15000 samples per run | `make bench-report`; `bench/results/` |
| F2, sample to alarm event acknowledged by Kafka, 50 msg/s | p50 35 (34-36), p95 55 (52-56) ms; 300 samples per run, no p99 | same |
| F1 at 2000 msg/s | p50 6 (6-6), p95 11 (10-16), p99 26 (19-59) ms; 600000 samples per run | same |
| F3, sustained throughput | 2000 msg/s, confirmed in 5 of 5 runs; 3000 msg/s not measurable validly (generator at its CPU limit) | same |
| Unit tests, `main` at `0604a08` | 459 Python, 119 Node.js, all passed | CI run 38496923949 |
| Python coverage | "Total coverage: 98.54%"; "845/874 branches, 96.68 %" | same |
| Node.js coverage | lines 100 % (1159/1159), branches 98.7 % (305/309) | same |
| Integration tests | core stack 16 passed, 16 skipped; full stack 32 passed | same |
| Fresh clone of `main` on Windows (2026-10-15) | `make install` 84 s, rc 0; `make test` 42 s, rc 0: 458 passed, 1 skipped (symbolic links), 119 Node.js | Task 3.6 STEP 0, pip and npm caches off |
| Mosquitto fix | 1,040 of 5,000 messages before, 5,000 of 5,000 after | `docs/internal/phase3_task3.3_closure_report.en.md`, 2.4 |
| Healthcheck fix | idle broker 18.0 % to 5.9 % of a core | same, 2.5 |
| ThingsBoard path | 10 msg/s per asset by configuration; 3,200 messages of one asset in 320 s, no drop | `config/thingsboard.yaml`; `test_a_backlog_of_one_asset_reaches_thingsboard_without_a_drop` |
| History secret scan | 1,266 file versions and every commit and tag message from all refs: 0 secrets; every secret-type match in `docs/internal/` and covered by the allow list | Task 3.6 STEP 0 |
| Repository size | pack 2.41 MiB; 957 tracked files, 633 in `bench/results/` | `git count-objects -vH`, Task 3.6 STEP 0 |

The CI figures on the Task 3.6 branch are in section 7.

The coverage figures were read from each run's log, as printed. Runs 38472910691 (`c576f2b`, the run the README cites), 38496923949 (`0604a08`) and 38829861901 (`8b1caf7`) print the same values: "Total coverage: 98.54%", "845/874 branches, 96.68 %", Node.js lines 100 % (1159/1159) and branches 98.7 % (305/309). Their unit test counts differ: 447, 459 and 467 Python; 119 Node.js in all three.

---

## 5. Deviations from the briefings

Deviations within Task 3.3 are listed in section 6 of `docs/internal/phase3_task3.3_closure_report.en.md` and are not repeated here.

| # | Briefing says | Done | Decision |
|---|---|---|---|
| 1 | 3.1-A: evaluate `Message.latency()` as option (c) | Measured and rejected; a dedicated delivery-report thread | Owner, DETENTE 3.1.a (`fe37d8f`) |
| 2 | 3.3: `benchmarks/` and `make benchmark` | `bench/` with `make bench-trial`, `make bench-plan` and `make bench-report` | Task 3.3 STEP 1 briefing (`bench/results/`) |
| 3 | 3.3 STEP 2: no change to the pipeline, the harness or `config/` during the runs, except an approved fix | Four product changes in the middle of STEP 2 (Mosquitto's queue, the drop monitor, the healthchecks, cp-server), then a full repeated series | Owner, DETENTE 3.3.h and 3.3.i |
| 4 | 3.3: figures for the path into ThingsBoard | F5 and F6 deferred, F7 not measurable while ThingsBoard ignores SIGTERM; out of the README | Owner, DETENTE 3.3.d and 3.3.g |
| 5 | 3.3: the throughput ladder ends at the first step that violates the criteria | It ends at 3000 msg/s because the generator reached its CPU limit, not the pipeline's criteria; no step between 2000 and 3000 | Owner, rule changed after seeing data (Task 3.3 report, section 3) |
| 6 | 3.6: a second secret-scanning tool such as gitleaks | In STEP 0, `check_secrets.py`'s patterns plus extra ones (tokens of common services, URLs with credentials, emails, paths, machine names) over every blob and message, by a one-off script not committed; gitleaks run once at the owner's request before approving DETENTE 3.6.b (section 7) | Task 3.6 briefing, STEP 0; owner, before DETENTE 3.6.b |
| 7 | 3.6: `pip-audit` and `npm audit` | Run at the owner's request before approving DETENTE 3.6.b (section 7); nothing changed | Owner, before DETENTE 3.6.b |
| 8 | 3.6: branch protection on `main` requiring CI | An owner action after publication (section 7) | Owner, before DETENTE 3.6.b |
| 9 | 3.6: clean machine with `make up`, `make test-integration`, `make smoke` and the README quickstart | `make install` and `make test` on a fresh clone; the stack, the integration tests and the smoke run on CI's clean runners; the quickstart was followed in Task 3.5 on an existing `.venv` | Task 3.6 briefing, step 5 |
| 10 | 3.6: bilingual closure reports for every phase, under `docs/reports/`, with a test comparing each pair | Not done: Phases 0 to 2 are in English only, under `docs/internal/`; only Task 3.3 and this report are in both languages; `docs/reports/` does not exist; no pair test | Owner, DETENTE 3.6.a: recorded as a deviation, `docs/reports/` not created |
| 11 | 3.6 (backlog): lock the Python dependency tree | Not done | Backlog (section 6) |
| 12 | 3.6 STEP 0: the first fresh-clone attempt | Failed on Windows' path length in the session's temporary folder, not in the repository; repeated in a short folder outside the work tree | — |

---

## 6. Deliberately not done, and the backlog

Open at closure. The source of each is `docs/internal/phase3_backlog.md` unless stated; the user-facing list is section 13 of `docs/ARCHITECTURE.md`.

- **ThingsBoard figures F5, F6 and F7**, and **a SIGTERM wrapper for ThingsBoard**, which F7 needs.
- **The bridge's per-message log line at DEBUG with a periodic INFO summary.** Measured effect recorded; every published figure would have to be re-measured.
- **Lock the Python dependency tree** (Task 3.6 item, not done).
- **Bound the fixed 65 s waits of the full profile.**
- **H2:** the bridge's in-memory queue has no bound.
- **H4:** one alarm consumer instance only, not enforced.
- **H5:** the Firebase node writes every message to one path without authentication; never verified against a live database.
- **H11:** the bridge with Schema Registry down after start-up, not verified.
- **`dlq_v1`'s `source` documentation** omits the ThingsBoard sink; to change with the next schema change.
- **The replay guard trusts the producers' clocks**, and **the restart delay of up to `persistence_samples − 1` samples**: documented, no change decided.
- **Persistent Kafka storage** (a volume instead of tmpfs) and **reconciliation only at `make up`**: open option, not decided.
- **Before the bridge receives a message, delivery is at most once** (persistent MQTT session not implemented).
- **Dashboard:** no gap in the charts, no drop counter, a calendar-month failure counter, white space in fullscreen.
- **Phase 2 items still open:** kafkajs maintenance risk (accepted), no security (anonymous MQTT, PLAINTEXT Kafka, no TLS), no range validation.

---

## 7. State at closure

- `main` at `0604a08`, CI green on all four jobs (run 38496923949). The Task 3.6 branch `chore/phase3-publication` is pushed: CI run 38829861901 on `8b1caf7` (the last fix before this report; figures read from its log): passed three jobs (lint and unit tests: 467 Python, 119 Node.js, "Total coverage: 98.54%", "845/874 branches, 96.68 %"; core stack: 16 passed, 16 skipped; pipeline in containers: smoke PASS); the full-stack job runs only on `main` and on a manual run, so it was skipped.
- Tags: `v0-coursework`, `v1-solid-base`, `v2-engineering-quality`. `v3-quantified-evidence` waits for the owner's approval, on the merge of this branch.
- Remote branches: `main`, and the Task 3.6 branch until its merge.
- The repository is private. The owner makes it public, adds the description and topics proposed at DETENTE 3.6.a and checks the CI badge.
- The temporary clone of STEP 0 was created outside the work tree and deleted afterwards.
- **Dependency audit** (2026-10-15, owner request before approving DETENTE 3.6.b; nothing changed):
  - `pip-audit` 2.10.1 in a throwaway venv outside the repository, over `requirements.txt` (22 packages resolved: 4 direct, 18 transitive) and over `requirements.txt` with `requirements-dev.txt`: "No known vulnerabilities found" both times.
  - `npm audit --omit=dev`: "found 0 vulnerabilities" (24 production packages).
  - `npm audit` with dev dependencies: 19 moderate, 0 high, 0 critical, all from one advisory, GHSA-hp3w-g68c-fv3c: `sprintf-js` 1.0.3, denial of service through unbounded precision specifiers, CVSS 5.3. Transitive and dev only, through the coverage instrumentation of the one direct dependency involved, `jest` 30.5.2: `@jest/core`, `@jest/transform`, `babel-plugin-istanbul` 8.0.0, `@istanbuljs/load-nyc-config` 1.1.0, `js-yaml` 3.15.2, `argparse` 1.0.10, `sprintf-js` 1.0.3. The other 18 entries are the packages on that chain. No fixed version exists within the pinned ranges: every published `sprintf-js` (up to 1.1.3) is in the advisory's range, and `argparse` 1.0.10 requires `~1.0.2`; npm's only proposal is `jest` 25.0.0, a major downgrade. The pipeline's own YAML parser is `yaml` 2.9.1, not on the chain.
- **gitleaks** 8.30.1 (release checksum verified; run once outside the repository, not added to it): `gitleaks git --log-opts="--all" --redact` over every branch and tag: 200 commits scanned, "no leaks found". All refs hold 234 commits: 32 merges, which add no content of their own, and 202 others; 3 of those add no lines (`4b9ac39` and `8741514` only delete, `9b61278` only moves files). gitleaks does not say which 2 it left out; not reconciled further.
- **Owner action after publication:** protect `main` on GitHub, requiring the CI checks to pass and allowing no direct push.
