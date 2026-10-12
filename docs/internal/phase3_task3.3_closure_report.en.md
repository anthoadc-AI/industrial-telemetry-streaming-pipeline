# Task 3.3: closure report (pipeline benchmark)

Status: ready for review. Branch `feature/phase3-benchmark`, not merged into `main`, not tagged. The README is not written.

The figures in section 1 are the output of `make bench-report` (`python -m bench.report`) at commit `b538247`, copied verbatim by a script; none was typed by hand. Figures elsewhere cite their source: a commit, a document in `docs/internal/` or a result in `bench/results/`.

## 1. F1 to F4 and F3 (final configuration)

Command: `make bench-report`. Percentiles are in whole milliseconds rounded up, as the median and the range across repetitions; "n per run" is the number of samples in each run.

### Final configuration (DETENTE 3.3.i)

The tables below use only the 23 runs on the final configuration (harness revision "final configuration (after 3.3.h and 3.3.i)"), made at commit(s) 8d5abfc, d53be71, e18d48e, eff697c, none with uncommitted changes. It differs from the earlier runs in Mosquitto's queue per client (60000 messages / 25 MiB instead of about 1040), the bridge's drop monitor, the broker and ZooKeeper healthchecks (no JVM; every earlier run had one running inside the broker's pinned vCPUs every ~11 s) and the cp-server telemetry reporter and Self-Balancing turned off. A run on it is valid only if Mosquitto dropped no message.

Earlier runs, kept as history in the Runs table and not in the tables below: latency 5/s: 5; latency 50/s: 5; throughput 250/s: 1; throughput 500/s: 1; throughput 1000/s: 1; throughput 2000/s: 8; throughput 3000/s: 2; throughput (diagnostic) 3000/s: 2.

### Repetitions (median and range across valid runs, whole ms)

| Mode | Rate/s | Bridge log | Revision | Runs | Figure | p50 | p95 | p99 | n per run |
|---|---|---|---|---|---|---|---|---|---|
| latency | 5 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F1 e2e | 8 (8-8) | 13 (12-13) | - | 1500 |
| latency | 5 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | ingest | 2 (2-2) | 4 (4-4) | - | 1500 |
| latency | 5 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | produce_to_visible | 7 (7-7) | 10 (10-10) | - | 1500 |
| latency | 5 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F2 alarm | 34 (34-39) | 55 (48-58) | - | 300 |
| latency | 5 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F4 produce-to-ack | 7 (7-7) | 9 (8-9) | - | 1500 |
| latency | 50 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F1 e2e | 7 (7-8) | 11 (11-12) | 13 (12-13) | 15000 |
| latency | 50 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | ingest | 1 (1-1) | 4 (4-4) | 4 (4-4) | 15000 |
| latency | 50 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | produce_to_visible | 7 (7-7) | 9 (9-9) | 10 (10-10) | 15000 |
| latency | 50 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F2 alarm | 35 (34-36) | 55 (52-56) | - | 300 |
| latency | 50 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F4 produce-to-ack | 6 (6-6) | 8 (8-8) | 8 (8-8) | 15000 |
| throughput | 2000 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F1 e2e | 6 (6-6) | 11 (10-16) | 26 (19-59) | 600000 |
| throughput | 2000 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | ingest | 2 (2-2) | 5 (4-8) | 18 (11-52) | 600000 |
| throughput | 2000 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | produce_to_visible | 5 (5-5) | 8 (8-8) | 10 (9-11) | 600000 |
| throughput | 2000 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F2 alarm | 28 (24-30) | 46 (37-48) | - | 6000 |
| throughput | 2000 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F4 produce-to-ack | 5 (5-5) | 8 (8-10) | 18 (12-28) | 600000-600001 |

### Throughput ladder (F3, D8; bridge at the product default log level)

- 250/s: sustainable
- 500/s: sustainable
- 1000/s: sustainable
- 2000/s: sustainable
- 3000/s: not sustainable - CPU-bound instrument(s): generator at 94 % of a core

Candidate: 2000/s; runs 5 of the 5 E2 needs; confirmed.

E2 attempts (6, all listed; an attempt invalid only because an instrument was at its limit is replaced by its valid repeat, owner DETENTE 3.3.g):
- 20261012T172558Z-throughput-2000-r1 (r1): valid, sustainable
- 20261012T175245Z-throughput-2000-r2 (r2): valid, sustainable
- 20261012T180130Z-throughput-2000-r3 (r3): invalid because of the instrument (CPU-bound instrument(s): generator at 102 % of a core; generator saturated (lateness p99 2.0 ms, max 272.2129821777344 ms)); replaced by its repeat 20261012T181010Z-throughput-2000-r3; bridge CPU peak 105 % of a core, Mosquitto dropped 0
- 20261012T181010Z-throughput-2000-r3 (r3): valid, sustainable
- 20261012T181850Z-throughput-2000-r4 (r4): valid, sustainable
- 20261012T182731Z-throughput-2000-r5 (r5): valid, sustainable
Limited by the measuring tools: the step above the candidate, 3000/s, could not be measured validly (an instrument at its limit); the ladder ends there, with no intermediate step.

### Log-level sensitivity (same steps, bridge at the sensitivity log level)

| Rate/s | Log level | Runs | Sustainable | F1 e2e p50/p95/p99 ms |
|---|---|---|---|---|
| 2000 | INFO | 5 | 5 of 5 | 6 / 12 / 29 (n=3000000) |
| 2000 | WARNING | 1 | 1 of 1 | 5 / 8 / 12 (n=600000) |

The product-default side of each row uses the runs E2 counts at that step (an attempt replaced by its repeat is left out, as in E2; owner, DETENTE 3.3.g).
The WARNING side at 2000/s is a single run.

**F3 statement, as the report prints it:** "Candidate: 2000/s; runs 5 of the 5 E2 needs; confirmed.", together with "Limited by the measuring tools: the step above the candidate, 3000/s, could not be measured validly (an instrument at its limit); the ladder ends there, with no intermediate step." The same cut at p99 < 100 ms (`make bench-report` with `--p99-ms 100`, an internal figure) gives the same candidate, 2000/s.

**Runs.** The final configuration has 23 runs with results:
- latency: 10, all valid;
- ladder and E2: 11 (8 valid; 3 invalid: 3000/s twice and the first r3 attempt at 2000/s, all because an instrument was at its limit);
- WARNING block: 2 (2000/s valid; the 3000/s diagnostic invalid, the generator at 93% of a core).

There are also 2 WARNING attempts without results (a harness failure, section 2.6). Every run on the final configuration requires Mosquitto to drop no message, and it dropped none in any of them.

## 2. Findings (problem, action, result)

### 2.1 The host-to-VM latency artifact (STEP 0)
- **Problem.** The briefing quoted about 16 ms in the smoke and about 42-44 ms with isolated messages for the bridge on the Windows host. The figures could not be used without knowing where they came from.
- **Action.** A diagnostic matrix in four passes, plus an added experiment (`bimodal.py`, 40 burst cells), to separate six hypotheses (`docs/internal/benchmark_protocol_proposal.md`, 1.3 and 1.4).
- **Result.** Bursts from the host fall, depending on the connection, into a slow mode with a round trip of about 45 ms: 17 of 32 burst cells on the host and 0 of 8 burst cells in the container, both in the same experiment (`bimodal-ac`; protocol, tables in 1.3 and 1.4). The "0 of 13" of the STEP 0 report counts every container cell, including the 5 of pass 4, which are not all bursts (protocol, 1.3: "5 in pass 4 and 8 in bimodal-ac"). Isolated messages stayed at 14.6-16.9 ms p50; the 42-44 ms did not reproduce. The location is confirmed (the host-to-VM path of Docker Desktop's port forwarding); the mechanism is not identified. Decision D1, option (A), with amendment E1 (the bridge, the consumer and the sink in containers, as a compose profile): the whole benchmark runs in containers inside the VM, so no figure crosses that path.

### 2.2 Docker Desktop's StopTimeout
- **Problem.** Docker Desktop 4.53 created this project's containers, and any other's, with a `Config.StopTimeout` of 1 s (`docker inspect`, 2026-10-09). No file in the repository sets it, and the engine's own default is 10 s. The alarm consumer needed 1.5 to 3.6 s to disconnect and was killed mid-stop; the bridge flushes its producer for up to 10 s. On Linux CI the same stop waits 10 s, so shutdown behaved differently in the two places.
- **Origin, identified at DETENTE 3.3.c (2026-10-10; message of `da7a728`):** Docker Desktop's API proxy. Shown by sending the same container-create request, with no StopTimeout, from a probe container to the two sockets of the Docker Desktop VM:
  - through `docker.raw.sock` (dockerd itself), the container got StopTimeout null, the engine default;
  - through `docker.sock` (the proxy, which the CLI and Compose use), it got 1;
  - a plain `docker run` and a minimal compose project outside the repository also got 1, and `--stop-timeout 7` was honoured.

  Why the proxy chooses 1 was not identified.
- **Action.**
  - `stop_grace_period: 30s` on the pipeline containers (`7386596`);
  - an explicit value for every service (`da7a728`): broker 30 s; ZooKeeper and Schema Registry 10 s; Mosquitto 5 s; ThingsBoard 5 s (section 2.3).
- **Result.**
  - Verified after `make up` (full profile) and `make down`: Mosquitto exited 0, and Schema Registry, the broker and ZooKeeper 143 (the JVM's normal exit on SIGTERM); none by SIGKILL (`da7a728`).
  - The pipeline's stops are clean: `tests/integration/test_containers.py` requires `run_finished` with exact totals and exit status 0.
  - Before, with a 60 s grace period for ThingsBoard, `make down` took 61.2 s, the whole grace period, because ThingsBoard ignores SIGTERM.
  - Now, the `docker compose down` over every profile at the end of the three complete local sessions of each profile on 2026-10-12 took 5.0-5.8 s (core) and 6.1-21.4 s (full), per pytest's "integration stack" summary. The 21.4 s case was not investigated.

### 2.3 ThingsBoard ignores SIGTERM
- **Problem.** In `thingsboard/tb-postgres:4.2.1.1`, PID 1 is `start-tb.sh`, a bash script with no `trap` that does not forward the signal. Checked on 2026-10-10: SIGTERM, then SIGKILL 60 s later, exit status 137. Postgres runs crash recovery on every start.
- **Action.** A 5 s grace period, documented. Option (b), a wrapper of our own with a `trap`, stays in the backlog, unscheduled.
- **Result.** Not resolved. That is why F7 (drain time after a ThingsBoard outage) was not measured: it needs a controlled stop of ThingsBoard.
- **F5 and F6 are a separate matter:** the owner deferred them by a scope decision on 2026-10-12 (DETENTE 3.3.g), not because of SIGTERM. They stay out of the README, with their backlog item (`b538247`).

### 2.4 Losses in Mosquitto's queue, and the fix
- **Problem.** With Mosquitto's defaults, a bridge that fell behind for under a second lost messages: Mosquitto dropped new ones and still sent the publisher its PUBACK.
  - The documented limit is `max_queued_messages` 1000, with no byte limit. The limit measured in the container (Mosquitto 2.1.2) was **1040 messages per client**: 1040 of 5000 arrived in the control (`97ecefe`), and section 0 of the design note records it.
  - 56,880 and 72,329 messages were lost at 3000 msg/s (block 3).
  - 785 were lost at 2000 msg/s (block 4, r2). **Attributing them to this mechanism is an inference:** r2 has no `$SYS` data and no Mosquitto log (both are saved from `141a259` on); only the pattern matches.
- **Action.** A diagnostic at 3000 msg/s with the full window (`0dcf77b`) confirmed the mechanism: 61,140 dropped per `$SYS/broker/publish/messages/dropped`, exactly the 61,140 missing from `telemetry.raw`. The fix, approved at DETENTE 3.3.h:
  - a queue of 60,000 messages and 25 MiB, with its sizing rule (`baef69b`);
  - a drop monitor in the bridge, on its own connection (`5c225e0`);
  - a pause test in CI and an overflow test in `integration-full` (`97ecefe`);
  - a Mosquitto drop makes a run invalid (`61c009f`).
- **Result.**
  - The pause test against the old configuration (control): 1,040 of 5,000 messages arrived, 3,960 dropped. With the fix: 5,000 of 5,000, 0 dropped.
  - Overflow: 65,000 messages; dropped, missing and logged by the bridge all agree (4,960). Mosquitto used 47.5 MiB at the bound.
  - No run in the final series had a drop. At 3000 msg/s the pipeline built up 8.3 s and 14.0 s of lag and lost nothing.
  - Delivery semantics recorded for Task 3.4 (`b772893`).

### 2.5 The JVM healthcheck load
- **Problem.** `cub kafka-ready` and `cub zk-ready` start a JVM on every check, every ~11 s, inside the broker container (on its pinned vCPUs) and the ZooKeeper container. **Every run before the final configuration carried that load.**
- **Action.** Idle comparisons of 300 s, with the harness's own method. Healthchecks without a JVM (`cd711fc`). The cp-server telemetry reporter and Self-Balancing turned off (`7825d3e`).
- **Result** (section 10 of `docs/internal/phase3_task3.3h_mosquitto_queue_design.md`):
  - idle broker: mean 18.0% of a core with `cub kafka-ready` and 5.9% with `nc -z`; samples at or above 50%: 17 against 1, and 15 of those 17 overlap a healthcheck run;
  - with the cp-server features off: mean 19.6% to 15.8%, and internal `_confluent*` topics created from 5 to 1;
  - five consecutive cold starts with the new configuration: all five passed, with no provisioning or start-up error.

### 2.6 The cost of per-message logging
- **Problem.** The bridge writes one `message_delivered` line per message at INFO, the product's default and the level everything is measured with.
- **Action.** A sensitivity block: the bridge at WARNING at 2000 msg/s (comparison) and at 3000 msg/s (diagnostic, not publishable). The first two attempts failed on a harness defect: it waited for an INFO line that a bridge at WARNING never writes. It was fixed by reading Mosquitto's subscription count (`8d5abfc`).
- **Result** (sensitivity table in section 1; bridge CPU from `cpu.csv` over the measured window):
  - at 2000 msg/s, the bridge's mean CPU falls from 81-87% of a core to 62%, and e2e p99 from about 26 ms to 12 ms; **this is a single run at WARNING**;
  - at 3000 msg/s at WARNING, the pipeline kept up (lag 0.12 s, e2e p99 52 ms), while at INFO it fell behind;
  - the product is not changed: INFO stays the default and 2000 msg/s is the published figure; the change is in the backlog (`b538247`).

## 3. Rules changed after seeing data

| Date | Rule | Reason | Commit |
|---|---|---|---|
| 2026-10-11 (DETENTE 3.3.f) | The CPU limit of 0.8 of a core applies to the instruments only (generator and meter). The bridge and the consumer are judged by D8 alone. | In block 3, both runs at 2000 msg/s were invalid only because of "CPU-bound client(s): bridge at 102 % of a core", with the pipeline within D8. A bridge at one core is the system under test at work, not a measurement failure. Recorded in `docs/internal/benchmark_step1_design.md`. | `df684f6` |
| 2026-10-11 | Ladder closing: a step that is invalid with an instrument at its limit ends the ladder ("limited by the measuring tools"), with no intermediate step. | At 3000 msg/s the generator was at 97-98% of a core. F3 closed at 2000 by the owner's decision. | `3d24ec3` |
| 2026-10-11 | E2 counts only the runs of the latest harness revision. Clock window corrected (from the sample before the step). | Runs from before and after a harness change are not mixed. The earlier window caught no negative sample. | `3d24ec3` |
| 2026-10-11 | **F3 reopened.** After r2 lost 785 messages at 2000 msg/s, the owner counted r2 as a failed repetition and reopened where F3 closes (3.3.f had closed it at 2000). | The mechanism was confirmed the same day by the 3000 msg/s diagnostic (`0dcf77b`). F3 was settled by the series on the final configuration: 2000 msg/s, confirmed 5 of 5. | `86edf0b`, `0dcf77b` |
| 2026-10-11/12 (DETENTE 3.3.h) | A run is valid only if Mosquitto dropped no message. | The loss mechanism of section 2.4. | `61c009f` |
| 2026-10-12 (DETENTE 3.3.i) | Only runs on the final configuration feed the published tables. Earlier ones stay as labelled history. | The product changed (queue, monitor, healthchecks, cp-server). | `d53be71` |
| 2026-10-12 (DETENTE 3.3.g) | E2 replacement: an attempt invalid only because an instrument was at its limit (no loss, no Mosquitto drop) is replaced by its valid repeat. Any other invalid attempt counts and fails E2. | r3 at 2000 msg/s: the generator stalled for 272 ms (at 102% of a core); nothing was lost and Mosquitto dropped 0. Unlike r2 of block 4, which lost 785 messages. The report lists all six attempts. | `8d5abfc`, `47c040b` |

## 4. Limitations that must accompany each figure

- **All of them.** One laptop: WSL2 with 8 vCPUs pinned per service, every component on one host, on a local Docker network; they say nothing about separate machines or real networks. 300 s windows, not hours. Millisecond timestamps. WSL2's clock steps are known: negative samples are counted, never dropped.
- **F1 (e2e) and `ingest`:** measured from the scheduled instant, so a stall of the generator counts as pipeline latency (the r3 attempt, section 3). At 5 msg/s there are too few samples for a p99.
- **F2 (alarm):** with a fixed fault pattern (bearing temperature high for 10 s out of every 20). At 5 and 50 msg/s, on the 5 real assets; the 100 clones (20 per asset) are used in throughput runs only, so F2 at 2000 msg/s is with 100 assets (`bench/harness.py`, `execute`).
- **F3:** 2000 msg/s is the highest step measured validly. The step at 3000 is limited by the generator, although the pipeline was also falling behind there at INFO. No step between 2000 and 3000 was measured. Bridge at INFO, the default.
  - **The generator is close to its own limit at 2000 msg/s in the full stack:** in the five valid runs it used 55.0-60.6% of a core on average, with peaks of 68-76%, against a limit of 80%.
  - One of the six attempts saturated: peak 102%, mean 57.8% and a 272 ms stall (CPU from `cpu.csv` over the measured window; peaks from the manifest).
- **F4 (`produce_to_ack`):** Kafka writes to tmpfs, with a single broker and replication factor 1, so `acks=all` costs nothing extra. It is optimistic compared with a disk-backed, replicated Kafka.
- **Payload and delivery:** synthetic messages of about 218 B. Before the bridge receives a message, delivery is at most once, through a bounded buffer with no backpressure. What is published while the bridge is down is lost (backlog).
- **Calibration:** not repeated on the final configuration (the instruments did not change). The ladder's cap (3000) comes from the earlier calibration runs.
- **ThingsBoard:** F5, F6 and F7 are not published. What is verified and may be stated is in the backlog: the limit of 10 msg/s per asset and loss-free delivery in two integration tests.

## 5. Still unverified

- The mechanism of Docker Desktop's slow forwarding mode, and why the 5 ms linger costs 12-13 ms on Windows (STEP 0).
- The broker's CPU peaks under load (156-189% of a core). They fall in the first 13 s of sending, and no logged event explains them (section 11 of the design note).
- Why the generator stalled for 272 ms in the r3 attempt.
- That the 785 messages lost in r2 (block 4, first series) came from Mosquitto's queue: an inference; that run has no `$SYS` data and no Mosquitto log.
- How much of each improvement between the earlier and the final series comes from each change: the queue, the healthchecks and cp-server changed together.
- The logging cost with more than one run at WARNING.
- ZooKeeper's idle memory and CPU after its healthcheck change (`srvr` was only tried in a separate container).
- F5, F6 and F7 (ThingsBoard).

## 6. Deviations from the three briefings

**STEP 0 (protocol).** All ten deviations are in section 6 of `docs/internal/benchmark_protocol_proposal.md`. The main ones:
- there is no `src/`, and `ts_to_ack_ms` no longer existed;
- the origin of the 16 against 42-44 ms figures is not in the repository;
- four passes instead of one (suspend, unplugged, battery), all kept;
- the `bimodal.py` experiment was added to the matrix;
- I relaxed the AC-power criterion on my own in pass 3, and the owner restored it.

**STEP 1 (implementation).** In `docs/internal/benchmark_step1_design.md` ("Desvíos y pendientes") and `docs/internal/benchmark_step2_plan.md` ("Desvíos de la implementación"):
- starting commit `9ace303`, not `78673d2`;
- the bridge read its producer settings from `config/` (D-F), which lifted the rule against touching the bridge;
- the generator schedules on the VM's wall clock;
- the lag slope allows 0.001 s/s;
- the harness's I/O functions carry `pragma: no cover`.

**STEP 2 (runs).** The briefing planned 29 runs (36 at worst) in five blocks, with no code change during the step except an approved fix. What happened:
- **Approved changes in the middle of the step, in this order:**
  - DETENTE 3.3.f: the CPU rule, clock quality and the report;
  - closing F3 and E2 by revision;
  - Mosquitto evidence and the diagnostic mode;
  - **product changes** (DETENTE 3.3.h and 3.3.i): Mosquitto's queue, the drop monitor, the healthchecks and cp-server;
  - the series runner, the E2 replacement and the `$SYS` wait.
- **Runs not in the plan:**
  - two diagnostics at 3000 msg/s;
  - **a full repeated series** on the final configuration;
  - idle comparisons (outside `bench/results/`).
- The sensitivity block ran once, at the end and on the final configuration, and the 3000 msg/s diagnostic was requested as not publishable.
- The attempts without results are kept as incomplete directories (`20261012T183625Z` and `20261012T184014Z`, with their `compose.override.yml`).
- **Commit `44088cc` is titled "block sensitivity (2 runs)" but holds those two crashed attempts without results.** The series runner wrote it before the fix, when it counted an attempt without results as a run. The real WARNING block is `b7cb2f9`, with the same title.
- F3 was reopened on 2026-10-11 and settled by the final series (section 3); the briefing expected it to close after block 3.

## 7. Runs and laptop hours

Source: the manifests in `bench/results/`, re-evaluated under the current rules. Sessions group runs less than 15 minutes apart.

| | Valid | Invalid |
|---|---|---|
| Final configuration | 19 (10 latency, 8 throughput, 1 sensitivity) | 4 (3 throughput because of an instrument, the WARNING diagnostic at 3000) + 2 attempts without results |
| Earlier runs (history) | 27 (5 generator calibration, 1 meter calibration, 10 latency, 10 throughput, 1 trial) | 7 (2 generator calibration, 3 throughput, 2 diagnostics) |

- **Laptop hours on AC power in STEP 2: 7.95 h** over 8 sessions (from the first start to the last finish, plus the first cooldown). Inside the runs: 5.42 h. The two attempts without results add about 8 minutes.
- Outside `bench/results/`: two idle comparisons of about 15 minutes each, five cold starts (about 15 minutes), the integration tests and STEP 0.

## 8. Notes for the closure

- **`pragma: no cover`.** The benchmark's I/O functions carry it:
  - in `bench/harness.py`, the orchestration (`Run`, `execute`, the calibrations, `SysSampler`, `SubscriptionWatch`, `CpuSampler` and the host and Docker helpers);
  - the `main()` of `bench/generator.py`, `bench/meter.py` (and `ClockSampler.run`), `bench/loader.py` and `bench/report.py`;
  - the `main()` of `bench/series.py`.

  This goes beyond the repository's rule of excluding only `main()`. The decisions (criteria, ladder, E2, series, report) are unit-tested: `bench/series.py` and `bench/analysis.py` at 100%. The orchestration is exercised by the real runs.
- **Lag-slope tolerance: 0.001 s/s, not 0** (DETENTE 3.3.d, before any data). With exactly 0, the noise around a steady lag gives small positive slopes and would fail stable runs. 0.001 is 0.3 s gained over 300 s, under the 1 s lag bound. It is `criteria.max_lag_slope_per_s` in `config/benchmark.yaml`.
