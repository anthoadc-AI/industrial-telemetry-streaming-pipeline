# Phase 3 Briefing — Quantified Evidence

Repository: `anthoadc-AI/industrial-telemetry-streaming-pipeline` (private until Task 3.6)
Starting point: `main` at tag `v2-engineering-quality`
Briefing date: 2026-10-02
Save as: `docs/internal/claude_code_briefings/phase3_briefing_quantified_evidence.md`

## 1. Purpose of this phase

Phases 0-2 built a pipeline that is tested and reproducible but invisible: the repo is private and the README is a stub. Phase 3 turns the pipeline into evidence a technical reviewer can check in ten minutes: measured numbers, a working operations view, a diagram that matches the code, and a README in which every claim traces to a file in the repo.

The phase ends with the repository public. Everything before Task 3.6 is preparation for that moment.

## 2. Ground rules (unchanged from Phase 2, restated because they bind every task)

1. **STEP 0 per task.** Before writing code for a task, inspect the current state, write a short design note (options considered, recommendation, risks, what will be measured to prove it worked) and stop at the design DETENTE. No implementation before the owner answers.
2. **DETENTE means stop.** Report and wait. Do not start the next task, do not merge, do not tag.
3. **Closures belong to the owner.** Task closure, phase closure, the tag `v3-quantified-evidence` and the visibility change are owner decisions.
4. **No unmeasured claim anywhere.** A number appears in a public file only if a committed raw result file contains it and a script reproduces it.
5. **Single source of truth.** Thresholds, topic names, rates and benchmark parameters live in `config/`. Nothing hardcodes a value that config already defines.
6. **Language policy.** English in every public artifact. Spanish only under `docs/internal/`. Owner decision 2026-10-02: phase closure reports that will be published exist in both English and Spanish (see Task 3.6).
7. **Git.** One feature branch per task, Conventional Commits with a narrative body recording the design decision, `merge --no-ff` into `main` only after the owner approves the task, CI green on the branch before requesting approval.
8. **Secrets.** `make check-secrets` clean before every push. No tokens, endpoints or JWTs in code, exports, logs or result files.
9. **Docker.** `docker compose down -v` requires explicit owner authorisation each time. The course folders and `its-pipeline-backup/` are never touched.
10. **Deviations.** Any departure from this briefing is listed with its reason in the task report, as in Phase 2.
11. **No decoration.** No emojis, no filler, no badges that do not report a real status.

## 3. STEP 0 of the phase — state verification (read-only)

Before Task 3.1:

- Confirm `main` is at `v2-engineering-quality`, tree clean, CI green on that commit, repo still `PRIVATE`.
- `make up`, `make test`, `make test-integration`, `make smoke` on the current tree. Report pass/fail and durations.
- Report the exact current definition of every latency field the bridge and the alarm consumer log (name, start timestamp, end timestamp, clock used, file and line).
- Report which processes run on the Windows host and which run inside the Docker/WSL 2 VM, and measure the clock offset between host and VM (method of your choice, state it). This decides which timestamps may be subtracted from each other in Task 3.3.
- Report how Kafka storage is mounted (tmpfs or volume) and the current topic configuration from `provision.py`.
- List what exists today for ThingsBoard: rule chains imported, devices, dashboards, and any code path that sends data to it.
- List open items from `docs/internal/phase2_backlog.md` and the Phase 2 closure report section 8, with a proposed disposition for each: do in Phase 3 (which task), or document as a known limitation in the README. Known inherited items: Firebase authentication, manual MQTT ack in the bridge, healthcheck on an empty volume, coverage/test report in CI, kafkajs maintenance risk.

**DETENTE 3.0.a.** Deliver the report above. No file is modified before approval.

## 4. Tasks

### Task 3.1 — Remove the two blockers

Two defects that would invalidate later tasks. One branch each.

**3.1-A `ts_to_ack_ms` bias.** The bridge observes delivery acknowledgements only when it polls the producer, and it waits up to 100 ms between polls. The logged value therefore includes polling delay and is not a latency of the pipeline.

STEP 0 must:
- Reproduce the bias with a measurement (distribution of the current field at the smoke rate; a sawtooth or a floor near the poll interval is the expected signature).
- Evaluate at least these options: (a) non-blocking `poll(0)` on every produce plus a short-timeout service loop; (b) a dedicated poll thread; (c) taking the produce-to-ack interval from the delivery report itself (`Message.latency()` in confluent-kafka, measured inside librdkafka and independent of when the callback is served) and adding the host-side interval from event timestamp to produce call. Verify (c) exists and behaves as described in the pinned client version before recommending it.
- Propose a stage breakdown with precise names instead of one opaque figure, for example: sensor timestamp to bridge receive, bridge receive to produce call, produce call to broker ack. State the clock used for each end of each interval.
- State what happens to the old field name. Recommendation: remove it; a renamed, correctly defined field is better than a corrected field carrying a name that previously meant something else.

Acceptance: before/after distributions from the same run conditions, a unit test that fails if ack time is taken at poll time, schema or log-format changes documented, `make smoke` still 200/200.

**3.1-B Rule chain import with `FIREBASE_ENABLED=false`.** The import leaves three REST nodes with an unresolved placeholder inside the Root Rule Chain. Once telemetry reaches ThingsBoard, each message would execute a failing REST call.

STEP 0 must:
- Show the three nodes and their connections in the exported JSON.
- Propose the transformation applied when Firebase is disabled: remove the REST nodes and their connections before import, leaving a valid chain. Disabling in place is not acceptable if the node still executes.
- Decide whether `TempToFirebase` and `AlarmToFirebase` are imported at all when Firebase is disabled. Recommendation: skip them.
- Keep the import idempotent in both directions (enabled to disabled and back).

Acceptance: unit tests on the JSON transformation; with the `full` profile up and Firebase disabled, send telemetry to ThingsBoard and show zero rule-engine failures (state the API or statistic used as evidence); the enabled path still resolves the placeholder from `.env`.

**DETENTE 3.1.a** — both design notes. **DETENTE 3.1.b** — results of both, branches unmerged.

### Task 3.2 — ThingsBoard as the operations layer

Goal: an operator opens one dashboard and sees the five simulated assets, their four channels, and the alarms raised by the pipeline.

Constraint decided in Phase 2 and not reopened: **one alarm engine.** Thresholds, persistence and hysteresis are evaluated only in the Node.js alarm consumer. ThingsBoard displays alarm state; it never evaluates a threshold. The course chain `CreateAndClearAlarms` contains thresholds and must not remain active in the telemetry path.

STEP 0 must decide and justify:
- **Sink component.** A Kafka consumer that reads `telemetry.raw` and `telemetry.alarms` and forwards to ThingsBoard. Language, consumer group, offset commit policy, behaviour when ThingsBoard is down (the sink must not block or slow the alarm consumer; it is a separate group).
- **Transport and device model.** Recommendation to evaluate first: the ThingsBoard Gateway MQTT API with one gateway credential, so each asset appears as its own device without per-device tokens. Compare against per-device tokens and against REST.
- **Alarm representation.** Compare: (a) alarm lifecycle events (`RAISED`/`CLEARED`, severity, `alarm_id`) arrive as messages and a small rule chain maps them to ThingsBoard create/clear alarm nodes with no threshold logic; (b) the sink calls the alarm REST API directly, which requires tenant credentials in the sink. Recommendation: (a), because the mapping is exportable JSON and no tenant JWT leaves the import script.
- **Idempotency.** `telemetry.alarms` is compacted and replayed at-least-once; replaying must not duplicate alarms in ThingsBoard. Use the deterministic `alarm_id`.
- **Dashboard as code.** Dashboards and the new rule chain are exported as JSON under version control and imported by `make up` in the `full` profile, like the existing chains. No manual UI step may be required to reach the working state. Device IDs embedded in dashboard JSON are instance-specific; state how the import resolves them (entity aliases by name or type are the usual answer).
- **Dashboard content.** One operations dashboard: per-asset time series for the four channels, active alarm table, alarm count by severity. No widget that shows nothing.
- **Sink failure path.** What goes to the dead-letter topic and what is retried.

Acceptance: unit tests for the sink at the Phase 2 coverage thresholds; an integration test in the `full` profile that injects a fault, then asserts through the ThingsBoard API that telemetry arrived for the asset, the alarm exists with the right severity, and it clears after the fault ends; a clean `make clean` + `make up` reproduces devices, chain and dashboard with no UI interaction; exports contain no credentials.

Owner action at the results DETENTE: open the dashboard during a fault-injection run and capture the screenshots that the README will use. Claude Code does not produce or edit screenshots.

**DETENTE 3.2.a** — design. **DETENTE 3.2.b** — results, with instructions for the owner's visual check.

### Task 3.3 — Benchmark: throughput and latency

This task produces every performance number the README will contain. A weak method here damages the whole repo, so the design DETENTE is the important one.

STEP 0 must deliver a measurement protocol covering:

- **What is measured.** End-to-end latency from sensor event timestamp to (i) broker ack of `telemetry.raw`, (ii) alarm event produced to `telemetry.alarms`, plus the stage breakdown from Task 3.1-A. Throughput defined as the highest sustained input rate with zero message loss and consumer lag that stays bounded for the whole run; state the lag criterion numerically.
- **Clocks.** Only subtract timestamps taken by the same clock, or bound the host/VM offset measured in STEP 0 and report it next to the results. Broker `LogAppendTime` comes from the VM clock.
- **Load generation.** The simulator at realistic rate is one data point, not a benchmark. Define a load mode (more assets, higher rate, or a dedicated generator publishing schema-valid messages) and show the generator is not the bottleneck. Open-loop generation with scheduled send times so that latency is measured from the intended send time and stalls are not hidden (coordinated omission).
- **Run design.** Warm-up excluded and stated; fixed duration per rate step; at least five repetitions per step; rate ladder from nominal up to the first step that violates the loss or lag criterion.
- **Statistics.** p50, p95, p99 and max per run with sample count; across repetitions report the median and the min-max range. No averages of percentiles presented as percentiles.
- **Environment record.** CPU model, cores, RAM, WSL 2 memory limit, Docker Desktop version, image tags, client library versions, partition counts, `acks`, idempotence, linger and batch settings, MQTT QoS, and the fact that everything runs on one machine. If Kafka storage is tmpfs, say so in the results: no disk flush is being measured.
- **Background noise.** Confirm the Airflow and Hadoop containers from other courses are down during runs.
- **Artifacts.** `benchmarks/` with the runner, the protocol document, raw per-run result files (JSON or CSV), and a script that regenerates the summary table from raw files. `make benchmark` runs the protocol; a short variant may exist for CI but CI numbers are never published.
- **Scope statement.** Draft the exact sentence that will accompany the numbers in the README: single-node, synthetic data, laptop-class hardware, not a capacity claim for production.

**DETENTE 3.3.a** — protocol only, no runs beyond what is needed to validate the generator. The owner and the design chat agree on which figures are publishable before any full run.

Then execute. If results look implausibly good or bad, investigate before reporting; report anomalies and discarded runs with reasons instead of dropping them silently.

**DETENTE 3.3.b** — results: summary table, raw files committed on the branch, environment record, anomalies, and the list of figures proposed for the README with the file and field each one comes from.

### Task 3.4 — Architecture diagram

STEP 0: propose format and content. Recommendation: Mermaid embedded in `docs/ARCHITECTURE.md` and reused in the README, because it renders on GitHub and diffs as text; no binary image that can drift from the code.

Requirements:
- Every box is a service in `docker-compose.yml` or a process started by the Makefile; every arrow is labelled with protocol, topic name and serialisation. Names come from `config/`.
- Optional and inherited elements are visually distinct: Firebase sink (course heritage, off by default), `full` profile components.
- `docs/ARCHITECTURE.md` also covers: delivery semantics per hop (MQTT QoS, bridge ack behaviour, at-least-once with deterministic `alarm_id`), schema evolution policy, alarm state machine summary, dead-letter flow, failure behaviour of each component, and an explicit "not implemented" list.
- A check, manual or scripted, that the topic and service names in the diagram exist in `config/` and compose. State which.

**DETENTE 3.4.a** — design and draft diagram source. **DETENTE 3.4.b** — final document rendered on the GitHub branch view.

### Task 3.5 — Final README

Written only after 3.1-3.4 are merged. Framing approved in Phase 0: a streaming telemetry pipeline with domain alarm logic (generic telemetry, industrial example), not a manufacturing project.

STEP 0: deliver an outline and a claims table before any prose. The claims table has one row per factual or numeric statement: the claim, the evidence file and field or the test that proves it, and the command that reproduces it. A claim without a row is not written.

Content requirements:
- First screen: what it is in two sentences, the diagram, the headline measured figures with their scope statement, and the quickstart.
- Problem-Action-Result structure for each engineering decision worth showing: Avro with Schema Registry and compatibility mode, alarm state machine (persistence, hysteresis, ISA-18.2 pattern), dead-letter design, at-least-once with deterministic keys instead of transactions, test strategy and the three defects the tests found, the `ts_to_ack_ms` bias found and removed, CI.
- Synthetic data stated plainly near the top and wherever a figure appears; a short description of the simulator's physical model and its limits.
- Origin: MIT xPRO Module 24 coursework preserved at tag `v0-coursework`, with a concise statement of what changed since. Firebase documented as an optional inherited sink.
- Known limitations and what a production deployment would change. This section is mandatory.
- Quickstart verified from a fresh clone on this machine and by the CI workflow; prerequisites with versions.
- No emojis, no adjectives doing the work of numbers, no badge except CI status and licence.
- A script or test that extracts the numeric figures from the README and compares them with the benchmark summary and test counts, failing on mismatch. State the mechanism in STEP 0.

**DETENTE 3.5.a** — outline and claims table. **DETENTE 3.5.b** — full draft on the branch. The owner reviews the prose with the design chat; expect at least one revision round.

### Task 3.6 — Publication readiness

Claude Code prepares; the owner publishes. Claude Code does not change repository visibility.

STEP 0: a pre-publication audit report, read-only:
- Secret scan over the **entire history, all branches and all tags**, not only HEAD, with a second tool besides `check_secrets.py` (for example gitleaks). Report every finding including false positives.
- Personal data in history: author emails, local paths, usernames, hostnames, anything in logs or result files.
- Inventory of `docs/internal/` as it will be seen publicly: files, language, and anything the owner may not want public. Present options with their cost: keep as is, remove from HEAD (remains in history), or rewrite history (invalidates existing tags and hashes). Do not act; this is an owner decision.
- Remote branches to delete, tags present, licence file, `.env.example` completeness, pinned image and dependency versions, dependency vulnerability audit (`pip-audit`, `npm audit`) with findings.
- Clean-machine check: fresh clone into a new directory, `make up && make test`, `make test-integration`, `make smoke`, and the README quickstart followed literally.
- Proposed GitHub metadata: description, topics, and branch protection on `main` requiring CI.

- Bilingual closure reports (owner decision 2026-10-02): every phase closure report that will be published must exist in English and Spanish. Inventory the existing reports (Phase 0 diagnostic, Phase 1, Phase 2), the language each is written in, and which version is missing. Propose: the public location (recommendation: `docs/reports/`, outside `docs/internal/`), the naming convention (recommendation: `phaseN_closure_report.md` for English and `phaseN_closure_report.es.md` for Spanish), and which language is the source of record (recommendation: English; the Spanish file states in its header that it is a translation and of which commit). Before translating, list anything in the existing reports that should not be public (local paths, credentials history, machine details) so it is resolved once, in the source.

**DETENTE 3.6.a** — audit report, the owner decision on `docs/internal/`, and the bilingual report plan.

Then: apply approved fixes; produce the missing language version of each published report; write the Phase 3 closure report in both languages (what was built, measured results, deviations from this briefing, what is deliberately not done); final CI run green on `main`.

Rules for the bilingual reports: translations are faithful, not rewritten; every number, hash, command, file path and identifier is identical in both versions, and a script or test compares the numeric figures and code spans of each pair and fails on mismatch; historical reports are translated as they were approved, with corrections recorded as dated errata instead of silent edits.

**DETENTE 3.6.b — Phase 3 closure.** Deliver the closure report and `git log --graph --oneline --all`. After owner approval: annotated tag `v3-quantified-evidence`. The owner then switches visibility to public and verifies the rendered README, the diagram and the CI badge from a logged-out browser.

## 5. Definition of done

- Latency fields have precise definitions and no polling artefact; the Firebase-disabled import leaves a rule chain with zero failing nodes.
- `make up` in the `full` profile yields devices, alarm mapping chain and operations dashboard with no manual step; an injected fault is visible as telemetry and as an alarm that raises and clears.
- `benchmarks/` contains protocol, runner, raw results and the summary generator; every published figure is regenerable.
- The diagram matches compose and config.
- Every README claim has a row in the claims table; README figures are checked against result files automatically.
- History-wide secret scan clean; fresh-clone verification passed; CI green on `main`.
- Every published closure report exists in English and Spanish, with figures verified identical across each pair.
- Closure report delivered; tag and visibility change performed only after owner approval.
