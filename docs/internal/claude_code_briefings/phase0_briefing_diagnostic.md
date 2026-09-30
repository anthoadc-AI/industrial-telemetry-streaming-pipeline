# Claude Code Briefing — PHASE 0: Diagnostic

**Project:** industrial-telemetry-streaming-pipeline
**Phase:** 0 of 3 — Diagnostic (read-only)
**Owner / approver:** Anthony Dávila Carrillo. Phase closure is reserved for the owner.
**Briefing version:** 1.0 — 2026-09-13

---

## 1. Purpose

Establish, with evidence, the exact state of the three MIT xPRO Module 24 course folders before anything is restructured. The output of this phase is a single diagnostic report. Nothing else is created, moved, renamed, or edited.

This phase exists because the course code will become the tagged `v0-coursework` baseline of a public portfolio repository. Anything we do not know about it now (exposed secrets, broken dependencies, undocumented deviations) becomes a public liability later.

## 2. Context you need

- The three folders are working coursework, completed and functional on this machine as of early September 2026:
  - `Project_24_Docker` — Mosquitto + ThingsBoard stack (Docker).
  - `Project_24_MQTT` — Python/Paho publisher → Mosquitto → ThingsBoard → Firebase Realtime Database, plus ThingsBoard rule chains (`CreateAndClearAlarms`, `TempToFirebase`, `AlarmToFirebase`).
  - `Project24_2_Kafka` — Python producer (simulated GPS) → Kafka topic → Node.js consumer (`kafkajs`), on a nine-container Confluent stack.
- Known deviations already applied and documented in course submissions:
  - `KafkaClient` removed in kafka-python 3.x → block deleted, NumPy integers wrapped with `int()`.
  - `node-rdkafka` incompatible with Node.js v24 → replaced by `kafkajs`, `myconsumer.js` rewritten preserving the function signature.
- Environment: Windows 11, WSL 2 backend, Docker Desktop, PowerShell, VS Code. Python 3.13, Node.js v24, `paho-mqtt` 1.6.1, `kafka-python` 3.x, `kafkajs`, NumPy.
- Absolute paths of the three folders: **`<OWNER FILLS IN BEFORE LAUNCH>`**

Known environment facts (do not rediscover, just confirm):
- When ThingsBoard runs alongside Mosquitto, ThingsBoard maps MQTT to host port **9883**, not 1883. Publishers targeting ThingsBoard use 9883.
- ThingsBoard image is `thingsboard/tb-postgres`.
- ThingsBoard REST API Call nodes fail with `Illegal character in scheme name` if the URL field has leading whitespace.
- Firebase receives a full JSON object via PUT to `/temperature.json` and nests fields under that key.

## 3. Hard rules for this phase

1. **Read-only.** Do not edit, rename, move, delete, or create any file inside the three course folders. Do not run `pip install`, `npm install`, or any formatter/linter that writes to disk inside them.
2. **No git.** Do not run `git init`, `git add`, or any git command that creates or modifies a repository. The repository does not exist yet and will not be created in this phase.
3. **Never run `docker compose down -v` or `docker volume rm`** against any of these stacks. The `-v` flag destroys ThingsBoard's PostgreSQL volume and the configured rule chains. Use `docker compose down` (no flags) or `docker compose stop`.
4. **Secrets are redacted in the report.** When you find a credential, token, database URL with an embedded secret, or `.json` service account, report the file, line number, and secret *type* only. Never copy the value into the report or into terminal output that will be pasted back.
5. **One stack at a time.** Do not bring up the ThingsBoard stack and the nine-container Confluent stack simultaneously unless Docker Desktop has at least 12 GB allocated. Check first (`docker info` → Total Memory). Run 24.1 and 24.2 sequentially and stop each before starting the next.
6. **Live cloud side effects require approval.** The 24.1 pipeline writes to a real Firebase Realtime Database. Do not execute any script that publishes telemetry until STOP 1 has been reviewed and the owner has explicitly authorized live runs.
7. **Report everything you could not verify** as "NOT VERIFIED — reason", never as an assumption. An honest gap is more useful than a guess.
8. All output you write (report, file names, headings) is in English.

## 4. Tasks

### Task 0.1 — Folder integrity and inventory (static)

For each of the three folders:

- Full file tree (`tree /F` on PowerShell or `find` in WSL), excluding `node_modules`, `__pycache__`, `.venv`, and Docker volume data. Note total size and file count.
- Classify every non-trivial file: source (Python / JS), Docker Compose, config (`.env`, `.json`, `.yml`, `.conf`), documentation, exported ThingsBoard artifacts, course-provided starter vs. owner-modified. Where you can tell a file is an unmodified course starter, say so and say how you know (e.g., template boilerplate, comment headers).
- List every file over 5 MB and every binary file. These are candidates for exclusion from the repo.
- Note any file that looks like a duplicate, backup (`*_old`, `*.bak`, `copia`), or leftover from a failed attempt.

### Task 0.2 — Dependency and version inventory (static)

- Python: for each script, list every `import`. Resolve each third-party import to the installed version (`pip show <pkg>` in the interpreter actually used to run the coursework; identify which interpreter that is — system Python, WSL Python, or a venv). Flag any `requirements.txt` that is missing, incomplete, or inconsistent with the imports.
- Node.js: read every `package.json` and `package-lock.json`. Record declared vs. locked versions of `kafkajs` and anything else. Confirm `node-rdkafka` is no longer referenced anywhere (including lockfiles).
- Docker: for every `docker-compose*.yml`, list every image with its tag. Flag images pinned to `latest` or untagged — these will have to be pinned in Phase 1. Record the Confluent Platform version implied by the tags.
- Runtimes: output of `python --version`, `node --version`, `npm --version`, `docker --version`, `docker compose version`, and the WSL distribution and kernel version.

### Task 0.3 — Secrets and sensitive data scan (static)

Scan all three folders (including any `.env`, `.json`, rule-chain exports, and notebooks) for:

- Firebase: database URLs (`*.firebaseio.com`, `*.firebasedatabase.app`), database secrets, `?auth=` query parameters, service-account JSON files.
- ThingsBoard: device access tokens, tenant credentials, JWT tokens, hardcoded admin passwords (`tenant@thingsboard.org` / `tenant` counts — report it even though it is the public default).
- Kafka / Confluent: SASL credentials, API keys, Control Center passwords.
- Generic: anything matching `password`, `secret`, `token`, `apikey`, `api_key`, private keys (`-----BEGIN`), personal email addresses, machine hostnames, absolute Windows paths with the owner's username.

Use at least two methods: a pattern grep, and a manual read of every config-type file. Report as a table: file, line, secret type, whether it appears to be live (used by running code) or dead (leftover). **Redact values.**

### STOP 1 — Static diagnostic review

Stop here. Produce the sections of the report covering Tasks 0.1–0.3 (report template in section 5). Do not proceed to Task 0.4 until the owner reviews the secrets table and explicitly authorizes live pipeline runs. State clearly which runs would write to Firebase.

### Task 0.4 — Functional verification: Pipeline 24.1 (MQTT → ThingsBoard → Firebase)

Only after STOP 1 authorization.

- Bring up the `Project_24_Docker` stack with `docker compose up -d`. Record container names, image tags, exposed ports, and time to healthy state.
- Confirm Mosquitto accepts a connection on its mapped port and ThingsBoard accepts MQTT on 9883 and HTTP on its UI port. Use a minimal probe (`mosquitto_sub` / `mosquitto_pub` from the container, or a 3-line Paho connect-only script written to a temp directory *outside* the course folders).
- If authorized: run the course publisher script once, for a bounded duration (state the duration). Confirm telemetry arrives in ThingsBoard (device latest telemetry) and, if the Firebase write path is authorized, that the record appears in Firebase. Capture the ThingsBoard rule chain list and confirm the three named chains exist. Export nothing yet — just confirm existence and node count.
- Stop the stack with `docker compose down` (**no `-v`**).
- Record every warning or error in container logs (`docker compose logs --tail=200`) even if the pipeline worked.

### Task 0.5 — Functional verification: Pipeline 24.2 (Python → Kafka → Node.js)

- Bring up the Confluent stack. Record time to all nine containers healthy, peak memory (`docker stats --no-stream` once stable), and any container that restarts.
- Confirm the topic used by the coursework exists or is auto-created (`kafka-topics --list` inside the broker container). Record the topic name, partition count, replication factor.
- Run the Python producer for a bounded number of messages (e.g., 20) and the `kafkajs` consumer concurrently. Confirm end-to-end delivery. Record the consumer group id and whether offsets are committed.
- Note whether Schema Registry, ksqlDB, Connect, and Control Center are actually used by the coursework or are merely present. This determines what survives into the single `docker-compose` of Phase 1.
- Stop the stack with `docker compose down` (**no `-v`**).

### Task 0.6 — Proposed repository structure (proposal only, nothing created)

Based on what you found, propose a layered structure for `industrial-telemetry-streaming-pipeline`. Constraints already decided by the owner:

- Single repository, organized by pipeline layer (edge / broker / streaming / processing / visualization), not by course sub-project.
- Course code enters as commit `v0-coursework`; the proposal must state exactly which files from the three folders go into that commit, which are excluded, and why (binaries, screenshots, `.docx` submissions, secrets, duplicates).
- Firebase is an optional sink, isolated so it can be disabled by configuration.
- `docs/internal/` holds Spanish-language working documents and Claude Code briefings; everything else is public and in English.
- `config/` is the single source of truth for thresholds and constants (Phase 1 will populate it).

Deliver the proposal as a tree with a one-line justification per top-level directory, plus a mapping table: `course path → proposed repo path` (or `EXCLUDED — reason`). Mark every point where you are proposing rather than following an owner decision.

### STOP 2 — Phase 0 report

Stop. Deliver the full report (section 5) as one Markdown file. Write it to a staging directory *outside* the three course folders, e.g. `<parent>/its-pipeline-staging/docs/internal/phase0_diagnostic_report.md`. This is the only file you create in this phase. Do not `git init` the staging directory. Await owner review.

## 5. Report template

```
# Phase 0 Diagnostic Report — industrial-telemetry-streaming-pipeline
Date / machine / interpreter and runtime versions

## 1. Executive summary (≤ 10 lines)
Green / yellow / red per folder. Top 3 risks for going public.

## 2. Folder inventory
Per folder: tree, file classification, sizes, duplicates/leftovers.

## 3. Dependency inventory
Python table (script → imports → installed version → in requirements?)
Node table (package → declared → locked)
Docker table (service → image → tag → pinned?)

## 4. Secrets and sensitive data
Table: file | line | type | live/dead | proposed remediation (env var name)
VALUES REDACTED.

## 5. Functional verification
5.1 Pipeline 24.1 — what was run, for how long, what was observed, log warnings.
5.2 Pipeline 24.2 — same. Peak memory, topic details, unused Confluent services.
Anything NOT VERIFIED, with reason.

## 6. Deviations from course material found in code
Confirm the two known deviations; list any others.

## 7. Proposed repository structure
Tree + justification + mapping table. Proposals clearly marked.

## 8. Open questions for the owner
Numbered. Each one blocks or shapes Phase 1.
```

## 6. Definition of done for Phase 0

- Report delivered at STOP 2 with every section filled or explicitly marked NOT VERIFIED.
- Zero modifications inside the three course folders (owner will verify with a file-hash or timestamp comparison).
- No git repository created.
- No secret values present in the report or in pasted terminal output.
- Both pipelines either demonstrated end-to-end or the exact failure point documented.

Phase closure is the owner's decision.
