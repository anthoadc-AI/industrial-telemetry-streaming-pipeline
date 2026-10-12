# Benchmark results

One directory per run, written by `bench/harness.py` (`make bench-trial`, or `python -m bench.harness run ...`):

| File | Content |
|---|---|
| `manifest.json` | environment, parameters, measured window, per-metric summaries, validity, problems, `trial` and `publishable` |
| `latency_<metric>.json` | fixed-resolution histograms (`bench/histogram.py`): `e2e` (F1), `bridge_share`, `read_share`, `alarm_e2e` (F2), `produce_to_ack` (F4), `generator_late` |
| `series.csv` | every monitor sample: scheduled messages, new `telemetry.raw` records, alarm consumer lag |
| `cpu.csv` | CPU per container (per cent of one core) over the run |
| `counters.json` | integrity counts: sent, acknowledged, records, distinct, duplicates, gaps, dead letters, alarms against expected |
| `compose.override.yml` | the run's generated compose override (cpusets, mounts, log level) |
| `raw/` | logs, the generated config and the components' own output: local only, not versioned |

Every figure table is generated from these files by `bench/report.py` (`make bench-report`); none is written
by hand.

**Trial runs (`*-trial-*`, `"trial": true`, `"publishable": false`) are not publishable.** They exist to
show that the harness works end to end (Task 3.3 STEP 1, 1.3.6). The publishable runs are STEP 2's.
