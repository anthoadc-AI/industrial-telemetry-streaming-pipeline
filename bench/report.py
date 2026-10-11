"""The figures tables, generated from bench/results/ (Task 3.3, 1.3.4): no figure is ever written by hand.

    python -m bench.report [--results bench/results] [--p99-ms 1000] [--max-lag-s 1] [--max-lag-slope 0.001] [--max-lost 0]

Reads every run's manifest.json, histograms, series.csv and counters.json and prints Markdown:
- what each figure and share contains;
- the calibration (owner, DETENTE 3.3.d and 3.3.e): every calibration run with its validity, CPU and minimum host
  free memory, re-evaluated from its stored data; the highest rate the generator and the meter sustain alone under
  the ladder's own rule for a client's CPU; the ladder capped at the lower of the two, only when both are calibrated;
- every pipeline run: mode, rate, bridge log level, validity, publishable, F1 (end to end) with its ingest and
  produce_to_visible shares, F2, F4, the generator's lateness and the problems;
- the throughput ladder (F3) re-evaluated from the raw results with the thresholds of config/benchmark.yaml or
  those given on the command line (D8: the cut can be recomputed without new runs), the intermediate step still to
  run (DETENTE 3.3.d), E2's confirmation, and the log-level sensitivity (INFO against WARNING, DETENTE 3.3.d).

Trial and calibration runs are never publishable.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

from bench import analysis, criteria
from bench.histogram import Histogram
from config.loader import REPO_ROOT, load_benchmark_config

DEFINITIONS = """## What the figures contain

- **F1 e2e**: the generator's scheduled instant (the message's ts) to the record being returned to a consumer of
  telemetry.raw (the meter), on the VM's wall clock. It splits at the record's CreateTime, set by the bridge's
  librdkafka when produce() is called:
  - **ingest** = CreateTime - ts: MQTT publish, Mosquitto, the bridge's queue, validation and serialisation, up to
    the bridge's produce() call;
  - **produce_to_visible** = visible - CreateTime: librdkafka's linger.ms, the produce request, the broker's append
    and high-watermark advance, and the consumer's fetch. It does not wait for the acknowledgement's return to the
    bridge. **It is dominated by linger.ms** (librdkafka's default, 5 ms, unless the run's bridge_producer says
    otherwise). Each share carries +-1 ms of quantisation (whole-millisecond time stamps).
- **F2 alarm**: the triggering sample's ts to the alarm consumer's acknowledgement of the alarm event.
- **F4 produce-to-ack**: the bridge's produce() call to the broker's acknowledgement reaching it (bridge at INFO).
- **Generator late**: the generator's own lateness, actual send minus scheduled instant.
"""


def load_run(run_dir: Path) -> dict:
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    hists = {p.stem[len("latency_"):]: Histogram.from_json(json.loads(p.read_text(encoding="utf-8")))
             for p in run_dir.glob("latency_*.json")}
    samples = []
    if (run_dir / "series.csv").is_file():
        with open(run_dir / "series.csv", encoding="utf-8") as f:
            samples = [analysis.LagSample(float(r["wall_ms"]), int(r["scheduled"]), int(r["raw_records"]), int(r["consumer_lag"]))
                       for r in csv.DictReader(f)]
    counters_path = run_dir / "counters.json"
    counters = json.loads(counters_path.read_text(encoding="utf-8")) if counters_path.is_file() else {}
    return {"dir": run_dir.name, "manifest": manifest, "hists": hists, "samples": samples, "counters": counters}


def run_input(run: dict) -> criteria.RunInput:
    m = run["manifest"]
    lag = analysis.lag_series(run["samples"], m["window"]["start_ms"], m["window"]["end_ms"])
    return criteria.RunInput(m["rate_per_s"], run["hists"]["e2e"], run["hists"]["generator_late"], lag,
                             run["counters"]["sent"], run["counters"]["meter_distinct"], list(m.get("run_problems", [])))


def fmt(value) -> str:
    if value is None:
        return "-"
    return f"{value:.1f}" if isinstance(value, float) else str(value)


def triple(h: Histogram | None) -> str:
    if h is None or h.total == 0:
        return "-"
    s = h.summary()
    return f"{fmt(s['p50'])} / {fmt(s['p95'])} / {fmt(s['p99'])} (n={s['n']})"


def environment_problems(m: dict) -> list[str]:
    """A calibration run's environment problems. Manifests written before DETENTE 3.3.e have no such field: their
    run_problems held the environment problems plus the generator's acknowledgement check ("sent N of M,
    acknowledged K"), now judged by criteria.generator_calibration from the counters instead."""
    if "environment_problems" in m:
        return list(m["environment_problems"])
    return [p for p in m.get("run_problems", []) if not p.startswith("sent ")]


def calibration(runs: list[dict], bench_fraction: float, t: criteria.Thresholds) -> tuple[float | None, list[str]]:
    """(ladder cap, lines): every calibration run re-evaluated from its stored data with the current criterion."""
    cal = sorted((r for r in runs if r["manifest"]["mode"] in ("calibrate-generator", "calibrate-meter")),
                 key=lambda r: (r["manifest"]["mode"], r["manifest"]["rate_per_s"], r["dir"]))
    if not cal:
        return None, ["No calibration runs."]
    lines = ["Every calibration run, re-evaluated here from its stored data with the current criterion (owner, "
             "DETENTE 3.3.e: the ladder's own rule for a client's CPU; pending acknowledgements at saturation mean "
             "\"not sustained\"). \"Recorded\" is the verdict its manifest wrote when it ran.", "",
             "| Run | Mode | Rate/s | Valid | Sustained | Recorded | Lateness p50/p95/p99 ms | Client CPU peak | "
             "Min host free GB | Reasons |", "|---|---|---|---|---|---|---|---|---|---|"]
    generator, meter_caps = [], []
    for r in cal:
        m = r["manifest"]
        mem = fmt(m.get("min_host_free_memory_gb"))
        recorded = f"valid {'yes' if m.get('valid') else 'no'}" + (
            f", sustained {'yes' if m.get('sustained') else 'no'}" if m["mode"] == "calibrate-generator" else "")
        if m["mode"] == "calibrate-generator":
            c, cpu = m["counters"], (m.get("cpu_peak_percent") or {}).get("generator")
            v = criteria.generator_calibration(c["planned"], c["sent"], c["acknowledged"], m.get("stopped_early", False),
                                               r["hists"]["generator_late"], cpu, environment_problems(m), t, bench_fraction)
            generator.append((m["rate_per_s"], v))
            lines.append(f"| {r['dir']} | generator | {m['rate_per_s']:g} | {'yes' if v.valid else 'no'} | "
                         f"{'yes' if v.sustained else 'no'} | {recorded} | {triple(r['hists'].get('generator_late'))} | "
                         f"generator {fmt(cpu)} % | {mem} | {'; '.join(v.reasons) or '-'} |")
        else:
            cal_m, cpu = m.get("calibration", {}), (m.get("cpu_peak_percent") or {}).get("meter-calibration")
            reasons = list(m.get("problems", []))
            if m.get("valid") and cal_m.get("records_per_s"):
                cap = criteria.meter_calibration_cap(cal_m["records_per_s"], cpu, bench_fraction)
                meter_caps.append(cap)
                reasons.append(f"read {cal_m['read']} records in {fmt(cal_m['elapsed_s'])} s, {fmt(cal_m['records_per_s'])}"
                               f" records/s; within {bench_fraction * 100:.0f} % of a core: {fmt(cap)}/s "
                               "(assumes its CPU proportional to its rate)")
            lines.append(f"| {r['dir']} | meter | {fmt(cal_m.get('records_per_s'))} | {'yes' if m.get('valid') else 'no'} | - | "
                         f"{recorded} | - | meter {fmt(cpu)} % | {mem} | {'; '.join(reasons) or '-'} |")
    gen_cap, meter_cap, cap, note = criteria.calibrated_cap(generator, meter_caps)
    rate = lambda v: "-" if v is None else f"{v:g}/s"  # noqa: E731
    lines.append("")
    lines.append(f"Generator cap {rate(gen_cap)}; meter cap {rate(meter_cap)}" + (f"; {note}" if note else "") + ".")
    if cap is not None:
        lines.append(f"Ladder cap: {cap:g}/s (the lower of generator {rate(gen_cap)} and meter {rate(meter_cap)}).")
    return cap, lines


def render(runs: list[dict], t: criteria.Thresholds, ladder_per_s: tuple[float, ...] = (),
           max_core_fraction: float = 0.8) -> str:
    lines = ["# Benchmark figures (generated by bench/report.py; do not edit)", "",
             f"Thresholds (D8): p99 < {t.p99_ms:g} ms, lag <= {t.max_lag_s:g} s, slope <= {t.max_lag_slope_per_s:g} s/s, "
             f"lost <= {t.max_lost}; generator lateness p99 <= {t.max_late_p99_ms:g} ms, max <= {t.max_late_max_ms:g} ms.", "",
             DEFINITIONS]

    cap, cal_lines = calibration(runs, max_core_fraction, t)
    effective, capped = criteria.cap_ladder(ladder_per_s, cap)
    lines += ["## Calibration", ""] + cal_lines
    if cap is not None:
        if capped:
            lines.append(f"The ladder is capped there: steps above {cap:g}/s are not run; a figure at the cap is "
                         "limited by the measuring tools, not by the pipeline.")

    pipeline = [r for r in runs if r["manifest"]["mode"] in ("latency", "throughput")]
    lines += ["", "## Runs", "",
              "| Run | Mode | Rate/s | Bridge log | Valid | Publishable | F1 e2e p50/p95/p99 ms | ingest | produce_to_visible | "
              "F2 alarm | F4 produce-to-ack | Generator late | Problems |",
              "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for run in pipeline:
        m, h = run["manifest"], run["hists"]
        v = criteria.evaluate(run_input(run), t)
        publishable = (not m["trial"]) and v.valid
        # D8 reasons belong to throughput runs only (see bench/harness.py); others show validity problems alone.
        shown = v.reasons if (m["mode"] == "throughput" and not m["trial"]) or not v.valid else []
        kind = "trial" if m["trial"] else m["mode"] + (" (sensitivity)" if m.get("sensitivity") else "")
        lines.append(f"| {run['dir']} | {kind} | {m['rate_per_s']:g} | {m.get('bridge_log_level', '-')} | "
                     f"{'yes' if v.valid else 'no'} | {'yes' if publishable else 'no'} | {triple(h.get('e2e'))} | "
                     f"{triple(h.get('ingest'))} | {triple(h.get('produce_to_visible'))} | {triple(h.get('alarm_e2e'))} | "
                     f"{triple(h.get('produce_to_ack'))} | {triple(h.get('generator_late'))} | {'; '.join(shown) or '-'} |")

    headline = [r for r in pipeline if r["manifest"]["mode"] == "throughput" and not r["manifest"]["trial"]
                and not r["manifest"].get("sensitivity")]
    lines += ["", "## Throughput ladder (F3, D8; bridge at the product default log level)", ""]
    if not headline:
        lines.append("No throughput runs.")
        return "\n".join(lines) + "\n"
    first = {}
    for r in sorted(headline, key=lambda r: (r["manifest"]["rate_per_s"], r["manifest"]["repetition"])):
        first.setdefault(r["manifest"]["rate_per_s"], r)
    result = criteria.ladder([run_input(r) for r in first.values()], t)
    for rate, v in result.steps:
        lines.append(f"- {rate:g}/s: {'sustainable' if v.sustainable else 'not sustainable'}"
                     f"{'' if v.sustainable else ' - ' + '; '.join(v.reasons)}")
    if result.max_sustainable_rate is None:
        lines.append("\nNo sustainable step.")
        return "\n".join(lines) + "\n"
    step = criteria.intermediate_needed(result, effective)
    if step is not None:
        lines.append(f"\nNext: the intermediate step {step:g}/s, between {result.max_sustainable_rate:g} and "
                     f"{result.first_failure[0]:g}, before the E2 repetitions.")
    reps = [run_input(r) for r in headline if r["manifest"]["rate_per_s"] == result.max_sustainable_rate]
    ok, _ = criteria.confirmed(reps, t)
    lines.append(f"\nCandidate: {result.max_sustainable_rate:g}/s; repetitions {len(reps)}; "
                 f"{'confirmed' if ok and len(reps) > 1 else 'not yet confirmed (E2)'}.")
    if result.limited_by_generator:
        lines.append("Limited by the generator: the step above the candidate failed because the generator saturated.")

    sensitivity = [r for r in pipeline if r["manifest"].get("sensitivity")]
    if sensitivity:
        lines += ["", "## Log-level sensitivity (same steps, bridge at the sensitivity log level)", "",
                  "| Rate/s | Log level | Runs | Sustainable | F1 e2e p50/p95/p99 ms |", "|---|---|---|---|---|"]
        for rate in sorted({r["manifest"]["rate_per_s"] for r in sensitivity}):
            for group in ([r for r in headline if r["manifest"]["rate_per_s"] == rate],
                          [r for r in sensitivity if r["manifest"]["rate_per_s"] == rate]):
                if not group:
                    continue
                merged = Histogram()
                for r in group:
                    merged = merged.merge(r["hists"]["e2e"])
                passed = sum(criteria.evaluate(run_input(r), t).sustainable for r in group)
                lines.append(f"| {rate:g} | {group[0]['manifest'].get('bridge_log_level', '-')} | {len(group)} | "
                             f"{passed} of {len(group)} | {triple(merged)} |")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:  # pragma: no cover - thin command line over render()
    bench = load_benchmark_config()
    p = argparse.ArgumentParser(prog="python -m bench.report")
    p.add_argument("--results", type=Path, default=REPO_ROOT / "bench" / "results")
    p.add_argument("--p99-ms", type=float, default=bench.criteria_p99_ms)
    p.add_argument("--max-lag-s", type=float, default=bench.criteria_max_lag_s)
    p.add_argument("--max-lag-slope", type=float, default=bench.criteria_max_lag_slope_per_s)
    p.add_argument("--max-lost", type=int, default=bench.criteria_max_lost)
    args = p.parse_args(argv)
    runs = [load_run(d) for d in sorted(args.results.iterdir()) if (d / "manifest.json").is_file()]
    t = criteria.Thresholds(args.p99_ms, args.max_lag_s, args.max_lag_slope, args.max_lost,
                            bench.max_late_p99_ms, bench.max_late_max_ms)
    sys.stdout.write(render(runs, t, bench.ladder_per_s, bench.max_client_core_fraction))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
