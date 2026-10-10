"""What the harness computes from a run's records, free of I/O (unit-tested).

- F2 from the alarm consumer's alarm_raised/alarm_cleared lines (event_to_alarm_ack_ms: the triggering sample's ts,
  which the generator sets to its scheduled instant, to the alarm's acknowledgement), and F4 from the bridge's
  message_delivered lines (produce_to_ack_ms), both for the measured window;
- the lag series (owner decision D8): at each monitor sample, the larger of the pipeline lag (messages already
  scheduled minus records in telemetry.raw) and the alarm consumer's lag (telemetry.raw records it has not
  committed), as (seconds since the window started, lag in messages);
- the integrity check (protocol 5.1.8): sent = new telemetry.raw records = distinct (asset, seq) seen by the meter,
  duplicates and gaps counted, no new dead letter, the consumer caught up, alarms as the pattern predicts;
- CPU per container: a client container above the configured share of one core flags the run;
- the run plan of STEP 2 with its duration estimate (amendment E2, cool-down pauses).
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from bench.criteria import RunInput
from bench.histogram import Histogram
from config.loader import BenchmarkConfig

CLIENTS = ("bridge", "alarm-consumer", "generator", "meter")


def json_lines(text: str) -> list[dict]:
    out = []
    for line in text.splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if isinstance(entry, dict):
            out.append(entry)
    return out


def alarm_latency(consumer_log: str, start_ms: int, end_ms: int) -> tuple[Histogram, dict[str, int], dict[str, int]]:
    """F2 histogram and the RAISED/CLEARED counts for samples in the window, and the counts over the whole run."""
    h, window, total = Histogram(), {"RAISED": 0, "CLEARED": 0}, {"RAISED": 0, "CLEARED": 0}
    for e in json_lines(consumer_log):
        state = {"alarm_raised": "RAISED", "alarm_cleared": "CLEARED"}.get(e.get("event"))
        if state is None:
            continue
        total[state] += 1
        if start_ms <= int(e["sample_ts"]) < end_ms:
            window[state] += 1
            h.add(float(e["event_to_alarm_ack_ms"]))
    return h, window, total


def produce_to_ack(bridge_log: str, start_ms: int, end_ms: int) -> Histogram:
    """F4: the bridge's produce_to_ack_ms for messages received in the window (needs the bridge at INFO)."""
    h = Histogram()
    for e in json_lines(bridge_log):
        if e.get("event") == "message_delivered" and start_ms <= float(e["receive_ts_ms"]) < end_ms:
            h.add(float(e["produce_to_ack_ms"]))
    return h


@dataclass(frozen=True)
class LagSample:
    wall_ms: float
    scheduled: int  # messages the generator should have sent by wall_ms
    raw_records: int  # new records in telemetry.raw since the run began
    consumer_lag: int  # telemetry.raw records the alarm consumer has not committed


def scheduled_by(wall_ms: float, t0_ms: int, rate_per_s: float, planned: int) -> int:
    if wall_ms < t0_ms:
        return 0
    return min(planned, int((wall_ms - t0_ms) / 1000 * rate_per_s) + 1)


def lag_series(samples: list[LagSample], start_ms: int, end_ms: int) -> list[tuple[float, float]]:
    return [((s.wall_ms - start_ms) / 1000, float(max(s.scheduled - s.raw_records, s.consumer_lag, 0)))
            for s in samples if start_ms <= s.wall_ms < end_ms]


def integrity(generator: dict, meter: dict, raw_new: int, dlq_new: int, consumer_lag_end: int,
              alarms_total: dict[str, int], alarms_expected: dict[str, int]) -> tuple[dict, list[str]]:
    counts = {"planned": generator["planned"], "sent": generator["sent"], "acknowledged": generator["acknowledged"],
              "raw_new_records": raw_new, "meter_records": meter["records"], "meter_distinct": meter["distinct"],
              "meter_duplicates": meter["duplicates"], "meter_gaps": meter["gaps"], "dlq_new_records": dlq_new,
              "consumer_lag_at_end": consumer_lag_end, "alarms": alarms_total, "alarms_expected": alarms_expected}
    problems = []
    if generator["sent"] != generator["planned"]:
        problems.append(f"generator sent {generator['sent']} of {generator['planned']}")
    if generator["acknowledged"] != generator["sent"]:
        problems.append(f"Mosquitto acknowledged {generator['acknowledged']} of {generator['sent']}")
    if raw_new != generator["sent"] + meter["duplicates"]:
        problems.append(f"telemetry.raw gained {raw_new} records for {generator['sent']} sent")
    if meter["distinct"] != generator["sent"] or meter["gaps"]:
        problems.append(f"meter saw {meter['distinct']} distinct of {generator['sent']} sent, {meter['gaps']} gap(s)")
    if dlq_new:
        problems.append(f"{dlq_new} new dead letter(s)")
    if consumer_lag_end:
        problems.append(f"alarm consumer {consumer_lag_end} record(s) behind at the end")
    if alarms_total != alarms_expected:
        problems.append(f"alarms {alarms_total} for {alarms_expected} expected")
    return counts, problems


def cpu_problems(cpu_samples: list[tuple[float, dict[str, float]]],
                 max_core_fraction: float) -> tuple[dict[str, float], list[str]]:
    """Peak CPU per container (per cent of one core) and the clients above the limit."""
    peaks: dict[str, float] = {}
    for _, row in cpu_samples:
        for name, pct in row.items():
            peaks[name] = max(peaks.get(name, 0.0), pct)
    over = [f"{name} at {pct:.0f} % of a core" for name, pct in sorted(peaks.items())
            if name in CLIENTS and pct > max_core_fraction * 100]
    return peaks, ([f"CPU-bound client(s): {', '.join(over)}"] if over else [])


def run_input(rate: float, meter: dict, generator: dict, lag: list[tuple[float, float]], problems: list[str]) -> RunInput:
    return RunInput(rate, Histogram.from_json(meter["histograms"]["e2e"]), Histogram.from_json(generator["late"]), lag,
                    generator["sent"], meter["distinct"], problems)


# --- run plan (1.3.7) ------------------------------------------------------------------

# Measured in the STEP 1 trial: 133 s in all for 60 s of sending, so set-up, drain and teardown took about 73 s.
SETUP_S = 60  # recreate the stack, build check, provision, start the pipeline containers, the meter and the generator
TEARDOWN_S = 15
DRAIN_TYPICAL_S = 15


def run_seconds(bench: BenchmarkConfig, measured_s: float | None = None) -> float:
    measured = bench.measured_s if measured_s is None else measured_s
    return SETUP_S + bench.warmup_s + measured + DRAIN_TYPICAL_S + TEARDOWN_S + bench.cooldown_s


def plan(bench: BenchmarkConfig) -> dict:
    """Every run of STEP 2 per E2, and the time on AC power: the ladder's repetitions depend on where it stops, so
    the estimate takes the worst case (repetitions at the two highest steps)."""
    runs = [{"mode": "latency", "rate_per_s": r, "repetition": k + 1}
            for r in bench.latency_rates_per_s for k in range(bench.latency_repetitions)]
    runs += [{"mode": "throughput", "rate_per_s": r, "repetition": 1} for r in bench.ladder_per_s]
    worst = bench.ladder_per_s[-2:] if len(bench.ladder_per_s) > 1 else bench.ladder_per_s
    reps = [{"mode": "throughput", "rate_per_s": r, "repetition": k + 2, "conditional": "E2: candidate step and the next"}
            for r in worst for k in range(bench.repetitions_at_candidate - 1)]
    per_run = run_seconds(bench)
    total = per_run * (len(runs) + len(reps))
    return {"runs": runs, "conditional_runs": reps, "seconds_per_run": per_run,
            "total_hours_worst_case": round(total / 3600, 2),
            "total_hours_without_repetitions": round(per_run * len(runs) / 3600, 2)}
