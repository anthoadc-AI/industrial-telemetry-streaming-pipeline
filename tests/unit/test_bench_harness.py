"""bench/: the meter's bookkeeping, the harness's analysis, the compose override, the report, and the
generator's and harness's pure helpers. Their I/O mains run in containers or on the host and are exercised by
the harness trial run (STEP 1, 1.3.6)."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from bench import analysis, compose, criteria, generator, harness, meter, report
from bench.histogram import Histogram
from config.loader import load_benchmark_config

BENCH = load_benchmark_config()


# --- meter -----------------------------------------------------------------------------


def test_seq_tracker_counts_distinct_duplicates_and_gaps():
    t = meter.SeqTracker()
    for asset, seq in [("a", 0), ("a", 1), ("a", 1), ("a", 3), ("b", 0), ("a", 20_000)]:
        t.add(asset, seq)
    assert (t.distinct, t.duplicates) == (5, 1)
    assert t.max_seq == {"a": 20_000, "b": 0}
    assert t.gaps() == (20_001 + 1) - 5
    with pytest.raises(ValueError):
        t.add("a", -1)


def test_meter_window_and_shares():
    m = meter.Meter(meter.Window(t0_ms=10_000, warmup_s=1, measured_s=2))  # window [11000, 13000)
    m.record("a", 0, 10_500, 10_520, 10_530.0)  # warm-up: counted for integrity, not measured
    m.record("a", 1, 11_000, 11_012, 11_020.5)
    m.record("a", 2, 12_999, None, 13_010.0)  # no CreateTime: e2e only
    m.record("a", 3, 13_000, 13_001, 13_002.0)  # end of window, excluded
    r = m.result()
    assert (r["records"], r["records_in_window"], r["distinct"], r["duplicates"], r["gaps"]) == (4, 2, 4, 0, 0)
    assert r["window"] == {"start_ms": 11_000, "end_ms": 13_000}
    assert r["summaries"]["e2e"]["n"] == 2 and r["summaries"]["bridge"]["n"] == 1
    assert Histogram.from_json(r["histograms"]["read"]).max_ms == 8.5
    assert r["series"][0][:2] == [10, 1]
    assert 11_000 in m.window and 13_000 not in m.window


# --- analysis ----------------------------------------------------------------------------


def log_lines(*entries: dict) -> str:
    return "\n".join(json.dumps(e) for e in entries) + "\nnot json\n[1, 2]\n"


def test_alarm_latency_and_produce_to_ack():
    consumer = log_lines({"event": "alarm_raised", "sample_ts": 1500, "event_to_alarm_ack_ms": 12.5},
                         {"event": "alarm_cleared", "sample_ts": 2500, "event_to_alarm_ack_ms": 8.0},
                         {"event": "alarm_raised", "sample_ts": 500, "event_to_alarm_ack_ms": 99.0},
                         {"event": "run_started"})
    h, window, total = analysis.alarm_latency(consumer, 1000, 3000)
    assert (h.total, window, total) == (2, {"RAISED": 1, "CLEARED": 1}, {"RAISED": 2, "CLEARED": 1})
    bridge = log_lines({"event": "message_delivered", "receive_ts_ms": 1000.5, "produce_to_ack_ms": 6.1},
                       {"event": "message_delivered", "receive_ts_ms": 999.0, "produce_to_ack_ms": 50.0})
    assert analysis.produce_to_ack(bridge, 1000, 2000).total == 1


def test_lag_series_takes_the_larger_lag_in_the_window():
    s = [analysis.LagSample(900, 10, 10, 0), analysis.LagSample(1000, 100, 90, 3), analysis.LagSample(2000, 200, 200, 25),
         analysis.LagSample(3000, 300, 310, 0)]
    assert analysis.lag_series(s, 1000, 3000) == [(0.0, 10.0), (1.0, 25.0)]
    assert analysis.scheduled_by(500, 1000, 10, 50) == 0
    assert analysis.scheduled_by(1000, 1000, 10, 50) == 1 and analysis.scheduled_by(99_999, 1000, 10, 50) == 50


def gen_result(sent=100, planned=100, acked=100):
    late = Histogram()
    late.add(0.5)
    return {"planned": planned, "sent": sent, "acknowledged": acked, "late": late.to_json()}


def meter_result(distinct=100, duplicates=0, gaps=0, records=100):
    e2e = Histogram()
    e2e.add(10.0)
    return {"records": records, "distinct": distinct, "duplicates": duplicates, "gaps": gaps,
            "histograms": {"e2e": e2e.to_json()}}


def test_integrity_clean_and_each_problem():
    alarms = {"RAISED": 4, "CLEARED": 4}
    counts, problems = analysis.integrity(gen_result(), meter_result(), 100, 0, 0, alarms, alarms)
    assert problems == [] and counts["raw_new_records"] == 100
    _, problems = analysis.integrity(gen_result(sent=99, acked=98), meter_result(distinct=97, gaps=1), 101, 2, 5,
                                     {"RAISED": 3, "CLEARED": 4}, alarms)
    assert [p.split()[0] for p in problems] == ["generator", "Mosquitto", "telemetry.raw", "meter", "2", "alarm", "alarms"]


def test_cpu_problems_flags_clients_only():
    samples = [(0, {"bridge": 50.0, "broker": 150.0}), (1, {"bridge": 95.0, "generator": 10.0})]
    peaks, problems = analysis.cpu_problems(samples, 0.8)
    assert peaks == {"bridge": 95.0, "broker": 150.0, "generator": 10.0}
    assert problems == ["CPU-bound client(s): bridge at 95 % of a core"]
    assert analysis.cpu_problems([], 0.8) == ({}, [])


def test_run_input_and_plan():
    ri = analysis.run_input(100.0, meter_result(), gen_result(), [(0, 1)], ["x"])
    assert (ri.sent, ri.seen_distinct, ri.environment_problems, ri.e2e.total) == (100, 100, ["x"], 1)
    p = analysis.plan(BENCH)
    assert len(p["runs"]) == len(BENCH.latency_rates_per_s) * BENCH.latency_repetitions + len(BENCH.ladder_per_s)
    assert len(p["conditional_runs"]) == 2 * (BENCH.repetitions_at_candidate - 1)
    assert p["seconds_per_run"] == analysis.run_seconds(BENCH)
    assert p["total_hours_worst_case"] > p["total_hours_without_repetitions"]
    assert analysis.run_seconds(BENCH, measured_s=30) < analysis.run_seconds(BENCH)


# --- compose override --------------------------------------------------------------------


def test_override_pins_mounts_and_adds_the_bench_services(tmp_path):
    o = compose.override(BENCH, tmp_path / "config", tmp_path / "out", "WARNING")["services"]
    assert o["broker"] == {"cpuset": "0-1"} and o["bridge"]["cpuset"] == "3"
    assert o["bridge"]["command"][-2:] == ["--log-level", "WARNING"]
    mount = o["alarm-consumer"]["volumes"][0]
    assert mount == {"type": "bind", "source": (tmp_path / "config").as_posix(), "target": "/app/config", "read_only": True}
    for name in ("generator", "meter"):
        assert o[name]["profiles"] == ["bench"] and o[name]["image"] == compose.BENCH_IMAGE and o[name]["read_only"]
        assert {"target": "/out", "read_only": False}.items() <= o[name]["volumes"][1].items()
    assert "${BENCH_T0_MS:-}" in o["generator"]["command"] and "${BENCH_T0_MS:-}" in o["meter"]["command"]


# --- generator and harness helpers ---------------------------------------------------------


def test_generator_args_and_acks(tmp_path):
    a = generator.parse_args(["--rate", "5", "--duration", "60", "--t0-ms", "1", "--out", str(tmp_path)])
    assert (a.rate, a.duration, a.t0_ms, a.out) == (5.0, 60.0, 1, tmp_path)
    acks = generator.Acks()
    acks.on_publish(None, None, 1, None, None)
    assert acks.count == 1
    m = meter.parse_args(["--t0-ms", "1", "--warmup", "30", "--measured", "300", "--out", str(tmp_path)])
    assert (m.warmup, m.measured) == (30.0, 300.0)


def test_harness_run_id_and_compose_command(tmp_path):
    now = datetime(2026, 10, 10, 12, 0, 0, tzinfo=timezone.utc)
    assert harness.run_id("latency", 5.0, 2, False, now) == "20261010T120000Z-latency-5-r2"
    assert harness.run_id("latency", 5.0, 1, True, now) == "20261010T120000Z-trial-5-r1"
    cmd = harness.compose_cmd(tmp_path / "o.yml", "up")
    assert cmd[:3] == ["docker", "compose", "-f"] and cmd[-3:] == ["-f", str(tmp_path / "o.yml"), "up"]


# --- report ----------------------------------------------------------------------------------


def write_run(root: Path, name: str, mode: str, rate: float, latency: float, trial: bool = False, repetition: int = 1,
              problems=None) -> None:
    d = root / name
    d.mkdir()
    e2e, late = Histogram(), Histogram()
    for _ in range(200):
        e2e.add(latency)
        late.add(0.4)
    window = {"start_ms": 0, "end_ms": 10_000}
    (d / "manifest.json").write_text(json.dumps({"mode": mode, "trial": trial, "rate_per_s": rate, "repetition": repetition,
                                                 "window": window, "run_problems": problems or []}), encoding="utf-8")
    (d / "latency_e2e.json").write_text(json.dumps(e2e.to_json()), encoding="utf-8")
    (d / "latency_generator_late.json").write_text(json.dumps(late.to_json()), encoding="utf-8")
    (d / "counters.json").write_text(json.dumps({"sent": 200, "meter_distinct": 200}), encoding="utf-8")
    with open(d / "series.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["wall_ms", "scheduled", "raw_records", "consumer_lag"])
        w.writerows([[t * 1000, 10 * t, 10 * t, 1] for t in range(10)])


def test_report_renders_runs_ladder_and_recomputes_the_cut(tmp_path):
    write_run(tmp_path, "a-trial", "latency", 5, 12.0, trial=True)
    write_run(tmp_path, "b-lat", "latency", 5, 14.0)
    write_run(tmp_path, "c-250", "throughput", 250, 20.0)
    write_run(tmp_path, "d-500", "throughput", 500, 20.0)
    write_run(tmp_path, "e-1000", "throughput", 1000, 1500.0)
    runs = [report.load_run(d) for d in sorted(tmp_path.iterdir())]
    t = criteria.Thresholds(1000, 1.0, 0.001, 0, 5.0, 100.0)
    text = report.render(runs, t)
    assert "| a-trial | trial | 5 | yes | no |" in text  # a trial is never publishable
    assert "- 1000/s: not sustainable - p99 not below 1000 ms" in text
    assert "Candidate: 500/s; repetitions 1; not yet confirmed (E2)." in text
    looser = report.render(runs, criteria.Thresholds(2000, 1.0, 0.001, 0, 5.0, 100.0))
    assert "Candidate: 1000/s" in looser  # same results, another threshold, no new run
    assert "No throughput runs." in report.render(runs[:2], t)
    assert report.fmt(None) == "-" and report.triple(None) == "-"


def test_report_no_sustainable_step_and_generator_limit(tmp_path):
    write_run(tmp_path, "a-250", "throughput", 250, 5000.0)
    t = criteria.Thresholds(1000, 1.0, 0.001, 0, 5.0, 100.0)
    assert "No sustainable step." in report.render([report.load_run(tmp_path / "a-250")], t)
    late = Histogram()
    for _ in range(10):
        late.add(50.0)
    write_run(tmp_path, "b-500", "throughput", 500, 10.0)
    (tmp_path / "b-500" / "latency_generator_late.json").write_text(json.dumps(late.to_json()), encoding="utf-8")
    write_run(tmp_path, "c-100", "throughput", 100, 10.0)
    runs = [report.load_run(tmp_path / n) for n in ("c-100", "b-500")]
    assert "Limited by the generator" in report.render(runs, t)
