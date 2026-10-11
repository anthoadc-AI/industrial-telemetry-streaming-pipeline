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
    assert r["summaries"]["e2e"]["n"] == 2 and r["summaries"]["ingest"]["n"] == 1
    assert Histogram.from_json(r["histograms"]["produce_to_visible"]).max_ms == 8.5
    assert r["series"][0][:2] == [10, 1]
    assert 11_000 in m.window and 13_000 not in m.window
    assert r["negatives"] == []


def test_meter_counts_negative_samples_and_keeps_their_time():
    """Negative samples (a later event read earlier than its cause, two clocks): counted, never dropped."""
    m = meter.Meter(meter.Window(t0_ms=0, warmup_s=0, measured_s=10))
    m.record("a", 0, 1_000, 998, 999.5)  # e2e -0.5, ingest -2, produce_to_visible 1.5
    m.record("a", 1, 2_000, 2_001, 2_003.0)
    r = m.result()
    assert r["negatives"] == [[999.5, "e2e", -0.5], [999.5, "ingest", -2]]
    assert Histogram.from_json(r["histograms"]["e2e"]).underflow == 1
    assert Histogram.from_json(r["histograms"]["ingest"]).underflow == 1


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


def test_cpu_rule_applies_to_the_instruments_only():
    """Owner, DETENTE 3.3.f: the bridge and the alarm consumer are the system under test, judged by D8 alone; their
    CPU is data. Only the generator and the meter above the limit invalidate a run."""
    samples = [(0, {"bridge": 50.0, "broker": 150.0}), (1, {"bridge": 102.0, "alarm-consumer": 95.0, "generator": 10.0})]
    peaks, problems = analysis.cpu_problems(samples, 0.8)
    assert peaks == {"bridge": 102.0, "broker": 150.0, "alarm-consumer": 95.0, "generator": 10.0}
    assert problems == []
    _, problems = analysis.cpu_problems([(0, {"generator": 89.4, "meter": 81.0, "bridge": 99.0})], 0.8)
    assert problems == ["CPU-bound instrument(s): generator at 89 % of a core, meter at 81 % of a core"]
    assert analysis.cpu_problems([], 0.8) == ({}, [])
    stored = ["CPU-bound client(s): bridge at 102 % of a core", "not on AC power"]
    assert analysis.without_system_under_test_cpu(stored) == ["not on AC power"]


def test_clock_quality_steps_drift_and_alignment():
    """Owner, DETENTE 3.3.f: wall minus monotonic, every ~100 ms; a backward step and the negatives after it."""
    samples = [(0.0, 5.0), (100.0, 5.01), (200.0, 5.0), (300.0, 0.2), (400.0, 0.21), (500.0, 1.5), (600.0, 1.5)]
    q = analysis.clock_quality(samples, [350.0, 900.0, 5000.0, 250.0])
    assert q["samples"] == 7 and q["largest_backward_step_ms"] == 4.8 and q["backward_steps"] == 1
    assert q["forward_steps"] == 1 and q["total_drift_ms"] == -3.5
    # 350 and 900 fall within 1 s after the step at 300; 5000 is too late; 250 comes before it.
    assert (q["negative_samples"], q["negatives_after_a_backward_step"]) == (4, 2)
    assert analysis.clock_quality([], [1.0]) == {"samples": 0, "largest_backward_step_ms": 0.0, "backward_steps": 0,
                                                 "forward_steps": 0, "total_drift_ms": 0.0, "negative_samples": 1,
                                                 "negatives_after_a_backward_step": 0, "window_ms": 1000.0}


def test_clock_sampler_records_wall_minus_monotonic():
    wall, mono = iter([1000.0, 1000.1]), iter([10.0, 10.1005])
    c = meter.ClockSampler(wall=lambda: next(wall), mono=lambda: next(mono))
    c.sample()
    c.sample()
    assert c.samples == [(1_000_000.0, 990_000.0), (1_000_100.0, 989_999.5)]


def test_run_input_and_plan():
    ri = analysis.run_input(100.0, meter_result(), gen_result(), [(0, 1)], ["x"])
    assert (ri.sent, ri.seen_distinct, ri.environment_problems, ri.e2e.total) == (100, 100, ["x"], 1)
    p = analysis.plan(BENCH)
    worst, expected = p["worst"], p["expected"]
    assert [b["block"][0] for b in worst["blocks"]] == ["1", "2", "3", "4", "5"]  # calibration first (DETENTE 3.3.d)
    blocks = {b["block"][0]: b["runs"] for b in worst["blocks"]}
    assert blocks["1"] == len(BENCH.calibration_generator_rates_per_s) + 1  # every generator rate, then the meter
    assert blocks["2"] == len(BENCH.latency_rates_per_s) * BENCH.latency_repetitions
    assert blocks["3"] == len(BENCH.ladder_per_s) + 1  # plus the intermediate step
    assert blocks["4"] == 2 * (BENCH.repetitions_at_candidate - 1) and blocks["5"] == 2 * BENCH.sensitivity_repetitions
    assert worst["runs"] == sum(blocks.values()) and expected["runs"] < worst["runs"] and expected["hours"] < worst["hours"]
    assert p["seconds_per_pipeline_run"] == analysis.run_seconds(BENCH)
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
    assert o["loader"]["command"] == ["--count", "${BENCH_COUNT:-}"] and o["loader"]["cpuset"] == "6"
    assert "--from-beginning" in o["meter-calibration"]["command"]


# --- generator and harness helpers ---------------------------------------------------------


def test_generator_args_and_acks(tmp_path):
    a = generator.parse_args(["--rate", "5", "--duration", "60", "--t0-ms", "1", "--out", str(tmp_path)])
    assert (a.rate, a.duration, a.t0_ms, a.out) == (5.0, 60.0, 1, tmp_path)
    acks = generator.Acks()
    acks.on_publish(None, None, 1, None, None)
    assert acks.count == 1
    m = meter.parse_args(["--t0-ms", "1", "--warmup", "30", "--measured", "300", "--out", str(tmp_path)])
    assert (m.warmup, m.measured, m.from_beginning, m.expect) == (30.0, 300.0, False, None)
    m = meter.parse_args(["--t0-ms", "0", "--warmup", "0", "--measured", "0", "--out", str(tmp_path), "--from-beginning",
                          "--expect", "10"])
    assert (m.from_beginning, m.expect) == (True, 10)
    from bench import loader
    assert loader.parse_args(["--count", "5"]).count == 5


def test_harness_run_id_and_compose_command(tmp_path):
    now = datetime(2026, 10, 10, 12, 0, 0, tzinfo=timezone.utc)
    assert harness.run_id("latency", 5.0, 2, False, now) == "20261010T120000Z-latency-5-r2"
    assert harness.run_id("latency", 5.0, 1, True, now) == "20261010T120000Z-trial-5-r1"
    cmd = harness.compose_cmd(tmp_path / "o.yml", "up")
    assert cmd[:3] == ["docker", "compose", "-f"] and cmd[-3:] == ["-f", str(tmp_path / "o.yml"), "up"]


# --- report ----------------------------------------------------------------------------------


def write_run(root: Path, name: str, mode: str, rate: float, latency: float, trial: bool = False, repetition: int = 1,
              problems=None, sensitivity: bool = False) -> None:
    d = root / name
    d.mkdir()
    e2e, late = Histogram(), Histogram()
    for _ in range(200):
        e2e.add(latency)
        late.add(0.4)
    window = {"start_ms": 0, "end_ms": 10_000}
    (d / "manifest.json").write_text(json.dumps({"mode": mode, "trial": trial, "rate_per_s": rate, "repetition": repetition,
                                                 "valid": not problems,
                                                 "window": window, "run_problems": problems or [], "sensitivity": sensitivity,
                                                 "bridge_log_level": "WARNING" if sensitivity else "INFO"}),
                                     encoding="utf-8")
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
    text = report.render(runs, t, (250.0, 500.0, 1000.0, 2000.0))
    assert "| a-trial | trial | 5 | INFO | yes | valid yes | no |" in text  # a trial is never publishable
    assert "- 1000/s: not sustainable - p99 not below 1000 ms" in text
    assert "Next: the intermediate step 750/s, between 500 and 1000" in text  # DETENTE 3.3.d
    assert "Candidate: 500/s; runs 1 of the 5 E2 needs; not yet confirmed (E2)." in text
    assert "| 12 / 12 / - (n=200) |" in text  # whole milliseconds, rounded up; n=200 has no p99 (DETENTE 3.3.f)
    assert "resolution" in text and "is 1 ms" in text and "Minimum and maximum latency are not results" in text
    assert "dominated by linger.ms" in text and "No calibration runs." in text
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


def write_calibration(root: Path, name: str, mode: str, rate: float, cpu: float = 50.0, records_per_s=None, acked=None,
                      late=0.3, manifest_extra=None) -> None:
    """A calibration run as the harness stores it; without manifest_extra, as written before DETENTE 3.3.e."""
    d = root / name
    d.mkdir()
    h = Histogram()
    for _ in range(100):
        h.add(late)
    manifest = {"mode": mode, "rate_per_s": rate, "trial": False, "valid": True, "sustained": True, "run_problems": [],
                "min_host_free_memory_gb": 5.7,
                "cpu_peak_percent": {"generator" if mode == "calibrate-generator" else "meter-calibration": cpu}}
    if mode == "calibrate-meter":
        manifest["calibration"] = {"read": 1000, "elapsed_s": 1000 / records_per_s, "records_per_s": records_per_s}
    else:
        sent = int(rate * 60)
        manifest["counters"] = {"planned": sent, "sent": sent, "acknowledged": sent if acked is None else acked}
        if acked is not None and acked < sent:  # the old acknowledgement check, as the old harness recorded it
            manifest.update({"valid": False, "sustained": False,
                             "run_problems": [f"sent {sent} of {sent}, acknowledged {acked}"]})
        (d / "latency_generator_late.json").write_text(json.dumps(h.to_json()), encoding="utf-8")
    manifest.update(manifest_extra or {})
    (d / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def test_report_calibration_caps_the_ladder_and_sensitivity(tmp_path):
    write_calibration(tmp_path, "c1", "calibrate-generator", 1000)
    write_calibration(tmp_path, "c2", "calibrate-generator", 2000)
    write_calibration(tmp_path, "c3", "calibrate-generator", 4000, cpu=89.4)
    write_calibration(tmp_path, "c4", "calibrate-meter", 600000, cpu=60.0, records_per_s=3000.0)
    write_run(tmp_path, "t1", "throughput", 250, 20.0)
    write_run(tmp_path, "t2", "throughput", 250, 15.0, sensitivity=True)
    runs = [report.load_run(d) for d in sorted(tmp_path.iterdir())]
    t = criteria.Thresholds(1000, 1.0, 0.001, 0, 5.0, 100.0)
    text = report.render(runs, t, (250.0, 500.0, 1000.0, 2000.0, 4000.0), 0.8)
    # Every calibration run, with validity, verdict, CPU and minimum host free memory (DETENTE 3.3.e).
    assert "| c3 | generator | 4000 | yes | no | valid yes, sustained yes |" in text
    assert "generator 89.4 % | 5.7 | generator at 89 % of a core (limit 80 %) |" in text
    assert "| c4 | meter | 3000.0 | yes | - | valid yes | - | meter 60.0 % | 5.7 |" in text
    assert "Ladder cap: 2000/s (the lower of generator 2000/s and meter 3000/s)." in text
    assert "steps above 2000/s are not run" in text
    assert "| 250 | INFO | 1 | 1 of 1 |" in text and "| 250 | WARNING | 1 | 1 of 1 |" in text
    assert "## Throughput ladder" in text and "- 250/s: sustainable" in text  # the sensitivity run is not on the ladder


def test_report_reevaluates_stored_runs_and_needs_both_calibrations(tmp_path):
    """Block 1's runs, from their stored data: the old acknowledgement check made 8000/s invalid; under the single
    criterion it is a valid run that did not sustain the rate. No meter run: no ladder cap."""
    write_calibration(tmp_path, "g1000", "calibrate-generator", 1000)
    write_calibration(tmp_path, "g8000", "calibrate-generator", 8000, cpu=101.0, acked=283_395, late=7.4)
    runs = [report.load_run(d) for d in sorted(tmp_path.iterdir())]
    text = report.render(runs, criteria.Thresholds(1000, 1.0, 0.001, 0, 5.0, 100.0), (1000.0, 8000.0), 0.8)
    assert "| g8000 | generator | 8000 | yes | no | valid no, sustained no |" in text
    assert "196605 acknowledgements still pending when its wait ended" in text
    assert "Generator cap 1000/s; meter cap -; no ladder cap: meter not calibrated." in text
    assert "Ladder cap" not in text and "are not run" not in text
    # A manifest written after the change keeps its environment problems apart: one of them makes the run invalid.
    write_calibration(tmp_path, "g2000", "calibrate-generator", 2000,
                      manifest_extra={"environment_problems": ["not on AC power"]})
    text = report.render([report.load_run(d) for d in sorted(tmp_path.iterdir())],
                         criteria.Thresholds(1000, 1.0, 0.001, 0, 5.0, 100.0), (), 0.8)
    assert "| g2000 | generator | 2000 | no | no |" in text and "Generator cap 1000/s" in text
    legacy = {"run_problems": ["host free memory fell to 1.2 GB", "sent 1 of 2, acknowledged 0"]}
    assert report.environment_problems(legacy) == ["host free memory fell to 1.2 GB"]


def test_report_reevaluates_the_cpu_rule_and_shows_both_verdicts(tmp_path):
    """Block 3's 2000 msg/s runs: recorded invalid for the bridge's CPU, valid under DETENTE 3.3.f's rule."""
    write_run(tmp_path, "a-1000", "throughput", 1000, 20.0)
    write_run(tmp_path, "b-2000", "throughput", 2000, 40.0, problems=["CPU-bound client(s): bridge at 102 % of a core"])
    write_run(tmp_path, "c-3000", "throughput", 3000, 40.0)
    m = json.loads((tmp_path / "b-2000" / "manifest.json").read_text(encoding="utf-8"))
    m.update({"valid": False, "sustainable": False, "cpu_peak_percent": {"bridge": 102.0, "generator": 61.0}})
    (tmp_path / "b-2000" / "manifest.json").write_text(json.dumps(m), encoding="utf-8")
    m = json.loads((tmp_path / "c-3000" / "manifest.json").read_text(encoding="utf-8"))
    m["cpu_peak_percent"] = {"generator": 85.0}  # an instrument over the limit: still invalid
    (tmp_path / "c-3000" / "manifest.json").write_text(json.dumps(m), encoding="utf-8")
    runs = [report.load_run(d) for d in sorted(tmp_path.iterdir())]
    t = criteria.Thresholds(1000, 1.0, 0.001, 0, 5.0, 100.0)
    text = report.render(runs, t, (1000.0, 2000.0, 3000.0), 0.8, 5)
    assert "| b-2000 | throughput | 2000 | INFO | yes | valid no, sustainable no |" in text
    assert "| c-3000 | throughput | 3000 | INFO | no |" in text and "CPU-bound instrument(s): generator at 85 %" in text
    assert "- 2000/s: sustainable" in text and "- 3000/s: not sustainable" in text
    assert report.run_problems({"run_problems": ["CPU-bound instrument(s): meter at 90 % of a core"],
                                "cpu_peak_percent": {"meter": 90.0}}, 0.8) == ["CPU-bound instrument(s): meter at 90 % of a core"]


def test_report_clock_negatives_and_repetitions(tmp_path):
    for i, latency in enumerate((7.2, 7.6, 8.4)):
        write_run(tmp_path, f"r{i}", "latency", 50, latency, repetition=i + 1)
    m = json.loads((tmp_path / "r0" / "manifest.json").read_text(encoding="utf-8"))
    m["clock"] = analysis.clock_quality([(0.0, 5.0), (100.0, 0.2)], [150.0])
    (tmp_path / "r0" / "manifest.json").write_text(json.dumps(m), encoding="utf-8")
    h = Histogram()
    h.add(-1.0)
    h.add(3.0)
    (tmp_path / "r0" / "latency_ingest.json").write_text(json.dumps(h.to_json()), encoding="utf-8")
    runs = [report.load_run(d) for d in sorted(tmp_path.iterdir())]
    text = report.render(runs, criteria.Thresholds(1000, 1.0, 0.001, 0, 5.0, 100.0), (), 0.8, 5)
    assert ("| 0 / 1 / - / - / - | 4.8 ms back (1 steps), drift -4.8 ms; 1 of 1 negatives within 1 s after a "
            "backward step |") in text
    assert "| 0 / - / - / - / - | not sampled |" in text  # runs before DETENTE 3.3.f
    # Median and range across the three repetitions, whole milliseconds rounded up: 7.2, 7.6, 8.4 -> 8 (8-9).
    assert "| latency | 50 | INFO | 3 | F1 e2e | 8 (8-9) | 8 (8-9) | - | 200 |" in text  # n=200: no p99
    assert report.spread([1.0, None]) == "-" and report.ms(None) == "-" and report.ms(7.0) == "7"


def test_report_ladder_complete_up_to_the_cap_and_e2_needs_its_runs(tmp_path):
    write_calibration(tmp_path, "c1", "calibrate-generator", 1000)
    write_calibration(tmp_path, "c2", "calibrate-meter", 600000, cpu=60.0, records_per_s=9000.0)
    for i in range(5):
        write_run(tmp_path, f"t{i}", "throughput", 1000, 20.0, repetition=i + 1)
    write_run(tmp_path, "u", "throughput", 500, 20.0)
    runs = [report.load_run(d) for d in sorted(tmp_path.iterdir())]
    text = report.render(runs, criteria.Thresholds(1000, 1.0, 0.001, 0, 5.0, 100.0), (500.0, 1000.0, 2000.0), 0.8, 5)
    assert "Every step up to the cap is sustainable: the result is at least 1000/s" in text
    assert "Candidate: 1000/s; runs 5 of the 5 E2 needs; confirmed." in text

