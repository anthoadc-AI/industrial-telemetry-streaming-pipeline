"""bench/: the fixed histogram, the open-loop schedule, the expected alarms, the D8 criterion and the ladder,
the generated config of clones, and config/benchmark.yaml."""

from __future__ import annotations

import math
import shutil
from pathlib import Path

import pytest

from bench import clones, criteria, schedule
from bench.histogram import N_BUCKETS, Histogram, bucket_bounds, bucket_index
from config.loader import ConfigError, FaultPattern, load_benchmark_config, load_config
from conftest import REPO_ROOT

PATTERN = FaultPattern("bearing_temperature_c", 100.0, 50.0, 20.0, 10.0)


# --- histogram -------------------------------------------------------------------


@pytest.mark.parametrize("value, lower, upper", [
    (0.0, 0.0, 0.1), (0.05, 0.0, 0.1), (0.3, 0.3, 0.4), (9.99, 9.9, 10.0), (10.0, 10.0, 11.0),
    (999.5, 999.0, 1000.0), (1000.0, 1000.0, 1010.0), (59999.0, 59990.0, 60000.0),
])
def test_bucket_edges(value, lower, upper):
    assert bucket_bounds(bucket_index(value)) == (lower, upper)


def test_bucket_count_and_overflow():
    assert N_BUCKETS == 100 + 990 + 5900
    assert bucket_index(60000.0) == bucket_index(1e9) == N_BUCKETS
    assert bucket_bounds(N_BUCKETS) == (60000.0, math.inf)


def test_percentiles_on_raw_samples_never_understate():
    h = Histogram()
    for v in range(1, 101):  # 1..100 ms
        h.add(float(v))
    assert h.percentile(50) == 51.0  # rank 50 is 50 ms, reported as its bucket's upper edge
    assert h.percentile(99) == 100.0  # capped at the largest value seen
    assert h.percentile(100) == 100.0
    assert h.fraction_below(51.0) == 0.5 and h.fraction_below(1000.0) == 1.0


def test_threshold_fraction_is_exact_at_a_bucket_edge():
    h = Histogram()
    for v in [999.9] * 99 + [1000.0]:
        h.add(v)
    assert h.fraction_below(1000.0) == 0.99


def test_underflow_overflow_and_empty():
    h = Histogram()
    assert h.percentile(50) is None and h.fraction_below(1.0) is None
    assert h.summary() == {"n": 0, "p50": None, "p95": None, "p99": None, "min": None, "max": None, "underflow": 0}
    h.add(-2.0)
    h.add(70000.0)
    assert h.underflow == 1 and h.percentile(50) == 0.0 and h.percentile(100) == 70000.0
    with pytest.raises(ValueError):
        h.add(float("nan"))
    with pytest.raises(ValueError):
        h.percentile(0)


def test_merge_and_json_round_trip():
    a, b = Histogram(), Histogram()
    for v in (1.0, 2.0, 3.0):
        a.add(v)
    b.add(-1.0)
    b.add(500.0)
    m = a.merge(b)
    assert (m.total, m.underflow, m.min_ms, m.max_ms) == (5, 1, -1.0, 500.0)
    again = Histogram.from_json(m.to_json())
    assert again.to_json() == m.to_json() and again.percentile(80) == m.percentile(80)
    assert Histogram.from_json(Histogram().to_json()).total == 0
    with pytest.raises(ValueError):
        Histogram.from_json({"format": "other"})


def test_p99_reported_only_with_enough_samples():
    h = Histogram()
    for _ in range(9_999):
        h.add(1.0)
    assert h.summary()["p99"] is None
    h.add(1.0)
    assert h.summary()["p99"] == 1.0


# --- schedule ----------------------------------------------------------------------


def test_plan_is_round_robin_with_seq_per_asset():
    p = schedule.plan_for(10.0, ("a", "b"), 1.0, t0_wall_ms=1_000)
    assert p.count == 10
    assert [p.asset_of(i) for i in range(4)] == ["a", "b", "a", "b"]
    assert [p.seq_of(i) for i in range(4)] == [0, 0, 1, 1]
    assert [p.ts_ms(i) for i in range(3)] == [1_000, 1_100, 1_200]
    assert schedule.plan_for(3.0, ("a",), 1.0, 0).ts_ms(1) == 333  # truncated, as the simulator does
    with pytest.raises(ValueError):
        schedule.plan_for(0, ("a",), 1.0, 0)


class FakeClock:
    """A monotonic clock that sleep() advances; send() can cost time, like a saturated generator."""

    def __init__(self):
        self.t = 100.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


def test_open_loop_no_coordinated_omission():
    """A slow send delays the following sends, never their ts: lateness is recorded instead."""
    clock = FakeClock()
    p = schedule.plan_for(10.0, ("a",), 1.0, t0_wall_ms=0)
    sent, late = [], []

    def send(i):
        sent.append((i, clock.now(), p.ts_ms(i)))
        clock.t += 0.35 if i == 2 else 0.0  # message 2 takes 350 ms to send

    n = schedule.run_schedule(p, t0=clock.now(), now=clock.now, sleep=clock.sleep, send=send, late_ms=late.append)
    assert n == 10
    assert [ts for _, _, ts in sent] == [i * 100 for i in range(10)]  # every ts is its scheduled instant
    # Messages 3..5 were due during the slow send: sent at once, late by 250, 150 and 50 ms.
    assert [round(x) for x in late[3:6]] == [250, 150, 50]
    assert late[6] == 0.0 and late[:3] == [0.0, 0.0, 0.0]
    assert sent[3][1] == sent[4][1] == sent[5][1]  # sent back to back, not spaced: open loop


def test_run_schedule_stops_on_request():
    clock = FakeClock()
    p = schedule.plan_for(10.0, ("a",), 1.0, 0)
    sent = []
    n = schedule.run_schedule(p, clock.now(), clock.now, clock.sleep, sent.append, lambda _: None, stop=lambda: len(sent) >= 3)
    assert n == 3


def test_fault_pattern_is_staggered_per_asset():
    p = schedule.plan_for(2.0, ("a", "b"), 20.0, 0)  # one sample per asset per second
    a = [schedule.is_high(p, PATTERN, i) for i in range(0, p.count, 2)]
    b = [schedule.is_high(p, PATTERN, i) for i in range(1, p.count, 2)]
    assert a == [True] * 10 + [False] * 10
    assert b == [False] * 10 + [True] * 10  # the second of two assets is shifted by half a period


def test_sample_message_is_valid_telemetry(cfg):
    from bridge.mqtt_to_kafka.validation import TelemetryValidator

    p = schedule.plan_for(5.0, tuple(a.id for a in cfg.telemetry.assets), 10.0, t0_wall_ms=1_790_000_000_000)
    validator = TelemetryValidator(cfg.topics.telemetry_raw.value_schema, cfg.topics.mqtt_telemetry.template)
    import json
    for i in range(10):
        message = schedule.sample(p, PATTERN, cfg, i)
        record = validator.validate(cfg.topics.mqtt_telemetry.for_asset(message["asset_id"]), json.dumps(message).encode())
        assert int(record["ts"].timestamp() * 1000) == p.ts_ms(i)
    values = schedule.normal_values(cfg, "motor-01")
    assert values["vibration_rms_mm_s"] == 2.25 and values["motor_current_a"] == round(57.75 / 2, 3)
    assert values["speed_rpm"] == 1000.0  # no rule


def test_expected_alarms_follow_the_consumer_semantics(cfg):
    assets = tuple(a.id for a in cfg.telemetry.assets)
    p = schedule.plan_for(5.0, assets, 40.0, 0)  # 1 Hz per asset, two full periods
    # Per asset and full period: WARNING and CRITICAL raised (3 samples at 100) and cleared (3 at 50). The phases
    # are staggered, so over the whole run the edges count too: assets 1..4 start high in their period and some
    # end high, raised but not yet cleared.
    assert schedule.expected_alarms(p, PATTERN, cfg) == {"RAISED": 22, "CLEARED": 16}
    # In the second period every asset completes one cycle: 5 assets x 2 severities, raised and cleared.
    window = schedule.expected_alarms(p, PATTERN, cfg, measured_from_ms=20_000, measured_until_ms=40_000)
    assert window == {"RAISED": 10, "CLEARED": 10}


def test_replay_needs_persistence_and_respects_hysteresis(cfg):
    rule = cfg.alarms.rule("pump-01", "bearing_temperature_c")  # warning 80, hysteresis 3, persistence 3
    replay = schedule._replay
    assert replay([90, 90, 50, 90, 90, 90], rule, 80) == [(5, "RAISED")]
    assert replay([90, 90, 90, 78, 78, 78, 76, 76, 76], rule, 80) == [(2, "RAISED"), (8, "CLEARED")]


# --- criteria ----------------------------------------------------------------------


T = criteria.Thresholds(p99_ms=1000, max_lag_s=1.0, max_lag_slope_per_s=0.001, max_lost=0, max_late_p99_ms=5, max_late_max_ms=100)


def run_input(rate=1000.0, latency=20.0, lag=None, late=0.5, sent=1000, seen=1000, problems=None):
    e2e, gen = Histogram(), Histogram()
    for _ in range(1000):
        e2e.add(latency)
        gen.add(late)
    lag = lag if lag is not None else [(t, 10.0) for t in range(60)]
    return criteria.RunInput(rate, e2e, gen, lag, sent, seen, problems or [])


def test_a_steady_run_is_sustainable():
    v = criteria.evaluate(run_input(), T)
    assert (v.valid, v.sustainable, v.generator_saturated, v.reasons) == (True, True, False, [])


@pytest.mark.parametrize("kwargs, reason", [
    ({"latency": 1500.0}, "p99 not below 1000 ms"),
    ({"lag": [(t, 2000.0) for t in range(60)]}, "lag reached 2.000 s"),
    ({"lag": [(t, 10.0 * t) for t in range(60)]}, "lag grows"),
    ({"seen": 999}, "1 message(s) lost"),
])
def test_each_condition_can_fail_a_run(kwargs, reason):
    v = criteria.evaluate(run_input(**kwargs), T)
    assert v.valid and not v.sustainable and any(r.startswith(reason) for r in v.reasons)


def test_saturated_generator_invalidates_the_run():
    v = criteria.evaluate(run_input(late=50.0), T)
    assert (v.valid, v.sustainable, v.generator_saturated) == (False, False, True)
    assert criteria.evaluate(run_input(problems=["not on AC power"]), T).valid is False


def test_slope():
    assert criteria.slope([]) == 0.0 and criteria.slope([(1, 5)]) == 0.0 and criteria.slope([(1, 1), (1, 2)]) == 0.0
    assert criteria.slope([(0, 0), (1, 2), (2, 4)]) == pytest.approx(2.0)


def test_ladder_stops_at_the_first_failure():
    runs = [run_input(rate=250), run_input(rate=500), run_input(rate=1000, latency=2000), run_input(rate=2000)]
    result = criteria.ladder(runs, T)
    assert result.max_sustainable_rate == 500 and not result.limited_by_generator
    assert result.first_failure[0] == 1000 and len(result.steps) == 3


def test_ladder_limited_by_the_generator():
    result = criteria.ladder([run_input(rate=250), run_input(rate=500, late=40.0)], T)
    assert result.max_sustainable_rate == 250 and result.limited_by_generator


def test_ladder_all_sustainable_and_confirmation():
    result = criteria.ladder([run_input(rate=250), run_input(rate=500)], T)
    assert result.max_sustainable_rate == 500 and result.first_failure is None
    ok, reasons = criteria.confirmed([run_input()] * 5, T)
    assert ok and reasons == [[]] * 5
    ok, _ = criteria.confirmed([run_input()] * 4 + [run_input(seen=10)], T)
    assert not ok and criteria.confirmed([], T)[0] is False


# --- clones (D-A) and linger (D2) ------------------------------------------------------


def test_clones_keep_overrides_and_load(tmp_path):
    cfg = clones.generate(tmp_path / "config", clones_per_asset=3, linger_ms=0)
    ids = [a.id for a in cfg.telemetry.assets]
    assert len(ids) == 15 and ids[:3] == ["motor-01-c00", "motor-01-c01", "motor-01-c02"]
    assert cfg.alarms.rule("motor-01-c02", "motor_current_a").warning == 57.75
    assert cfg.alarms.rule("pump-02-c00", "bearing_temperature_c").warning == 80.0
    assert cfg.telemetry.fault_schedule == () and dict(cfg.bridge.producer)["linger.ms"] == 0
    assert (tmp_path / "config" / "telemetry.yaml").read_text(encoding="utf-8").startswith("# GENERATED")
    assert (tmp_path / "config" / "loader.py").is_file()  # the copy replaces the containers' whole config/ package


def test_no_clones_is_the_real_config(tmp_path):
    cfg = clones.generate(tmp_path / "config")
    assert [a.id for a in cfg.telemetry.assets] == [a.id for a in load_config(env={}).telemetry.assets]
    assert "linger.ms" not in cfg.bridge.producer
    clones.generate(tmp_path / "config", clones_per_asset=1)  # replaces an existing target
    with pytest.raises(ValueError):
        clones.generate(tmp_path / "x", clones_per_asset=-1)


# --- config/benchmark.yaml ----------------------------------------------------------


def test_real_benchmark_config():
    b = load_benchmark_config()
    assert b.criteria_p99_ms == 1000 and b.ladder_per_s[0] == 250 and b.clones_per_asset == 20
    assert b.fault_pattern.channel == "bearing_temperature_c" and b.bridge_linger_ms is None
    assert dict(b.cpusets)["bridge"] == "3" and b.trial_measured_s == 30


def benchmark_broken(tmp_path: Path, old: str, new: str) -> Path:
    target = tmp_path / "config"
    shutil.copytree(REPO_ROOT / "config", target, ignore=shutil.ignore_patterns("__pycache__", "*.py"))
    text = (target / "benchmark.yaml").read_text(encoding="utf-8")
    assert old in text, f"fixture out of date: {old!r}"
    (target / "benchmark.yaml").write_text(text.replace(old, new, 1), encoding="utf-8")
    return target


@pytest.mark.parametrize("old, new, message", [
    ("  require_ac_power: true", "  require_ac_power: yes please",
     "benchmark.yaml.environment.require_ac_power: expected true or false"),
    ("  ladder_per_s: [250, 500,", "  ladder_per_s: [500, 250,",
     "benchmark.yaml.throughput.ladder_per_s: must be strictly increasing"),
    ("  rates_per_s: [5, 50]", "  rates_per_s: []", "benchmark.yaml.latency.rates_per_s: expected a non-empty list"),
    ("    channel: bearing_temperature_c", "    channel: humidity",
     "benchmark.yaml.generator.fault_pattern.channel: unknown channel 'humidity'"),
    ("    high_s: 10", "    high_s: 20", "benchmark.yaml.generator.fault_pattern.high_s: must be shorter than period_s"),
    ("    high_value: 100.0", "    high_value: 40.0",
     "benchmark.yaml.generator.fault_pattern.high_value: must be above normal_value"),
    ("    bridge: \"3\"", "    bridge: \"3-1\"", "benchmark.yaml.cpu.cpusets.bridge: expected a cpuset such as \"3\" or \"0-1\""),
    ("    meter: \"6\"", "    meter: \"6\"\n    printer: \"7\"", "benchmark.yaml.cpu.cpusets: unknown service(s) ['printer']"),
    ("  log_level_latency: INFO", "  log_level_latency: LOUD",
     "benchmark.yaml.bridge.log_level_latency: must be one of ['DEBUG', 'INFO', 'WARNING', 'ERROR']"),
    ("  linger_ms: null", "  linger_ms: -1", "benchmark.yaml.bridge.linger_ms: must be >= 0"),
    ("meter:\n", "metre:\n", "benchmark.yaml: missing key(s) ['meter']"),
])
def test_benchmark_config_rejections(tmp_path, old, new, message):
    with pytest.raises(ConfigError) as info:
        load_benchmark_config(benchmark_broken(tmp_path, old, new))
    assert str(info.value) == message


def test_benchmark_linger_override(tmp_path):
    assert load_benchmark_config(benchmark_broken(tmp_path, "  linger_ms: null", "  linger_ms: 0")).bridge_linger_ms == 0
