"""The benchmark harness (Task 3.3, 1.3.2): one run of the pipeline benchmark, end to end, on the Windows host.

    python -m bench.harness run --mode latency --rate 5 [--repetition 1]
    python -m bench.harness run --mode throughput --rate 1000
    python -m bench.harness run --mode throughput --rate 1000 --sensitivity   (bridge at the sensitivity log level)
    python -m bench.harness run --mode calibrate-generator --rate 4000        (the generator alone)
    python -m bench.harness run --mode calibrate-meter                        (the meter alone, preloaded records)
    python -m bench.harness run --trial          (30 s measured at a low rate; never publishable)
    python -m bench.harness plan                 (the runs of STEP 2 and their duration)

The bridge runs at INFO, the product default, in every run except --sensitivity (owner, DETENTE 3.3.d). The two
calibration modes measure the highest rate the generator and the meter sustain on their own, with the ladder's own
rule for a client's CPU (owner, DETENTE 3.3.e); the ladder is capped at the lower one.

Every parameter comes from config/benchmark.yaml. A run:

1. checks the environment and records it: AC power, host free memory, no container of another project running,
   Docker's CPUs and memory, versions, .wslconfig, the git commit; any failure stops before anything starts;
2. writes the run's config/ (bench/clones.py: clones for throughput, linger for D2) into <run>/raw/config and its
   compose override (bench/compose.py: cpusets, mounts, bridge log level, generator and meter) into
   bench/results/<run id>/;
3. builds the images, recreates the stack (make down, then up with the override), provisions the topics and
   starts the bridge and the alarm consumer in containers, then the meter, then the generator, with t0 a few
   seconds ahead;
4. while the generator runs, samples every monitor_interval_s: power, free memory, foreign containers, the
   two lags, and CPU per container (docker stats, in its own thread);
5. after the last send, waits for both lags to reach 0 (drain_timeout_s), stops the meter (it writes its
   results), collects the components' logs, and checks for a system suspend in the Windows event log
   (Kernel-Power 42 or 107) and for a wall/monotonic clock jump;
6. computes F1 (meter), F2 (consumer log), F4 (bridge log, INFO runs), integrity, validity and the D8 verdict,
   and the clock quality (the meter's samples of wall minus monotonic clock, DETENTE 3.3.f), writes manifest.json,
   latency_*.json histograms, series.csv, clock.csv and counters.json, and takes the stack down (never with -v).

A run's CPU limit (max_client_core_fraction) applies to the measuring instruments only, the generator and the
meter; the bridge and the alarm consumer, the system under test, are judged by D8 alone, their CPU recorded as data
(owner, DETENTE 3.3.f).

Raw logs go to <run>/raw/, which is not versioned. Exit status: 0 for a valid run, 1 for an invalid one, 3 when
the environment check refused to start.
"""

from __future__ import annotations

import argparse
import csv
import ctypes
import json
import os
import platform
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

from bench import analysis, clones, compose, criteria, schedule
from bench.histogram import Histogram
from config.loader import REPO_ROOT, BenchmarkConfig, load_benchmark_config

RESULTS_DIR = REPO_ROOT / "bench" / "results"
PROJECT = "industrial-telemetry"
ALL_PROFILES = ["--profile", "full", "--profile", "pipeline", "--profile", "pipeline-tb", "--profile", "simulator",
                "--profile", "bench"]
PY = sys.executable
BOOTSTRAP = "localhost:9092"
T0_LEAD_S = 8.0
# Every manifest records which harness rules produced it (results are never re-run under new rules, only
# re-evaluated by bench/report.py from their stored data). DETENTE 3.3.h changed the product (Mosquitto's queue, the
# bridge's drop monitor): runs from then on are labelled with it. The order is bench.analysis.HARNESS_REVISIONS.
HARNESS_REVISION = analysis.HARNESS_REVISIONS[-1]


# --- environment (host side) -----------------------------------------------------------


class _PowerStatus(ctypes.Structure):
    _fields_ = [("ACLineStatus", ctypes.c_byte), ("BatteryFlag", ctypes.c_byte), ("BatteryLifePercent", ctypes.c_byte),
                ("SystemStatusFlag", ctypes.c_byte), ("BatteryLifeTime", ctypes.c_ulong), ("BatteryFullLifeTime", ctypes.c_ulong)]


def on_ac_power() -> bool | None:  # pragma: no cover - Windows host only
    if platform.system() != "Windows":
        return None
    s = _PowerStatus()
    ctypes.windll.kernel32.GetSystemPowerStatus(ctypes.byref(s))
    return {0: False, 1: True}.get(s.ACLineStatus)


def free_memory_gb() -> float | None:  # pragma: no cover - Windows host only
    if platform.system() != "Windows":
        return None

    class MemoryStatus(ctypes.Structure):
        _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong), ("ullTotalPhys", ctypes.c_ulonglong),
                    ("ullAvailPhys", ctypes.c_ulonglong), ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong), ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong), ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
    m = MemoryStatus()
    m.dwLength = ctypes.sizeof(MemoryStatus)
    ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m))
    return round(m.ullAvailPhys / 2**30, 2)


def sh(*cmd: str, env: dict | None = None, timeout: float = 900, check: bool = False) -> subprocess.CompletedProcess:
    result = subprocess.run(list(cmd), cwd=REPO_ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace",
                            timeout=timeout, env={**os.environ, **(env or {})})
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd[:4])} ... failed ({result.returncode}): {result.stderr[-1500:]}")
    return result


def foreign_containers() -> list[str]:  # pragma: no cover - needs Docker
    out = sh("docker", "ps", "--format", '{{.Names}}|{{.Label "com.docker.compose.project"}}').stdout.split()
    return [line.split("|")[0] for line in out if line.split("|")[1] != PROJECT]


def environment() -> dict:  # pragma: no cover - needs the host
    info = json.loads(sh("docker", "info", "--format", "{{json .}}").stdout or "{}")
    wslconfig = Path.home() / ".wslconfig"
    return {
        "host": {"platform": platform.platform(), "processor": platform.processor(), "logical_cpus": os.cpu_count()},
        "docker": {"server_version": info.get("ServerVersion"), "operating_system": info.get("OperatingSystem"),
                   "kernel": info.get("KernelVersion"), "cpus": info.get("NCPU"), "memory_bytes": info.get("MemTotal")},
        "wslconfig": wslconfig.read_text(encoding="utf-8") if wslconfig.is_file() else None,
        "power_plan": sh("powercfg", "/getactivescheme").stdout.strip() if platform.system() == "Windows" else None,
        "ac_power": on_ac_power(), "free_memory_gb": free_memory_gb(),
        "git": {"commit": sh("git", "rev-parse", "HEAD").stdout.strip(),
                "dirty": bool(sh("git", "status", "--porcelain", "--untracked-files=no").stdout.strip())},
    }


def preflight(bench: BenchmarkConfig) -> list[str]:  # pragma: no cover - needs the host
    problems = []
    if bench.require_ac_power and on_ac_power() is False:
        problems.append("not on AC power")
    free = free_memory_gb()
    if free is not None and free < bench.min_host_free_memory_gb:
        problems.append(f"host free memory {free} GB below {bench.min_host_free_memory_gb} GB")
    foreign = foreign_containers()
    if foreign:
        problems.append(f"containers of other projects running: {', '.join(foreign)}")
    return problems


def suspend_events(since: datetime, until: datetime) -> list[str]:  # pragma: no cover - Windows host only
    if platform.system() != "Windows":
        return []
    fmt = "%Y-%m-%dT%H:%M:%S"
    ps = (f"Get-WinEvent -FilterHashtable @{{LogName='System'; ProviderName='Microsoft-Windows-Kernel-Power'; Id=42,107; "
          f"StartTime=[datetime]'{since.astimezone().strftime(fmt)}'; EndTime=[datetime]'{until.astimezone().strftime(fmt)}'}} "
          f"-ErrorAction SilentlyContinue | ForEach-Object {{ $_.TimeCreated.ToString('s') + ' ' + $_.Id }}")
    return [line for line in sh("powershell", "-NoProfile", "-Command", ps).stdout.splitlines() if line.strip()]


# --- Kafka (host side) --------------------------------------------------------------------


def end_offsets(topic: str) -> dict[int, int]:  # pragma: no cover - needs Kafka
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"bench-harness-{os.getpid()}"})
    try:
        meta = c.list_topics(topic, timeout=10).topics[topic]
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in meta.partitions}
    finally:
        c.close()


def committed(group: str, topic: str, partitions) -> dict[int, int]:  # pragma: no cover - needs Kafka
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "enable.auto.commit": False})
    try:
        return {tp.partition: max(tp.offset, 0) for tp in c.committed([TopicPartition(topic, p) for p in partitions], timeout=10)}
    finally:
        c.close()


class SysSampler(threading.Thread):  # pragma: no cover - needs Mosquitto
    """Mosquitto's $SYS counters (analysis.MOSQUITTO_SYS), as it publishes them, until stopped."""

    def __init__(self):
        super().__init__(daemon=True)
        import paho.mqtt.client as mqtt
        self.samples: list[tuple[float, str, float]] = []
        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=f"bench-sys-{os.getpid()}")
        self.client.on_message = self._message
        self.halt = threading.Event()

    def _message(self, client, userdata, msg):
        name = analysis.MOSQUITTO_SYS.get(msg.topic)
        if name:
            try:
                self.samples.append((time.time() * 1000, name, float(msg.payload.decode())))
            except ValueError:
                pass

    def run(self):
        self.client.connect("localhost", 1883)
        self.client.subscribe("$SYS/broker/#", qos=0)
        self.client.loop_start()
        self.halt.wait()
        self.client.loop_stop()
        self.client.disconnect()


class CpuSampler(threading.Thread):  # pragma: no cover - needs Docker
    """docker stats of the project's containers, as fast as it answers (about every 2 s), until stopped."""

    def __init__(self):
        super().__init__(daemon=True)
        self.samples: list[tuple[float, dict[str, float]]] = []
        self.halt = threading.Event()

    def run(self):
        while not self.halt.is_set():
            out = sh("docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}", timeout=60).stdout
            row = {}
            for line in out.splitlines():
                name, _, pct = line.partition("\t")
                if name.startswith(PROJECT + "-") and pct.endswith("%"):
                    row[name[len(PROJECT) + 1:].rsplit("-", 1)[0]] = float(pct[:-1])
            self.samples.append((time.time() * 1000, row))


# --- one run -------------------------------------------------------------------------------


def run_id(mode: str, rate: float, repetition: int, trial: bool, now: datetime) -> str:
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{'trial' if trial else mode}-{rate:g}-r{repetition}"


def compose_cmd(override_file: Path, *args: str) -> list[str]:
    return ["docker", "compose", "-f", str(REPO_ROOT / "docker-compose.yml"), "-f", str(override_file), *args]


def container_log(override_file: Path, service: str) -> str:  # pragma: no cover - needs Docker
    return sh(*compose_cmd(override_file, *ALL_PROFILES, "logs", "--no-color", "--no-log-prefix", service)).stdout


def wait_for_event(override_file: Path, service: str, event: str, timeout: float = 60) -> None:  # pragma: no cover
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if any(e.get("event") == event for e in analysis.json_lines(container_log(override_file, service))):
            return
        time.sleep(0.5)
    raise RuntimeError(f"{service}: no {event} within {timeout:g} s")


class Run:  # pragma: no cover - orchestration on the host, exercised by the trial and the calibrations
    """What every run shares: environment check and record, run directory, generated config and override,
    the environment monitor and CPU sampler, the suspend and clock-jump checks, and the result files."""

    def __init__(self, bench: BenchmarkConfig, mode: str, rate: float, repetition: int, trial: bool,
                 clones_per_asset: int, log_level: str, sensitivity: bool):
        self.bench, self.mode, self.rate, self.repetition, self.trial = bench, mode, rate, repetition, trial
        self.sensitivity, self.log_level, self.clones_per_asset = sensitivity, log_level, clones_per_asset
        self.started = datetime.now(timezone.utc)
        self.rid = run_id(mode + ("-warning" if sensitivity else ""), rate, repetition, trial, self.started)
        self.dir = RESULTS_DIR / self.rid
        self.raw, self.out = self.dir / "raw", self.dir / "raw" / "out"
        self.out.mkdir(parents=True)
        self.env_record = environment()
        # Under raw/ (not versioned): derived deterministically from config/ and the manifest's parameters, and it
        # carries a copy of the loader's code.
        self.config_dir = self.raw / "config"
        self.config = clones.generate(self.config_dir, clones_per_asset, bench.bridge_linger_ms)
        self.override_file = self.dir / "compose.override.yml"
        services = compose.override(bench, self.config_dir.relative_to(REPO_ROOT), self.out.relative_to(REPO_ROOT), log_level)
        self.override_file.write_text("# GENERATED by bench/compose.py from config/benchmark.yaml for this run. Do not edit.\n"
                                      + yaml.safe_dump(services, sort_keys=False), encoding="utf-8")
        self.env_problems: list[str] = []
        self.min_free: float | None = None
        self.cpu = CpuSampler()
        self.wall0, self.mono0 = time.time(), time.monotonic()
        print(json.dumps({"event": "run_started", "run_id": self.rid, "mode": mode, "rate_per_s": rate, "trial": trial,
                          "bridge_log_level": log_level}), flush=True)

    def compose(self, *args: str, env: dict | None = None, check: bool = True, timeout: float = 900):
        return sh(*compose_cmd(self.override_file, *args), env=env, check=check, timeout=timeout)

    def start_stack(self, provision: bool = True) -> None:
        self.compose(*ALL_PROFILES, "down", check=False, timeout=300)
        sh("docker", "compose", "--profile", "pipeline", "build", check=True)
        sh("docker", "build", "-q", "-t", compose.BENCH_IMAGE, "-f", "docker/bench/Dockerfile", ".", check=True)
        self.compose("up", "-d", "--wait", "--wait-timeout", "600", "--force-recreate")
        if provision:
            sh(PY, "-m", "broker.kafka.provision", check=True)

    def check_environment(self) -> None:
        if self.bench.require_ac_power and on_ac_power() is False and "not on AC power" not in self.env_problems:
            self.env_problems.append("not on AC power")
        free = free_memory_gb()
        if free is not None:
            self.min_free = free if self.min_free is None else min(self.min_free, free)
            if free < self.bench.min_host_free_memory_gb and not any(p.startswith("host free memory") for p in self.env_problems):
                self.env_problems.append(f"host free memory fell to {free} GB")
        foreign = foreign_containers()
        if foreign and not any(p.startswith("containers of other projects") for p in self.env_problems):
            self.env_problems.append(f"containers of other projects running: {', '.join(foreign)}")

    def exited(self, service: str) -> bool:
        ps = self.compose("--profile", "bench", "ps", "--all", "--format", "{{.State}}", service, check=False)
        return ps.stdout.strip() == "exited"

    def finish_checks(self) -> datetime:
        self.cpu.halt.set()
        clock_jump_s = abs((time.time() - self.wall0) - (time.monotonic() - self.mono0))
        finished = datetime.now(timezone.utc)
        suspends = suspend_events(self.started, finished)
        if suspends:
            self.env_problems.append(f"system suspend or resume during the run: {suspends}")
        if clock_jump_s > 1.0:
            self.env_problems.append(f"wall and monotonic clocks diverged by {clock_jump_s:.1f} s (suspend?)")
        self.check_environment()
        return finished

    def save_logs(self, *services: str) -> dict[str, str]:
        logs = {s: container_log(self.override_file, s) for s in services}
        for service, text in logs.items():
            (self.raw / f"{service}.log").write_text(text, encoding="utf-8")
        return logs

    def write(self, manifest: dict, histograms: dict[str, dict]) -> None:
        for name, h in histograms.items():
            (self.dir / f"latency_{name}.json").write_text(json.dumps(h) + "\n", encoding="utf-8")
        with open(self.dir / "cpu.csv", "w", newline="", encoding="utf-8") as f:
            names = sorted({n for _, row in self.cpu.samples for n in row})
            w = csv.writer(f)
            w.writerow(["wall_ms", *names])
            w.writerows([[round(t), *[row.get(n, "") for n in names]] for t, row in self.cpu.samples])
        base = {"run_id": self.rid, "harness_revision": HARNESS_REVISION, "mode": self.mode, "trial": self.trial,
                "sensitivity": self.sensitivity,
                "rate_per_s": self.rate, "repetition": self.repetition, "assets": len(self.config.telemetry.assets),
                "clones_per_asset": self.clones_per_asset, "bridge_log_level": self.log_level,
                "bridge_producer": dict(self.config.bridge.producer), "started": self.started.isoformat(),
                "environment": self.env_record, "min_host_free_memory_gb": self.min_free,
                "cpu_peak_percent": analysis.cpu_problems(self.cpu.samples, self.bench.max_client_core_fraction)[0],
                "cpusets": dict(self.bench.cpusets),
                "summaries": {name: Histogram.from_json(h).summary() for name, h in histograms.items()}}
        (self.dir / "manifest.json").write_text(json.dumps({**base, **manifest}, indent=1) + "\n", encoding="utf-8")

    def down(self) -> None:
        self.compose(*ALL_PROFILES, "down", "--remove-orphans", check=False, timeout=300)


def thresholds_of(bench: BenchmarkConfig) -> criteria.Thresholds:
    return criteria.Thresholds(bench.criteria_p99_ms, bench.criteria_max_lag_s, bench.criteria_max_lag_slope_per_s,
                               bench.criteria_max_lost, bench.max_late_p99_ms, bench.max_late_max_ms)


def execute(bench: BenchmarkConfig, mode: str, rate: float, repetition: int, trial: bool,
            sensitivity: bool = False, diagnostic: bool = False, full_window: bool = False) -> int:  # pragma: no cover
    """One pipeline run (latency, throughput, or the throughput log-level sensitivity), as the module docstring says.

    diagnostic: a throughput run kept short (trial.measured_s; run.measured_s with full_window) to reproduce a
    behaviour; never publishable, never on
    the ladder or in E2 (owner, after the E2 loss at 2000 msg/s)."""
    throughput_run = mode == "throughput" and not trial
    log_level = (bench.bridge_log_level_sensitivity if sensitivity else
                 bench.bridge_log_level_throughput if throughput_run else bench.bridge_log_level_latency)
    r = Run(bench, mode + ("-diagnostic" if diagnostic else ""), rate, repetition, trial,
            bench.clones_per_asset if throughput_run else 0, log_level, sensitivity)
    r.mode = mode
    measured_s = bench.trial_measured_s if trial or (diagnostic and not full_window) else bench.measured_s
    r.start_stack()
    r.compose("--profile", "pipeline", "up", "-d", "--wait", "--no-deps", "--force-recreate", "bridge", "alarm-consumer")
    wait_for_event(r.override_file, "bridge", "mqtt_subscribed")
    wait_for_event(r.override_file, "alarm-consumer", "run_started")

    raw_topic, dlq_topic = r.config.topics.telemetry_raw.name, r.config.topics.kafka["telemetry_dlq"].name
    raw_start, dlq_start = end_offsets(raw_topic), end_offsets(dlq_topic)
    duration = bench.warmup_s + measured_s
    t0_ms = int((time.time() + T0_LEAD_S) * 1000)
    bench_env = {"BENCH_RATE": f"{rate:g}", "BENCH_DURATION_S": f"{duration:g}", "BENCH_T0_MS": str(t0_ms),
                 "BENCH_WARMUP_S": f"{bench.warmup_s:g}", "BENCH_MEASURED_S": f"{measured_s:g}"}
    r.compose("--profile", "bench", "up", "-d", "--no-deps", "meter", env=bench_env)
    wait_for_event(r.override_file, "meter", "meter_ready")
    r.compose("--profile", "bench", "up", "-d", "--no-deps", "generator", env=bench_env)

    plan = schedule.plan_for(rate, tuple(a.id for a in r.config.telemetry.assets), duration, t0_ms)
    r.cpu.start()
    mosquitto_sys = SysSampler()
    mosquitto_sys.start()
    samples: list[analysis.LagSample] = []

    def sample_once() -> int:
        raw_now = end_offsets(raw_topic)
        raw_new = sum(raw_now[p] - raw_start.get(p, 0) for p in raw_now)
        done = committed("alarm-consumer", raw_topic, raw_now)
        consumer_lag = sum(raw_now[p] - done.get(p, 0) for p in raw_now)
        now_ms = time.time() * 1000
        samples.append(analysis.LagSample(now_ms, analysis.scheduled_by(now_ms, t0_ms, rate, plan.count), raw_new, consumer_lag))
        return consumer_lag

    while True:
        time.sleep(bench.monitor_interval_s)
        sample_once()
        r.check_environment()
        if r.exited("generator"):
            break
        if time.time() * 1000 > t0_ms + (duration + bench.drain_timeout_s) * 1000:
            r.env_problems.append("generator did not finish in time")
            break
    drain_deadline = time.monotonic() + bench.drain_timeout_s
    while time.monotonic() < drain_deadline:
        consumer_lag = sample_once()
        if consumer_lag == 0 and samples[-1].raw_records >= samples[-1].scheduled:
            break
        time.sleep(1)
    r.compose("--profile", "bench", "stop", "meter", check=False, timeout=120)
    mosquitto_sys.halt.set()
    finished = r.finish_checks()
    # Mosquitto, the Kafka broker and ZooKeeper too (owner, after the E2 loss): the broker's own record of a
    # dropped message, of a segment roll, a rebalance or a controller event.
    logs = r.save_logs("bridge", "alarm-consumer", "generator", "meter", "mosquitto", "broker", "zookeeper")
    r.compose("--profile", "full", "cp", "broker:/var/log/kafka/kafkaServer-gc.log", str(r.raw / "broker-gc.log"),
              check=False)

    generator = json.loads((r.out / "generator.json").read_text(encoding="utf-8"))
    meter = json.loads((r.out / "meter.json").read_text(encoding="utf-8"))
    window = meter["window"]
    raw_end, dlq_end = end_offsets(raw_topic), end_offsets(dlq_topic)
    raw_new = sum(raw_end[p] - raw_start.get(p, 0) for p in raw_end)
    dlq_new = sum(dlq_end[p] - dlq_start.get(p, 0) for p in dlq_end)
    f2, alarms_window, alarms_total = analysis.alarm_latency(logs["alarm-consumer"], window["start_ms"], window["end_ms"])
    f4 = analysis.produce_to_ack(logs["bridge"], window["start_ms"], window["end_ms"])
    expected = schedule.expected_alarms(plan, bench.fault_pattern, r.config)
    counts, integrity_problems = analysis.integrity(generator, meter, raw_new, dlq_new, samples[-1].consumer_lag,
                                                    alarms_total, expected)
    _, cpu_flags = analysis.cpu_problems(r.cpu.samples, bench.max_client_core_fraction)
    lag = analysis.lag_series(samples, window["start_ms"], window["end_ms"])
    thresholds = thresholds_of(bench)
    # Mosquitto's own record (owner, after the E2 loss): its $SYS counters and its drop notices, against what is
    # missing from telemetry.raw. A drop invalidates the run (DETENTE 3.3.h).
    with open(r.dir / "mosquitto_sys.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["wall_ms", "counter", "value"])
        w.writerows([(round(t), name, value) for t, name, value in mosquitto_sys.samples])
    mosquitto = {**analysis.mosquitto_summary(mosquitto_sys.samples),
                 "drop_notices": analysis.mosquitto_drop_notices(logs["mosquitto"]),
                 "missing_from_telemetry_raw": generator["sent"] - raw_new}
    all_problems = r.env_problems + integrity_problems + cpu_flags + analysis.mosquitto_problems(mosquitto)
    verdict = criteria.evaluate(analysis.run_input(rate, meter, generator, lag, all_problems), thresholds)
    # D8 (sustainable or not) applies to throughput runs only; at latency rates one message is a large share of a
    # second, so the lag slope is quantisation noise. Other runs report validity alone.
    sustainable = verdict.sustainable if throughput_run else None
    reported = verdict.reasons if throughput_run or not verdict.valid else []

    histograms = {"e2e": meter["histograms"]["e2e"], "ingest": meter["histograms"]["ingest"],
                  "produce_to_visible": meter["histograms"]["produce_to_visible"], "alarm_e2e": f2.to_json(),
                  "generator_late": generator["late"], "produce_to_ack": f4.to_json()}
    with open(r.dir / "series.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["wall_ms", "scheduled", "raw_records", "consumer_lag"])
        w.writerows([[round(s.wall_ms), s.scheduled, s.raw_records, s.consumer_lag] for s in samples])
    (r.dir / "counters.json").write_text(json.dumps({**counts, "alarms_in_window": alarms_window}, indent=1) + "\n",
                                         encoding="utf-8")
    # Clock quality (owner, DETENTE 3.3.f): the meter's samples of wall minus monotonic clock, against the
    # negative latency samples it saw (their wall time).
    clock_samples = [tuple(c) for c in meter.get("clock", [])]
    with open(r.dir / "clock.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["wall_ms", "wall_minus_monotonic_ms"])
        w.writerows(clock_samples)
    negatives = meter.get("negatives", [])
    clock = analysis.clock_quality(clock_samples, [n[0] for n in negatives])
    r.write({"publishable": not trial and not diagnostic and verdict.valid, "warmup_s": bench.warmup_s,
             "measured_s": measured_s,
             "t0_ms": t0_ms, "window": window, "finished": finished.isoformat(),
             "fetch_wait_max_ms": meter.get("fetch_wait_max_ms"),
             "lag_peak_s": max((y for _, y in lag), default=0.0) / rate,
             "lag_slope_s_per_s": criteria.slope([(x, y / rate) for x, y in lag]),
             "valid": verdict.valid, "sustainable": sustainable, "generator_saturated": verdict.generator_saturated,
             "problems": reported, "thresholds": thresholds.__dict__, "clock": clock, "negatives": negatives,
             "diagnostic": diagnostic, "diagnostic_full_window": full_window, "mosquitto": mosquitto,
             # Environment, integrity and CPU problems alone, so bench/report.py can re-evaluate D8 with other thresholds.
             "run_problems": all_problems}, histograms)
    r.down()
    print(json.dumps({"event": "run_finished", "run_id": r.rid, "valid": verdict.valid, "sustainable": sustainable,
                      "problems": reported, "e2e": Histogram.from_json(histograms["e2e"]).summary()}), flush=True)
    return 0 if verdict.valid else 1


def calibrate_generator(bench: BenchmarkConfig, rate: float, repetition: int) -> int:  # pragma: no cover
    """The generator alone at one rate (owner, DETENTE 3.3.d): no bridge is subscribed, so Mosquitto acknowledges
    and drops; valid when the generator stays within its lateness limits and every message is acknowledged."""
    r = Run(bench, "calibrate-generator", rate, repetition, False, bench.clones_per_asset, bench.bridge_log_level_throughput,
            False)
    r.start_stack(provision=False)
    duration = bench.calibration_generator_measured_s
    t0_ms = int((time.time() + T0_LEAD_S) * 1000)
    r.compose("--profile", "bench", "up", "-d", "--no-deps", "generator",
              env={"BENCH_RATE": f"{rate:g}", "BENCH_DURATION_S": f"{duration:g}", "BENCH_T0_MS": str(t0_ms)})
    r.cpu.start()
    deadline = t0_ms / 1000 + duration + bench.drain_timeout_s
    while not r.exited("generator"):
        time.sleep(bench.monitor_interval_s)
        r.check_environment()
        if time.time() > deadline:
            r.env_problems.append("generator did not finish in time")
            break
    finished = r.finish_checks()
    r.save_logs("generator")
    generator = json.loads((r.out / "generator.json").read_text(encoding="utf-8"))
    late = Histogram.from_json(generator["late"])
    # One criterion with the ladder (owner, DETENTE 3.3.e): lateness, CPU share of a core, and pending
    # acknowledgements at saturation make the rate "not sustained"; only environment or integrity failures make the
    # run invalid. bench/report.py re-evaluates every stored run with the same function.
    peaks, _ = analysis.cpu_problems(r.cpu.samples, bench.max_client_core_fraction)
    verdict = criteria.generator_calibration(generator["planned"], generator["sent"], generator["acknowledged"],
                                             generator.get("stopped_early", False), late, peaks.get("generator"),
                                             r.env_problems, thresholds_of(bench), bench.max_client_core_fraction)
    r.write({"publishable": False, "finished": finished.isoformat(), "measured_s": duration, "t0_ms": t0_ms,
             "valid": verdict.valid, "generator_saturated": verdict.valid and not verdict.sustained,
             "sustained": verdict.sustained, "stopped_early": generator.get("stopped_early", False),
             "counters": {k: generator[k] for k in ("planned", "sent", "acknowledged", "send_cpu_s", "send_wall_s")},
             "problems": verdict.reasons, "environment_problems": list(r.env_problems), "run_problems": list(r.env_problems)},
            {"generator_late": generator["late"]})
    r.down()
    print(json.dumps({"event": "run_finished", "run_id": r.rid, "valid": verdict.valid, "sustained": verdict.sustained,
                      "reasons": verdict.reasons, "late": late.summary()}), flush=True)
    return 0 if verdict.valid else 1


def calibrate_meter(bench: BenchmarkConfig, repetition: int) -> int:  # pragma: no cover
    """The meter alone (owner, DETENTE 3.3.d): calibration.meter_records preloaded by the loader, then read from the
    beginning as fast as the meter can; its rate is records / seconds from the first to the last record."""
    count = bench.calibration_meter_records
    r = Run(bench, "calibrate-meter", float(count), repetition, False, bench.clones_per_asset,
            bench.bridge_log_level_throughput, False)
    r.start_stack()
    env = {"BENCH_COUNT": str(count)}
    loaded = r.compose("--profile", "bench", "run", "--rm", "-T", "--no-deps", "loader", env=env, check=False, timeout=3600)
    (r.raw / "loader.log").write_text(loaded.stdout + loaded.stderr, encoding="utf-8")
    problems = [] if loaded.returncode == 0 else [f"loader failed ({loaded.returncode})"]
    r.cpu.start()
    r.compose("--profile", "bench", "up", "-d", "--no-deps", "meter-calibration", env=env)
    deadline = time.monotonic() + 3600
    while not r.exited("meter-calibration"):
        time.sleep(bench.monitor_interval_s)
        r.check_environment()
        if time.monotonic() > deadline:
            r.env_problems.append("meter did not finish in time")
            break
    finished = r.finish_checks()
    r.save_logs("meter-calibration")
    meter = json.loads((r.out / "meter.json").read_text(encoding="utf-8"))
    calibration = meter["calibration"]
    environment_problems = list(r.env_problems)
    problems = environment_problems + problems
    if calibration["read"] < count:
        problems.append(f"meter read {calibration['read']} of {count}")
    # Its rate within max_client_core_fraction of a core, the ladder's rule for a client (owner, DETENTE 3.3.e).
    peaks, _ = analysis.cpu_problems(r.cpu.samples, bench.max_client_core_fraction)
    if calibration.get("records_per_s"):
        calibration["cap_per_s"] = criteria.meter_calibration_cap(calibration["records_per_s"], peaks.get("meter-calibration"),
                                                                  bench.max_client_core_fraction)
    r.write({"publishable": False, "finished": finished.isoformat(), "valid": not problems, "calibration": calibration,
             "problems": problems, "environment_problems": environment_problems, "run_problems": problems}, {})
    r.down()
    print(json.dumps({"event": "run_finished", "run_id": r.rid, "valid": not problems, "calibration": calibration}), flush=True)
    return 0 if not problems else 1


def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    p = argparse.ArgumentParser(prog="python -m bench.harness")
    sub = p.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run", help="one run")
    r.add_argument("--mode", choices=["latency", "throughput", "calibrate-generator", "calibrate-meter"], default="latency")
    r.add_argument("--rate", type=float, help="messages per second (default: the trial rate with --trial)")
    r.add_argument("--repetition", type=int, default=1)
    r.add_argument("--trial", action="store_true", help="the short end-to-end trial; never publishable")
    r.add_argument("--full-window", action="store_true",
                   help="with --diagnostic: the normal measured window (run.measured_s) instead of trial.measured_s")
    r.add_argument("--diagnostic", action="store_true",
                   help="throughput, measured for trial.measured_s, to reproduce a behaviour; never publishable")
    r.add_argument("--sensitivity", action="store_true",
                   help="throughput with the bridge at bridge.log_level_sensitivity (owner, DETENTE 3.3.d)")
    sub.add_parser("plan", help="print the runs of STEP 2 and the time they need")
    args = p.parse_args(argv)
    bench = load_benchmark_config()
    if args.command == "plan":
        print(json.dumps(analysis.plan(bench), indent=1))
        return 0
    if args.mode == "calibrate-meter":
        runner = lambda: calibrate_meter(bench, args.repetition)  # noqa: E731
    else:
        rate = args.rate if args.rate is not None else (bench.trial_rate_per_s if args.trial else None)
        if rate is None:
            p.error("--rate is required unless --trial or --mode calibrate-meter")
        if args.sensitivity and args.mode != "throughput":
            p.error("--sensitivity applies to --mode throughput only")
        if args.diagnostic and (args.mode != "throughput" or args.trial or args.sensitivity):
            p.error("--diagnostic applies to --mode throughput only, without --trial or --sensitivity")
        if args.full_window and not args.diagnostic:
            p.error("--full-window applies to --diagnostic only")
        if args.mode == "calibrate-generator":
            runner = lambda: calibrate_generator(bench, rate, args.repetition)  # noqa: E731
        else:
            runner = lambda: execute(bench, args.mode, rate, args.repetition, args.trial, args.sensitivity,  # noqa: E731
                                     args.diagnostic, args.full_window)
    try:
        return runner()
    except BaseException:
        # Whatever stopped the run, nothing of it stays up: --remove-orphans also takes the benchmark services, which
        # only the run's override defines. Never -v.
        sh("docker", "compose", *ALL_PROFILES, "down", "--remove-orphans", timeout=300)
        raise


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
