"""The benchmark harness (Task 3.3, 1.3.2): one run of the pipeline benchmark, end to end, on the Windows host.

    python -m bench.harness run --mode latency --rate 5 [--repetition 1]
    python -m bench.harness run --mode throughput --rate 1000
    python -m bench.harness run --trial          (30 s measured at a low rate; never publishable)
    python -m bench.harness plan                 (the runs of STEP 2 and their duration)

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
   writes manifest.json, latency_*.json histograms, series.csv and counters.json, and takes the stack down
   (never with -v).

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


def execute(bench: BenchmarkConfig, mode: str, rate: float, repetition: int, trial: bool) -> int:  # pragma: no cover
    """One run, as the module docstring describes. Exercised end to end by the trial run (STEP 1, 1.3.6)."""
    problems = preflight(bench)
    if problems:
        print(json.dumps({"event": "environment_refused", "problems": problems}))
        return 3
    started = datetime.now(timezone.utc)
    rid = run_id(mode, rate, repetition, trial, started)
    run_dir = RESULTS_DIR / rid
    raw, out = run_dir / "raw", run_dir / "raw" / "out"
    out.mkdir(parents=True)
    env_record = environment()
    measured_s = bench.trial_measured_s if trial else bench.measured_s
    clones_per_asset = bench.clones_per_asset if mode == "throughput" and not trial else 0
    # Under raw/ (not versioned): it is derived deterministically from config/ and the manifest's parameters, and it
    # carries a copy of the loader's code.
    config_dir = raw / "config"
    config = clones.generate(config_dir, clones_per_asset, bench.bridge_linger_ms)
    log_level = bench.bridge_log_level_throughput if mode == "throughput" and not trial else bench.bridge_log_level_latency
    override_file = run_dir / "compose.override.yml"
    services = compose.override(bench, config_dir.relative_to(REPO_ROOT), out.relative_to(REPO_ROOT), log_level)
    override_file.write_text("# GENERATED by bench/compose.py from config/benchmark.yaml for this run. Do not edit.\n"
                             + yaml.safe_dump(services, sort_keys=False), encoding="utf-8")
    print(json.dumps({"event": "run_started", "run_id": rid, "mode": mode, "rate_per_s": rate, "trial": trial}), flush=True)

    sh(*compose_cmd(override_file, *ALL_PROFILES, "down"), timeout=300)
    sh("docker", "compose", "--profile", "pipeline", "build", check=True)
    sh("docker", "build", "-q", "-t", compose.BENCH_IMAGE, "-f", "docker/bench/Dockerfile", ".", check=True)
    sh(*compose_cmd(override_file, "up", "-d", "--wait", "--wait-timeout", "600", "--force-recreate"), check=True)
    sh(PY, "-m", "broker.kafka.provision", check=True)
    sh(*compose_cmd(override_file, "--profile", "pipeline", "up", "-d", "--wait", "--no-deps", "--force-recreate",
                    "bridge", "alarm-consumer"), check=True)
    wait_for_event(override_file, "bridge", "mqtt_subscribed")
    wait_for_event(override_file, "alarm-consumer", "run_started")

    raw_topic, dlq_topic = config.topics.telemetry_raw.name, config.topics.kafka["telemetry_dlq"].name
    raw_start, dlq_start = end_offsets(raw_topic), end_offsets(dlq_topic)
    duration = bench.warmup_s + measured_s
    t0_ms = int((time.time() + T0_LEAD_S) * 1000)
    bench_env = {"BENCH_RATE": f"{rate:g}", "BENCH_DURATION_S": f"{duration:g}", "BENCH_T0_MS": str(t0_ms),
                 "BENCH_WARMUP_S": f"{bench.warmup_s:g}", "BENCH_MEASURED_S": f"{measured_s:g}"}
    sh(*compose_cmd(override_file, "--profile", "bench", "up", "-d", "--no-deps", "meter"), env=bench_env, check=True)
    wait_for_event(override_file, "meter", "meter_ready")
    sh(*compose_cmd(override_file, "--profile", "bench", "up", "-d", "--no-deps", "generator"), env=bench_env, check=True)

    plan = schedule.plan_for(rate, tuple(a.id for a in config.telemetry.assets), duration, t0_ms)
    cpu = CpuSampler()
    cpu.start()
    samples, env_problems, min_free = [], [], None
    wall0, mono0 = time.time(), time.monotonic()

    def sample_once() -> int:
        raw_now = end_offsets(raw_topic)
        raw_new = sum(raw_now[p] - raw_start.get(p, 0) for p in raw_now)
        done = committed("alarm-consumer", raw_topic, raw_now)
        consumer_lag = sum(raw_now[p] - done.get(p, 0) for p in raw_now)
        now_ms = time.time() * 1000
        samples.append(analysis.LagSample(now_ms, analysis.scheduled_by(now_ms, t0_ms, rate, plan.count), raw_new, consumer_lag))
        return consumer_lag

    def check_environment() -> None:
        nonlocal min_free
        if bench.require_ac_power and on_ac_power() is False and "not on AC power" not in env_problems:
            env_problems.append("not on AC power")
        free = free_memory_gb()
        if free is not None:
            min_free = free if min_free is None else min(min_free, free)
            if free < bench.min_host_free_memory_gb and not any(p.startswith("host free memory") for p in env_problems):
                env_problems.append(f"host free memory fell to {free} GB")
        foreign = foreign_containers()
        if foreign and not any(p.startswith("containers of other projects") for p in env_problems):
            env_problems.append(f"containers of other projects running: {', '.join(foreign)}")

    gen_ps = compose_cmd(override_file, "--profile", "bench", "ps", "--all", "--format", "{{.State}}", "generator")
    while True:
        time.sleep(bench.monitor_interval_s)
        sample_once()
        check_environment()
        if sh(*gen_ps).stdout.strip() == "exited":
            break
        if time.time() * 1000 > t0_ms + (duration + bench.drain_timeout_s) * 1000:
            env_problems.append("generator did not finish in time")
            break
    drain_deadline = time.monotonic() + bench.drain_timeout_s
    while time.monotonic() < drain_deadline:
        consumer_lag = sample_once()
        last = samples[-1]
        if consumer_lag == 0 and last.raw_records >= last.scheduled:
            break
        time.sleep(1)
    cpu.halt.set()
    clock_jump_s = abs((time.time() - wall0) - (time.monotonic() - mono0))
    sh(*compose_cmd(override_file, "--profile", "bench", "stop", "meter"), timeout=120)
    logs = {s: container_log(override_file, s) for s in ("bridge", "alarm-consumer", "generator", "meter")}
    for service, text in logs.items():
        (raw / f"{service}.log").write_text(text, encoding="utf-8")
    finished = datetime.now(timezone.utc)
    suspends = suspend_events(started, finished)
    if suspends:
        env_problems.append(f"system suspend or resume during the run: {suspends}")
    if clock_jump_s > 1.0:
        env_problems.append(f"wall and monotonic clocks diverged by {clock_jump_s:.1f} s (suspend?)")
    check_environment()

    generator = json.loads((out / "generator.json").read_text(encoding="utf-8"))
    meter = json.loads((out / "meter.json").read_text(encoding="utf-8"))
    window = meter["window"]
    raw_end, dlq_end = end_offsets(raw_topic), end_offsets(dlq_topic)
    raw_new = sum(raw_end[p] - raw_start.get(p, 0) for p in raw_end)
    dlq_new = sum(dlq_end[p] - dlq_start.get(p, 0) for p in dlq_end)
    f2, alarms_window, alarms_total = analysis.alarm_latency(logs["alarm-consumer"], window["start_ms"], window["end_ms"])
    f4 = analysis.produce_to_ack(logs["bridge"], window["start_ms"], window["end_ms"])
    expected = schedule.expected_alarms(plan, bench.fault_pattern, config)
    counts, integrity_problems = analysis.integrity(generator, meter, raw_new, dlq_new, samples[-1].consumer_lag,
                                                    alarms_total, expected)
    peaks, cpu_flags = analysis.cpu_problems(cpu.samples, bench.max_client_core_fraction)
    lag = analysis.lag_series(samples, window["start_ms"], window["end_ms"])
    thresholds = criteria.Thresholds(bench.criteria_p99_ms, bench.criteria_max_lag_s, bench.criteria_max_lag_slope_per_s,
                                     bench.criteria_max_lost, bench.max_late_p99_ms, bench.max_late_max_ms)
    all_problems = env_problems + integrity_problems + cpu_flags
    verdict = criteria.evaluate(analysis.run_input(rate, meter, generator, lag, all_problems), thresholds)
    # D8 (sustainable or not) applies to throughput runs only; at latency rates one message is a large share of a
    # second, so the lag slope is quantisation noise. Other runs report validity alone.
    throughput_run = mode == "throughput" and not trial
    sustainable = verdict.sustainable if throughput_run else None
    reported = verdict.reasons if throughput_run or not verdict.valid else []

    histograms = {"e2e": meter["histograms"]["e2e"], "bridge_share": meter["histograms"]["bridge"],
                  "read_share": meter["histograms"]["read"], "alarm_e2e": f2.to_json(), "generator_late": generator["late"],
                  "produce_to_ack": f4.to_json()}
    for name, h in histograms.items():
        (run_dir / f"latency_{name}.json").write_text(json.dumps(h) + "\n", encoding="utf-8")
    with open(run_dir / "series.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["wall_ms", "scheduled", "raw_records", "consumer_lag"])
        w.writerows([[round(s.wall_ms), s.scheduled, s.raw_records, s.consumer_lag] for s in samples])
    with open(run_dir / "cpu.csv", "w", newline="", encoding="utf-8") as f:
        names = sorted({n for _, row in cpu.samples for n in row})
        w = csv.writer(f)
        w.writerow(["wall_ms", *names])
        w.writerows([[round(t), *[row.get(n, "") for n in names]] for t, row in cpu.samples])
    (run_dir / "counters.json").write_text(json.dumps({**counts, "alarms_in_window": alarms_window}, indent=1) + "\n",
                                           encoding="utf-8")
    manifest = {
        "run_id": rid, "mode": mode, "trial": trial, "publishable": not trial and verdict.valid,
        "rate_per_s": rate, "repetition": repetition, "assets": len(config.telemetry.assets),
        "clones_per_asset": clones_per_asset, "bridge_log_level": log_level, "bridge_producer": dict(config.bridge.producer),
        "warmup_s": bench.warmup_s, "measured_s": measured_s, "t0_ms": t0_ms, "window": window,
        "started": started.isoformat(), "finished": finished.isoformat(), "environment": env_record,
        "min_host_free_memory_gb": min_free, "cpu_peak_percent": peaks, "cpusets": dict(bench.cpusets),
        "fetch_wait_max_ms": meter.get("fetch_wait_max_ms"),
        "summaries": {name: Histogram.from_json(h).summary() for name, h in histograms.items()},
        "lag_peak_s": max((y for _, y in lag), default=0.0) / rate,
        "lag_slope_s_per_s": criteria.slope([(x, y / rate) for x, y in lag]),
        "valid": verdict.valid, "sustainable": sustainable, "generator_saturated": verdict.generator_saturated,
        "problems": reported, "thresholds": thresholds.__dict__,
        # Environment, integrity and CPU problems alone, so bench/report.py can re-evaluate D8 with other thresholds.
        "run_problems": all_problems,
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n", encoding="utf-8")
    sh(*compose_cmd(override_file, *ALL_PROFILES, "down"), timeout=300)
    print(json.dumps({"event": "run_finished", "run_id": rid, "valid": verdict.valid, "sustainable": sustainable,
                      "problems": reported, "e2e": manifest["summaries"]["e2e"]}), flush=True)
    return 0 if verdict.valid else 1


def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    p = argparse.ArgumentParser(prog="python -m bench.harness")
    sub = p.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run", help="one run")
    r.add_argument("--mode", choices=["latency", "throughput"], default="latency")
    r.add_argument("--rate", type=float, help="messages per second (default: the trial rate with --trial)")
    r.add_argument("--repetition", type=int, default=1)
    r.add_argument("--trial", action="store_true", help="the short end-to-end trial; never publishable")
    sub.add_parser("plan", help="print the runs of STEP 2 and the time they need")
    args = p.parse_args(argv)
    bench = load_benchmark_config()
    if args.command == "plan":
        print(json.dumps(analysis.plan(bench), indent=1))
        return 0
    rate = args.rate if args.rate is not None else (bench.trial_rate_per_s if args.trial else None)
    if rate is None:
        p.error("--rate is required unless --trial")
    try:
        return execute(bench, args.mode, rate, args.repetition, args.trial)
    except BaseException:
        # Whatever stopped the run, nothing of it stays up: --remove-orphans also takes the generator and the
        # meter, which only the run's override defines. Never -v.
        sh("docker", "compose", *ALL_PROFILES, "down", "--remove-orphans", timeout=300)
        raise


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
