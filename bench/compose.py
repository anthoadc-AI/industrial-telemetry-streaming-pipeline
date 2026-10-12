"""The compose override of one benchmark run, generated from config/benchmark.yaml (never written by hand).

Layered over docker-compose.yml (docker compose -f docker-compose.yml -f <run>/compose.override.yml), it:
- pins every service to its vCPUs (cpu.cpusets, owner decision D-E): pinning instead of a CPU quota, so no
  service is throttled mid-request by the CFS period;
- mounts the run's generated config/ read-only over /app/config of the bridge, the alarm consumer, the generator
  and the meter (D-A clones, D2 linger), so they all see the same assets and settings;
- sets the bridge's log level (INFO when F4 needs its per-message lines, WARNING otherwise);
- adds the generator and the meter (profile bench, the benchmark image), and for the calibration the loader and
  meter-calibration, whose run-specific values come from
  environment variables the harness sets: BENCH_RATE, BENCH_DURATION_S, BENCH_T0_MS, BENCH_WARMUP_S,
  BENCH_MEASURED_S, BENCH_COUNT (empty by default, so other compose commands with the override print no warning).
"""

from __future__ import annotations

from pathlib import Path

from config.loader import BenchmarkConfig

BENCH_IMAGE = "industrial-telemetry/bench:local"
HARDENING = {"init": True, "read_only": True, "cap_drop": ["ALL"], "security_opt": ["no-new-privileges:true"],
             "stop_grace_period": "30s"}


def _bind(source: Path, target: str, read_only: bool) -> dict:
    # POSIX form: compose resolves a relative source from the project directory (the repository root), so the
    # versioned override carries no machine-specific absolute path.
    return {"type": "bind", "source": source.as_posix(), "target": target, "read_only": read_only}


def override(bench: BenchmarkConfig, config_dir: Path, out_dir: Path, bridge_log_level: str) -> dict:
    config_mount = _bind(config_dir, "/app/config", True)
    out_mount = _bind(out_dir, "/out", False)
    services: dict[str, dict] = {name: {"cpuset": cpuset} for name, cpuset in bench.cpusets.items()}
    services.setdefault("bridge", {}).update({
        "command": ["python", "-m", "bridge.mqtt_to_kafka", "--log-level", bridge_log_level],
        "volumes": [config_mount]})
    services.setdefault("alarm-consumer", {}).update({"volumes": [config_mount]})
    services.setdefault("generator", {}).update({
        "image": BENCH_IMAGE, "pull_policy": "never", "profiles": ["bench"],
        "entrypoint": ["python", "-m", "bench.generator"],
        "command": ["--rate", "${BENCH_RATE:-}", "--duration", "${BENCH_DURATION_S:-}", "--t0-ms", "${BENCH_T0_MS:-}",
                    "--out", "/out"],
        "environment": {"MQTT_HOST": "mosquitto", "MQTT_PORT": "1883"},
        "volumes": [config_mount, out_mount], "depends_on": {"mosquitto": {"condition": "service_healthy"}},
        **HARDENING})
    services.setdefault("meter", {}).update({
        "image": BENCH_IMAGE, "pull_policy": "never", "profiles": ["bench"],
        "entrypoint": ["python", "-m", "bench.meter"],
        "command": ["--t0-ms", "${BENCH_T0_MS:-}", "--warmup", "${BENCH_WARMUP_S:-}", "--measured", "${BENCH_MEASURED_S:-}",
                    "--out", "/out"],
        "environment": {"KAFKA_BOOTSTRAP": "broker:29092", "SCHEMA_REGISTRY_URL": "http://schema-registry:8081"},
        "volumes": [config_mount, out_mount],
        "depends_on": {"broker": {"condition": "service_healthy"}, "schema-registry": {"condition": "service_healthy"}},
        **HARDENING})
    # Calibration (owner, DETENTE 3.3.d): the loader preloads BENCH_COUNT records; meter-calibration reads them
    # from the beginning and reports its own rate. Both on the meter's vCPUs.
    kafka_env = {"KAFKA_BOOTSTRAP": "broker:29092", "SCHEMA_REGISTRY_URL": "http://schema-registry:8081"}
    kafka_deps = {"broker": {"condition": "service_healthy"}, "schema-registry": {"condition": "service_healthy"}}
    for name, entrypoint, command in (
            ("loader", ["python", "-m", "bench.loader"], ["--count", "${BENCH_COUNT:-}"]),
            ("meter-calibration", ["python", "-m", "bench.meter"],
             ["--t0-ms", "0", "--warmup", "0", "--measured", "0", "--from-beginning", "--expect", "${BENCH_COUNT:-}",
              "--out", "/out"])):
        services[name] = {"image": BENCH_IMAGE, "pull_policy": "never", "profiles": ["bench"], "entrypoint": entrypoint,
                          "command": command, "environment": dict(kafka_env), "volumes": [config_mount, out_mount],
                          "depends_on": kafka_deps, **HARDENING}
        if "meter" in bench.cpusets:
            services[name]["cpuset"] = bench.cpusets["meter"]
    return {"services": services}
