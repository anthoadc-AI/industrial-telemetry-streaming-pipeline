"""Integration: the pipeline's components in containers (Phase 3, Task 3.3, owner decision E1).

The session stack is the usual one (core or full); these tests add the pipeline containers of
docker-compose.yml with --no-deps, so the running stack is never recreated, and always remove them
again, so no later test shares the stack with a containerised bridge (two bridges would both receive
every MQTT message). The consumer and the sink run on a group of their own, positioned at the end of
the topics, so every count below is exact. The images are built once per module.
"""

from __future__ import annotations

import json
import os
import subprocess
import time

import pytest

import integration_support as it

PIPELINE = ("--profile", "pipeline")
WITH_SINK = ("--profile", "full", "--profile", "pipeline-tb")


def compose(*args: str, env: dict[str, str] | None = None, timeout: float = 900) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "compose", *args], cwd=it.REPO_ROOT, capture_output=True, text=True,
                          encoding="utf-8", timeout=timeout, env={**os.environ, **(env or {})})


def logs(profiles: tuple[str, ...], service: str) -> str:
    return compose(*profiles, "logs", "--no-color", "--no-log-prefix", service).stdout


def events(text: str, name: str) -> list[dict]:
    return [e for e in it.json_events(text) if e.get("event") == name]


def non_json_lines(text: str) -> int:
    bad = 0
    for line in text.splitlines():
        if line.strip():
            try:
                json.loads(line)
            except ValueError:
                bad += 1
    return bad


def wait_for(profiles: tuple[str, ...], service: str, event: str, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if events(logs(profiles, service), event):
            return
        time.sleep(0.5)
    pytest.fail(f"{service}: no {event} within {timeout:g} s:\n{logs(profiles, service)[-2000:]}")


def run_simulator(*args: str) -> subprocess.CompletedProcess:
    return compose("--profile", "simulator", "run", "--rm", "-T", "--no-deps", "simulator", *args, timeout=300)


@pytest.fixture(scope="module")
def images(stack):
    built = compose(*PIPELINE, "build")
    assert built.returncode == 0, built.stderr[-2000:]


@pytest.fixture
def pipeline_containers(images):
    """Start the given pipeline services; remove them at the end whatever happens."""
    started: list[tuple[tuple[str, ...], tuple[str, ...]]] = []

    def start(profiles: tuple[str, ...], *services: str, env: dict[str, str] | None = None) -> None:
        up = compose(*profiles, "up", "-d", "--no-deps", "--force-recreate", *services, env=env)
        assert up.returncode == 0, up.stderr[-2000:]
        started.append((profiles, services))

    yield start
    for profiles, services in started:
        compose(*profiles, "rm", "--stop", "--force", *services)


def stop(profiles: tuple[str, ...], *services: str) -> None:
    stopped = compose(*profiles, "stop", *services)
    assert stopped.returncode == 0, stopped.stderr[-2000:]


def exit_code(profiles: tuple[str, ...], service: str) -> int:
    ps = compose(*profiles, "ps", "--all", "--format", "json", service).stdout
    rows = [json.loads(line) for line in ps.splitlines() if line.strip()]
    return rows[0]["ExitCode"]


def test_pipeline_in_containers_end_to_end(stack, pipeline_containers, offsets_mark, fresh_group):
    """Simulator, bridge and alarm consumer as containers: every message through, the fault's alarm raised and
    then cleared, and both long-running components stop cleanly on SIGTERM with exact totals.

    The second simulator run, with no fault, clears the alarm: an alarm left active on telemetry.alarms would be
    restored by every later consumer in the session (test_restore_compaction counts its transitions)."""
    group = fresh_group()
    pipeline_containers(PIPELINE, "bridge", "alarm-consumer", env={"ALARM_CONSUMER_GROUP": group})
    wait_for(PIPELINE, "bridge", "mqtt_subscribed")
    wait_for(PIPELINE, "alarm-consumer", "run_started")

    started_ms = int(time.time() * 1000)
    sim = run_simulator("--count", "200", "--seed", "42", "--fault", "pump-01:imbalance:20")
    assert sim.returncode == 0, sim.stderr[-2000:]
    finished = events(sim.stdout + sim.stderr, "run_finished")[0]
    assert (finished["messages_sent"], finished["messages_acked"]) == (200, 200)
    calm = run_simulator("--count", "50", "--seed", "43")
    assert calm.returncode == 0, calm.stderr[-2000:]

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        new = sum(it.watermarks("telemetry.raw")[p] - offsets_mark["telemetry.raw"].get(p, 0)
                  for p in it.watermarks("telemetry.raw"))
        if new >= 250 and it.committed(group, "telemetry.raw") == it.watermarks("telemetry.raw"):
            break
        time.sleep(1)
    stop(PIPELINE, "bridge", "alarm-consumer")

    bridge_log, consumer_log = logs(PIPELINE, "bridge"), logs(PIPELINE, "alarm-consumer")
    b = events(bridge_log, "run_finished")
    c = events(consumer_log, "run_finished")
    assert b and c, f"no run_finished:\n{bridge_log[-1500:]}\n{consumer_log[-1500:]}"
    assert (b[0]["reason"], b[0]["messages_received"], b[0]["messages_delivered"], b[0]["messages_invalid"]) == \
        ("signal", 250, 250, 0)
    assert (c[0]["reason"], c[0]["messages_processed"], c[0]["messages_dead_lettered"], c[0]["active_at_exit"]) == \
        ("signal", 250, 0, 0)
    assert exit_code(PIPELINE, "bridge") == 0 and exit_code(PIPELINE, "alarm-consumer") == 0

    records = it.read_since("telemetry.raw", offsets_mark["telemetry.raw"])
    assert len(records) == 250 and all(r["key"].decode() == r["value"]["asset_id"] for r in records)
    alarms = [r["value"] for r in it.read_since("telemetry.alarms", offsets_mark["telemetry.alarms"])
              if r["value"] and int(r["value"]["ts"].timestamp() * 1000) >= started_ms]
    raised = {a["alarm_id"] for a in alarms if a["state"] == "RAISED"
              and (a["asset_id"], a["channel"], a["severity"]) == ("pump-01", "vibration_rms_mm_s", "WARNING")}
    cleared = {a["alarm_id"] for a in alarms if a["state"] == "CLEARED"}
    assert raised and raised <= cleared
    assert not it.read_since("telemetry.dlq", offsets_mark["telemetry.dlq"], deserialize=False)
    assert non_json_lines(sim.stdout + sim.stderr + calm.stdout + calm.stderr) == non_json_lines(bridge_log) == \
        non_json_lines(consumer_log) == 0


@pytest.mark.full
def test_sink_in_container_forwards_telemetry(stack, pipeline_containers, offsets_mark):
    """The ThingsBoard sink as a container reaches ThingsBoard by service name and forwards every record."""
    import test_thingsboard_sink as tbs  # its ThingsBoard helpers


    tb = tbs.irc.ThingsBoard(f"http://localhost:{tbs.irc.setting('TB_HTTP_PORT', '8080')}")
    tb.login(tbs.irc.setting("TB_TENANT_USERNAME"), tbs.irc.setting("TB_TENANT_PASSWORD"))
    group = tbs.fresh_sink_group("it-sink-ctr")
    pipeline_containers(WITH_SINK, "thingsboard-sink", env={"THINGSBOARD_SINK_GROUP": group})
    wait_for(WITH_SINK, "thingsboard-sink", "thingsboard_connected")

    started = int(time.time() * 1000)
    records = [{"asset_id": "pump-02", "asset_type": "pump", "seq": i, "ts": started + i * 100, "synthetic": True,
                "bearing_temperature_c": 50.0, "vibration_rms_mm_s": 2.0, "motor_current_a": 20.0, "speed_rpm": 2900.0,
                "injected_fault": None} for i in range(20)]
    it.produce_telemetry(records)

    def arrived():
        d = tbs.device(tb, "pump-02")
        v = tb.request("GET", f"/api/plugins/telemetry/DEVICE/{d['id']['id']}/values/timeseries",
                       query={"keys": "seq", "startTs": started, "endTs": started + 20 * 100, "limit": 100})
        seqs = sorted(int(s["value"]) for s in v.get("seq", []))
        return seqs if seqs == list(range(20)) else None

    assert tbs.wait_until(arrived), "the 20 records did not all reach ThingsBoard within 90 s"
    stop(WITH_SINK, "thingsboard-sink")
    sink_log = logs(WITH_SINK, "thingsboard-sink")
    assert exit_code(WITH_SINK, "thingsboard-sink") == 0 and non_json_lines(sink_log) == 0
