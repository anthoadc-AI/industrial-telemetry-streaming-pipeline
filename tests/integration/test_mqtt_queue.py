"""Integration: Mosquitto's queue for the bridge (Phase 3, Task 3.3, DETENTE 3.3.h).

The benchmark found Mosquitto dropping messages for the bridge once its queue for the bridge's client was full
(about 1040 messages with Mosquitto's defaults), while it still acknowledged every publish. mosquitto.conf now sets
max_queued_messages 60000 and max_queued_bytes 25 MiB, and the bridge logs mqtt_messages_dropped at ERROR when
Mosquitto's dropped counter grows.

The bridge runs as its container and is frozen with `docker compose pause`: the process stops with its connection
open, so it stops acknowledging and Mosquitto queues for it (at most max_inflight_messages, 20, go out unacknowledged).
A pause stays well below 1.5 times the bridge's 60 s keepalive, after which Mosquitto would drop the connection and,
with a clean session, everything queued.

- test_a_paused_bridge_loses_nothing (core): 5000 messages while paused, almost five times the old limit; every one
  reaches telemetry.raw and Mosquitto drops none. Against the old mosquitto.conf it fails (shown once, DETENTE 3.3.i).
- test_an_overflow_is_logged_with_its_count (full only): 65000 messages while paused, 5000 above the new limit; the
  bridge's mqtt_messages_dropped deltas, Mosquitto's counter and what is missing from telemetry.raw agree.
"""

from __future__ import annotations

import subprocess
import time

import pytest

import integration_support as it
from test_containers import PIPELINE, compose, events, images, logs, pipeline_containers, run_simulator, wait_for  # noqa: F401

MAX_PAUSE_S = 60  # below 1.5 x the bridge's keepalive (60 s)


def kafka_new(mark: dict[int, int]) -> int:
    now = it.watermarks("telemetry.raw")
    return sum(now[p] - mark.get(p, 0) for p in now)


def mosquitto_memory() -> str:
    container = compose("ps", "-q", "mosquitto").stdout.strip()
    stats = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", container],
                              capture_output=True, text=True, timeout=60)
    return stats.stdout.strip()


def burst_while_paused(count: int, rate: float, sys: it.MosquittoSys) -> dict:
    """Pause the bridge, publish count messages, wait for Mosquitto's next $SYS update, unpause. Returns what was
    seen while paused."""
    paused_at = time.monotonic()
    paused = compose(*PIPELINE, "pause", "bridge")
    assert paused.returncode == 0, paused.stderr[-2000:]
    try:
        sim = run_simulator("--count", str(count), "--rate", f"{rate:g}", "--seed", "7")
        assert sim.returncode == 0, sim.stderr[-2000:]
        finished = events(sim.stdout + sim.stderr, "run_finished")[0]
        assert (finished["messages_sent"], finished["messages_acked"]) == (count, count)
        published_s = time.monotonic() - paused_at
        sys.value(it.SYS_STORED, after=time.monotonic())  # one $SYS update after the last publish
        stored = sys.peak(it.SYS_STORED, since=paused_at)
        memory = mosquitto_memory()
    finally:
        unpaused = compose(*PIPELINE, "unpause", "bridge")
    paused_s = time.monotonic() - paused_at
    assert unpaused.returncode == 0, unpaused.stderr[-2000:]
    assert paused_s < MAX_PAUSE_S, f"paused {paused_s:.0f} s: Mosquitto may have dropped the bridge's connection"
    return {"published_s": round(published_s, 1), "paused_s": round(paused_s, 1), "stored": stored,
            "mosquitto_memory": memory}


def drain(mark: dict[int, int], expected: int, timeout: float = 120, still_s: float = 15) -> int:
    """Records added to telemetry.raw since mark, once there are expected of them or the count has stopped
    moving for still_s seconds."""
    deadline = time.monotonic() + timeout
    last, since = -1, time.monotonic()
    while time.monotonic() < deadline:
        n = kafka_new(mark)
        if n >= expected:
            return n
        if n != last:
            last, since = n, time.monotonic()
        elif time.monotonic() - since >= still_s:
            return n
        time.sleep(1)
    return kafka_new(mark)


def run_case(pipeline_containers, offsets_mark, count: int, rate: float) -> dict:  # noqa: F811
    pipeline_containers(PIPELINE, "bridge")
    wait_for(PIPELINE, "bridge", "mqtt_subscribed")
    with it.MosquittoSys(it.SYS_DROPPED, it.SYS_STORED) as sys:
        dropped_before = sys.value(it.SYS_DROPPED, after=0)  # the retained value
        seen = burst_while_paused(count, rate, sys)
        arrived = drain(offsets_mark["telemetry.raw"], count)
        dropped = sys.value(it.SYS_DROPPED, after=time.monotonic()) - dropped_before
    records = it.read_since("telemetry.raw", offsets_mark["telemetry.raw"])
    pairs = {(r["value"]["asset_id"], r["value"]["seq"]) for r in records}
    print(f"\n{count} published while the bridge was paused: {seen}; in telemetry.raw {arrived} "
          f"({len(pairs)} distinct); Mosquitto dropped {dropped:g}")
    return {**seen, "arrived": arrived, "distinct": len(pairs), "dropped": dropped}


def logged_drops() -> int:
    """The sum of the deltas of the bridge's mqtt_messages_dropped events."""
    return sum(e["delta"] for e in events(logs(PIPELINE, "bridge"), "mqtt_messages_dropped"))


def test_a_paused_bridge_loses_nothing(stack, pipeline_containers, offsets_mark):  # noqa: F811
    count = 5000
    r = run_case(pipeline_containers, offsets_mark, count, rate=200)  # 200 cycles of 5 assets: 1000 msg/s
    assert (r["arrived"], r["distinct"], r["dropped"]) == (count, count, 0), r
    assert logged_drops() == 0
    # The case exercised the old limit: Mosquitto held more than 4000 messages for the paused bridge (its own
    # retained $SYS messages, about 60, included), where the old configuration held about 1040.
    assert r["stored"] > 4000, r


@pytest.mark.full
def test_an_overflow_is_logged_with_its_count(stack, pipeline_containers, offsets_mark):  # noqa: F811
    count = 65000
    r = run_case(pipeline_containers, offsets_mark, count, rate=2000)
    missing = count - r["distinct"]
    assert missing > 0 and r["dropped"] == missing, r
    deadline = time.monotonic() + it.SYS_INTERVAL_S * 2
    while logged_drops() != missing and time.monotonic() < deadline:
        time.sleep(1)
    assert logged_drops() == missing, (logged_drops(), r)
