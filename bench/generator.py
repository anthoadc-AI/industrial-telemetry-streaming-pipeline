"""Open-loop load generator (Task 3.3, 1.3.1): publishes telemetry to Mosquitto on a fixed schedule.

Runs in a container of the benchmark image (bench/harness.py starts it), with the run's generated config/
mounted read-only, so it publishes for exactly the assets the bridge and the alarm consumer know:

    python -m bench.generator --rate 1000 --duration 330 --t0-ms 1791700000000 --out /out

It waits until the wall clock reaches --t0-ms (the harness picks it a few seconds ahead and gives the same value
to the meter), then sends message i at t0 + i / rate on the wall clock with ts = its scheduled instant
(bench/schedule.py), QoS 1 as config/topics.yaml says, with a paho in-flight window of
generator.max_inflight. Its own lateness goes into a fixed histogram. At the end it waits for the broker's
acknowledgements and writes generator.json into --out: sent and acknowledged counts (total and per asset), the
lateness histogram, t0, the rate, its CPU time and wall time. Stops early on SIGTERM.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from collections import Counter
from pathlib import Path

import paho.mqtt.client as mqtt

from bench import schedule
from bench.histogram import Histogram
from common import jsonlog
from config.loader import load_benchmark_config, load_config

log = logging.getLogger("bench.generator")
ACK_WAIT_S = 30.0


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m bench.generator")
    p.add_argument("--rate", type=float, required=True, help="messages per second, all assets together")
    p.add_argument("--duration", type=float, required=True, help="seconds of sending (warm-up plus measured)")
    p.add_argument("--t0-ms", type=int, required=True, help="epoch ms of message 0's scheduled instant")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--mqtt-host", default=os.environ.get("MQTT_HOST", "localhost"))
    p.add_argument("--mqtt-port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    return p.parse_args(argv)


class Acks:
    def __init__(self):
        self.lock = threading.Lock()
        self.count = 0

    def on_publish(self, client, userdata, mid, reason_code, properties):
        with self.lock:
            self.count += 1


def main(argv: list[str] | None = None) -> int:  # pragma: no cover - runs in a container, exercised by the harness trial
    args = parse_args(argv)
    jsonlog.configure("INFO")
    config, bench = load_config(env={}), load_benchmark_config()
    topic = config.topics.mqtt_telemetry
    assets = tuple(a.id for a in config.telemetry.assets)
    plan = schedule.plan_for(args.rate, assets, args.duration, args.t0_ms)
    normals = {a: schedule.normal_values(config, a) for a in assets}

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    acks = Acks()
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=f"bench-generator-{os.getpid()}")
    client.max_inflight_messages_set(bench.generator_max_inflight)
    client.max_queued_messages_set(0)
    client.on_publish = acks.on_publish
    client.connect(args.mqtt_host, args.mqtt_port, keepalive=60)
    client.loop_start()

    late, per_asset = Histogram(), Counter()

    def send(i: int) -> None:
        message = schedule.sample(plan, bench.fault_pattern, config, i, normals[plan.asset_of(i)])
        client.publish(topic.for_asset(message["asset_id"]), json.dumps(message, separators=(",", ":")), qos=topic.qos)
        per_asset[message["asset_id"]] += 1

    wait = args.t0_ms / 1000 - time.time()
    log.info("generator_ready", extra={"rate": args.rate, "count": plan.count, "assets": len(assets),
                                       "starts_in_s": round(wait, 3)})
    if wait > 0:
        time.sleep(wait)
    cpu0, wall0 = time.process_time(), time.monotonic()
    # On the wall clock, the clock of ts (see bench/schedule.py).
    sent = schedule.run_schedule(plan, args.t0_ms / 1000, time.time, time.sleep, send, late.add, stop=stop.is_set)
    send_wall_s, send_cpu_s = time.monotonic() - wall0, time.process_time() - cpu0

    deadline = time.monotonic() + ACK_WAIT_S
    while acks.count < sent and time.monotonic() < deadline:
        time.sleep(0.05)
    client.disconnect()
    client.loop_stop()

    result = {
        "rate_per_s": args.rate, "t0_ms": args.t0_ms, "duration_s": args.duration, "planned": plan.count, "sent": sent,
        "acknowledged": acks.count, "per_asset_sent": dict(per_asset), "assets": len(assets),
        "late": late.to_json(), "late_summary": late.summary(), "send_wall_s": round(send_wall_s, 3),
        "send_cpu_s": round(send_cpu_s, 3), "stopped_early": stop.is_set(),
    }
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "generator.json").write_text(json.dumps(result, indent=1) + "\n", encoding="utf-8")
    log.info("generator_finished", extra={k: result[k] for k in ("planned", "sent", "acknowledged", "send_wall_s", "send_cpu_s")})
    return 0 if sent == plan.count and acks.count == sent else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
