"""Industrial telemetry simulator: publishes synthetic asset telemetry over MQTT.

Run from the repository root:

    python -m edge.simulator --count 200
    python -m edge.simulator --duration 120 --fault pump-01:imbalance:30

Each cycle produces one sample per configured asset and publishes it as JSON,
QoS 1, to the per-asset MQTT topic from config/topics.yaml. Every message
carries ``synthetic: true`` and a ``ts`` in ISO 8601 UTC. Assets, channels,
model parameters and fault modes come from config/telemetry.yaml.

Stopping: the run ends at --count messages, after --duration seconds, or on
SIGINT, SIGTERM or (Windows) SIGBREAK, whichever comes first. In every case
the simulator waits for the broker to acknowledge each message already
published, up to --flush-timeout, then logs the totals and exits. On Windows,
signals sent from outside a console do not reach a native Python process,
so --count or --duration is the reliable way to bound a run there.

Exit status: 0 when every published message was acknowledged, 1 otherwise
or on a start-up error, 2 on invalid arguments.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import sys
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from common import jsonlog
from config.loader import ConfigError, load_config, validate_fault
from edge.simulator.model import AssetModel

log = logging.getLogger("edge.simulator")

CONNECT_TIMEOUT_S = 10.0


def _positive_int(text: str) -> int:
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def _positive_float(text: str) -> float:
    value = float(text)
    if not value > 0:
        raise argparse.ArgumentTypeError("must be a positive number")
    return value


def _fault(text: str) -> tuple[str, str, float]:
    parts = text.split(":")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError("expected <asset>:<mode>:<start_s>")
    try:
        return parts[0], parts[1], float(parts[2])
    except ValueError:
        raise argparse.ArgumentTypeError("start_s must be a number") from None


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m edge.simulator", description="Publish synthetic asset telemetry over MQTT.")
    p.add_argument("--count", type=_positive_int, help="stop after this many messages in total, across all assets")
    p.add_argument("--duration", type=_positive_float, help="stop after this many seconds")
    p.add_argument("--rate", type=_positive_float,
                   help="sample cycles per second; each cycle publishes one message per asset "
                        "(default: 1 / sample_interval_s from config/telemetry.yaml)")
    p.add_argument("--fault", type=_fault, action="append", default=[], metavar="ASSET:MODE:START_S",
                   help="inject a fault mode on an asset, START_S seconds after start; repeatable")
    p.add_argument("--seed", type=int, help="random seed, for a reproducible sequence of values")
    p.add_argument("--mqtt-host", default=os.environ.get("MQTT_HOST", "localhost"), help="default: $MQTT_HOST or localhost")
    p.add_argument("--mqtt-port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")), help="default: $MQTT_PORT or 1883")
    p.add_argument("--client-id", default=f"edge-simulator-{os.getpid()}")
    p.add_argument("--flush-timeout", type=_positive_float, default=10.0,
                   help="seconds to wait for outstanding acknowledgements at shutdown (default: 10)")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args(argv)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Publisher:
    """paho-mqtt 2.x client that counts broker acknowledgements for QoS 1 publishes."""

    def __init__(self, host: str, port: int, client_id: str):
        self.host, self.port = host, port
        self.connected = threading.Event()
        self._lock = threading.Lock()
        self.sent = 0
        self.acked = 0
        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("mqtt_connect_refused", extra={"reason": str(reason_code)})
            return
        log.info("mqtt_connected", extra={"host": self.host, "port": self.port})
        self.connected.set()

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.connected.clear()
        if reason_code.is_failure:
            log.warning("mqtt_disconnected", extra={"reason": str(reason_code)})

    def _on_publish(self, client, userdata, mid, reason_code, properties):
        with self._lock:
            self.acked += 1

    def start(self) -> bool:
        try:
            self.client.connect(self.host, self.port, keepalive=60)
        except OSError as e:
            log.error("mqtt_connect_failed", extra={"host": self.host, "port": self.port, "error": str(e)})
            return False
        self.client.loop_start()
        if not self.connected.wait(CONNECT_TIMEOUT_S):
            log.error("mqtt_connect_timeout", extra={"host": self.host, "port": self.port, "timeout_s": CONNECT_TIMEOUT_S})
            self.client.loop_stop()
            return False
        return True

    def publish(self, topic: str, payload: str, qos: int) -> None:
        info = self.client.publish(topic, payload, qos=qos)
        # NO_CONN means paho queued the message for delivery after reconnecting.
        if info.rc not in (mqtt.MQTT_ERR_SUCCESS, mqtt.MQTT_ERR_NO_CONN):
            log.error("mqtt_publish_failed", extra={"topic": topic, "rc": info.rc})
            return
        with self._lock:
            self.sent += 1

    def flush_and_close(self, timeout_s: float) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            with self._lock:
                if self.acked >= self.sent:
                    break
            time.sleep(0.05)
        with self._lock:
            complete = self.acked >= self.sent
        self.client.disconnect()
        self.client.loop_stop()
        return complete


def install_signal_handlers(stop: threading.Event) -> None:
    def handler(signum, frame):
        log.info("signal_received", extra={"signal": signal.Signals(signum).name})
        stop.set()

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), handler)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    jsonlog.configure(args.log_level)

    try:
        cfg = load_config()
    except ConfigError as e:
        log.error("config_invalid", extra={"error": str(e)})
        return 1
    try:
        extra_faults = [validate_fault(cfg.telemetry, a, m, s, f"--fault {a}:{m}:{s:g}") for a, m, s in args.fault]
    except ConfigError as e:
        log.error("argument_invalid", extra={"error": str(e)})
        return 2
    telemetry, topic = cfg.telemetry, cfg.topics.mqtt_telemetry
    schedule = list(telemetry.fault_schedule) + extra_faults

    rate = args.rate or 1.0 / telemetry.sample_interval_s
    period = 1.0 / rate
    rng = random.Random(args.seed)
    models = [AssetModel.build(a, telemetry.ambient_temperature_c, dict(telemetry.faults), schedule, rng)
              for a in telemetry.assets]
    seq = {a.id: 0 for a in telemetry.assets}

    stop = threading.Event()
    install_signal_handlers(stop)

    publisher = Publisher(args.mqtt_host, args.mqtt_port, args.client_id)
    if not publisher.start():
        return 1

    log.info("run_started", extra={
        "assets": [a.id for a in telemetry.assets], "rate_hz": rate, "count": args.count,
        "duration_s": args.duration, "seed": args.seed, "synthetic": True,
        "faults": [f"{s.asset}:{s.mode}:{s.start_s:g}" for s in schedule],
    })

    announced: set[tuple[str, str]] = set()
    start = time.monotonic()
    cycles = 0
    reason = None
    while reason is None:
        if stop.is_set():
            reason = "signal"
            break
        if args.duration is not None and time.monotonic() - start >= args.duration:
            reason = "duration"
            break

        t = cycles * period
        for m in models:
            if args.count is not None and publisher.sent >= args.count:
                break
            sample = m.step(t, period)
            asset = m.asset
            for fault in sample.active_faults:
                if (asset.id, fault) not in announced:
                    announced.add((asset.id, fault))
                    log.info("fault_active", extra={"asset_id": asset.id, "fault": fault, "sim_time_s": round(t, 3)})
            message = {
                "asset_id": asset.id,
                "asset_type": asset.type,
                "seq": seq[asset.id],
                "ts": utc_now_iso(),
                "synthetic": True,
                **{ch: (round(sample.values[ch], 3) if ch in sample.values else None) for ch in telemetry.channels},
                "injected_fault": ",".join(sample.active_faults) or None,
            }
            seq[asset.id] += 1
            publisher.publish(topic.for_asset(asset.id), json.dumps(message), topic.qos)
            log.debug("message_published", extra={"asset_id": asset.id, "seq": message["seq"]})
        cycles += 1

        if args.count is not None and publisher.sent >= args.count:
            reason = "count"
            break
        remaining = start + cycles * period - time.monotonic()
        if remaining > 0:
            stop.wait(remaining)

    complete = publisher.flush_and_close(args.flush_timeout)
    summary = {
        "reason": reason, "messages_sent": publisher.sent, "messages_acked": publisher.acked,
        "cycles": cycles, "elapsed_s": round(time.monotonic() - start, 3),
    }
    if complete:
        log.info("run_finished", extra=summary)
        return 0
    log.warning("run_finished_unacknowledged", extra={**summary, "flush_timeout_s": args.flush_timeout})
    return 1


if __name__ == "__main__":
    sys.exit(main())
