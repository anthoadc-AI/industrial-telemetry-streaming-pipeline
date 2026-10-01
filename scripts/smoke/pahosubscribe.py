"""MQTT subscribe probe: print and count the messages arriving on a topic filter.

Adapted from the coursework scratch script of the same name: translated to
English, moved to the paho-mqtt 2.x callback API, and bounded with --count
and --duration. By default it subscribes to the pipeline's telemetry filter
from config/topics.yaml.

    python scripts/smoke/pahosubscribe.py --count 200 --duration 120 --quiet
"""

import argparse
import os
import sys
import threading
import time
from collections import Counter
from pathlib import Path

import paho.mqtt.client as mqtt

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.loader import load_config  # noqa: E402


def main() -> int:
    # Line-buffered, so progress shows up promptly when output is redirected.
    sys.stdout.reconfigure(line_buffering=True)
    p = argparse.ArgumentParser(description="Subscribe to an MQTT topic filter and count messages.")
    p.add_argument("--host", default=os.environ.get("MQTT_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    p.add_argument("--topic", help="topic filter (default: the telemetry subscription in config/topics.yaml)")
    p.add_argument("--count", type=int, help="stop after this many messages")
    p.add_argument("--duration", type=float, help="stop after this many seconds")
    p.add_argument("--quiet", action="store_true", help="print only the summary, not every message")
    args = p.parse_args()

    topic = args.topic or load_config().topics.mqtt_telemetry.subscription
    received = Counter()
    done = threading.Event()
    subscribed = threading.Event()

    def on_connect(client, userdata, flags, reason_code, properties):
        print(f"connected to {args.host}:{args.port} ({reason_code})")
        client.subscribe(topic, qos=1)

    def on_subscribe(client, userdata, mid, reason_code_list, properties):
        print(f"subscribed to {topic}")
        subscribed.set()

    def on_message(client, userdata, msg):
        received[msg.topic] += 1
        if not args.quiet:
            print(f"{msg.topic} {msg.payload.decode('utf-8', errors='replace')}")
        if args.count is not None and sum(received.values()) >= args.count:
            done.set()

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=f"smoke-sub-{os.getpid()}")
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    try:
        client.connect(args.host, args.port, keepalive=60)
    except OSError as e:
        print(f"cannot connect to {args.host}:{args.port}: {e}", file=sys.stderr)
        return 1
    client.loop_start()

    deadline = None if args.duration is None else time.monotonic() + args.duration
    try:
        while not done.is_set() and (deadline is None or time.monotonic() < deadline):
            done.wait(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()
        client.loop_stop()

    total = sum(received.values())
    print(f"received {total} message(s) on {len(received)} topic(s)")
    for t, n in sorted(received.items()):
        print(f"  {t}: {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
