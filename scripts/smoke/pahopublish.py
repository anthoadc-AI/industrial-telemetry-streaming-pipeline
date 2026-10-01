"""MQTT publish probe: publish numbered test messages to a broker.

Adapted from the coursework scratch script of the same name: translated to
English, moved to the paho-mqtt 2.x callback API, and bounded with --count
and --duration. Payloads are synthetic.

    python scripts/smoke/pahopublish.py --count 5
"""

import argparse
import json
import os
import random
import sys
import time

import paho.mqtt.client as mqtt


def main() -> int:
    p = argparse.ArgumentParser(description="Publish synthetic test messages to an MQTT broker.")
    p.add_argument("--host", default=os.environ.get("MQTT_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    p.add_argument("--topic", default="smoke/probe")
    p.add_argument("--count", type=int, default=10, help="messages to publish (default: 10)")
    p.add_argument("--duration", type=float, help="stop after this many seconds, even if --count is not reached")
    p.add_argument("--interval", type=float, default=1.0, help="seconds between messages (default: 1)")
    args = p.parse_args()

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=f"smoke-pub-{os.getpid()}")
    try:
        client.connect(args.host, args.port, keepalive=60)
    except OSError as e:
        print(f"cannot connect to {args.host}:{args.port}: {e}", file=sys.stderr)
        return 1
    client.loop_start()
    print(f"connected to {args.host}:{args.port}")

    start = time.monotonic()
    sent = 0
    try:
        for i in range(args.count):
            if args.duration is not None and time.monotonic() - start >= args.duration:
                break
            payload = json.dumps({"probe_seq": i, "temperature": random.randint(0, 100), "synthetic": True})
            client.publish(args.topic, payload, qos=1).wait_for_publish(timeout=5)
            sent += 1
            print(f"published to {args.topic}: {payload}")
            if i + 1 < args.count:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()
        client.loop_stop()
    print(f"published {sent} message(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
