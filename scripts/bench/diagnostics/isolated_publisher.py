"""Diagnostic (Task 3.3 STEP 0, disposable): valid telemetry for one asset, one message per second.

The simulator always publishes one message per asset per cycle, five back to
back; this publisher sends isolated messages through the real bridge, so the
bridge's own produce_to_ack_ms can be read at the isolated cadence. Values
are fixed and valid against schemas/telemetry_v1.avsc; QoS 1, the asset's
topic from config/topics.yaml.

    python scripts/bench/diagnostics/isolated_publisher.py --count 220 --interval 1.0
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=220)
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--asset", default="pump-01")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=1883)
    args = p.parse_args()

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="bench-diag-isolated-pub")
    client.connect(args.host, args.port, keepalive=60)
    client.loop_start()
    start = time.monotonic() + 0.5
    for seq in range(args.count):
        delay = start + seq * args.interval - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        message = {
            "asset_id": args.asset, "asset_type": "pump", "seq": seq,
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "synthetic": True, "bearing_temperature_c": 53.1, "vibration_rms_mm_s": 2.6,
            "motor_current_a": 25.0, "speed_rpm": 2962.0, "injected_fault": None,
        }
        client.publish(f"plant/{args.asset}/telemetry", json.dumps(message), qos=1).wait_for_publish(10)
    client.disconnect()
    client.loop_stop()
    print(json.dumps({"event": "isolated_publisher_finished", "sent": args.count}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
