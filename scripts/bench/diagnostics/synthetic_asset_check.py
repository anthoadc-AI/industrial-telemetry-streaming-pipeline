"""Diagnostic (Task 3.3 STEP 1, item 1.1.1, disposable): what the pipeline does with an unknown asset_id.

Publishes, interleaved, SAMPLES valid telemetry messages for an asset that is
not in config/telemetry.yaml (synthetic-0001, type pump) and as many for a
configured one (pump-01, the control), all with a bearing temperature of
100 C: at or above both the warning (80) and critical (95) thresholds of
config/alarms.yaml, so the control raises two alarms at its third sample
(persistence_samples 3). Run with the core stack up and with the bridge and
the alarm consumer already running (see the STEP 1 report for the commands).

    python scripts/bench/diagnostics/synthetic_asset_check.py
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

SAMPLES = 4
ASSETS = [("synthetic-0001", "pump"), ("pump-01", "pump")]


def main() -> int:
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="bench-diag-synthetic-check")
    client.connect("localhost", 1883, keepalive=60)
    client.loop_start()
    for seq in range(SAMPLES):
        for asset_id, asset_type in ASSETS:
            message = {
                "asset_id": asset_id, "asset_type": asset_type, "seq": seq,
                "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "synthetic": True, "bearing_temperature_c": 100.0, "vibration_rms_mm_s": 2.6,
                "motor_current_a": 25.0, "speed_rpm": 2962.0, "injected_fault": None,
            }
            client.publish(f"plant/{asset_id}/telemetry", json.dumps(message), qos=1).wait_for_publish(10)
        time.sleep(1.0)
    client.disconnect()
    client.loop_stop()
    print(json.dumps({"event": "synthetic_check_published", "per_asset": SAMPLES, "assets": [a for a, _ in ASSETS]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
