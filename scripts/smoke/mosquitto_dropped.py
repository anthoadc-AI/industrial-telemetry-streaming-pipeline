"""Mosquitto's dropped-messages counter for the smoke runs (DETENTE 3.3.h).

    dropped_total()                 the counter now (its retained value)
    dropped_total(after=instant)    the counter as of Mosquitto's first $SYS update at or after a monotonic instant

$SYS values are retained and a counter is published again only when it changes; $SYS/broker/uptime changes at every
update (every sys_interval, 10 s), so it marks one. Measured on Mosquitto 2.1.2. The counter is broker-wide.
"""

from __future__ import annotations

import os
import time

import paho.mqtt.client as mqtt

DROPPED = "$SYS/broker/publish/messages/dropped"
UPTIME = "$SYS/broker/uptime"
TIMEOUT_S = 25.0  # two sys_interval and a margin


def dropped_total(after: float | None = None, host: str = "localhost", port: int = 1883) -> int:
    samples: list[tuple[float, str, bytes, bool]] = []
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=f"smoke-sys-{os.getpid()}")
    client.on_message = lambda c, userdata, msg: samples.append((time.monotonic(), msg.topic, msg.payload, msg.retain))
    client.connect(host, port, keepalive=60)
    client.subscribe([(DROPPED, 0), (UPTIME, 0)])
    client.loop_start()
    try:
        deadline = time.monotonic() + TIMEOUT_S
        while time.monotonic() < deadline:
            seen = list(samples)
            values = [int(p) for _, topic, p, _ in seen if topic == DROPPED]
            updated = after is None or any(topic == UPTIME and not retained and t >= after
                                           for t, topic, _, retained in seen)
            if values and updated:
                return values[-1]
            time.sleep(0.2)
    finally:
        client.loop_stop()
        client.disconnect()
    raise RuntimeError(f"no $SYS update of {DROPPED} from Mosquitto at {host}:{port} within {TIMEOUT_S:g} s")
