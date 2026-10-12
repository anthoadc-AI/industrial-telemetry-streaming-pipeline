"""Mosquitto's dropped-messages counter, watched by the bridge (Phase 3, Task 3.3, DETENTE 3.3.h).

Mosquitto queues QoS 1 messages for each subscribed client up to max_queued_messages / max_queued_bytes
(broker/mosquitto/mosquitto.conf); past either limit it drops new messages for that client and still sends the
publisher its PUBACK. Nothing reaches the bridge, so the bridge cannot count them itself: it reads Mosquitto's own
cumulative counter, $SYS/broker/publish/messages/dropped, and logs every increase as mqtt_messages_dropped at ERROR
with the increase (delta) and the new total.

What the counter is, measured on Mosquitto 2.1.2 (DETENTE 3.3.i):
- Broker-wide: it counts messages dropped for any client, not only for the bridge.
- Updated every sys_interval (10 s, set in mosquitto.conf): a drop is logged up to 10 s after it happens, and drops
  in the last seconds before the bridge stops may not be logged at all.
- Retained, so its current value arrives on subscribing: that value is the baseline and is not reported (drops
  before the bridge started are not the bridge's). It is published again only when it changes. A value below the
  previous one means Mosquitto restarted: it becomes the new baseline.
- A warning, not a recovery: a dropped message cannot be fetched again.
- It does not see messages published while no bridge is subscribed: with a clean session Mosquitto has no queue
  for the bridge to overflow, and accepts and discards them.

$SYS/broker/uptime changes at every update, so it is published every sys_interval; when none arrives for
SILENT_AFTER_S the counter cannot be trusted (for example $SYS disabled) and mosquitto_sys_silent is logged.

The monitor uses an MQTT connection of its own (client id <bridge client id>-drops): the bridge's telemetry
connection is the congested one when Mosquitto drops, and the counter's update would queue behind the telemetry
waiting for the bridge.
"""

from __future__ import annotations

import logging
import threading
import time

import paho.mqtt.client as mqtt

log = logging.getLogger("bridge.mqtt_to_kafka")

DROPPED_TOPIC = "$SYS/broker/publish/messages/dropped"
UPTIME_TOPIC = "$SYS/broker/uptime"
SYS_INTERVAL_S = 10.0  # sys_interval in broker/mosquitto/mosquitto.conf
SILENT_AFTER_S = 2 * SYS_INTERVAL_S + 5


class DropMonitor:
    def __init__(self, host: str, port: int, client_id: str, started: float):
        """started: the monotonic instant the silence of $SYS is measured from until the first update."""
        self.host, self.port, self.client_id = host, port, client_id
        self.total: int | None = None
        self.dropped_during_run = 0
        self.last_update = started
        self.silent = False
        self.subscribed = threading.Event()
        self.client: mqtt.Client | None = None
        # on_message runs on paho's network thread, check_silence on the bridge's main thread.
        self.lock = threading.Lock()

    # --- the counter (paho network thread) -------------------------------

    def on_value(self, topic: str, payload: bytes, now: float) -> None:
        if topic == UPTIME_TOPIC:
            with self.lock:
                self.last_update = now
                resumed, self.silent = self.silent, False
            if resumed:
                log.info("mosquitto_sys_resumed", extra={"broker": self.broker})
            return
        try:
            value = int(payload.decode("ascii"))
        except (UnicodeDecodeError, ValueError):
            log.warning("mosquitto_drop_counter_invalid", extra={"topic": topic, "payload": repr(payload[:100])})
            return
        with self.lock:
            previous, self.total = self.total, value
            delta = value - previous if previous is not None and value >= previous else 0
            self.dropped_during_run += delta
        if previous is None:
            log.info("mqtt_drop_monitor_baseline", extra={"broker": self.broker, "total": value})
        elif value < previous:
            log.warning("mosquitto_drop_counter_reset", extra={"broker": self.broker, "previous": previous,
                                                               "total": value})
        elif delta:
            log.error("mqtt_messages_dropped", extra={"broker": self.broker, "delta": delta, "total": value,
                                                      "scope": "broker-wide", "sys_interval_s": SYS_INTERVAL_S})

    def check_silence(self, now: float) -> None:
        """Called by the bridge's main loop: logs mosquitto_sys_silent once per silence."""
        with self.lock:
            silent = not self.silent and now - self.last_update >= SILENT_AFTER_S
            if silent:
                self.silent = True
                since = now - self.last_update
        if silent:
            log.warning("mosquitto_sys_silent", extra={"broker": self.broker, "seconds_without_update": round(since, 1),
                                                       "topic": UPTIME_TOPIC})

    @property
    def broker(self) -> str:
        return f"{self.host}:{self.port}"

    # --- the connection ---------------------------------------------------

    def start(self, timeout_s: float) -> bool:
        """Connect and subscribe; False (logged) when that fails within timeout_s."""
        client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id)
        client.on_connect = self._on_connect
        client.on_subscribe = self._on_subscribe
        client.on_message = lambda c, userdata, msg: self.on_value(msg.topic, msg.payload, time.monotonic())
        try:
            client.connect(self.host, self.port, keepalive=60)
        except OSError as e:
            log.error("drop_monitor_connect_failed", extra={"broker": self.broker, "error": str(e)})
            return False
        client.loop_start()
        self.client = client
        if not self.subscribed.wait(timeout_s):
            log.error("drop_monitor_subscribe_timeout", extra={"broker": self.broker})
            return False
        return True

    def stop(self) -> None:
        if self.client is not None:
            self.client.disconnect()
            self.client.loop_stop()
            self.client = None

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("drop_monitor_connect_refused", extra={"broker": self.broker, "reason": str(reason_code)})
            return
        # Subscribing here also restores the subscription after a reconnect.
        client.subscribe([(DROPPED_TOPIC, 0), (UPTIME_TOPIC, 0)])

    def _on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        if any(rc.is_failure for rc in reason_code_list):
            log.error("drop_monitor_subscribe_refused", extra={"broker": self.broker,
                                                               "reasons": [str(rc) for rc in reason_code_list]})
            return
        log.info("drop_monitor_subscribed", extra={"broker": self.broker, "topics": [DROPPED_TOPIC, UPTIME_TOPIC]})
        self.subscribed.set()
