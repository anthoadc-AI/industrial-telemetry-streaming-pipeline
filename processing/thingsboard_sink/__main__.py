"""ThingsBoard sink: telemetry and alarm events from Kafka into ThingsBoard.

Run from the repository root (stack up with `make up`, full profile):

    python -m processing.thingsboard_sink

Consumes telemetry.raw (telemetry_v1) and telemetry.alarms (alarm_v1) in its
own consumer group, separate from the alarm consumer's, so it never slows or
blocks the alarm path. It publishes to ThingsBoard through the gateway MQTT
API with a single gateway token: every asset appears as its own device,
created by ThingsBoard on first use with the asset's type, so it gets the
device profile the import script created for that type.

- A telemetry record becomes one time-series entry on the asset's device,
  stamped with the sample's own ts: the four channels, seq, injected_fault.
- An alarm event becomes one entry under the key alarm_<channel>_<severity>,
  one key per alarm type, so two alarm types of one asset at the same ts never
  overwrite each other; its value is the event as JSON (alarm_id, state,
  severity, channel, value, threshold, raised_ts, type). The rule chain
  AlarmMapping turns it into a ThingsBoard alarm of that type. raised_ts is
  taken from the alarm_id, which ends with the ts of the RAISED sample; type
  is "<channel label> high (warning|critical)", the label from
  config/telemetry.yaml, the same text the dashboard shows for the channel.
- A record with no value on telemetry.alarms (a tombstone, such as the
  integration tests' roll markers) is skipped and counted, never failed or
  dead-lettered.

Delivery: at-least-once to ThingsBoard's MQTT transport. A record's offset is
stored for commit only after ThingsBoard has acknowledged its publish (QoS 1
PUBACK). That acknowledgement means the transport accepted the message onto
its queue; it does not mean the rule engine has processed it: a message
accepted and then failed by a rule node is not retried by the sink. Rule-engine
failures are visible in ThingsBoard's queue statistics. Duplicates (a record
published again after a restart) are harmless: telemetry overwrites the same
key at the same ts, and AlarmMapping drops alarm events it has already applied.

Failures:
- ThingsBoard unreachable, or a publish not acknowledged, and Schema Registry
  failures other than "schema not found": retried, with backoff, never
  committed past; consumption waits meanwhile (lag grows, nothing is lost
  while the record is in the topic's retention).
- telemetry.raw record that cannot be decoded: skipped and counted, not
  dead-lettered: the alarm consumer reads the same record and dead-letters it
  already.
- telemetry.alarms record that cannot be decoded: dead-lettered to
  telemetry.dlq (dlq_v1, source thingsboard_sink), since only this pipeline
  writes that topic.

Configuration, all from the environment with command-line overrides, so the
sink runs unchanged in a container: KAFKA_BOOTSTRAP, SCHEMA_REGISTRY_URL,
TB_HOST, TB_MQTT_PORT, TB_GATEWAY_TOKEN, THINGSBOARD_SINK_GROUP. Topic names
and asset types come from config/.

Stopping: after --count records, after --duration seconds, or on SIGINT,
SIGTERM or (Windows) SIGBREAK. Exit status 0 after a clean stop, 1 on a
start-up failure.
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
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.serialization import SerializationError

from common import jsonlog
from config.loader import ConfigError, load_config

log = logging.getLogger("processing.thingsboard_sink")

GATEWAY_CONNECT = "v1/gateway/connect"
GATEWAY_TELEMETRY = "v1/gateway/telemetry"
PUBACK_TIMEOUT_S = 10.0
CONNECT_TIMEOUT_S = 15.0
BACKOFF_MAX_S = 30.0
DETAIL_MAX = 1000
SOURCE = "thingsboard_sink"


class Transient(Exception):
    """A failure worth retrying: the record is not committed past."""


# --- mapping (pure) -----------------------------------------------------------------------


def telemetry_entry(record: dict, channels: list[str]) -> tuple[str, str, dict]:
    """(asset_id, asset_type, gateway entry) for a telemetry_v1 record; null channels are left out."""
    values = {ch: record[ch] for ch in channels if record.get(ch) is not None}
    values["seq"] = record["seq"]
    if record.get("injected_fault"):
        values["injected_fault"] = record["injected_fault"]
    return record["asset_id"], record["asset_type"], {"ts": _epoch_ms(record["ts"]), "values": values}


def alarm_type(label: str, severity: str) -> str:
    """The ThingsBoard alarm type: one per channel and severity, so HI and HIHI stay independent alarms."""
    return f"{label} high ({severity.lower()})"


def alarm_entry(event: dict, labels: dict[str, str]) -> tuple[str, dict]:
    """(asset_id, gateway entry) for an alarm_v1 event: one key per alarm type, the event as JSON."""
    raised_ts = int(event["alarm_id"].rsplit("/", 1)[1])
    body = {"alarm_id": event["alarm_id"], "state": event["state"], "severity": event["severity"],
            "channel": event["channel"], "value": event["value"], "threshold": event["threshold"],
            "raised_ts": raised_ts, "type": alarm_type(labels[event["channel"]], event["severity"])}
    key = f"alarm_{event['channel']}_{event['severity']}"
    return event["asset_id"], {"ts": _epoch_ms(event["ts"]), "values": {key: json.dumps(body, separators=(",", ":"))}}


def is_transient(error: Exception) -> bool:
    """Whether a decoding failure is worth retrying: the registry unreachable or failing, not the bytes.

    Measured against this Schema Registry client (confluent-kafka 2.15.1): an unknown schema id is a
    SchemaRegistryError with HTTP status 404 (permanent); bad framing is a SerializationError (permanent);
    malformed bytes after a valid header raise assorted ValueError/TypeError (permanent); an unreachable
    registry is an httpx transport error (transient); any other registry status is transient. Treating
    unknown errors as transient would retry a corrupt record forever.
    """
    if isinstance(error, SerializationError):
        return False
    status = getattr(error, "http_status_code", None)
    if status is not None:
        return status != 404
    return isinstance(error, (OSError, TimeoutError)) or type(error).__module__.split(".")[0] == "httpx"


def _epoch_ms(ts) -> int:
    if isinstance(ts, datetime):
        return int(ts.timestamp() * 1000)
    return int(ts)


# --- ThingsBoard gateway over MQTT ---------------------------------------------------------


class Gateway:
    """The ThingsBoard gateway MQTT API: connect each device once per session, publish with QoS 1."""

    def __init__(self, host: str, port: int, token: str, client_id: str, client_factory=mqtt.Client):
        self.host, self.port = host, port
        self.connected = threading.Event()
        self.devices: set[str] = set()
        self.client = client_factory(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        self.client.username_pw_set(token)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    def start(self, timeout: float = CONNECT_TIMEOUT_S) -> bool:
        try:
            self.client.connect_async(self.host, self.port, keepalive=60)
        except (OSError, ValueError) as e:
            log.error("thingsboard_connect_failed", extra={"host": self.host, "port": self.port, "error": str(e)})
            return False
        self.client.loop_start()  # paho reconnects by itself after a disconnect
        if not self.connected.wait(timeout):
            log.error("thingsboard_unreachable", extra={"host": self.host, "port": self.port, "timeout_s": timeout})
            return False
        return True

    def stop(self) -> None:
        self.client.disconnect()
        self.client.loop_stop()

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("thingsboard_connect_refused", extra={"reason": str(reason_code)})
            return
        self.devices = set()  # a new session: devices are announced again
        self.connected.set()
        log.info("thingsboard_connected", extra={"host": self.host, "port": self.port})

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.connected.clear()
        log.warning("thingsboard_disconnected", extra={"reason": str(reason_code)})

    def publish(self, topic: str, payload: dict, timeout: float = PUBACK_TIMEOUT_S) -> None:
        """Publish with QoS 1 and wait for the PUBACK; Transient if it does not come."""
        if not self.connected.is_set():
            raise Transient("not connected to ThingsBoard")
        info = self.client.publish(topic, json.dumps(payload, separators=(",", ":")), qos=1)
        try:
            info.wait_for_publish(timeout)
        except (RuntimeError, ValueError) as e:  # paho raises these when the message cannot be sent
            raise Transient(f"publish failed: {e}") from None
        if not info.is_published():
            raise Transient(f"no PUBACK within {timeout} s")

    def send(self, device: str, device_type: str | None, entry: dict) -> None:
        if device not in self.devices:
            self.publish(GATEWAY_CONNECT, {"device": device, **({"type": device_type} if device_type else {})})
            self.devices.add(device)
        self.publish(GATEWAY_TELEMETRY, {device: [entry]})


# --- the sink ------------------------------------------------------------------------------


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    env = os.environ.get
    p = argparse.ArgumentParser(prog="python -m processing.thingsboard_sink",
                                description="Forward telemetry and alarm events from Kafka to ThingsBoard.")
    p.add_argument("--count", type=int, help="stop after this many records (tombstones included)")
    p.add_argument("--duration", type=float, help="stop after this many seconds")
    p.add_argument("--kafka-bootstrap", default=env("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--schema-registry-url", default=env("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    p.add_argument("--tb-host", default=env("TB_HOST", "localhost"))
    p.add_argument("--tb-mqtt-port", type=int, default=int(env("TB_MQTT_PORT", "9883")))
    p.add_argument("--group-id", default=env("THINGSBOARD_SINK_GROUP", "thingsboard-sink"))
    p.add_argument("--client-id", default=f"thingsboard-sink-{os.getpid()}")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args(argv)


class Sink:
    def __init__(self, args: argparse.Namespace, token: str | None = None):
        self.args = args
        cfg = load_config()
        self.raw_topic = cfg.topics.telemetry_raw.name
        self.alarms_topic = cfg.topics.kafka["telemetry_alarms"].name
        dlq = cfg.topics.kafka["telemetry_dlq"]
        self.dlq_topic = dlq.name
        self.dlq_schema_str = dlq.value_schema.read_text(encoding="utf-8")
        self.channels = list(cfg.telemetry.channels)
        self.labels = {name: channel.label for name, channel in cfg.telemetry.channels.items()}
        self.asset_types = {a.id: a.type for a in cfg.telemetry.assets}
        self.token = token if token is not None else os.environ.get("TB_GATEWAY_TOKEN", "")
        self.counts: Counter = Counter()
        self.consumer = None
        self.producer = None
        self.gateway: Gateway | None = None
        self.decode = None
        self.dlq_serializer = None
        self.dlq_context = None

    # start-up -------------------------------------------------------------------------------

    def start_kafka(self) -> bool:
        self.consumer = Consumer({
            "bootstrap.servers": self.args.kafka_bootstrap, "group.id": self.args.group_id,
            "auto.offset.reset": "earliest", "enable.auto.commit": True,
            # Offsets are stored by hand, only once a record is safely handled.
            "enable.auto.offset.store": False, "logger": logging.getLogger("librdkafka"),
        })
        self.producer = Producer({"bootstrap.servers": self.args.kafka_bootstrap, "acks": "all",
                                  "enable.idempotence": True, "logger": logging.getLogger("librdkafka")})
        try:
            metadata = self.consumer.list_topics(timeout=10)
        except KafkaException as e:
            log.error("kafka_unreachable", extra={"bootstrap": self.args.kafka_bootstrap, "error": str(e)})
            return False
        for name in (self.raw_topic, self.alarms_topic, self.dlq_topic):
            topic = metadata.topics.get(name)
            if topic is None or topic.error is not None:
                log.error("kafka_topic_missing", extra={"topic": name})
                return False
        self.consumer.subscribe([self.raw_topic, self.alarms_topic])
        return True

    def start_schema_registry(self) -> None:
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
        from confluent_kafka.serialization import MessageField, SerializationContext

        client = SchemaRegistryClient({"url": self.args.schema_registry_url})
        deserializer = AvroDeserializer(client)
        self.decode = lambda value, topic: deserializer(value, SerializationContext(topic, MessageField.VALUE))
        self.dlq_serializer = AvroSerializer(client, self.dlq_schema_str)
        self.dlq_context = SerializationContext(self.dlq_topic, MessageField.VALUE)

    def start_gateway(self) -> bool:
        if not self.token:
            log.error("settings_missing", extra={"settings": ["TB_GATEWAY_TOKEN"]})
            return False
        self.gateway = Gateway(self.args.tb_host, self.args.tb_mqtt_port, self.token, self.args.client_id)
        return self.gateway.start()

    # one record -----------------------------------------------------------------------------

    def handle(self, msg) -> None:
        topic, value = msg.topic(), msg.value()
        if topic == self.alarms_topic and value is None:
            self.counts["tombstones_skipped"] += 1
            log.debug("tombstone_skipped", extra={"topic": topic, "partition": msg.partition(), "offset": msg.offset()})
            return
        try:
            record = self.decode(value, topic)
        except Exception as e:  # the deserializer raises many types; see is_transient
            if is_transient(e):
                raise Transient(f"schema registry: {type(e).__name__}: {e}") from None
            self.undecodable(msg, f"{type(e).__name__}: {e}")
            return
        if record is None:
            self.undecodable(msg, "empty value")
            return
        if topic == self.raw_topic:
            device, device_type, entry = telemetry_entry(record, self.channels)
            self.gateway.send(device, device_type, entry)
            self.counts["telemetry_forwarded"] += 1
        else:
            device, entry = alarm_entry(record, self.labels)
            self.gateway.send(device, self.asset_types.get(device), entry)
            self.counts["alarm_events_forwarded"] += 1

    def undecodable(self, msg, detail: str) -> None:
        if msg.topic() == self.raw_topic:  # the alarm consumer dead-letters these
            self.counts["raw_undecodable_skipped"] += 1
            log.warning("message_undecodable", extra={"topic": msg.topic(), "partition": msg.partition(),
                                                      "offset": msg.offset(), "detail": detail[:DETAIL_MAX],
                                                      "action": "skipped; dead-lettered by the alarm consumer"})
            return
        value = msg.value() or b""
        schema_id = int.from_bytes(value[1:5], "big") if value[:1] == b"\0" and len(value) >= 5 else None
        record = {"payload": value, "reason": "undecodable", "detail": detail[:DETAIL_MAX], "source": SOURCE,
                  "source_topic": msg.topic(), "source_partition": msg.partition(), "source_offset": msg.offset(),
                  "source_key": msg.key(), "schema_id": schema_id, "ts": datetime.now(timezone.utc)}
        self.producer.produce(self.dlq_topic, key=msg.key(), value=self.dlq_serializer(record, self.dlq_context))
        if self.producer.flush(10) != 0:
            raise Transient("dead letter not acknowledged")
        self.counts["dead_lettered"] += 1
        log.warning("message_dead_lettered", extra={"reason": "undecodable", "source": SOURCE, "source_topic": msg.topic(),
                                                    "source_partition": msg.partition(), "source_offset": msg.offset(),
                                                    "detail": detail[:DETAIL_MAX]})

    # the loop -------------------------------------------------------------------------------

    def run(self, stop: threading.Event) -> str:
        start = time.monotonic()
        handled = 0
        while True:
            if stop.is_set():
                return "signal"
            if self.args.duration is not None and time.monotonic() - start >= self.args.duration:
                return "duration"
            if self.args.count is not None and handled >= self.args.count:
                return "count"
            msg = self.consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                log.warning("consumer_error", extra={"error": str(msg.error())})
                continue
            backoff = 0.5
            while True:
                try:
                    self.handle(msg)
                    break
                except Transient as e:
                    self.counts["retries"] += 1
                    log.warning("forward_retry", extra={"topic": msg.topic(), "partition": msg.partition(),
                                                        "offset": msg.offset(), "error": str(e), "retry_in_s": backoff})
                    if stop.wait(backoff):
                        return "signal"  # not stored: read again on the next start
                    backoff = min(backoff * 2, BACKOFF_MAX_S)
            self.consumer.store_offsets(msg)
            handled += 1

    def shutdown(self, reason: str) -> None:
        if self.consumer is not None:
            self.consumer.close()  # commits the stored offsets
        if self.producer is not None:
            self.producer.flush(10)
        if self.gateway is not None:
            self.gateway.stop()
        event = "shutdown_after_startup_failure" if reason == "startup_failed" else "run_finished"
        log.info(event, extra={"reason": reason, **dict(sorted(self.counts.items()))})


def install_signal_handlers(stop: threading.Event) -> None:
    def handler(signum, frame):
        log.info("signal_received", extra={"signal": signal.Signals(signum).name})
        stop.set()

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), handler)


# Orchestration entry point: wires real Kafka, Schema Registry and MQTT clients and runs
# until stopped. Exercised by the full-profile integration tests, not by unit tests;
# excluded from unit coverage, as the other components' main() are (owner decision, Task 2.4).
def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    args = parse_args(argv)
    jsonlog.configure(args.log_level)
    # The Schema Registry client logs every HTTP request at INFO through httpx.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        sink = Sink(args)
    except ConfigError as e:
        log.error("config_invalid", extra={"error": str(e)})
        return 1
    stop = threading.Event()
    install_signal_handlers(stop)
    if not sink.start_kafka():
        sink.shutdown("startup_failed")
        return 1
    sink.start_schema_registry()
    if not sink.start_gateway():
        sink.shutdown("startup_failed")
        return 1
    log.info("run_started", extra={"group_id": args.group_id, "topics": [sink.raw_topic, sink.alarms_topic],
                                   "thingsboard": f"{args.tb_host}:{args.tb_mqtt_port}", "count": args.count,
                                   "duration_s": args.duration})
    reason = sink.run(stop)
    sink.shutdown(reason)
    return 0


if __name__ == "__main__":
    sys.exit(main())
