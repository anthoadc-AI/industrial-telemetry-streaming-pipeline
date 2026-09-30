"""MQTT to Kafka bridge: validated telemetry from Mosquitto to Kafka as Avro.

Run from the repository root:

    python -m bridge.mqtt_to_kafka --count 200

The bridge subscribes to the telemetry subscription in config/topics.yaml,
validates each JSON payload against schemas/telemetry_v1.avsc, serialises it
to Avro through Schema Registry, and produces it to the telemetry.raw topic
keyed by asset_id. Invalid messages are logged with their reason and dropped.

Start-up fails fast, before subscribing, when Kafka is unreachable, when the
target topic does not exist (the bridge never creates topics), or when the
schema cannot be registered.

Threads: the paho network thread only enqueues received messages; the main
thread validates, serialises, produces, serves delivery reports and handles
signals and run bounds.

Stopping: at --count received messages, after --duration seconds, or on
SIGINT, SIGTERM or (Windows) SIGBREAK. The bridge then disconnects from MQTT,
processes what it has already received (on --count, anything received beyond
the count is reported as unprocessed instead), flushes the producer for up to
--flush-timeout seconds, logs the totals and exits: 0 when every produced
message was delivered, 1 otherwise or on a start-up error.

Delivery semantics: MQTT QoS 1 messages are acknowledged to the broker on
receipt, before Kafka confirms them. A clean stop loses nothing already
received; a crash can lose messages still queued in the bridge.

Latency fields (stable names, used by the Phase 3 benchmark), logged per
delivered message in event "message_delivered", all in milliseconds from the
message's own ts (the simulator's clock, the same host in this setup):
    ts_to_receive_ms   ts -> the bridge received the MQTT message
    ts_to_produce_ms   ts -> the bridge handed the record to the producer
    ts_to_ack_ms       ts -> Kafka acknowledged the record
"""

from __future__ import annotations

import argparse
import logging
import os
import queue
import signal
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass, field

import paho.mqtt.client as mqtt
from confluent_kafka import KafkaException, Producer
from confluent_kafka.serialization import SerializationError

from bridge.mqtt_to_kafka import jsonlog
from bridge.mqtt_to_kafka.validation import InvalidMessage, TelemetryValidator
from config.loader import ConfigError, load_config

log = logging.getLogger("bridge.mqtt_to_kafka")

CONNECT_TIMEOUT_S = 10.0
KAFKA_METADATA_TIMEOUT_S = 10.0


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


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m bridge.mqtt_to_kafka", description="Bridge MQTT telemetry to Kafka as Avro.")
    p.add_argument("--count", type=_positive_int, help="stop after this many received MQTT messages, valid or not")
    p.add_argument("--duration", type=_positive_float, help="stop after this many seconds")
    p.add_argument("--mqtt-host", default=os.environ.get("MQTT_HOST", "localhost"), help="default: $MQTT_HOST or localhost")
    p.add_argument("--mqtt-port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")), help="default: $MQTT_PORT or 1883")
    p.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
                   help="default: $KAFKA_BOOTSTRAP or localhost:9092")
    p.add_argument("--schema-registry-url", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
                   help="default: $SCHEMA_REGISTRY_URL or http://localhost:8081")
    p.add_argument("--client-id", default=f"mqtt-to-kafka-bridge-{os.getpid()}")
    p.add_argument("--flush-timeout", type=_positive_float, default=10.0,
                   help="seconds to wait for outstanding Kafka deliveries at shutdown (default: 10)")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args(argv)


@dataclass
class Stats:
    received: int = 0
    produced: int = 0
    delivered: int = 0
    delivery_failed: int = 0
    invalid: Counter = field(default_factory=Counter)

    def as_dict(self) -> dict:
        return {"messages_received": self.received, "messages_invalid": sum(self.invalid.values()),
                "invalid_by_reason": dict(self.invalid), "messages_produced": self.produced,
                "messages_delivered": self.delivered, "messages_delivery_failed": self.delivery_failed}


class Bridge:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        cfg = load_config()
        self.kafka_topic = cfg.topics.telemetry_raw
        self.mqtt_topic = cfg.topics.mqtt_telemetry
        self.validator = TelemetryValidator(self.kafka_topic.value_schema, self.mqtt_topic.template)
        self.inbox: queue.Queue[tuple[str, bytes, float]] = queue.Queue()
        self.subscribed = threading.Event()
        self.stats = Stats()
        self.producer: Producer | None = None
        self.serializer = None
        self.serialization_context = None
        self.mqtt: mqtt.Client | None = None

    # --- start-up --------------------------------------------------------

    def start_kafka(self) -> bool:
        self.producer = Producer({
            "bootstrap.servers": self.args.kafka_bootstrap,
            "client.id": self.args.client_id,
            "acks": "all",
            "enable.idempotence": True,
            # librdkafka's own log lines go through logging (as JSON) instead
            # of being written to stderr directly. They are emitted from
            # poll() and flush().
            "logger": logging.getLogger("librdkafka"),
        })
        try:
            metadata = self.producer.list_topics(timeout=KAFKA_METADATA_TIMEOUT_S)
        except KafkaException as e:
            log.error("kafka_unreachable", extra={"bootstrap": self.args.kafka_bootstrap, "error": str(e)})
            return False
        topic = metadata.topics.get(self.kafka_topic.name)
        if topic is None or topic.error is not None:
            log.error("kafka_topic_missing", extra={"topic": self.kafka_topic.name,
                                                    "error": str(topic.error) if topic else "not found"})
            return False
        log.info("kafka_ready", extra={"bootstrap": self.args.kafka_bootstrap, "topic": self.kafka_topic.name,
                                       "partitions": len(topic.partitions)})
        return True

    def start_schema_registry(self) -> bool:
        # Imported here, after logging is configured: importing the Schema
        # Registry client raises a third-party deprecation warning, which
        # must reach stderr as a JSON log line.
        from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import MessageField, SerializationContext

        client = SchemaRegistryClient({"url": self.args.schema_registry_url})
        subject = f"{self.kafka_topic.name}-value"
        try:
            schema_id = client.register_schema(subject, Schema(self.validator.schema_str, "AVRO"))
        except Exception as e:  # the client raises several unrelated types for network and HTTP errors
            log.error("schema_registration_failed", extra={"url": self.args.schema_registry_url, "subject": subject,
                                                           "error": str(e)})
            return False
        self.serializer = AvroSerializer(client, self.validator.schema_str)
        self.serialization_context = SerializationContext(self.kafka_topic.name, MessageField.VALUE)
        log.info("schema_registered", extra={"subject": subject, "schema_id": schema_id})
        return True

    def start_mqtt(self) -> bool:
        client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=self.args.client_id)
        client.on_connect = self._on_connect
        client.on_subscribe = self._on_subscribe
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        try:
            client.connect(self.args.mqtt_host, self.args.mqtt_port, keepalive=60)
        except OSError as e:
            log.error("mqtt_connect_failed", extra={"host": self.args.mqtt_host, "port": self.args.mqtt_port, "error": str(e)})
            return False
        client.loop_start()
        self.mqtt = client
        if not self.subscribed.wait(CONNECT_TIMEOUT_S):
            log.error("mqtt_subscribe_timeout", extra={"host": self.args.mqtt_host, "port": self.args.mqtt_port})
            return False
        return True

    # --- MQTT callbacks (paho network thread) ----------------------------

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("mqtt_connect_refused", extra={"reason": str(reason_code)})
            return
        log.info("mqtt_connected", extra={"host": self.args.mqtt_host, "port": self.args.mqtt_port})
        # Subscribing here also restores the subscription after a reconnect.
        client.subscribe(self.mqtt_topic.subscription, qos=self.mqtt_topic.qos)

    def _on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        if any(rc.is_failure for rc in reason_code_list):
            log.error("mqtt_subscribe_refused", extra={"reasons": [str(rc) for rc in reason_code_list]})
            return
        log.info("mqtt_subscribed", extra={"subscription": self.mqtt_topic.subscription, "qos": self.mqtt_topic.qos})
        self.subscribed.set()

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.warning("mqtt_disconnected", extra={"reason": str(reason_code)})

    def _on_message(self, client, userdata, message):
        self.inbox.put((message.topic, message.payload, time.time()))

    # --- processing (main thread) ----------------------------------------

    def handle(self, mqtt_topic: str, payload: bytes, received_at: float) -> None:
        self.stats.received += 1
        try:
            record = self.validator.validate(mqtt_topic, payload)
        except InvalidMessage as e:
            self.stats.invalid[e.reason] += 1
            log.warning("message_invalid", extra={"mqtt_topic": mqtt_topic, "reason": e.reason, "detail": e.detail,
                                                  "payload_bytes": len(payload)})
            # TODO(phase-2): produce the raw payload and the reason to the
            # dead-letter topic (config/topics.yaml: kafka.telemetry_dlq)
            # instead of dropping it.
            return

        try:
            value = self.serializer(record, self.serialization_context)
        except SerializationError as e:
            self.stats.invalid["serialization_error"] += 1
            log.error("message_serialization_failed", extra={"mqtt_topic": mqtt_topic, "error": str(e)})
            return

        source_ts = record["ts"].timestamp()
        context = {"asset_id": record["asset_id"], "seq": record["seq"],
                   "ts_to_receive_ms": round((received_at - source_ts) * 1000, 3)}

        def on_delivery(err, msg):
            if err is not None:
                self.stats.delivery_failed += 1
                log.error("message_delivery_failed", extra={**context, "error": str(err)})
                return
            self.stats.delivered += 1
            log.info("message_delivered", extra={
                **context, "topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
                "ts_to_ack_ms": round((time.time() - source_ts) * 1000, 3),
            })

        while True:
            produced_at = time.time()
            context["ts_to_produce_ms"] = round((produced_at - source_ts) * 1000, 3)
            try:
                self.producer.produce(self.kafka_topic.name, key=record["asset_id"].encode("utf-8"), value=value,
                                      on_delivery=on_delivery)
                break
            except BufferError:
                # Local queue full: serve delivery reports, then retry.
                self.producer.poll(0.5)
        self.stats.produced += 1
        self.producer.poll(0)

    def run(self, stop: threading.Event) -> str:
        start = time.monotonic()
        while True:
            if stop.is_set():
                return "signal"
            if self.args.duration is not None and time.monotonic() - start >= self.args.duration:
                return "duration"
            if self.args.count is not None and self.stats.received >= self.args.count:
                return "count"
            try:
                item = self.inbox.get(timeout=0.1)
            except queue.Empty:
                self.producer.poll(0)
                continue
            self.handle(*item)

    def shutdown(self, reason: str) -> bool:
        if self.mqtt is not None:
            self.mqtt.disconnect()
            self.mqtt.loop_stop()
        unprocessed = 0
        while True:
            try:
                item = self.inbox.get_nowait()
            except queue.Empty:
                break
            if reason == "count":
                unprocessed += 1
            else:
                self.handle(*item)
        remaining = self.producer.flush(self.args.flush_timeout) if self.producer is not None else 0
        summary = {"reason": reason, **self.stats.as_dict(), "messages_unprocessed": unprocessed,
                   "messages_undelivered_at_exit": remaining}
        complete = remaining == 0 and self.stats.delivery_failed == 0
        if reason == "startup_failed":
            log.info("shutdown_after_startup_failure", extra=summary)
        elif complete:
            log.info("run_finished", extra=summary)
        else:
            log.warning("run_finished_incomplete", extra={**summary, "flush_timeout_s": self.args.flush_timeout})
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
        bridge = Bridge(args)
    except ConfigError as e:
        log.error("config_invalid", extra={"error": str(e)})
        return 1

    stop = threading.Event()
    install_signal_handlers(stop)

    if not (bridge.start_kafka() and bridge.start_schema_registry() and bridge.start_mqtt()):
        bridge.shutdown("startup_failed")
        return 1
    log.info("run_started", extra={"count": args.count, "duration_s": args.duration,
                                   "mqtt_subscription": bridge.mqtt_topic.subscription,
                                   "kafka_topic": bridge.kafka_topic.name})
    reason = bridge.run(stop)
    return 0 if bridge.shutdown(reason) else 1


if __name__ == "__main__":
    sys.exit(main())
