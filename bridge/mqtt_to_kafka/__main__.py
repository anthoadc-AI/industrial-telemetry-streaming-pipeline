"""MQTT to Kafka bridge: validated telemetry from Mosquitto to Kafka as Avro.

Run from the repository root:

    python -m bridge.mqtt_to_kafka --count 200

The bridge subscribes to the telemetry subscription in config/topics.yaml,
validates each JSON payload against schemas/telemetry_v1.avsc, serialises it
to Avro through Schema Registry, and produces it to the telemetry.raw topic
keyed by asset_id. A message that fails validation or serialisation goes to
the dead-letter topic, telemetry.dlq, intact (dlq_v1: payload bytes, reason,
detail), keyed by the asset_id in the MQTT topic.

Start-up fails fast, before subscribing, when Kafka is unreachable, when
telemetry.raw or telemetry.dlq does not exist (the bridge never creates
topics), or when either schema cannot be registered.

Dead-letter send failures: the bridge logs them at ERROR with the payload
base64-encoded in the log line and carries on; the run then ends with exit
status 1. The alarm consumer does the opposite and stops without committing.
The asymmetry follows the source: a Kafka input can be read again from its
uncommitted offset, so the consumer can afford to stop and retry; an MQTT
message is acknowledged to the broker on receipt and cannot be fetched again,
so stopping the bridge would not save it and would halt all valid telemetry
behind it. The ERROR line is then the message's last copy.

Threads: the paho network thread only enqueues received messages, with their
receive times; the main thread validates, serialises, produces and handles
signals and run bounds; a delivery-report thread sits in producer.poll(),
which returns as soon as a report is queued, and runs every delivery
callback, for telemetry and for dead letters. Counters written by callbacks
are guarded by a lock. If the delivery-report thread dies (an exception out of
poll() or out of a callback), the bridge stops producing, logs
delivery_reports_failed and exits 1: producing with nobody serving reports
would leave every delivery unconfirmed.

Stopping: at --count received messages, after --duration seconds, or on
SIGINT, SIGTERM or (Windows) SIGBREAK. The bridge then disconnects from MQTT,
processes what it has already received (on --count, anything received beyond
the count is reported as unprocessed instead), flushes the producer for up to
--flush-timeout seconds, logs the totals and exits: 0 when every produced
message was delivered, 1 otherwise or on a start-up error.

Delivery semantics: MQTT QoS 1 messages are acknowledged to the broker on
receipt, before Kafka confirms them. A clean stop loses nothing already
received; a crash can lose messages still queued in the bridge. Before
receipt, Mosquitto holds a bounded queue for the bridge (mosquitto.conf:
60000 messages or 25 MiB) and past it drops new messages, still acknowledging
the publisher; there is no backpressure. The bridge cannot see those messages:
with drop_monitor enabled in config/bridge.yaml (the default) it watches
Mosquitto's dropped counter on a connection of its own and logs each increase
as mqtt_messages_dropped at ERROR (drop_monitor.py: broker-wide, updated every
10 s), refusing to start if it cannot subscribe. Messages published while the
bridge is not connected are not delivered (clean session) and are not counted.

Latency fields, logged per delivered message in event "message_delivered",
in milliseconds. One per stage; every clock is on this host (no broker
timestamp is used):
    event_to_receive_ms    message ts -> the bridge received the MQTT message
                           (wall clock: the simulator's datetime.now and the
                           bridge's time.time_ns, the same system clock)
    receive_to_produce_ms  received -> handed to producer.produce()
                           (time.perf_counter, monotonic)
    produce_to_ack_ms      produce() -> the broker's acknowledgement, observed
                           in the delivery callback (time.perf_counter); the
                           delivery-report thread serves callbacks as soon as
                           they are queued, so this carries no polling delay
    event_to_ack_ms        the sum of the three
    receive_ts_ms          absolute receive time, milliseconds since the
                           epoch with three decimals (time.time_ns() / 1e6,
                           to the microsecond), so a benchmark can join these
                           lines with its own send times by (asset_id, seq).
                           Same unit as the alarm consumer's alarm_ack_ts_ms;
                           a double holds it exactly, so Python and
                           JavaScript read the same value
The message ts has millisecond resolution and is truncated, not rounded, by
the simulator: event_to_receive_ms and event_to_ack_ms overstate by 0 to 1 ms.
These replace ts_to_receive_ms, ts_to_produce_ms and ts_to_ack_ms (Phase 3,
Task 3.1-A). ts_to_ack_ms was taken when the main loop next polled the
producer, up to 100 ms after the acknowledgement; it is gone, not redefined.
"""

from __future__ import annotations

import argparse
import base64
import logging
import os
import queue
import signal
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from confluent_kafka import KafkaException, Producer
from confluent_kafka.serialization import SerializationError

from bridge.mqtt_to_kafka.drop_monitor import DropMonitor
from bridge.mqtt_to_kafka.validation import InvalidMessage, TelemetryValidator
from common import jsonlog
from config.loader import ConfigError, load_config

log = logging.getLogger("bridge.mqtt_to_kafka")

CONNECT_TIMEOUT_S = 10.0
KAFKA_METADATA_TIMEOUT_S = 10.0
DETAIL_MAX = 1000
# producer.poll() returns as soon as a delivery report is queued; the timeout
# only bounds how long the delivery-report thread takes to notice a stop.
POLL_TIMEOUT_S = 0.2
# A full local producer queue is retried after this pause; the delivery-report
# thread drains it meanwhile.
BUFFER_RETRY_S = 0.05


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
    dlq_delivered: int = 0
    dlq_failed: int = 0
    invalid: Counter = field(default_factory=Counter)
    # Delivery callbacks run on the delivery-report thread; every counter they
    # write is changed under this lock, from either thread.
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def add(self, name: str, n: int = 1) -> None:
        with self.lock:
            setattr(self, name, getattr(self, name) + n)

    def as_dict(self) -> dict:
        return {"messages_received": self.received, "messages_invalid": sum(self.invalid.values()),
                "invalid_by_reason": dict(self.invalid), "messages_produced": self.produced,
                "messages_delivered": self.delivered, "messages_delivery_failed": self.delivery_failed,
                "dead_letters_delivered": self.dlq_delivered, "dead_letters_failed": self.dlq_failed}


class Bridge:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        cfg = load_config()
        self.kafka_topic = cfg.topics.telemetry_raw
        # Producer settings from config/bridge.yaml (acks, idempotence, and any
        # librdkafka setting the benchmark varies); the bridge adds its own below.
        self.producer_settings = dict(cfg.bridge.producer)
        self.dlq_topic = cfg.topics.kafka["telemetry_dlq"]
        self.dlq_schema_str = self.dlq_topic.value_schema.read_text(encoding="utf-8")
        self.mqtt_topic = cfg.topics.mqtt_telemetry
        self.validator = TelemetryValidator(self.kafka_topic.value_schema, self.mqtt_topic.template)
        # (MQTT topic, payload, receive wall time in ns, receive perf_counter)
        self.inbox: queue.Queue[tuple[str, bytes, int, float]] = queue.Queue()
        self.subscribed = threading.Event()
        self.stats = Stats()
        self.producer: Producer | None = None
        self.serializer = None
        self.serialization_context = None
        self.dlq_serializer = None
        self.dlq_context = None
        self.mqtt: mqtt.Client | None = None
        self._reports_stop = threading.Event()
        self.reports_failed = threading.Event()
        self._reports_thread: threading.Thread | None = None
        self.drop_monitor = (DropMonitor(args.mqtt_host, args.mqtt_port, f"{args.client_id}-drops", time.monotonic())
                             if cfg.bridge.drop_monitor_enabled else None)

    # --- start-up --------------------------------------------------------

    def start_kafka(self) -> bool:
        self.producer = Producer({
            **self.producer_settings,
            "bootstrap.servers": self.args.kafka_bootstrap,
            "client.id": self.args.client_id,
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
        for name in (self.kafka_topic.name, self.dlq_topic.name):
            topic = metadata.topics.get(name)
            if topic is None or topic.error is not None:
                log.error("kafka_topic_missing", extra={"topic": name, "error": str(topic.error) if topic else "not found"})
                return False
        log.info("kafka_ready", extra={"bootstrap": self.args.kafka_bootstrap, "topic": self.kafka_topic.name,
                                       "producer_settings": self.producer_settings,
                                       "partitions": len(metadata.topics[self.kafka_topic.name].partitions),
                                       "dlq_topic": self.dlq_topic.name})
        return True

    def start_schema_registry(self) -> bool:
        # Imported here, after logging is configured: importing the Schema
        # Registry client raises a third-party deprecation warning, which
        # must reach stderr as a JSON log line.
        from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import MessageField, SerializationContext

        client = SchemaRegistryClient({"url": self.args.schema_registry_url})
        for topic, schema_str in ((self.kafka_topic.name, self.validator.schema_str),
                                  (self.dlq_topic.name, self.dlq_schema_str)):
            subject = f"{topic}-value"
            try:
                schema_id = client.register_schema(subject, Schema(schema_str, "AVRO"))
            except Exception as e:  # the client raises several unrelated types for network and HTTP errors
                log.error("schema_registration_failed", extra={"url": self.args.schema_registry_url,
                                                               "subject": subject, "error": str(e)})
                return False
            log.info("schema_registered", extra={"subject": subject, "schema_id": schema_id})
        self.serializer = AvroSerializer(client, self.validator.schema_str)
        self.serialization_context = SerializationContext(self.kafka_topic.name, MessageField.VALUE)
        self.dlq_serializer = AvroSerializer(client, self.dlq_schema_str)
        self.dlq_context = SerializationContext(self.dlq_topic.name, MessageField.VALUE)
        return True

    def start_drop_monitor(self) -> bool:
        """Subscribe to Mosquitto's dropped counter (drop_monitor.py), before the telemetry subscription; True when
        the monitor is disabled."""
        if self.drop_monitor is None:
            log.warning("drop_monitor_disabled", extra={"config": "config/bridge.yaml drop_monitor.enabled"})
            return True
        return self.drop_monitor.start(CONNECT_TIMEOUT_S)

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

    # --- delivery reports (their own thread) -----------------------------

    def start_delivery_reports(self) -> None:
        """Serve delivery reports from a dedicated thread, as soon as they are queued."""
        self._reports_thread = threading.Thread(target=self._serve_delivery_reports, name="delivery-reports",
                                                daemon=True)
        self._reports_thread.start()

    def _serve_delivery_reports(self) -> None:
        try:
            while not self._reports_stop.is_set():
                self.producer.poll(POLL_TIMEOUT_S)
        except BaseException as e:  # whatever escapes poll() or a callback ends the service; report it
            self.reports_failed.set()
            log.error("delivery_reports_failed", extra={"error": f"{type(e).__name__}: {e}"})

    def stop_delivery_reports(self) -> None:
        if self._reports_thread is not None:
            self._reports_stop.set()
            self._reports_thread.join()
            self._reports_thread = None

    def _produce(self, topic: str, key: bytes | None, value: bytes, on_delivery, timing: dict) -> bool:
        """produce(), retried while the local queue is full.

        timing["produce"] is set to the perf_counter of the accepted call
        before the call is made: the callback can run on the delivery-report
        thread before produce() has even returned. False when the
        delivery-report thread has failed: nothing would drain the queue or
        confirm the record, so it is not produced.
        """
        while True:
            if self.reports_failed.is_set():
                return False
            timing["produce"] = time.perf_counter()
            try:
                self.producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                return True
            except BufferError:
                time.sleep(BUFFER_RETRY_S)

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
        self.inbox.put((message.topic, message.payload, time.time_ns(), time.perf_counter()))

    # --- processing (main thread) ----------------------------------------

    def handle(self, mqtt_topic: str, payload: bytes, received_ns: int, received_perf: float) -> None:
        self.stats.add("received")
        try:
            record = self.validator.validate(mqtt_topic, payload)
        except InvalidMessage as e:
            self.dead_letter(mqtt_topic, payload, e.reason, e.detail)
            return

        try:
            value = self.serializer(record, self.serialization_context)
        except SerializationError as e:
            self.dead_letter(mqtt_topic, payload, "serialization_error", str(e))
            return

        event_to_receive_s = received_ns / 1e9 - record["ts"].timestamp()
        context = {"asset_id": record["asset_id"], "seq": record["seq"], "receive_ts_ms": round(received_ns / 1e6, 3)}
        timing: dict[str, float] = {}

        def on_delivery(err, msg):  # delivery-report thread
            if err is not None:
                self.stats.add("delivery_failed")
                log.error("message_delivery_failed", extra={**context, "error": str(err)})
                return
            acked = time.perf_counter()
            receive_to_produce_s = timing["produce"] - received_perf
            produce_to_ack_s = acked - timing["produce"]
            self.stats.add("delivered")
            log.info("message_delivered", extra={
                **context, "topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
                "event_to_receive_ms": round(event_to_receive_s * 1000, 3),
                "receive_to_produce_ms": round(receive_to_produce_s * 1000, 3),
                "produce_to_ack_ms": round(produce_to_ack_s * 1000, 3),
                "event_to_ack_ms": round((event_to_receive_s + receive_to_produce_s + produce_to_ack_s) * 1000, 3),
            })

        if self._produce(self.kafka_topic.name, record["asset_id"].encode("utf-8"), value, on_delivery, timing):
            self.stats.add("produced")

    def dead_letter(self, mqtt_topic: str, payload: bytes, reason: str, detail: str) -> None:
        """Produce a rejected MQTT message, intact, to telemetry.dlq.

        The key is the asset_id named by the MQTT topic, which does not depend
        on the payload; None when the topic does not match the template. A
        failed dead-letter send is logged at ERROR with the payload in base64
        and the bridge carries on: an MQTT message cannot be fetched again, so
        stopping would not save it (the alarm consumer, reading Kafka, stops
        instead; see the module docstring). The delivery callback runs on the
        delivery-report thread.
        """
        with self.stats.lock:
            self.stats.invalid[reason] += 1
        key = self.validator.topics.asset_id(mqtt_topic)
        context = {"reason": reason, "detail": detail[:DETAIL_MAX], "source": "bridge", "mqtt_topic": mqtt_topic,
                   "key": key, "payload_bytes": len(payload)}

        def failed(error: str) -> None:  # main thread or delivery-report thread
            self.stats.add("dlq_failed")
            log.error("dlq_delivery_failed", extra={**context, "error": error,
                                                    "payload_b64": base64.b64encode(payload).decode("ascii")})

        record = {
            "payload": payload, "reason": reason, "detail": detail[:DETAIL_MAX], "source": "bridge",
            "source_topic": mqtt_topic, "source_partition": None, "source_offset": None, "source_key": None,
            "schema_id": None, "ts": datetime.now(timezone.utc),
        }
        try:
            value = self.dlq_serializer(record, self.dlq_context)
        except SerializationError as e:
            failed(f"cannot serialise the dead letter: {e}")
            return

        def on_delivery(err, msg):  # delivery-report thread
            if err is not None:
                failed(str(err))
                return
            self.stats.add("dlq_delivered")
            log.warning("message_dead_lettered", extra={**context, "topic": msg.topic(), "partition": msg.partition(),
                                                        "offset": msg.offset()})

        if not self._produce(self.dlq_topic.name, key.encode("utf-8") if key else None, value, on_delivery, {}):
            failed("not produced: the delivery-report thread has failed")

    def run(self, stop: threading.Event) -> str:
        start = time.monotonic()
        while True:
            if self.reports_failed.is_set():
                return "delivery_reports_failed"
            if stop.is_set():
                return "signal"
            if self.args.duration is not None and time.monotonic() - start >= self.args.duration:
                return "duration"
            if self.args.count is not None and self.stats.received >= self.args.count:
                return "count"
            if self.drop_monitor is not None:
                self.drop_monitor.check_silence(time.monotonic())
            try:
                item = self.inbox.get(timeout=0.1)
            except queue.Empty:
                continue
            self.handle(*item)

    def shutdown(self, reason: str) -> bool:
        if self.mqtt is not None:
            self.mqtt.disconnect()
            self.mqtt.loop_stop()
        if self.drop_monitor is not None:
            self.drop_monitor.stop()
        # Anything still queued is processed while the delivery-report thread
        # runs, unless the run ended at --count or because that thread failed.
        unprocessed = 0
        while True:
            try:
                item = self.inbox.get_nowait()
            except queue.Empty:
                break
            if reason in ("count", "delivery_reports_failed"):
                unprocessed += 1
            else:
                self.handle(*item)
        self.stop_delivery_reports()
        # flush() serves the remaining delivery reports on this thread.
        remaining = 0
        if self.producer is not None:
            try:
                remaining = self.producer.flush(self.args.flush_timeout)
            except Exception as e:  # a callback failing again here must not hide the summary
                log.error("flush_failed", extra={"error": f"{type(e).__name__}: {e}"})
                remaining = len(self.producer)  # messages still in the producer queue
        summary = {"reason": reason, **self.stats.as_dict(), "messages_unprocessed": unprocessed,
                   "messages_undelivered_at_exit": remaining,
                   # Broker-wide, as logged by mqtt_messages_dropped; None with the monitor disabled.
                   "mosquitto_dropped_during_run": self.drop_monitor.dropped_during_run if self.drop_monitor else None}
        complete = (remaining == 0 and self.stats.delivery_failed == 0 and self.stats.dlq_failed == 0
                    and not self.reports_failed.is_set())
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


# Orchestration entry point: wires real MQTT, Kafka and Schema Registry
# clients and runs until stopped. Exercised by the integration tests
# (Task 2.5), not by unit tests; excluded from unit coverage by owner
# decision (Task 2.4), the only exclusion in this file.
def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    args = parse_args(argv)
    jsonlog.configure(args.log_level)
    try:
        bridge = Bridge(args)
    except ConfigError as e:
        log.error("config_invalid", extra={"error": str(e)})
        return 1

    stop = threading.Event()
    install_signal_handlers(stop)

    if not (bridge.start_kafka() and bridge.start_schema_registry()):
        bridge.shutdown("startup_failed")
        return 1
    bridge.start_delivery_reports()
    if not (bridge.start_drop_monitor() and bridge.start_mqtt()):
        bridge.shutdown("startup_failed")
        return 1
    log.info("run_started", extra={"count": args.count, "duration_s": args.duration,
                                   "mqtt_subscription": bridge.mqtt_topic.subscription,
                                   "kafka_topic": bridge.kafka_topic.name})
    reason = bridge.run(stop)
    return 0 if bridge.shutdown(reason) else 1


if __name__ == "__main__":
    sys.exit(main())
