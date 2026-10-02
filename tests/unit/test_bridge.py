"""bridge/mqtt_to_kafka/__main__.py: the Bridge class with Kafka, Schema Registry and MQTT faked.

Avro serialisation is real (confluent-kafka serializers on its in-process mock
registry); the producer is FakeProducer from conftest, whose poll() the tests
call by hand, or ThreadedProducer below when the bridge's own delivery-report
thread serves the reports. Covers the three Task 2.3 paths that need a fault
to be provoked (serialization_error, a failed dead-letter delivery, a failed
dead-letter serialisation) and the Task 3.1-A ones: reports served without the
main loop, and the bridge stopping when the delivery-report thread dies.
"""

from __future__ import annotations

import argparse
import base64
import json
import signal
import threading
import time
import types
import warnings

import pytest
from confluent_kafka import KafkaException
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

import bridge.mqtt_to_kafka.__main__ as bridge_main
from bridge.mqtt_to_kafka.__main__ import Bridge, Stats, parse_args
from conftest import FakeProducer, telemetry

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import confluent_kafka.schema_registry as sr
    from confluent_kafka.schema_registry._sync.mock_schema_registry_client import MockSchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer

TOPIC = "plant/pump-01/telemetry"
OK = types.SimpleNamespace(is_failure=False)
FAILED = types.SimpleNamespace(is_failure=True, __str__=lambda self: "refused")


@pytest.fixture
def registry(monkeypatch):
    client = MockSchemaRegistryClient({"url": "mock://bridge-tests"})
    monkeypatch.setattr(sr, "SchemaRegistryClient", lambda conf: client)
    return client


@pytest.fixture
def bridge(registry, json_log):
    b = Bridge(parse_args(["--schema-registry-url", "mock://bridge-tests", "--flush-timeout", "1"]))
    assert b.start_schema_registry()
    b.producer = FakeProducer()
    return b


def decode(registry, topic, value):
    return AvroDeserializer(registry)(value, SerializationContext(topic, MessageField.VALUE))


RECEIVED_NS = 1_790_889_302_300_000_000


def received(payload=None, topic=TOPIC):
    """An inbox item as _on_message builds it."""
    raw = payload if isinstance(payload, bytes) else json.dumps(payload or telemetry()).encode()
    return (topic, raw, RECEIVED_NS, time.perf_counter())


def handle(b, payload, topic=TOPIC):
    item = received(payload, topic)
    b.handle(*item)
    b.producer.poll()
    return item[1]


def events(lines, name):
    return [line for line in lines() if line["event"] == name]


# --- valid telemetry -----------------------------------------------------------


def test_valid_message_goes_to_telemetry_raw_keyed_by_asset(bridge, registry, json_log):
    handle(bridge, telemetry())
    [record] = bridge.producer.on("telemetry.raw")
    assert record["key"] == b"pump-01"
    assert decode(registry, "telemetry.raw", record["value"])["seq"] == 3
    assert bridge.producer.on("telemetry.dlq") == []
    [delivered] = events(json_log, "message_delivered")
    stages = ("event_to_receive_ms", "receive_to_produce_ms", "produce_to_ack_ms")
    assert set(stages) | {"event_to_ack_ms", "receive_ts_ms"} <= delivered.keys()
    assert not {"ts_to_receive_ms", "ts_to_produce_ms", "ts_to_ack_ms"} & delivered.keys()
    assert delivered["event_to_ack_ms"] == pytest.approx(sum(delivered[f] for f in stages), abs=0.002)
    # telemetry() has ts 2026-10-01T23:50:15.087Z; RECEIVED_NS is 1790889302.300 s
    assert delivered["event_to_receive_ms"] == pytest.approx((1790889302.300 - 1790898615.087) * 1000, abs=0.01)
    assert delivered["receive_ts_ms"] == RECEIVED_NS / 1e6 == 1790889302300.0
    assert (bridge.stats.received, bridge.stats.produced, bridge.stats.delivered) == (1, 1, 1)


def test_buffer_full_is_retried(bridge):
    bridge.producer.buffer_errors = 1
    handle(bridge, telemetry())
    assert len(bridge.producer.on("telemetry.raw")) == 1


def test_failed_telemetry_delivery_is_counted_and_makes_the_run_incomplete(bridge, json_log):
    bridge.producer.fail_topics = {"telemetry.raw": "KafkaError{code=_MSG_TIMED_OUT}"}
    handle(bridge, telemetry())
    assert bridge.stats.delivery_failed == 1
    assert events(json_log, "message_delivery_failed")[0]["error"].startswith("KafkaError")
    assert bridge.shutdown("count") is False


# --- dead letters --------------------------------------------------------------


@pytest.mark.parametrize("payload, reason", [
    (b'{"asset_id": "pump-01", oops', "invalid_json"),
    (telemetry(ts="yesterday"), "invalid_ts"),
    (telemetry(seq="three"), "schema_mismatch"),
    (telemetry("pump-02"), "topic_mismatch"),
    (json.dumps(telemetry()).replace('"vibration_rms_mm_s": 2.6', '"vibration_rms_mm_s": NaN').encode(), "non_finite_value"),
])
def test_rejected_messages_are_dead_lettered_intact(bridge, registry, json_log, payload, reason):
    raw = handle(bridge, payload)
    assert bridge.producer.on("telemetry.raw") == []
    [record] = bridge.producer.on("telemetry.dlq")
    assert record["key"] == b"pump-01"  # from the MQTT topic, not the payload
    letter = decode(registry, "telemetry.dlq", record["value"])
    assert letter["payload"] == raw
    assert (letter["reason"], letter["source"], letter["source_topic"]) == (reason, "bridge", TOPIC)
    assert letter["source_partition"] is letter["source_offset"] is letter["source_key"] is letter["schema_id"] is None
    [logged] = events(json_log, "message_dead_lettered")
    assert logged["level"] == "WARNING" and logged["reason"] == reason and "payload_b64" not in logged
    assert bridge.stats.invalid[reason] == 1 and bridge.stats.dlq_delivered == 1


def test_dead_letter_key_is_null_when_the_topic_names_no_asset(bridge, registry):
    handle(bridge, telemetry(), topic="other/pump-01/x")
    [record] = bridge.producer.on("telemetry.dlq")
    assert record["key"] is None
    assert decode(registry, "telemetry.dlq", record["value"])["reason"] == "topic_mismatch"


def test_detail_is_truncated_to_1000_characters(bridge, registry):
    bridge.dead_letter(TOPIC, b"x", "invalid_json", "d" * 5000)
    bridge.producer.poll()
    letter = decode(registry, "telemetry.dlq", bridge.producer.on("telemetry.dlq")[0]["value"])
    assert len(letter["detail"]) == 1000


def test_serialization_error_is_dead_lettered(bridge, registry):
    """Task 2.3 pending path: a record that validates but does not serialise."""
    def failing_serializer(record, ctx):
        raise SerializationError("forced by the test")
    bridge.serializer = failing_serializer
    raw = handle(bridge, telemetry())
    assert bridge.producer.on("telemetry.raw") == []
    letter = decode(registry, "telemetry.dlq", bridge.producer.on("telemetry.dlq")[0]["value"])
    assert (letter["reason"], letter["detail"], letter["payload"]) == ("serialization_error", "forced by the test", raw)


def test_failed_dead_letter_delivery_logs_the_payload_and_the_bridge_carries_on(bridge, json_log):
    """Task 2.3 pending path: the dead-letter send fails in the bridge."""
    bridge.producer.fail_topics = {"telemetry.dlq": "KafkaError{code=_MSG_TIMED_OUT}"}
    raw = handle(bridge, b'{"broken": ')
    [failed] = events(json_log, "dlq_delivery_failed")
    assert failed["level"] == "ERROR"
    assert base64.b64decode(failed["payload_b64"]) == raw
    assert (failed["reason"], failed["key"]) == ("invalid_json", "pump-01")
    assert bridge.stats.dlq_failed == 1

    handle(bridge, telemetry())  # the next valid message still flows
    assert bridge.stats.delivered == 1

    assert bridge.shutdown("count") is False
    [summary] = events(json_log, "run_finished_incomplete")
    assert summary["dead_letters_failed"] == 1


def test_failed_dead_letter_serialisation_is_logged_with_the_payload(bridge, json_log):
    def failing(record, ctx):
        raise SerializationError("cannot encode")
    bridge.dlq_serializer = failing
    raw = handle(bridge, b"not json")
    [failed] = events(json_log, "dlq_delivery_failed")
    assert "cannot serialise the dead letter" in failed["error"]
    assert base64.b64decode(failed["payload_b64"]) == raw
    assert bridge.producer.on("telemetry.dlq") == []


def test_dead_letter_buffer_full_is_retried(bridge):
    bridge.producer.buffer_errors = 1
    handle(bridge, b"not json")
    assert len(bridge.producer.on("telemetry.dlq")) == 1


# --- run loop and shutdown -----------------------------------------------------


def test_run_stops_at_count(bridge):
    bridge.args.count = 2
    for _ in range(3):
        bridge.inbox.put(received())
    assert bridge.run(threading.Event()) == "count"
    assert bridge.stats.received == 2


def test_run_stops_on_signal_and_on_duration(bridge):
    stop = threading.Event()
    stop.set()
    assert bridge.run(stop) == "signal"
    bridge.args.duration = 0.05
    assert bridge.run(threading.Event()) == "duration"


def test_shutdown_drains_or_counts_queued_messages(bridge, json_log):
    fake_mqtt = types.SimpleNamespace(disconnect=lambda: None, loop_stop=lambda: None)
    bridge.mqtt = fake_mqtt
    bridge.inbox.put(received())
    assert bridge.shutdown("signal") is True  # drained and delivered
    assert bridge.stats.delivered == 1
    bridge.inbox.put(received())
    bridge.shutdown("count")
    assert events(json_log, "run_finished")[-1]["messages_unprocessed"] == 1


def test_shutdown_incomplete_when_flush_leaves_messages(bridge, json_log):
    bridge.producer.flush_remaining = 2
    assert bridge.shutdown("duration") is False
    assert events(json_log, "run_finished_incomplete")[0]["messages_undelivered_at_exit"] == 2


def test_shutdown_after_startup_failure_logs_its_own_event(bridge, json_log):
    bridge.shutdown("startup_failed")
    assert events(json_log, "shutdown_after_startup_failure")


# --- delivery-report thread (Task 3.1-A) ---------------------------------------


class ThreadedProducer:
    """Producer stand-in for the delivery-report thread.

    Each record is "acknowledged by the broker" ack_delay seconds after
    produce(); poll(timeout) blocks until an acknowledged report is due or the
    timeout ends, then runs the due callbacks, as librdkafka's poll() does.
    poll_error makes poll() raise, as an exception escaping a callback would.
    """

    def __init__(self, ack_delay=0.02, poll_error=None):
        self.ack_delay, self.poll_error = ack_delay, poll_error
        self.cond = threading.Condition()
        self.pending = []  # (due perf_counter, callback, message)
        self.records = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        with self.cond:
            self.records.append({"topic": topic, "key": key, "value": value})
            msg = types.SimpleNamespace(topic=lambda: topic, partition=lambda: 0, offset=lambda: len(self.records) - 1)
            self.pending.append((time.perf_counter() + self.ack_delay, on_delivery, msg))
            self.cond.notify_all()

    def poll(self, timeout=0):
        if self.poll_error:
            raise self.poll_error
        deadline = time.perf_counter() + timeout
        with self.cond:
            while True:
                now = time.perf_counter()
                due = [p for p in self.pending if p[0] <= now]
                if due or now >= deadline:
                    break
                waits = [p[0] - now for p in self.pending] + [deadline - now]
                self.cond.wait(min(waits))
            self.pending = [p for p in self.pending if p[0] > now]
        for _, callback, msg in due:
            callback(None, msg)
        return len(due)

    def flush(self, timeout=0):
        end = time.perf_counter() + timeout
        while self.pending and time.perf_counter() < end:
            self.poll(0.05)
        return len(self.pending)

    def __len__(self):
        return len(self.pending)

    def on(self, topic):
        return [r for r in self.records if r["topic"] == topic]


def wait_for(predicate, timeout=2.0):
    end = time.perf_counter() + timeout
    while time.perf_counter() < end:
        if predicate():
            return True
        time.sleep(0.005)
    return False


def test_ack_is_timed_when_the_broker_acknowledges_not_when_the_main_loop_polls(bridge, json_log):
    """Fails on the Phase 2 design, which served reports only from the main loop.

    The main loop runs as in production: it waits on the inbox in 100 ms steps.
    The fake broker acknowledges 20 ms after produce(). produce_to_ack_ms must
    reflect those 20 ms, not the next main-loop poll about 100 ms later.
    """
    bridge.producer = ThreadedProducer(ack_delay=0.02)
    bridge.start_delivery_reports()
    bridge.args.duration = 1.0
    bridge.inbox.put(received())
    loop = threading.Thread(target=bridge.run, args=(threading.Event(),))
    loop.start()
    try:
        assert wait_for(lambda: events(json_log, "message_delivered"), timeout=0.5), "report not served within 0.5 s"
        [delivered] = events(json_log, "message_delivered")
        assert 18 <= delivered["produce_to_ack_ms"] < 60
    finally:
        loop.join()
        bridge.stop_delivery_reports()


def test_all_reports_are_counted_across_threads(bridge):
    bridge.producer = ThreadedProducer(ack_delay=0.0)
    bridge.start_delivery_reports()
    try:
        for i in range(300):
            bridge.handle(*received(telemetry(seq=i)))
            bridge.dead_letter(TOPIC, b"x", "invalid_json", "")
        assert wait_for(lambda: bridge.stats.delivered == 300 and bridge.stats.dlq_delivered == 300)
    finally:
        bridge.stop_delivery_reports()
    assert (bridge.stats.received, bridge.stats.produced, bridge.stats.invalid["invalid_json"]) == (300, 300, 300)


def test_bridge_stops_when_the_delivery_report_thread_dies(bridge, json_log):
    bridge.producer = ThreadedProducer(poll_error=RuntimeError("poll broke"))
    bridge.start_delivery_reports()
    assert wait_for(bridge.reports_failed.is_set)
    [failure] = events(json_log, "delivery_reports_failed")
    assert failure["level"] == "ERROR" and failure["error"] == "RuntimeError: poll broke"

    for _ in range(3):
        bridge.inbox.put(received())
    assert bridge.run(threading.Event()) == "delivery_reports_failed"
    assert bridge.producer.records == []  # nothing produced once reports are not served
    bridge.mqtt = types.SimpleNamespace(disconnect=lambda: None, loop_stop=lambda: None)
    assert bridge.shutdown("delivery_reports_failed") is False
    [summary] = events(json_log, "run_finished_incomplete")
    assert summary["reason"] == "delivery_reports_failed" and summary["messages_unprocessed"] == 3


def test_an_exception_in_a_delivery_callback_stops_the_bridge(bridge, json_log, monkeypatch):
    bridge.producer = ThreadedProducer(ack_delay=0.0)

    def broken_info(event, *a, **k):
        if event == "message_delivered":
            raise OSError("log handler failed")
    monkeypatch.setattr(bridge_main.log, "info", broken_info)
    bridge.start_delivery_reports()
    bridge.handle(*received())
    assert wait_for(bridge.reports_failed.is_set)
    assert events(json_log, "delivery_reports_failed")[0]["error"] == "OSError: log handler failed"
    assert bridge.run(threading.Event()) == "delivery_reports_failed"


def test_nothing_is_produced_after_the_delivery_report_thread_failed(bridge, json_log):
    bridge.reports_failed.set()
    bridge.handle(*received())
    bridge.dead_letter(TOPIC, b"x", "invalid_json", "")
    assert bridge.producer.records == []
    assert (bridge.stats.produced, bridge.stats.dlq_failed) == (0, 1)
    assert "delivery-report thread has failed" in events(json_log, "dlq_delivery_failed")[0]["error"]


def test_flush_failure_is_reported_in_the_summary(bridge, json_log):
    def failing_flush(timeout=0):
        raise RuntimeError("callback failed again")
    bridge.producer.flush = failing_flush
    bridge.producer.pending.append((None, None))
    assert bridge.shutdown("signal") is False
    assert events(json_log, "flush_failed")[0]["error"] == "RuntimeError: callback failed again"
    assert events(json_log, "run_finished_incomplete")[0]["messages_undelivered_at_exit"] == 1


# --- start-up ------------------------------------------------------------------


class FakeMetadataProducer:
    def __init__(self, topics=None, error=None):
        self._topics, self._error = topics or {}, error

    def __call__(self, conf):
        return self

    def list_topics(self, timeout):
        if self._error:
            raise KafkaException(self._error)
        return types.SimpleNamespace(topics=self._topics)


def topic_meta(partitions=3, error=None):
    return types.SimpleNamespace(partitions={i: None for i in range(partitions)}, error=error)


def test_start_kafka_requires_both_topics(registry, json_log, monkeypatch):
    b = Bridge(parse_args([]))
    both = {"telemetry.raw": topic_meta(), "telemetry.dlq": topic_meta(1)}
    monkeypatch.setattr(bridge_main, "Producer", FakeMetadataProducer(both))
    assert b.start_kafka() is True
    assert events(json_log, "kafka_ready")[0]["dlq_topic"] == "telemetry.dlq"

    monkeypatch.setattr(bridge_main, "Producer", FakeMetadataProducer({"telemetry.raw": topic_meta()}))
    assert b.start_kafka() is False
    assert events(json_log, "kafka_topic_missing")[0]["topic"] == "telemetry.dlq"

    monkeypatch.setattr(bridge_main, "Producer", FakeMetadataProducer({"telemetry.raw": topic_meta(error="UNKNOWN_TOPIC")}))
    assert b.start_kafka() is False


def test_start_kafka_fails_fast_when_kafka_is_unreachable(registry, json_log, monkeypatch):
    monkeypatch.setattr(bridge_main, "Producer", FakeMetadataProducer(error="transport failure"))
    assert Bridge(parse_args([])).start_kafka() is False
    assert events(json_log, "kafka_unreachable")


def test_start_schema_registry_registers_both_subjects(registry, json_log):
    b = Bridge(parse_args([]))
    assert b.start_schema_registry() is True
    assert {e["subject"] for e in events(json_log, "schema_registered")} == {"telemetry.raw-value", "telemetry.dlq-value"}


def test_start_schema_registry_fails_fast(monkeypatch, json_log):
    class Unreachable:
        def __init__(self, conf):
            pass

        def register_schema(self, subject, schema):
            raise ConnectionError("registry down")
    monkeypatch.setattr(sr, "SchemaRegistryClient", Unreachable)
    assert Bridge(parse_args([])).start_schema_registry() is False
    assert events(json_log, "schema_registration_failed")[0]["error"] == "registry down"


class FakeMqtt:
    """paho Client stand-in: loop_start() fires on_connect, subscribe() fires on_subscribe."""

    def __init__(self, connect_error=None, connect_rc=OK, subscribe_rc=OK):
        self.connect_error, self.connect_rc, self.subscribe_rc = connect_error, connect_rc, subscribe_rc

    def __call__(self, callback_api_version, client_id):
        return self

    def connect(self, host, port, keepalive):
        if self.connect_error:
            raise self.connect_error

    def loop_start(self):
        self.on_connect(self, None, None, self.connect_rc, None)

    def subscribe(self, topic, qos):
        self.on_subscribe(self, None, 1, [self.subscribe_rc], None)


def test_start_mqtt_subscribes(registry, json_log, monkeypatch):
    monkeypatch.setattr(bridge_main.mqtt, "Client", FakeMqtt())
    assert Bridge(parse_args([])).start_mqtt() is True
    assert events(json_log, "mqtt_subscribed")[0]["subscription"] == "plant/+/telemetry"


@pytest.mark.parametrize("fake, event", [
    (FakeMqtt(connect_error=OSError("refused")), "mqtt_connect_failed"),
    (FakeMqtt(connect_rc=FAILED), "mqtt_connect_refused"),
    (FakeMqtt(subscribe_rc=FAILED), "mqtt_subscribe_refused"),
])
def test_start_mqtt_failures(registry, json_log, monkeypatch, fake, event):
    monkeypatch.setattr(bridge_main.mqtt, "Client", fake)
    monkeypatch.setattr(bridge_main, "CONNECT_TIMEOUT_S", 0.05)
    assert Bridge(parse_args([])).start_mqtt() is False
    assert events(json_log, event)


def test_mqtt_callbacks(bridge, json_log):
    bridge._on_disconnect(None, None, None, FAILED, None)
    bridge._on_disconnect(None, None, None, OK, None)
    assert len(events(json_log, "mqtt_disconnected")) == 1
    bridge._on_message(None, None, types.SimpleNamespace(topic=TOPIC, payload=b"x"))
    topic, payload, received_ns, received_perf = bridge.inbox.get_nowait()
    assert (topic, payload) == (TOPIC, b"x")
    assert abs(received_ns / 1e9 - time.time()) < 5 and received_perf <= time.perf_counter()


# --- small pieces --------------------------------------------------------------


def test_argument_validators():
    assert bridge_main._positive_int("3") == 3
    assert bridge_main._positive_float("0.5") == 0.5
    for fn, text in ((bridge_main._positive_int, "0"), (bridge_main._positive_float, "-1")):
        with pytest.raises(argparse.ArgumentTypeError):
            fn(text)


def test_defaults_come_from_the_environment(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "kafka:29092")
    monkeypatch.setenv("MQTT_PORT", "18830")
    args = parse_args([])
    assert (args.kafka_bootstrap, args.mqtt_port) == ("kafka:29092", 18830)


def test_stats_summary():
    s = Stats(received=3, produced=1, delivered=1)
    s.invalid["invalid_json"] = 2
    assert s.as_dict()["messages_invalid"] == 2


def test_signal_handlers_set_the_stop_event(json_log):
    saved = {name: signal.getsignal(getattr(signal, name)) for name in ("SIGINT", "SIGTERM", "SIGBREAK") if hasattr(signal, name)}
    try:
        stop = threading.Event()
        bridge_main.install_signal_handlers(stop)
        signal.getsignal(signal.SIGINT)(signal.SIGINT, None)
        assert stop.is_set()
        assert events(json_log, "signal_received")[0]["signal"] == "SIGINT"
    finally:
        for name, handler in saved.items():
            signal.signal(getattr(signal, name), handler)
