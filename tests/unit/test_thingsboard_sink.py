"""processing/thingsboard_sink: the mapping, the gateway client, and the record handling, with Kafka and MQTT faked.

Avro is real where it matters: telemetry and alarm records are encoded with confluent-kafka's serializer on its
in-process mock Schema Registry and decoded by the sink's own deserializer.
"""

from __future__ import annotations

import json
import signal
import threading
import types
import warnings
from datetime import datetime, timezone

import pytest
from confluent_kafka import KafkaException
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

import processing.thingsboard_sink.__main__ as ts
from conftest import REPO_ROOT, telemetry

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import confluent_kafka.schema_registry as sr
    from confluent_kafka.schema_registry._sync.mock_schema_registry_client import MockSchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

OK = types.SimpleNamespace(is_failure=False)
REFUSED = types.SimpleNamespace(is_failure=True, __str__=lambda self: "not authorised")
ALARM = {"alarm_id": "pump-01/vibration_rms_mm_s/WARNING/1790889306000", "asset_id": "pump-01",
         "channel": "vibration_rms_mm_s", "severity": "WARNING", "state": "RAISED", "direction": "high",
         "value": 6.5, "threshold": 4.5, "hysteresis": 0.3, "persistence_samples": 5, "source_seq": 4,
         "ts": datetime(2026, 10, 1, 23, 50, 6, tzinfo=timezone.utc), "synthetic": True}


def events(lines, name):
    return [line for line in lines() if line["event"] == name]


# --- mapping ---------------------------------------------------------------------------------


def test_telemetry_entry_uses_the_sample_ts_and_drops_null_channels():
    record = {**telemetry(), "ts": datetime(2026, 10, 1, 23, 50, 15, 87000, tzinfo=timezone.utc), "speed_rpm": None,
              "injected_fault": "imbalance"}
    device, device_type, entry = ts.telemetry_entry(record, ["bearing_temperature_c", "vibration_rms_mm_s", "speed_rpm"])
    assert (device, device_type) == ("pump-01", "pump")
    assert entry == {"ts": 1790898615087, "values": {"bearing_temperature_c": 53.1, "vibration_rms_mm_s": 2.6, "seq": 3,
                                                     "injected_fault": "imbalance"}}
    _, _, plain = ts.telemetry_entry({**record, "injected_fault": None}, ["speed_rpm"])
    assert "injected_fault" not in plain["values"]


LABELS = {"vibration_rms_mm_s": "Vibration RMS"}


def test_alarm_entry_has_one_key_per_alarm_type_and_the_raised_ts():
    device, entry = ts.alarm_entry({**ALARM, "state": "CLEARED", "ts": 1790889316000}, LABELS)
    assert device == "pump-01" and entry["ts"] == 1790889316000
    [(key, value)] = entry["values"].items()
    assert key == "alarm_vibration_rms_mm_s_WARNING"
    assert json.loads(value) == {"alarm_id": ALARM["alarm_id"], "state": "CLEARED", "severity": "WARNING",
                                 "channel": "vibration_rms_mm_s", "value": 6.5, "threshold": 4.5, "raised_ts": 1790889306000,
                                 "type": "Vibration RMS high (warning)"}
    _, critical = ts.alarm_entry({**ALARM, "severity": "CRITICAL", "alarm_id": "pump-01/vibration_rms_mm_s/CRITICAL/1"}, LABELS)
    assert list(critical["values"]) == ["alarm_vibration_rms_mm_s_CRITICAL"]  # same ts, another key: no overwrite
    assert json.loads(critical["values"]["alarm_vibration_rms_mm_s_CRITICAL"])["type"] == "Vibration RMS high (critical)"


# --- gateway ----------------------------------------------------------------------------------


class FakeInfo:
    def __init__(self, published=True, error=None):
        self.published, self.error = published, error

    def wait_for_publish(self, timeout):
        if self.error:
            raise self.error

    def is_published(self):
        return self.published


class FakeMqtt:
    """paho Client stand-in; loop_start() fires on_connect with connect_rc unless connect_rc is None."""

    def __init__(self, connect_rc=OK, connect_error=None):
        self.connect_rc, self.connect_error = connect_rc, connect_error
        self.published, self.next_info = [], FakeInfo()

    def __call__(self, callback_api_version, client_id):
        return self

    def username_pw_set(self, token):
        self.token = token

    def connect_async(self, host, port, keepalive):
        if self.connect_error:
            raise self.connect_error

    def loop_start(self):
        if self.connect_rc is not None:
            self.on_connect(self, None, None, self.connect_rc, None)

    def loop_stop(self):
        self.stopped = True

    def disconnect(self):
        self.disconnected = True

    def publish(self, topic, payload, qos):
        self.published.append((topic, json.loads(payload), qos))
        return self.next_info


def gateway(fake=None, json_log=None):
    fake = fake or FakeMqtt()
    return ts.Gateway("tb", 1883, "token", "client", client_factory=fake), fake


def test_gateway_connects_and_announces_each_device_once_per_session(json_log):
    g, fake = gateway()
    assert g.start() and fake.token == "token"
    g.send("pump-01", "pump", {"ts": 1, "values": {"a": 1}})
    g.send("pump-01", "pump", {"ts": 2, "values": {"a": 2}})
    g.send("x-01", None, {"ts": 3, "values": {}})
    assert [(t, p) for t, p, _ in fake.published] == [
        ("v1/gateway/connect", {"device": "pump-01", "type": "pump"}),
        ("v1/gateway/telemetry", {"pump-01": [{"ts": 1, "values": {"a": 1}}]}),
        ("v1/gateway/telemetry", {"pump-01": [{"ts": 2, "values": {"a": 2}}]}),
        ("v1/gateway/connect", {"device": "x-01"}),
        ("v1/gateway/telemetry", {"x-01": [{"ts": 3, "values": {}}]}),
    ]
    assert all(qos == 1 for _, _, qos in fake.published)
    g._on_disconnect(None, None, None, OK, None)
    assert events(json_log, "thingsboard_disconnected")
    g._on_connect(None, None, None, OK, None)  # a new session: announce again
    g.send("pump-01", "pump", {"ts": 4, "values": {}})
    assert fake.published[-2][0] == "v1/gateway/connect"
    g.stop()
    assert fake.stopped and fake.disconnected


def test_gateway_start_failures(json_log, monkeypatch):
    monkeypatch.setattr(ts, "CONNECT_TIMEOUT_S", 0.05)
    assert gateway(FakeMqtt(connect_error=OSError("refused")))[0].start() is False
    assert events(json_log, "thingsboard_connect_failed")
    assert gateway(FakeMqtt(connect_rc=REFUSED))[0].start(timeout=0.05) is False
    assert events(json_log, "thingsboard_connect_refused") and events(json_log, "thingsboard_unreachable")
    assert gateway(FakeMqtt(connect_rc=None))[0].start(timeout=0.05) is False


@pytest.mark.parametrize("info, message", [
    (FakeInfo(published=False), "no PUBACK"),
    (FakeInfo(error=RuntimeError("message queue full")), "publish failed"),
])
def test_publish_without_puback_is_transient(info, message):
    g, fake = gateway()
    g.start()
    fake.next_info = info
    with pytest.raises(ts.Transient, match=message):
        g.publish("t", {})


def test_publish_while_disconnected_is_transient():
    g, _ = gateway(FakeMqtt(connect_rc=None))
    with pytest.raises(ts.Transient, match="not connected"):
        g.publish("t", {})


# --- the sink --------------------------------------------------------------------------------


class Msg:
    def __init__(self, topic, value, key=b"k", partition=0, offset=7, error=None):
        self._t, self._v, self._k, self._p, self._o, self._e = topic, value, key, partition, offset, error

    def topic(self):
        return self._t

    def value(self):
        return self._v

    def key(self):
        return self._k

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def error(self):
        return self._e


class FakeGateway:
    def __init__(self, failures=0):
        self.sent, self.failures = [], failures

    def send(self, device, device_type, entry):
        if self.failures:
            self.failures -= 1
            raise ts.Transient("ThingsBoard down")
        self.sent.append((device, device_type, entry))


class FakeProducer:
    def __init__(self, remaining=0):
        self.produced, self.remaining = [], remaining

    def produce(self, topic, key=None, value=None):
        self.produced.append((topic, key, value))

    def flush(self, timeout):
        return self.remaining


@pytest.fixture
def registry(monkeypatch):
    client = MockSchemaRegistryClient({"url": "mock://sink-tests"})
    monkeypatch.setattr(sr, "SchemaRegistryClient", lambda conf: client)
    return client


@pytest.fixture
def sink(registry, json_log):
    s = ts.Sink(ts.parse_args(["--schema-registry-url", "mock://sink-tests"]), token="token")
    s.start_schema_registry()
    s.gateway, s.producer = FakeGateway(), FakeProducer()
    return s


def encode(registry, topic, schema_file, record):
    schema = (REPO_ROOT / "schemas" / schema_file).read_text(encoding="utf-8")
    return AvroSerializer(registry, schema)(record, SerializationContext(topic, MessageField.VALUE))


def raw_record(**overrides):
    return {**telemetry(), "ts": datetime(2026, 10, 1, 23, 50, 15, 87000, tzinfo=timezone.utc), **overrides}


def test_telemetry_record_is_forwarded_with_its_type(sink, registry):
    sink.handle(Msg("telemetry.raw", encode(registry, "telemetry.raw", "telemetry_v1.avsc", raw_record())))
    [(device, device_type, entry)] = sink.gateway.sent
    assert (device, device_type, entry["ts"]) == ("pump-01", "pump", 1790898615087)
    assert set(entry["values"]) == {"bearing_temperature_c", "vibration_rms_mm_s", "motor_current_a", "speed_rpm", "seq"}
    assert sink.counts["telemetry_forwarded"] == 1


def test_alarm_event_is_forwarded_with_the_configured_asset_type(sink, registry):
    sink.handle(Msg("telemetry.alarms", encode(registry, "telemetry.alarms", "alarm_v1.avsc", ALARM)))
    [(device, device_type, entry)] = sink.gateway.sent
    assert (device, device_type) == ("pump-01", "pump")
    body = json.loads(entry["values"]["alarm_vibration_rms_mm_s_WARNING"])
    assert (body["raised_ts"], body["type"]) == (1790889306000, "Vibration RMS high (warning)")  # label from config/
    assert sink.counts["alarm_events_forwarded"] == 1


def test_tombstones_on_alarms_are_skipped(sink, json_log):
    sink.handle(Msg("telemetry.alarms", None, key=b"it-roll-marker/0"))
    assert sink.gateway.sent == [] and sink.producer.produced == []
    assert sink.counts["tombstones_skipped"] == 1 and events(json_log, "tombstone_skipped")


def test_undecodable_telemetry_is_skipped_not_dead_lettered(sink, json_log):
    sink.handle(Msg("telemetry.raw", b"\x01not avro"))
    sink.handle(Msg("telemetry.raw", None))
    assert sink.producer.produced == [] and sink.counts["raw_undecodable_skipped"] == 2
    assert "dead-lettered by the alarm consumer" in events(json_log, "message_undecodable")[0]["action"]


def test_undecodable_alarm_record_is_dead_lettered(sink, registry, json_log):
    sink.handle(Msg("telemetry.alarms", b"\x00\x00\x00\x00\x09garbage", key=b"some-alarm", partition=2, offset=11))
    [(topic, key, value)] = sink.producer.produced
    assert (topic, key) == ("telemetry.dlq", b"some-alarm")
    letter = AvroDeserializer(registry)(value, SerializationContext("telemetry.dlq", MessageField.VALUE))
    assert (letter["source"], letter["reason"], letter["source_topic"]) == ("thingsboard_sink", "undecodable", "telemetry.alarms")
    assert (letter["source_partition"], letter["source_offset"], letter["schema_id"]) == (2, 11, 9)
    assert letter["payload"] == b"\x00\x00\x00\x00\x09garbage"
    assert sink.counts["dead_lettered"] == 1 and events(json_log, "message_dead_lettered")


def test_a_dead_letter_not_acknowledged_is_transient(sink):
    sink.producer = FakeProducer(remaining=1)
    with pytest.raises(ts.Transient, match="dead letter not acknowledged"):
        sink.handle(Msg("telemetry.alarms", b"\x01x"))


class RegistryError(Exception):
    def __init__(self, status):
        super().__init__(f"HTTP {status}")
        self.http_status_code = status


def test_registry_failures_404_is_permanent_others_transient(sink):
    def failing(status):
        def decode(value, topic):
            raise RegistryError(status)
        return decode
    sink.decode = failing(404)
    sink.handle(Msg("telemetry.raw", b"\x00\x00\x00\x00\x05x"))
    assert sink.counts["raw_undecodable_skipped"] == 1
    sink.decode = failing(503)
    with pytest.raises(ts.Transient, match="schema registry"):
        sink.handle(Msg("telemetry.raw", b"\x00\x00\x00\x00\x05x"))


class HttpxConnectError(Exception):
    pass


HttpxConnectError.__module__ = "httpx._exceptions"  # stands in for httpx.ConnectError, without importing httpx


@pytest.mark.parametrize("error, transient", [
    (SerializationError("bad framing"), False),
    (RegistryError(404), False),
    (RegistryError(500), True),
    (RegistryError(503), True),
    (HttpxConnectError("connection refused"), True),
    (ConnectionRefusedError("refused"), True),
    (TimeoutError("timed out"), True),
    (ValueError("bytes is not a 16-char string"), False),  # malformed bytes after a valid header
    (TypeError("unexpected"), False),
])
def test_only_registry_and_network_failures_are_transient(error, transient):
    assert ts.is_transient(error) is transient


def test_a_corrupt_alarm_record_is_dead_lettered_not_retried(sink):
    def decode(value, topic):
        raise ValueError("bytes is not a 16-char string")
    sink.decode = decode
    sink.handle(Msg("telemetry.alarms", b"\x00\x01\x02\x03\x04\x05"))
    assert sink.counts["dead_lettered"] == 1


def test_a_serialization_error_is_permanent(sink):
    def decode(value, topic):
        raise SerializationError("bad bytes")
    sink.decode = decode
    sink.handle(Msg("telemetry.raw", b"x"))
    assert sink.counts["raw_undecodable_skipped"] == 1


class FakeConsumer:
    def __init__(self, messages):
        self.messages, self.stored, self.closed = list(messages), [], False

    def poll(self, timeout):
        return self.messages.pop(0) if self.messages else None

    def store_offsets(self, msg):
        self.stored.append(msg.offset())

    def close(self):
        self.closed = True


def test_run_stores_an_offset_only_after_the_record_is_handled(sink, registry, json_log, monkeypatch):
    monkeypatch.setattr(ts, "BACKOFF_MAX_S", 0.01)
    value = encode(registry, "telemetry.raw", "telemetry_v1.avsc", raw_record())
    sink.gateway = FakeGateway(failures=2)  # ThingsBoard down for two attempts
    sink.consumer = FakeConsumer([Msg("telemetry.raw", value, offset=1), Msg("telemetry.raw", None, error="broker"),
                                  Msg("telemetry.raw", value, offset=2)])
    sink.args.count = 2
    original = threading.Event.wait
    monkeypatch.setattr(threading.Event, "wait", lambda self, t=None: original(self, 0.001))
    assert sink.run(threading.Event()) == "count"
    assert sink.consumer.stored == [1, 2] and len(sink.gateway.sent) == 2
    assert sink.counts["retries"] == 2 and len(events(json_log, "forward_retry")) == 2
    assert events(json_log, "consumer_error")


def test_a_stop_during_retries_leaves_the_offset_unstored(sink, registry):
    sink.gateway = FakeGateway(failures=10)
    sink.consumer = FakeConsumer([Msg("telemetry.raw", encode(registry, "telemetry.raw", "telemetry_v1.avsc", raw_record()))])
    stop = threading.Event()
    stop.wait = lambda t=None: True  # a signal arrives while waiting to retry
    assert sink.run(stop) == "signal"
    assert sink.consumer.stored == []


def test_run_stops_on_signal_and_on_duration(sink):
    sink.consumer = FakeConsumer([])
    stop = threading.Event()
    stop.set()
    assert sink.run(stop) == "signal"
    sink.args.duration = 0.01
    assert sink.run(threading.Event()) == "duration"


def test_shutdown_logs_the_counts(sink, json_log):
    sink.consumer, sink.gateway = FakeConsumer([]), types.SimpleNamespace(stop=lambda: None)
    sink.counts.update({"telemetry_forwarded": 3})
    sink.shutdown("count")
    assert events(json_log, "run_finished")[0]["telemetry_forwarded"] == 3 and sink.consumer.closed
    ts.Sink(ts.parse_args([]), token="t").shutdown("startup_failed")
    assert events(json_log, "shutdown_after_startup_failure")


# --- start-up --------------------------------------------------------------------------------


class FakeKafka:
    def __init__(self, topics=(), error=None):
        self.topics, self.error, self.subscribed = set(topics), error, None

    def __call__(self, conf):
        self.conf = conf
        return self

    def list_topics(self, timeout):
        if self.error:
            raise KafkaException(self.error)
        return types.SimpleNamespace(topics={t: types.SimpleNamespace(error=None) for t in self.topics})

    def subscribe(self, topics):
        self.subscribed = topics


def test_start_kafka(json_log, monkeypatch):
    all_topics = FakeKafka({"telemetry.raw", "telemetry.alarms", "telemetry.dlq"})
    monkeypatch.setattr(ts, "Consumer", all_topics)
    monkeypatch.setattr(ts, "Producer", FakeKafka())
    s = ts.Sink(ts.parse_args([]), token="t")
    assert s.start_kafka() is True
    assert all_topics.subscribed == ["telemetry.raw", "telemetry.alarms"]
    assert all_topics.conf["enable.auto.offset.store"] is False and all_topics.conf["group.id"] == "thingsboard-sink"
    monkeypatch.setattr(ts, "Consumer", FakeKafka({"telemetry.raw"}))
    assert s.start_kafka() is False and events(json_log, "kafka_topic_missing")
    monkeypatch.setattr(ts, "Consumer", FakeKafka(error="down"))
    assert s.start_kafka() is False and events(json_log, "kafka_unreachable")


def test_start_gateway_needs_a_token(json_log, monkeypatch):
    assert ts.Sink(ts.parse_args([]), token="").start_gateway() is False
    assert events(json_log, "settings_missing")[0]["settings"] == ["TB_GATEWAY_TOKEN"]
    monkeypatch.setattr(ts, "Gateway", lambda *a: types.SimpleNamespace(start=lambda: True))
    assert ts.Sink(ts.parse_args([]), token="t").start_gateway() is True


def test_configuration_comes_from_the_environment(monkeypatch):
    for k, v in {"KAFKA_BOOTSTRAP": "broker:29092", "SCHEMA_REGISTRY_URL": "http://sr:8081", "TB_HOST": "thingsboard",
                 "TB_MQTT_PORT": "1883", "THINGSBOARD_SINK_GROUP": "g2", "TB_GATEWAY_TOKEN": "env-token"}.items():
        monkeypatch.setenv(k, v)
    args = ts.parse_args([])
    assert (args.kafka_bootstrap, args.schema_registry_url, args.tb_host, args.tb_mqtt_port, args.group_id) == \
        ("broker:29092", "http://sr:8081", "thingsboard", 1883, "g2")
    assert ts.Sink(args).token == "env-token"


def test_signal_handlers_set_the_stop_event(json_log):
    saved = {n: signal.getsignal(getattr(signal, n)) for n in ("SIGINT", "SIGTERM", "SIGBREAK") if hasattr(signal, n)}
    try:
        stop = threading.Event()
        ts.install_signal_handlers(stop)
        signal.getsignal(signal.SIGINT)(signal.SIGINT, None)
        assert stop.is_set()
    finally:
        for n, h in saved.items():
            signal.signal(getattr(signal, n), h)


# --- rate limit per asset (config/thingsboard.yaml) ------------------------------------------------------------


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def test_the_limiter_lets_one_second_of_messages_through_then_paces_at_the_rate():
    clock = Clock()
    limiter = ts.AssetRateLimiter(10, clock)
    assert limiter.burst == 10  # one second of messages
    assert [limiter.delay("pump-01") for _ in range(10)] == [0.0] * 10
    # A backlog, all at once: each further message waits one more tenth of a second.
    assert [round(limiter.delay("pump-01"), 3) for _ in range(3)] == [0.1, 0.2, 0.3]
    clock.now += 0.3  # the caller waited: the tokens taken in advance are paid back, nothing more
    assert round(limiter.delay("pump-01"), 3) == 0.1
    clock.now += 0.1
    # Steady state after the backlog: exactly the rate, never faster.
    waits = []
    for _ in range(50):
        w = limiter.delay("pump-01")
        waits.append(w)
        clock.now += w
    assert waits[0] == pytest.approx(0.1) and all(w == pytest.approx(0.1) for w in waits)


def test_assets_have_their_own_buckets_and_a_quiet_asset_refills_only_to_the_burst():
    clock = Clock()
    limiter = ts.AssetRateLimiter(10, clock)
    for _ in range(10):
        limiter.delay("pump-01")
    assert limiter.delay("pump-02") == 0.0  # another asset is not slowed by pump-01's backlog
    clock.now += 3600  # an hour quiet: still one second of messages, not an hour's worth
    assert [limiter.delay("pump-01") for _ in range(11)][-1] == pytest.approx(0.1)


def test_live_traffic_never_waits():
    """1 message per second per asset, as the simulator sends: far below the limit, no delay ever."""
    clock = Clock()
    limiter = ts.AssetRateLimiter(10, clock)
    for _ in range(600):
        assert limiter.delay("pump-01") == 0.0
        clock.now += 1.0


def test_a_rate_below_one_keeps_a_burst_of_one():
    clock = Clock()
    limiter = ts.AssetRateLimiter(0.5, clock)
    assert (limiter.burst, limiter.delay("a"), limiter.delay("a")) == (1.0, 0.0, 2.0)
    with pytest.raises(ValueError):
        ts.AssetRateLimiter(0)


def test_the_sink_paces_with_the_configured_rate(sink, registry):
    assert sink.limiter.rate == 10  # config/thingsboard.yaml
    clock, waited = Clock(), []
    sink.limiter = ts.AssetRateLimiter(10, clock)
    sink.wait = lambda t: waited.append(round(t, 3)) or False
    value = encode(registry, "telemetry.raw", "telemetry_v1.avsc", raw_record())
    for _ in range(12):
        sink.handle(Msg("telemetry.raw", value))
    assert waited == [0.1, 0.2] and len(sink.gateway.sent) == 12
    assert (sink.counts["rate_limited"], sink.counts["rate_limited_ms"]) == (2, 300)


def test_a_stop_while_paced_leaves_the_offset_unstored(sink, registry):
    sink.limiter = ts.AssetRateLimiter(1, Clock())
    value = encode(registry, "telemetry.raw", "telemetry_v1.avsc", raw_record())
    sink.consumer = FakeConsumer([Msg("telemetry.raw", value, offset=1), Msg("telemetry.raw", value, offset=2)])
    stop = threading.Event()
    stop.wait = lambda t=None: True  # a signal arrives while waiting for the rate limit
    assert sink.run(stop) == "signal"
    assert sink.consumer.stored == [1] and len(sink.gateway.sent) == 1  # the paced record was not sent nor stored
