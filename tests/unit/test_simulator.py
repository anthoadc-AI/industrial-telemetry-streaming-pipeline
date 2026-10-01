"""edge/simulator/__main__.py: the MQTT Publisher (acknowledgement counting and flush) and the CLI helpers.

The paho client is replaced by a fake; the run loop itself is in main(),
which is exercised by the integration tests.
"""

from __future__ import annotations

import argparse
import re
import signal
import threading
import types

import pytest

import edge.simulator.__main__ as sim
from edge.simulator.__main__ import Publisher

OK = types.SimpleNamespace(is_failure=False)
FAILED = types.SimpleNamespace(is_failure=True, __str__=lambda self: "not authorised")


class FakeMqtt:
    """paho Client stand-in. Publishes are acknowledged by calling ack()."""

    def __init__(self, connect_error=None, connect_rc=OK, publish_rc=0):
        self.connect_error, self.connect_rc, self.publish_rc = connect_error, connect_rc, publish_rc
        self.published, self.disconnected, self.stopped = [], False, False

    def __call__(self, callback_api_version, client_id):
        return self

    def connect(self, host, port, keepalive):
        if self.connect_error:
            raise self.connect_error

    def loop_start(self):
        self.on_connect(self, None, None, self.connect_rc, None)

    def loop_stop(self):
        self.stopped = True

    def disconnect(self):
        self.disconnected = True

    def publish(self, topic, payload, qos):
        self.published.append((topic, payload, qos))
        return types.SimpleNamespace(rc=self.publish_rc, mid=len(self.published))

    def ack(self, n=None):
        for mid in range(n or len(self.published)):
            self.on_publish(self, None, mid, OK, None)


@pytest.fixture
def fake(monkeypatch):
    client = FakeMqtt()
    monkeypatch.setattr(sim.mqtt, "Client", client)
    return client


def test_publishes_count_and_acknowledgements(fake, json_log):
    p = Publisher("localhost", 1883, "test")
    assert p.start() is True
    for i in range(3):
        p.publish("plant/pump-01/telemetry", f'{{"seq": {i}}}', 1)
    fake.ack(3)
    assert (p.sent, p.acked) == (3, 3)
    assert p.flush_and_close(1.0) is True
    assert fake.disconnected and fake.stopped
    assert [e["event"] for e in json_log()] == ["mqtt_connected"]


def test_flush_reports_unacknowledged_messages(fake):
    p = Publisher("localhost", 1883, "test")
    p.start()
    p.publish("t", "a", 1)
    p.publish("t", "b", 1)
    fake.ack(1)
    assert p.flush_and_close(0.1) is False
    assert (p.sent, p.acked) == (2, 1)


def test_publish_while_disconnected_is_queued_and_counted(monkeypatch):
    client = FakeMqtt(publish_rc=sim.mqtt.MQTT_ERR_NO_CONN)
    monkeypatch.setattr(sim.mqtt, "Client", client)
    p = Publisher("localhost", 1883, "test")
    p.start()
    p.publish("t", "a", 1)
    assert p.sent == 1


def test_publish_error_is_logged_and_not_counted(monkeypatch, json_log):
    client = FakeMqtt(publish_rc=sim.mqtt.MQTT_ERR_PROTOCOL)
    monkeypatch.setattr(sim.mqtt, "Client", client)
    p = Publisher("localhost", 1883, "test")
    p.start()
    p.publish("t", "a", 1)
    assert p.sent == 0
    assert [e for e in json_log() if e["event"] == "mqtt_publish_failed"][0]["rc"] == sim.mqtt.MQTT_ERR_PROTOCOL


@pytest.mark.parametrize("client, event", [
    (FakeMqtt(connect_error=OSError("refused")), "mqtt_connect_failed"),
    (FakeMqtt(connect_rc=FAILED), "mqtt_connect_timeout"),
])
def test_start_failures(monkeypatch, json_log, client, event):
    monkeypatch.setattr(sim.mqtt, "Client", client)
    monkeypatch.setattr(sim, "CONNECT_TIMEOUT_S", 0.05)
    assert Publisher("localhost", 1883, "test").start() is False
    assert event in [e["event"] for e in json_log()]


def test_refused_connection_and_unexpected_disconnect_are_logged(fake, json_log):
    p = Publisher("localhost", 1883, "test")
    p._on_connect(None, None, None, FAILED, None)
    p._on_disconnect(None, None, None, FAILED, None)
    p._on_disconnect(None, None, None, OK, None)
    events = [e["event"] for e in json_log()]
    assert events == ["mqtt_connect_refused", "mqtt_disconnected"]
    assert not p.connected.is_set()


def test_utc_timestamps_are_iso_8601_with_milliseconds():
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", sim.utc_now_iso())


def test_fault_argument():
    assert sim._fault("pump-01:imbalance:30") == ("pump-01", "imbalance", 30.0)
    for bad in ("pump-01:imbalance", "pump-01:imbalance:soon"):
        with pytest.raises(argparse.ArgumentTypeError):
            sim._fault(bad)


def test_numeric_arguments():
    assert sim._positive_int("5") == 5 and sim._positive_float("0.5") == 0.5
    for fn, text in ((sim._positive_int, "0"), (sim._positive_float, "0")):
        with pytest.raises(argparse.ArgumentTypeError):
            fn(text)


def test_parse_args(monkeypatch):
    monkeypatch.setenv("MQTT_HOST", "broker.local")
    args = sim.parse_args(["--count", "10", "--fault", "pump-01:imbalance:5", "--fault", "compressor-01:imbalance:5"])
    assert (args.count, args.mqtt_host, len(args.fault)) == (10, "broker.local", 2)
    with pytest.raises(SystemExit):
        sim.parse_args(["--count", "-1"])


def test_signal_handlers_set_the_stop_event(json_log):
    saved = {n: signal.getsignal(getattr(signal, n)) for n in ("SIGINT", "SIGTERM", "SIGBREAK") if hasattr(signal, n)}
    try:
        stop = threading.Event()
        sim.install_signal_handlers(stop)
        signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
        assert stop.is_set()
    finally:
        for name, handler in saved.items():
            signal.signal(getattr(signal, name), handler)
