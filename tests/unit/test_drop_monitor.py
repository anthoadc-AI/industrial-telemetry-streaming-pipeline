"""bridge/mqtt_to_kafka/drop_monitor.py: Mosquitto's dropped counter turned into log events (DETENTE 3.3.h).

The MQTT client is faked; the $SYS behaviour it stands for was measured on Mosquitto 2.1.2 (values retained, a counter
published again only when it changes, uptime every sys_interval).
"""

from __future__ import annotations

import types

import pytest

import bridge.mqtt_to_kafka.drop_monitor as dm
from bridge.mqtt_to_kafka.drop_monitor import DROPPED_TOPIC, SILENT_AFTER_S, UPTIME_TOPIC, DropMonitor

OK = types.SimpleNamespace(is_failure=False)
FAILED = types.SimpleNamespace(is_failure=True, __str__=lambda self: "refused")


def events(lines, name):
    return [line for line in lines() if line["event"] == name]


@pytest.fixture
def monitor(json_log):
    return DropMonitor("mosquitto", 1883, "bridge-1-drops", started=100.0)


def test_the_first_value_is_the_baseline_and_increases_are_errors_with_their_delta(monitor, json_log):
    monitor.on_value(DROPPED_TOPIC, b"40", 101.0)  # retained: drops before the bridge started
    assert events(json_log, "mqtt_drop_monitor_baseline")[0]["total"] == 40
    assert not events(json_log, "mqtt_messages_dropped")
    monitor.on_value(DROPPED_TOPIC, b"40", 111.0)
    monitor.on_value(DROPPED_TOPIC, b"434", 121.0)
    monitor.on_value(DROPPED_TOPIC, b"500", 131.0)
    dropped = events(json_log, "mqtt_messages_dropped")
    assert [(e["delta"], e["total"], e["level"]) for e in dropped] == [(394, 434, "ERROR"), (66, 500, "ERROR")]
    assert dropped[0]["scope"] == "broker-wide" and dropped[0]["broker"] == "mosquitto:1883"
    assert dropped[0]["sys_interval_s"] == 10
    assert monitor.dropped_during_run == 460


def test_a_lower_value_is_a_broker_restart_and_the_new_baseline(monitor, json_log):
    monitor.on_value(DROPPED_TOPIC, b"500", 101.0)
    monitor.on_value(DROPPED_TOPIC, b"3", 111.0)
    monitor.on_value(DROPPED_TOPIC, b"10", 121.0)
    [reset] = events(json_log, "mosquitto_drop_counter_reset")
    assert (reset["previous"], reset["total"]) == (500, 3)
    assert [e["delta"] for e in events(json_log, "mqtt_messages_dropped")] == [7]
    assert monitor.dropped_during_run == 7


def test_an_invalid_value_is_a_warning_and_changes_nothing(monitor, json_log):
    monitor.on_value(DROPPED_TOPIC, b"12", 101.0)
    monitor.on_value(DROPPED_TOPIC, b"twelve", 111.0)
    monitor.on_value(DROPPED_TOPIC, b"\xff", 112.0)
    assert len(events(json_log, "mosquitto_drop_counter_invalid")) == 2
    assert monitor.total == 12 and monitor.dropped_during_run == 0


def test_silence_of_sys_is_reported_once_and_its_end_too(monitor, json_log):
    monitor.check_silence(100.0 + SILENT_AFTER_S - 1)
    assert not events(json_log, "mosquitto_sys_silent")
    monitor.check_silence(100.0 + SILENT_AFTER_S)
    monitor.check_silence(100.0 + SILENT_AFTER_S + 5)
    [silent] = events(json_log, "mosquitto_sys_silent")
    assert silent["level"] == "WARNING" and silent["seconds_without_update"] == SILENT_AFTER_S
    monitor.on_value(UPTIME_TOPIC, b"40 seconds", 130.0)
    assert len(events(json_log, "mosquitto_sys_resumed")) == 1
    monitor.check_silence(131.0)
    assert len(events(json_log, "mosquitto_sys_silent")) == 1
    monitor.on_value(UPTIME_TOPIC, b"50 seconds", 140.0)
    assert len(events(json_log, "mosquitto_sys_resumed")) == 1  # only after a silence


class FakeMqtt:
    """paho Client stand-in: loop_start() fires on_connect, subscribe() fires on_subscribe."""

    def __init__(self, connect_error=None, connect_rc=OK, subscribe_rc=OK):
        self.connect_error, self.connect_rc, self.subscribe_rc = connect_error, connect_rc, subscribe_rc
        self.subscriptions, self.calls = [], []

    def __call__(self, callback_api_version, client_id):
        self.client_id = client_id
        return self

    def connect(self, host, port, keepalive):
        if self.connect_error:
            raise self.connect_error

    def loop_start(self):
        self.on_connect(self, None, None, self.connect_rc, None)

    def subscribe(self, topics):
        self.subscriptions.append(topics)
        self.on_subscribe(self, None, 1, [self.subscribe_rc] * len(topics), None)

    def disconnect(self):
        self.calls.append("disconnect")

    def loop_stop(self):
        self.calls.append("loop_stop")


def test_start_subscribes_to_the_counter_and_the_uptime_on_a_connection_of_its_own(monitor, json_log, monkeypatch):
    fake = FakeMqtt()
    monkeypatch.setattr(dm.mqtt, "Client", fake)
    assert monitor.start(0.05) is True
    assert fake.client_id == "bridge-1-drops"
    assert fake.subscriptions == [[(DROPPED_TOPIC, 0), (UPTIME_TOPIC, 0)]]
    assert events(json_log, "drop_monitor_subscribed")
    fake.on_message(fake, None, types.SimpleNamespace(topic=DROPPED_TOPIC, payload=b"7"))
    assert monitor.total == 7
    monitor.stop()
    monitor.stop()  # a second stop does nothing
    assert fake.calls == ["disconnect", "loop_stop"]


@pytest.mark.parametrize("fake, event", [
    (FakeMqtt(connect_error=OSError("refused")), "drop_monitor_connect_failed"),
    (FakeMqtt(connect_rc=FAILED), "drop_monitor_connect_refused"),
    (FakeMqtt(subscribe_rc=FAILED), "drop_monitor_subscribe_refused"),
])
def test_start_failures(monitor, json_log, monkeypatch, fake, event):
    monkeypatch.setattr(dm.mqtt, "Client", fake)
    assert monitor.start(0.05) is False
    assert events(json_log, event)
    if event != "drop_monitor_connect_failed":
        assert events(json_log, "drop_monitor_subscribe_timeout")
