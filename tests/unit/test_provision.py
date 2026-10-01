"""broker/kafka/provision.py: topic creation and verification, and the Schema Registry compatibility level.

AdminClient is replaced by a fake returning concurrent futures; urllib is
replaced for the Schema Registry calls.
"""

from __future__ import annotations

import io
import json
import types
import urllib.error
from concurrent.futures import Future

import pytest
from confluent_kafka import KafkaException

import broker.kafka.provision as provision


def done(value=None, error=None):
    f = Future()
    if error:
        f.set_exception(error)
    else:
        f.set_result(value)
    return f


class FakeAdmin:
    def __init__(self, existing=None, configs=None, unreachable=False, create_error=None, describe_error=None):
        self.existing = existing or {}
        self.configs = configs or {}
        self.unreachable, self.create_error, self.describe_error = unreachable, create_error, describe_error
        self.created = []

    def __call__(self, conf):
        return self

    def list_topics(self, timeout):
        if self.unreachable:
            raise KafkaException("transport failure")
        return types.SimpleNamespace(topics={n: types.SimpleNamespace(partitions=dict.fromkeys(range(p))) for n, p in self.existing.items()})

    def create_topics(self, new_topics, operation_timeout):
        self.created.extend(new_topics)
        return {t.topic: done(error=KafkaException(self.create_error) if self.create_error else None) for t in new_topics}

    def describe_configs(self, resources):
        r = resources[0]
        if self.describe_error:
            return {r: done(error=KafkaException(self.describe_error))}
        entries = {k: types.SimpleNamespace(value=v) for k, v in self.configs.get(r.name, {}).items()}
        return {r: done(entries)}


def run(monkeypatch, admin):
    monkeypatch.setattr(provision, "AdminClient", admin)
    return provision.provision_topics("localhost:9092")


def events(json_log, name):
    return [e for e in json_log() if e["event"] == name]


def test_creates_missing_topics_with_their_settings(monkeypatch, json_log):
    admin = FakeAdmin()
    assert run(monkeypatch, admin) is True
    created = {t.topic: t for t in admin.created}
    assert set(created) == {"telemetry.raw", "telemetry.alarms", "telemetry.dlq"}
    assert created["telemetry.alarms"].num_partitions == 3
    assert created["telemetry.alarms"].config == {"cleanup.policy": "compact"}
    assert created["telemetry.dlq"].config == {"cleanup.policy": "delete", "retention.ms": "604800000"}
    assert len(events(json_log, "topic_created")) == 3


def test_existing_topics_that_match_are_left_alone(monkeypatch, json_log):
    admin = FakeAdmin(existing={"telemetry.raw": 3, "telemetry.alarms": 3, "telemetry.dlq": 1},
                      configs={"telemetry.alarms": {"cleanup.policy": "compact"},
                               "telemetry.dlq": {"cleanup.policy": "delete", "retention.ms": "604800000"}})
    assert run(monkeypatch, admin) is True
    assert admin.created == [] and len(events(json_log, "topic_exists")) == 3


def test_partition_mismatch_is_reported_not_altered(monkeypatch, json_log):
    admin = FakeAdmin(existing={"telemetry.raw": 1, "telemetry.alarms": 3, "telemetry.dlq": 1},
                      configs={"telemetry.alarms": {"cleanup.policy": "compact"},
                               "telemetry.dlq": {"cleanup.policy": "delete", "retention.ms": "604800000"}})
    assert run(monkeypatch, admin) is False
    [mismatch] = events(json_log, "topic_partition_mismatch")
    assert (mismatch["topic"], mismatch["partitions"], mismatch["configured_partitions"], mismatch["altered"]) == ("telemetry.raw", 1, 3, False)


def test_setting_mismatch_is_reported_not_altered(monkeypatch, json_log):
    admin = FakeAdmin(existing={"telemetry.raw": 3, "telemetry.alarms": 3, "telemetry.dlq": 1},
                      configs={"telemetry.alarms": {"cleanup.policy": "delete"},
                               "telemetry.dlq": {"cleanup.policy": "delete"}})
    assert run(monkeypatch, admin) is False
    mismatches = {e["topic"]: e["actual"] for e in events(json_log, "topic_config_mismatch")}
    assert mismatches == {"telemetry.alarms": {"cleanup.policy": "delete"}, "telemetry.dlq": {"retention.ms": None}}


def test_unreadable_settings_fail(monkeypatch, json_log):
    admin = FakeAdmin(existing={"telemetry.raw": 3, "telemetry.alarms": 3, "telemetry.dlq": 1}, describe_error="denied")
    assert run(monkeypatch, admin) is False
    assert events(json_log, "topic_config_unreadable")


def test_creation_failure_and_unreachable_kafka(monkeypatch, json_log):
    assert run(monkeypatch, FakeAdmin(create_error="policy violation")) is False
    assert len(events(json_log, "topic_create_failed")) == 3
    assert run(monkeypatch, FakeAdmin(unreachable=True)) is False
    assert events(json_log, "kafka_unreachable")


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def registry(monkeypatch, level=None, error=None):
    calls = []

    def urlopen(request, timeout):
        calls.append(getattr(request, "method", "GET") if not isinstance(request, str) else "GET")
        if error:
            raise error
        return FakeResponse(json.dumps({"compatibilityLevel": level}).encode())
    monkeypatch.setattr(provision.urllib.request, "urlopen", urlopen)
    return calls


def test_compatibility_is_set_and_read_back(monkeypatch, json_log):
    calls = registry(monkeypatch, level="BACKWARD")
    assert provision.set_compatibility("http://localhost:8081/") is True
    assert calls == ["PUT", "GET"]
    assert events(json_log, "schema_registry_compatibility")[0]["compatibility"] == "BACKWARD"


@pytest.mark.parametrize("level, error, event", [
    ("FULL", None, "schema_registry_compatibility_mismatch"),
    (None, urllib.error.URLError("refused"), "schema_registry_unreachable"),
])
def test_compatibility_failures(monkeypatch, json_log, level, error, event):
    registry(monkeypatch, level=level, error=error)
    assert provision.set_compatibility("http://localhost:8081") is False
    assert events(json_log, event)
