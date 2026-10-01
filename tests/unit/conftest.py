"""Shared test doubles for the Python unit tests.

Nothing here talks to a network: Kafka producers, Schema Registry and MQTT are
replaced by small in-memory fakes, and the Avro serialisation uses
confluent-kafka's real serializers against its in-process mock registry.
"""

from __future__ import annotations

import io
import json
import logging
from pathlib import Path

import pytest

from common import jsonlog
from config.loader import load_config

REPO_ROOT = Path(__file__).resolve().parents[2]


class FakeMessage:
    def __init__(self, topic, partition, offset, key, value):
        self._t, self._p, self._o, self._k, self._v = topic, partition, offset, key, value

    def topic(self):
        return self._t

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def key(self):
        return self._k

    def value(self):
        return self._v


class FakeProducer:
    """confluent_kafka.Producer stand-in: records produce() calls; poll()/flush() deliver.

    fail_topics maps a topic to the error string reported for every record on
    it; buffer_errors makes the first N produce() calls raise BufferError.
    """

    def __init__(self, fail_topics=None, buffer_errors=0, flush_remaining=0):
        self.records = []
        self.pending = []
        self.fail_topics = fail_topics or {}
        self.buffer_errors = buffer_errors
        self.flush_remaining = flush_remaining
        self.offsets = {}

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self.buffer_errors:
            self.buffer_errors -= 1
            raise BufferError("local queue full")
        offset = self.offsets.get(topic, 0)
        self.offsets[topic] = offset + 1
        self.records.append({"topic": topic, "key": key, "value": value})
        self.pending.append((on_delivery, FakeMessage(topic, 0, offset, key, value)))

    def poll(self, timeout=0):
        delivered, self.pending = self.pending, []
        for callback, msg in delivered:
            if callback:
                callback(self.fail_topics.get(msg.topic()), msg)
        return len(delivered)

    def flush(self, timeout=0):
        self.poll()
        return self.flush_remaining

    def on(self, topic):
        return [r for r in self.records if r["topic"] == topic]


@pytest.fixture
def cfg():
    return load_config(env={})


@pytest.fixture
def json_log():
    """Route every log record into a StringIO as JSON lines; yields a reader of parsed lines."""
    buffer = io.StringIO()
    root = logging.getLogger()
    saved = list(root.handlers), root.level
    jsonlog.configure("DEBUG", stream=buffer)

    def lines():
        return [json.loads(line) for line in buffer.getvalue().splitlines()]

    yield lines
    root.handlers[:] = saved[0]
    root.setLevel(saved[1])
    logging.captureWarnings(False)


def telemetry(asset="pump-01", **overrides):
    record = {"asset_id": asset, "asset_type": "pump", "seq": 3, "ts": "2026-10-01T23:50:15.087Z", "synthetic": True,
              "bearing_temperature_c": 53.1, "vibration_rms_mm_s": 2.6, "motor_current_a": 25.0, "speed_rpm": 2962.0,
              "injected_fault": None}
    record.update(overrides)
    return record
