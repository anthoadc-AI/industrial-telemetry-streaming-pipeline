"""bridge/mqtt_to_kafka/validation.py: every acceptance rule and every rejection reason."""

from __future__ import annotations

import json
from datetime import timezone

import pytest

from bridge.mqtt_to_kafka.validation import InvalidMessage, TelemetryValidator, TopicPattern
from conftest import telemetry

TOPIC = "plant/pump-01/telemetry"


@pytest.fixture
def validator(cfg):
    return TelemetryValidator(cfg.topics.telemetry_raw.value_schema, cfg.topics.mqtt_telemetry.template)


def reject(validator, payload, topic=TOPIC):
    with pytest.raises(InvalidMessage) as info:
        validator.validate(topic, payload if isinstance(payload, bytes) else json.dumps(payload).encode())
    return info.value


def test_valid_message_is_accepted_and_ts_becomes_aware_utc(validator):
    record = validator.validate(TOPIC, json.dumps(telemetry()).encode())
    assert record["asset_id"] == "pump-01"
    assert record["ts"].tzinfo == timezone.utc
    assert record["ts"].microsecond == 87000


def test_ts_with_offset_is_converted_to_utc(validator):
    record = validator.validate(TOPIC, json.dumps(telemetry(ts="2026-10-01T21:50:15.087-02:00")).encode())
    assert record["ts"].hour == 23


def test_nullable_channels_accept_null(validator):
    validator.validate(TOPIC, json.dumps(telemetry(speed_rpm=None, injected_fault="imbalance")).encode())


@pytest.mark.parametrize("payload, reason", [
    (b'{"asset_id": "pump-01", oops', "invalid_json"),
    (b"\xff\xfe not utf-8", "invalid_json"),
    (b"[1, 2]", "invalid_json"),
    (b'"just a string"', "invalid_json"),
])
def test_payloads_that_are_not_a_json_object(validator, payload, reason):
    assert reject(validator, payload).reason == reason


@pytest.mark.parametrize("ts, detail", [
    (None, "ts must be an ISO 8601 string"),
    (1790889302206, "ts must be an ISO 8601 string"),
    ("yesterday", "Invalid isoformat string"),
    ("2026-10-01T23:50:15", "ts has no UTC offset"),
])
def test_bad_timestamps(validator, ts, detail):
    record = telemetry()
    if ts is None:
        del record["ts"]
    else:
        record["ts"] = ts
    error = reject(validator, record)
    assert error.reason == "invalid_ts"
    assert detail in error.detail


def test_unknown_field(validator):
    error = reject(validator, telemetry(oil_pressure_bar=2.0))
    assert (error.reason, error.detail) == ("schema_mismatch", "unknown field(s) ['oil_pressure_bar']")


def test_wrong_type(validator):
    error = reject(validator, telemetry(seq="three"))
    assert error.reason == "schema_mismatch"
    assert "seq" in error.detail and "expected long" in error.detail


def test_missing_required_field(validator):
    record = telemetry()
    del record["asset_type"]
    error = reject(validator, record)
    assert error.reason == "schema_mismatch"
    assert "asset_type" in error.detail


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_values(validator, literal):
    raw = json.dumps(telemetry()).replace('"vibration_rms_mm_s": 2.6', f'"vibration_rms_mm_s": {literal}').encode()
    error = reject(validator, raw)
    assert (error.reason, error.detail) == ("non_finite_value", "NaN or infinite value in field(s) ['vibration_rms_mm_s']")


def test_topic_must_match_asset(validator):
    error = reject(validator, telemetry("pump-02"))
    assert error.reason == "topic_mismatch"
    assert "pump-02" in error.detail


def test_foreign_topic_is_a_topic_mismatch(validator):
    assert reject(validator, telemetry(), topic="other/pump-01/x").reason == "topic_mismatch"


def test_invalid_message_carries_reason_and_detail():
    error = InvalidMessage("invalid_json", "boom")
    assert str(error) == "invalid_json: boom"


@pytest.mark.parametrize("topic, expected", [
    ("plant/pump-01/telemetry", "pump-01"),
    ("plant/compressor-01/telemetry", "compressor-01"),
    ("plant//telemetry", None),
    ("plant/a/b/telemetry", None),
    ("plant/pump-01/status", None),
    ("site/pump-01/telemetry", None),
])
def test_topic_pattern_extracts_the_asset(topic, expected):
    assert TopicPattern("plant/{asset_id}/telemetry").asset_id(topic) == expected
