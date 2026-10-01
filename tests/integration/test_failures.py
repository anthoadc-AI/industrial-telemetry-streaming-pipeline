"""Integration: failure paths that need a real broker or registry.

- kafkajs leaves a throwing handler's offset uncommitted (dead-letter send to
  a topic that no longer exists);
- the bridge logs a failed dead-letter delivery with the payload and exits 1;
- with Schema Registry stopped, the consumer stops without committing and
  without touching the DLQ, then processes the message after the restart.
"""

from __future__ import annotations

import base64
import time
import urllib.request
from datetime import datetime, timezone

import integration_support as it
from test_pipeline import publish


def advanced(before: dict[int, int], after: dict[int, int]) -> int:
    return sum(after[p] - max(before[p], 0) for p in after)


def test_offset_not_committed_when_handler_throws(stack, fresh_group, log_dir, request):
    request.addfinalizer(it.provision)  # recreate telemetry.dlq
    group = fresh_group()
    before = it.committed(group, "telemetry.raw")
    consumer = it.alarm_consumer(log_dir, "consumer", group, count=1, duration=120)
    it.delete_topic("telemetry.dlq")  # after start-up, which checks the topic exists
    it.produce_raw("telemetry.raw", b"\x01undecodable")

    assert consumer.finish(120) == 1
    assert consumer.find("consumer_crashed") and consumer.find("run_failed")[0]["messages_dead_lettered"] == 0
    assert it.committed(group, "telemetry.raw") == before  # the throwing handler's offset was not committed

    it.provision()
    retry = it.alarm_consumer(log_dir, "consumer-retry", group, count=1, duration=120)
    assert retry.finish(120) == 0
    assert advanced(before, it.committed(group, "telemetry.raw")) == 1
    [letter] = it.read_since("telemetry.dlq", {})  # recreated topic: read from 0
    assert letter["value"]["reason"] == "undecodable" and letter["value"]["payload"] == b"\x01undecodable"
    assert consumer.non_json_lines() == retry.non_json_lines() == 0


def test_bridge_dead_letter_send_failure_real(stack, log_dir, request):
    request.addfinalizer(it.provision)
    # librdkafka fails a record for a missing topic once metadata confirms it
    # (topic.metadata.propagation.max.ms, 30 s); the flush timeout outlasts that.
    b = it.bridge(log_dir, "bridge", 1, flush_timeout=45)
    it.delete_topic("telemetry.dlq")
    payload = b'{"asset_id": "pump-01", broken'
    publish([("invalid_json", payload)])

    assert b.finish(120) == 1
    [failed] = b.find("dlq_delivery_failed")
    assert failed["level"] == "ERROR" and base64.b64decode(failed["payload_b64"]) == payload
    assert b.find("run_finished_incomplete")[0]["dead_letters_failed"] == 1
    assert b.non_json_lines() == 0


def registry_up(timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{it.REGISTRY}/subjects", timeout=3):
                return
        except OSError:
            time.sleep(1)
    raise AssertionError("Schema Registry did not come back")


def test_schema_registry_down(stack, offsets_mark, fresh_group, log_dir, request):
    def restore_registry():
        it.compose("start", "schema-registry")
        registry_up()
    request.addfinalizer(restore_registry)

    schema = it.telemetry_schema()
    schema_id = it.register("telemetry.raw-value", schema)
    group = fresh_group()
    before = it.committed(group, "telemetry.raw")
    consumer = it.alarm_consumer(log_dir, "consumer", group, count=1, duration=120)  # has not decoded any telemetry yet

    assert it.compose("stop", "schema-registry").returncode == 0
    record = {"asset_id": "pump-01", "asset_type": "pump", "seq": 0, "ts": datetime.now(timezone.utc), "synthetic": True,
              "bearing_temperature_c": 53.0, "vibration_rms_mm_s": 2.6, "motor_current_a": 25.0, "speed_rpm": 2962.0,
              "injected_fault": None}
    it.produce_raw("telemetry.raw", it.framed(schema_id, schema, record))  # framed locally, no registry call

    assert consumer.finish(120) == 1
    [unavailable] = consumer.find("schema_registry_unavailable")
    assert "status" in unavailable["error"]
    assert it.committed(group, "telemetry.raw") == before
    assert it.watermarks("telemetry.dlq") == offsets_mark["telemetry.dlq"]

    restore_registry()
    retry = it.alarm_consumer(log_dir, "consumer-retry", group, count=1, duration=120)
    assert retry.finish(120) == 0
    assert advanced(before, it.committed(group, "telemetry.raw")) == 1
    assert it.watermarks("telemetry.dlq") == offsets_mark["telemetry.dlq"]
    assert consumer.non_json_lines() == retry.non_json_lines() == 0
