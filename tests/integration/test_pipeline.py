"""Integration: provisioning, the end-to-end pipeline, and dead letters against the real stack."""

from __future__ import annotations

import json
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import integration_support as it


def test_stack_provisioned(stack):
    """Topics exist with config/'s partitions and the broker's real settings; registry is BACKWARD."""
    topics = it.admin().list_topics(timeout=10).topics
    assert {t: len(topics[t].partitions) for t in ("telemetry.raw", "telemetry.alarms", "telemetry.dlq")} == \
        {"telemetry.raw": 3, "telemetry.alarms": 3, "telemetry.dlq": 1}
    described = it.describe_topic_configs("telemetry.alarms", "telemetry.dlq")
    assert described["telemetry.alarms"]["cleanup.policy"] == "compact"
    assert described["telemetry.dlq"]["cleanup.policy"] == "delete"
    assert described["telemetry.dlq"]["retention.ms"] == "604800000"
    with urllib.request.urlopen(f"{it.REGISTRY}/config", timeout=10) as r:
        assert json.load(r)["compatibilityLevel"] == "BACKWARD"


def test_pipeline_end_to_end(stack, offsets_mark, log_dir):
    """Simulator -> Mosquitto (QoS 1 acknowledged by the broker) -> bridge -> Kafka, decoded with the registered schema."""
    b = it.bridge(log_dir, "bridge", 50)
    s = it.simulator(log_dir, "simulator", 50, seed=11)
    assert s.finish() == 0 and b.finish() == 0

    sim = s.find("run_finished")[0]
    assert (sim["messages_sent"], sim["messages_acked"]) == (50, 50)
    run = b.find("run_finished")[0]
    assert (run["messages_received"], run["messages_invalid"], run["messages_delivered"]) == (50, 0, 50)

    records = it.read_since("telemetry.raw", offsets_mark["telemetry.raw"])
    assert len(records) == 50
    schema_id = it.latest_schema_id("telemetry.raw-value")
    assert {int.from_bytes(r["raw"][1:5], "big") for r in records} == {schema_id}
    seqs = defaultdict(list)
    for r in records:
        assert r["key"].decode() == r["value"]["asset_id"] and r["value"]["synthetic"] is True
        seqs[r["value"]["asset_id"]].append(r["value"]["seq"])
    assert {a: sorted(v) for a, v in seqs.items()} == {a: list(range(10)) for a in seqs} and len(seqs) == 5

    delivered = b.find("message_delivered")
    assert len(delivered) == 50 and all({"ts_to_receive_ms", "ts_to_produce_ms", "ts_to_ack_ms"} <= d.keys() for d in delivered)
    assert s.non_json_lines() == b.non_json_lines() == 0


def invalid_mqtt_messages() -> list[tuple[str, bytes]]:
    good = {"asset_id": "pump-01", "asset_type": "pump", "seq": 0,
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"), "synthetic": True,
            "bearing_temperature_c": 53.1, "vibration_rms_mm_s": 2.6, "motor_current_a": 25.0,
            "speed_rpm": 2962.0, "injected_fault": None}
    return [
        ("invalid_json", b'{"asset_id": "pump-01", oops'),
        ("invalid_ts", json.dumps({**good, "ts": "yesterday"}).encode()),
        ("schema_mismatch", json.dumps({**good, "seq": "three"}).encode()),
        ("topic_mismatch", json.dumps({**good, "asset_id": "pump-02"}).encode()),
        ("non_finite_value", json.dumps(good).replace('"vibration_rms_mm_s": 2.6', '"vibration_rms_mm_s": NaN').encode()),
    ]


def publish(messages: list[tuple[str, bytes]], topic: str = "plant/pump-01/telemetry") -> None:
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=it.unique("it-pub"))
    client.connect("localhost", 1883)
    client.loop_start()
    for _, payload in messages:
        client.publish(topic, payload, qos=1).wait_for_publish(10)
    client.disconnect()
    client.loop_stop()


NO_SEQ = {"type": "record", "name": "TelemetryWithoutSeq", "namespace": "integration",
          "fields": [{"name": "asset_id", "type": "string"}, {"name": "asset_type", "type": "string"},
                     {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                     {"name": "synthetic", "type": "boolean"}]}


def test_dead_letters_real(stack, offsets_mark, fresh_group, log_dir):
    """Every injectable reason reaches telemetry.dlq as dlq_v1 through the real registry; nothing valid does."""
    consumer = it.alarm_consumer(log_dir, "consumer", fresh_group(), count=2)
    b = it.bridge(log_dir, "bridge", 5)
    mqtt_cases = invalid_mqtt_messages()
    publish(mqtt_cases)
    assert b.finish() == 0

    no_seq_id = it.register("it-missing-field-value", NO_SEQ)
    kafka_cases = [
        ("undecodable", b"\x01not Avro, no Confluent header"),
        ("missing_field", it.framed(no_seq_id, NO_SEQ, {"asset_id": "pump-01", "asset_type": "pump",
                                                        "ts": datetime.now(timezone.utc), "synthetic": True})),
    ]
    for _, payload in kafka_cases:
        it.produce_raw("telemetry.raw", payload)
    assert consumer.finish() == 0

    letters = it.read_since("telemetry.dlq", offsets_mark["telemetry.dlq"])
    expected = {payload: reason for reason, payload in mqtt_cases + kafka_cases}
    assert len(letters) == len(expected) == 7
    for letter in letters:
        v = letter["value"]
        assert expected[v["payload"]] == v["reason"]
        assert letter["key"] == b"pump-01"
        assert v["source"] == ("alarm_consumer" if v["reason"] in ("undecodable", "missing_field") else "bridge")
    assert next(d["value"] for d in letters if d["value"]["reason"] == "missing_field")["schema_id"] == no_seq_id
    # The bridge put nothing on telemetry.raw: the only new records there are the two injected ones.
    assert len(it.read_since("telemetry.raw", offsets_mark["telemetry.raw"], deserialize=False)) == 2
    assert b.non_json_lines() == consumer.non_json_lines() == 0


def test_provision_is_idempotent(stack):
    result = it.provision()
    events = [e["event"] for e in it.json_events(result.stderr)]
    assert events.count("topic_exists") == 3 and "topic_created" not in events
