"""Kafka smoke check: list every alarm event on telemetry.alarms, in offset order.

    python scripts/smoke/alarms_dump.py

Reads the topic from the beginning with a fresh consumer group, decodes each
record through Schema Registry and prints one line per event: partition,
offset, key, state, severity, value, threshold, source_seq. It then checks
that every key equals its record's alarm_id and summarises, per alarm_id,
the latest state, which is what the compacted topic keeps.

Exit status: 0 when every record decoded and every key matched, 1 otherwise.
"""

import argparse
import os
import sys
import time
import uuid
import warnings
from collections import Counter
from pathlib import Path

from confluent_kafka import Consumer

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.loader import load_config  # noqa: E402

# Importing the Schema Registry client raises a third-party deprecation
# warning that ignores warning filters; warning display is off for that import.
_showwarning, warnings.showwarning = warnings.showwarning, lambda *args, **kwargs: None
from confluent_kafka.schema_registry import SchemaRegistryClient  # noqa: E402
from confluent_kafka.schema_registry.avro import AvroDeserializer  # noqa: E402
from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: E402
warnings.showwarning = _showwarning


def main() -> int:
    p = argparse.ArgumentParser(description="List alarm events on telemetry.alarms.")
    p.add_argument("--idle-timeout", type=float, default=10.0, help="stop after this many seconds without a record")
    p.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--schema-registry-url", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    args = p.parse_args()

    topic = load_config().topics.kafka["telemetry_alarms"].name
    deserialize = AvroDeserializer(SchemaRegistryClient({"url": args.schema_registry_url}))
    context = SerializationContext(topic, MessageField.VALUE)
    consumer = Consumer({"bootstrap.servers": args.kafka_bootstrap, "group.id": f"smoke-alarms-{uuid.uuid4()}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([topic])

    events, problems = [], []
    last = time.monotonic()
    try:
        while time.monotonic() - last < args.idle_timeout:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                problems.append(f"consumer error: {msg.error()}")
                continue
            last = time.monotonic()
            key = msg.key().decode("utf-8") if msg.key() else None
            try:
                ev = deserialize(msg.value(), context)
            except Exception as e:  # report every decoding failure, whatever its type
                problems.append(f"{msg.partition()}:{msg.offset()}: cannot decode ({e})")
                continue
            if key != ev["alarm_id"]:
                problems.append(f"{msg.partition()}:{msg.offset()}: key {key!r} != alarm_id {ev['alarm_id']!r}")
            events.append((msg.partition(), msg.offset(), ev))
    finally:
        consumer.close()

    events.sort(key=lambda e: (e[2]["ts"], e[0], e[1]))
    print(f"topic: {topic}, events: {len(events)}")
    for partition, offset, ev in events:
        print(f"  p{partition}@{offset:<3} {ev['state']:<7} {ev['severity']:<8} value={ev['value']:<8.3f} "
              f"threshold={ev['threshold']:<6} seq={ev['source_seq']:<4} {ev['alarm_id']}")
    latest = {}
    for _, _, ev in events:
        latest[ev["alarm_id"]] = ev["state"]
    print(f"events by state: {dict(Counter(ev['state'] for _, _, ev in events))}")
    print(f"alarms: {len(latest)}, latest state: {dict(Counter(latest.values()))}")
    for problem in problems:
        print(f"PROBLEM: {problem}")
    print("result:", "PASS" if not problems else "FAIL")
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
