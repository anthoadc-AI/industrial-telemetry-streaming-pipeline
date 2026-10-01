"""Kafka smoke check: list every dead letter on telemetry.dlq.

    python scripts/smoke/dlq_dump.py [--out dlq.jsonl]

Reads the topic from the beginning with a fresh consumer group, decodes each
record (dlq_v1) through Schema Registry and prints one line per record: offset,
reason, source, key, source coordinates, schema id, and the payload in base64,
the only place outside the logs where the payload appears as text. With --out
it also writes each record as a JSON line (payload and source_key in base64)
for comparison with the original messages.

Exit status: 0 when every record decoded, 1 otherwise.
"""

import argparse
import base64
import json
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


def b64(value: bytes | None) -> str | None:
    return None if value is None else base64.b64encode(value).decode("ascii")


def main() -> int:
    p = argparse.ArgumentParser(description="List dead letters on telemetry.dlq.")
    p.add_argument("--idle-timeout", type=float, default=10.0, help="stop after this many seconds without a record")
    p.add_argument("--out", type=Path, help="also write every record as a JSON line to this file")
    p.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--schema-registry-url", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    args = p.parse_args()

    topic = load_config().topics.kafka["telemetry_dlq"].name
    deserialize = AvroDeserializer(SchemaRegistryClient({"url": args.schema_registry_url}))
    context = SerializationContext(topic, MessageField.VALUE)
    consumer = Consumer({"bootstrap.servers": args.kafka_bootstrap, "group.id": f"smoke-dlq-{uuid.uuid4()}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([topic])

    records, problems = [], []
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
            try:
                rec = deserialize(msg.value(), context)
            except Exception as e:  # report every decoding failure, whatever its type
                problems.append(f"offset {msg.offset()}: cannot decode ({e})")
                continue
            rec["_offset"] = msg.offset()
            rec["_key"] = msg.key().decode("utf-8") if msg.key() else None
            records.append(rec)
    finally:
        consumer.close()

    records.sort(key=lambda r: r["_offset"])
    print(f"topic: {topic}, records: {len(records)}")
    for r in records:
        coords = r["source_topic"] + (f"[{r['source_partition']}]@{r['source_offset']}" if r["source_offset"] is not None else "")
        print(f"  @{r['_offset']:<3} {r['reason']:<17} {r['source']:<15} key={r['_key']!s:<14} {coords} "
              f"schema_id={r['schema_id']} payload_b64={b64(r['payload'])}")
        print(f"       detail: {r['detail'][:160]}")
    print(f"by reason: {dict(Counter(r['reason'] for r in records))}")
    print(f"by source: {dict(Counter(r['source'] for r in records))}")
    if args.out:
        with args.out.open("w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps({**r, "payload": b64(r["payload"]), "source_key": b64(r["source_key"]),
                                    "ts": r["ts"].isoformat()}, ensure_ascii=False) + "\n")
    for problem in problems:
        print(f"PROBLEM: {problem}")
    print("result:", "PASS" if not problems else "FAIL")
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
