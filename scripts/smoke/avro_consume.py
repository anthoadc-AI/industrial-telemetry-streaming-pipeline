"""Kafka smoke check: read telemetry.raw from the start and verify every record.

    python scripts/smoke/avro_consume.py --count 200 --bridge-log bridge.jsonl

Uses a fresh consumer group each run, so it always reads the topic from the
earliest offset. For each record it checks that:

- the value decodes as Avro through Schema Registry (Confluent wire format);
- the key equals the record's asset_id;
- synthetic is true.

It then reports the count, the schema ids seen, records per partition and per
asset, and whether each asset's seq runs contiguously from 0 without
duplicates. With --bridge-log (the bridge's JSON-lines stderr), it also
reports min, max and mean of the bridge latency fields of the delivered
messages.

Exit status: 0 when exactly --count records were read and every check
passed, 1 otherwise.
"""

import argparse
import json
import os
import statistics
import sys
import time
import uuid
import warnings
from collections import Counter, defaultdict
from pathlib import Path

from confluent_kafka import Consumer

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.loader import load_config  # noqa: E402

# Importing the Schema Registry client raises a third-party deprecation
# warning that ignores warning filters, so warning display is switched off
# for that import only.
_showwarning, warnings.showwarning = warnings.showwarning, lambda *args, **kwargs: None
from confluent_kafka.schema_registry import SchemaRegistryClient  # noqa: E402
from confluent_kafka.schema_registry.avro import AvroDeserializer  # noqa: E402
warnings.showwarning = _showwarning
from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: E402

LATENCY_FIELDS = ("ts_to_receive_ms", "ts_to_produce_ms", "ts_to_ack_ms")


def latency_summary(path: Path) -> dict:
    values = defaultdict(list)
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("{"):
            continue
        entry = json.loads(line)
        if entry.get("event") == "message_delivered":
            for f in LATENCY_FIELDS:
                values[f].append(entry[f])
    return {f: {"n": len(v), "min": min(v), "max": max(v), "mean": round(statistics.fmean(v), 3)}
            for f, v in values.items() if v}


def main() -> int:
    p = argparse.ArgumentParser(description="Verify Avro telemetry records on Kafka.")
    p.add_argument("--count", type=int, required=True, help="number of records expected")
    p.add_argument("--idle-timeout", type=float, default=15.0, help="stop after this many seconds without a record")
    p.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--schema-registry-url", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    p.add_argument("--bridge-log", type=Path, help="bridge stderr (JSON lines) to summarise latency from")
    args = p.parse_args()

    topic = load_config().topics.telemetry_raw.name
    deserialize = AvroDeserializer(SchemaRegistryClient({"url": args.schema_registry_url}))
    context = SerializationContext(topic, MessageField.VALUE)
    consumer = Consumer({"bootstrap.servers": args.kafka_bootstrap, "group.id": f"smoke-{uuid.uuid4()}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([topic])

    records, problems = [], []
    schema_ids, partitions = Counter(), Counter()
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
            raw = msg.value()
            schema_ids[int.from_bytes(raw[1:5], "big") if raw and raw[0] == 0 else None] += 1
            partitions[msg.partition()] += 1
            try:
                record = deserialize(raw, context)
            except Exception as e:  # report every decoding failure, whatever its type
                problems.append(f"offset {msg.offset()}: cannot decode ({e})")
                continue
            key = msg.key().decode("utf-8") if msg.key() else None
            if key != record["asset_id"]:
                problems.append(f"offset {msg.offset()}: key {key!r} != asset_id {record['asset_id']!r}")
            if record["synthetic"] is not True:
                problems.append(f"offset {msg.offset()}: synthetic is not true")
            records.append(record)
    finally:
        consumer.close()

    per_asset = defaultdict(list)
    for r in records:
        per_asset[r["asset_id"]].append(r["seq"])
    for asset, seqs in per_asset.items():
        if sorted(seqs) != list(range(len(seqs))):
            problems.append(f"{asset}: seq not contiguous from 0 or duplicated")
    if len(records) != args.count:
        problems.append(f"read {len(records)} records, expected {args.count}")

    faults = Counter(r["injected_fault"] for r in records if r["injected_fault"])
    print(f"topic: {topic}")
    print(f"records decoded: {len(records)} (expected {args.count})")
    print(f"schema ids: {dict(schema_ids)}")
    print(f"records per partition: {dict(sorted(partitions.items()))}")
    print(f"records per asset: {dict(sorted((a, len(s)) for a, s in per_asset.items()))}")
    print(f"records with injected_fault: {dict(faults)}")
    if records:
        print(f"first record: {records[0]}")
    if args.bridge_log:
        for field, s in latency_summary(args.bridge_log).items():
            print(f"bridge {field}: n={s['n']} min={s['min']} max={s['max']} mean={s['mean']}")
    for problem in problems:
        print(f"PROBLEM: {problem}")
    print("result:", "PASS" if not problems else "FAIL")
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
