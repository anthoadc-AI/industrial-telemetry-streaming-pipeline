"""Preload telemetry.raw for the meter's calibration (owner, DETENTE 3.3.d).

Writes --count valid telemetry_v1 records, keyed by asset as the bridge writes them, built by the generator's
own sample() so the meter decodes exactly what it decodes in a run. It writes as fast as it can (a large
linger and batch): its own rate is not a figure, only the meter's reading rate is.

    python -m bench.loader --count 600000
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

from bench import schedule
from common import jsonlog
from config.loader import load_benchmark_config, load_config

log = logging.getLogger("bench.loader")


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m bench.loader")
    p.add_argument("--count", type=int, required=True)
    p.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--schema-registry-url", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:  # pragma: no cover - runs in a container, exercised by the calibration
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    args = parse_args(argv)
    jsonlog.configure("INFO")
    config, bench = load_config(env={}), load_benchmark_config()
    topic = config.topics.telemetry_raw
    serialize = AvroSerializer(SchemaRegistryClient({"url": args.schema_registry_url}),
                               topic.value_schema.read_text(encoding="utf-8"))
    ctx = SerializationContext(topic.name, MessageField.VALUE)
    producer = Producer({"bootstrap.servers": args.kafka_bootstrap, "acks": "all", "enable.idempotence": True,
                         "linger.ms": 50, "batch.num.messages": 10000, "logger": logging.getLogger("librdkafka")})
    assets = tuple(a.id for a in config.telemetry.assets)
    plan = schedule.plan_for(1000.0, assets, args.count / 1000.0, int(time.time() * 1000))
    normals = {a: schedule.normal_values(config, a) for a in assets}
    started = time.monotonic()
    for i in range(plan.count):
        message = schedule.sample(plan, bench.fault_pattern, config, i, normals[plan.asset_of(i)])
        record = dict(message)
        record["ts"] = datetime.fromisoformat(message["ts"].replace("Z", "+00:00"))
        while True:
            try:
                producer.produce(topic.name, key=message["asset_id"].encode(), value=serialize(record, ctx))
                break
            except BufferError:
                producer.poll(0.05)
        producer.poll(0)
    remaining = producer.flush(120)
    result = {"count": plan.count, "undelivered": remaining, "seconds": round(time.monotonic() - started, 3)}
    log.info("loader_finished", extra=result)
    print(json.dumps(result))
    return 0 if remaining == 0 else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
