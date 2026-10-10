"""The meter (Task 3.3, owner decisions D-G and D-H): when does each record become visible in telemetry.raw?

Runs in a container of the benchmark image, on the same VM clock as the generator (protocol 2.3). A consumer
of its own reads telemetry.raw from the end offsets at its start (the harness starts it before the generator),
decodes each record and, for records whose ts falls in the measured window, adds to three fixed histograms
(names approved at DETENTE 3.3.d):

    e2e                 visible - ts           F1: scheduled instant -> visible to a consumer
    ingest              CreateTime - ts        MQTT publish, Mosquitto, the bridge's queue, validation and
                                               serialisation, up to the bridge's produce() call
    produce_to_visible  visible - CreateTime   librdkafka's linger.ms, the produce request, the broker's append
                                               and high-watermark advance, and this consumer's fetch; it does not
                                               wait for the acknowledgement's return to the bridge. Dominated by
                                               linger.ms (5 ms by default)

visible is the wall clock when the consumer returned the record; CreateTime is the producer timestamp the
bridge's librdkafka puts on the record when produce() is called (confluent-kafka: "Default value is current
time"; the bridge passes none). Both ts and CreateTime are whole milliseconds, so each share carries +-1 ms of
quantisation.

Calibration (owner, DETENTE 3.3.d): with --from-beginning --expect N the meter reads N records preloaded into
telemetry.raw (bench/loader.py) as fast as it can and reports its own rate, records / seconds from the first
record to the Nth; the ladder is capped at it.

It also tracks, for every record of the run (window or not), each asset's seq: distinct, duplicates and the
highest seq, for the integrity check; and a per-second series (records visible, worst e2e). On SIGTERM it
writes meter.json into --out.

    python -m bench.meter --t0-ms 1791700000000 --warmup 30 --measured 300 --out /out
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict
from pathlib import Path

from bench.histogram import Histogram
from common import jsonlog
from config.loader import load_benchmark_config, load_config

log = logging.getLogger("bench.meter")


class SeqTracker:
    """Distinct and duplicate seq per asset, in a growable bitmap (a run can hold millions of records)."""

    def __init__(self):
        self.bits: dict[str, bytearray] = defaultdict(bytearray)
        self.distinct = 0
        self.duplicates = 0
        self.max_seq: dict[str, int] = {}

    def add(self, asset: str, seq: int) -> None:
        if seq < 0:
            raise ValueError("negative seq")
        bits = self.bits[asset]
        byte, bit = divmod(seq, 8)
        if byte >= len(bits):
            bits.extend(b"\x00" * (byte + 1 - len(bits) + 1024))
        if bits[byte] >> bit & 1:
            self.duplicates += 1
            return
        bits[byte] |= 1 << bit
        self.distinct += 1
        self.max_seq[asset] = max(self.max_seq.get(asset, -1), seq)

    def gaps(self) -> int:
        """Seqs below an asset's highest that were never seen."""
        return sum(m + 1 for m in self.max_seq.values()) - self.distinct


class Window:
    def __init__(self, t0_ms: int, warmup_s: float, measured_s: float):
        self.start_ms = t0_ms + round(warmup_s * 1000)
        self.end_ms = self.start_ms + round(measured_s * 1000)

    def __contains__(self, ts_ms: int) -> bool:
        return self.start_ms <= ts_ms < self.end_ms


class Meter:
    def __init__(self, window: Window):
        self.window = window
        self.e2e, self.ingest, self.produce_to_visible = Histogram(), Histogram(), Histogram()
        self.seqs = SeqTracker()
        self.records = 0
        self.in_window = 0
        self.series: dict[int, list] = {}  # visible second -> [records, worst e2e ms]

    def record(self, asset: str, seq: int, ts_ms: int, create_ms: int | None, visible_ms: float) -> None:
        self.records += 1
        self.seqs.add(asset, seq)
        e2e = visible_ms - ts_ms
        second = int(visible_ms // 1000)
        row = self.series.setdefault(second, [0, 0.0])
        row[0] += 1
        row[1] = max(row[1], e2e)
        if ts_ms in self.window:
            self.in_window += 1
            self.e2e.add(e2e)
            if create_ms is not None:
                self.ingest.add(create_ms - ts_ms)
                self.produce_to_visible.add(visible_ms - create_ms)

    def result(self) -> dict:
        return {
            "window": {"start_ms": self.window.start_ms, "end_ms": self.window.end_ms},
            "records": self.records, "records_in_window": self.in_window,
            "distinct": self.seqs.distinct, "duplicates": self.seqs.duplicates, "gaps": self.seqs.gaps(),
            "max_seq_per_asset": self.seqs.max_seq,
            "histograms": {"e2e": self.e2e.to_json(), "ingest": self.ingest.to_json(),
                           "produce_to_visible": self.produce_to_visible.to_json()},
            "summaries": {"e2e": self.e2e.summary(), "ingest": self.ingest.summary(),
                          "produce_to_visible": self.produce_to_visible.summary()},
            "series": [[s, c, round(w, 3)] for s, (c, w) in sorted(self.series.items())],
        }


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m bench.meter")
    p.add_argument("--t0-ms", type=int, required=True)
    p.add_argument("--warmup", type=float, required=True)
    p.add_argument("--measured", type=float, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--from-beginning", action="store_true", help="calibration: read the topic from its first record")
    p.add_argument("--expect", type=int, help="calibration: stop after this many records and report the meter's rate")
    p.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--schema-registry-url", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:  # pragma: no cover - runs in a container, exercised by the harness trial
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    args = parse_args(argv)
    jsonlog.configure("INFO")
    config, bench = load_config(env={}), load_benchmark_config()
    topic = config.topics.telemetry_raw.name
    stop = {"now": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(now=True))

    consumer = Consumer({"bootstrap.servers": args.kafka_bootstrap, "group.id": f"bench-meter-{os.getpid()}-{time.time_ns()}",
                         "enable.auto.commit": False, "fetch.wait.max.ms": bench.meter_fetch_wait_max_ms,
                         "logger": logging.getLogger("librdkafka")})
    meta = consumer.list_topics(topic, timeout=10).topics[topic]
    which = 0 if args.from_beginning else 1  # low or high watermark
    starts = [TopicPartition(topic, p, consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[which])
              for p in meta.partitions]
    consumer.assign(starts)
    decode = AvroDeserializer(SchemaRegistryClient({"url": args.schema_registry_url}))
    ctx = SerializationContext(topic, MessageField.VALUE)
    meter = Meter(Window(args.t0_ms, args.warmup, args.measured))
    log.info("meter_ready", extra={"topic": topic, "start_offsets": {tp.partition: tp.offset for tp in starts},
                                   "fetch_wait_max_ms": bench.meter_fetch_wait_max_ms})
    errors, first_s, last_s = 0, None, None
    while not stop["now"] and not (args.expect and meter.records >= args.expect):
        # poll(), one record at a time: it returns as soon as a record is there. consume(num_messages=N) waits
        # for N records or its timeout, which at low rates added up to the whole timeout to every record (the
        # first STEP 1 trial: a read share of 95 ms against a produce-to-ack of 6 ms).
        msg = consumer.poll(0.2)
        if msg is None:
            continue
        visible_ms = time.time() * 1000
        if msg.error():
            errors += 1
            continue
        value = decode(msg.value(), ctx)
        kind, create_ms = msg.timestamp()
        meter.record(value["asset_id"], value["seq"], int(value["ts"].timestamp() * 1000),
                     create_ms if kind == 1 else None, visible_ms)  # 1: TIMESTAMP_CREATE_TIME
        first_s = visible_ms / 1000 if first_s is None else first_s
        last_s = visible_ms / 1000
    consumer.close()
    result = {**meter.result(), "consumer_errors": errors, "fetch_wait_max_ms": bench.meter_fetch_wait_max_ms}
    if args.expect:
        elapsed = (last_s - first_s) if first_s is not None and last_s > first_s else None
        result["calibration"] = {"expected": args.expect, "read": meter.records, "elapsed_s": elapsed,
                                 "records_per_s": round(meter.records / elapsed, 1) if elapsed else None}
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "meter.json").write_text(json.dumps(result) + "\n", encoding="utf-8")
    log.info("meter_finished", extra={k: result[k] for k in ("records", "records_in_window", "distinct", "duplicates", "gaps")})
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
