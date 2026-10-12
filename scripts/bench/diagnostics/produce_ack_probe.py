"""Diagnostic (Task 3.3 STEP 0, disposable): produce() -> delivery report latency.

Reproduces the bridge's Kafka producer and its delivery-report threading
(bridge/mqtt_to_kafka/__main__.py: acks=all, enable.idempotence, a thread in
producer.poll(0.2), perf_counter before produce() and in the callback), with
no MQTT, no validation and no Avro serialisation: the payload is random bytes
of a telemetry record's size. One run is one cell of the STEP 0 matrix.

    python scripts/bench/diagnostics/produce_ack_probe.py --cell host-iso-base \
        --cadence isolated --n 220 --out-dir scripts/bench/diagnostics/results

Cadences:
    isolated   one message per second (one asset)
    burst      five messages back to back every second, one per asset, as
               the simulator publishes them (the smoke's cadence)
    singles10  one message every 100 ms (one asset): isolated messages, but
               ten times as frequent, to separate idle effects from batching

One-variable overrides: --linger-ms, --nagle-disable, --timer-1ms (Windows
only: timeBeginPeriod(1) for this process). librdkafka statistics
(statistics.interval.ms 1000) are written beside the samples: per broker,
int_latency (produce() -> request built, includes linger), outbuf_latency
(in the output buffer) and rtt (request -> response).

Writes <cell>.csv (one row per message) and <cell>.stats.jsonl, and prints a
JSON summary line. Not part of the pipeline; not covered by tests.
"""

from __future__ import annotations

import argparse
import csv
import json
import platform
import random
import statistics
import sys
import threading
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

ASSETS = ["motor-01", "motor-02", "pump-01", "pump-02", "compressor-01"]


def windows_timer_resolution_ms() -> dict | None:
    """Current, minimum and maximum timer resolution from NtQueryTimerResolution (100 ns units)."""
    if platform.system() != "Windows":
        return None
    import ctypes
    lo, hi, cur = ctypes.c_ulong(), ctypes.c_ulong(), ctypes.c_ulong()
    ctypes.windll.ntdll.NtQueryTimerResolution(ctypes.byref(lo), ctypes.byref(hi), ctypes.byref(cur))
    # "minimum" in the API is the coarsest period, "maximum" the finest.
    return {"coarsest_ms": lo.value / 1e4, "finest_ms": hi.value / 1e4, "current_ms": cur.value / 1e4}


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
    futures[topic].result(timeout=30)
    time.sleep(1.0)


def schedule(cadence: str, n: int) -> list[tuple[float, str, int]]:
    """(offset from start in s, key, position within its burst) for every message."""
    if cadence == "isolated":
        return [(float(i), ASSETS[2], 0) for i in range(n)]
    if cadence == "singles10":
        return [(i * 0.1, ASSETS[2], 0) for i in range(n)]
    if cadence == "burst":
        return [(float(i // len(ASSETS)), ASSETS[i % len(ASSETS)], i % len(ASSETS)) for i in range(n)]
    raise ValueError(cadence)


def pct(values: list[float], q: float) -> float:
    """Nearest-rank percentile on the raw samples."""
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(-(-q * len(s) // 100)) - 1))
    return s[k]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--cell", required=True)
    p.add_argument("--cadence", choices=["isolated", "burst", "singles10"], required=True)
    p.add_argument("--n", type=int, default=220)
    p.add_argument("--discard", type=int, default=20)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="bench.diag.produce-ack")
    p.add_argument("--payload-bytes", type=int, default=72)
    p.add_argument("--linger-ms", type=float)
    p.add_argument("--nagle-disable", action="store_true")
    p.add_argument("--timer-1ms", action="store_true")
    p.add_argument("--out-dir", type=Path, required=True)
    args = p.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    if args.timer_1ms:
        import ctypes
        ctypes.windll.winmm.timeBeginPeriod(1)
    timer = windows_timer_resolution_ms()
    ensure_topic(args.bootstrap, args.topic, 3)

    stats_lines: list[str] = []
    conf = {
        "bootstrap.servers": args.bootstrap, "client.id": f"bench-diag-{args.cell}",
        "acks": "all", "enable.idempotence": True,
        "statistics.interval.ms": 1000, "stats_cb": stats_lines.append,
    }
    if args.linger_ms is not None:
        conf["linger.ms"] = args.linger_ms
    if args.nagle_disable:
        conf["socket.nagle.disable"] = True
    producer = Producer(conf)
    producer.list_topics(timeout=10)  # connect before the first timed message

    stop = threading.Event()

    def serve() -> None:
        while not stop.is_set():
            producer.poll(0.2)

    reports = threading.Thread(target=serve, daemon=True)
    reports.start()

    rows: list[dict] = []
    lock = threading.Lock()
    payload = random.Random(1).randbytes(args.payload_bytes)
    plan = schedule(args.cadence, args.n)
    start = time.perf_counter() + 0.5
    for i, (offset, key, pos) in enumerate(plan):
        delay = start + offset - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        timing: dict[str, float] = {}

        def on_delivery(err, msg, i=i, key=key, pos=pos, timing=timing):
            acked = time.perf_counter()
            with lock:
                rows.append({"i": i, "key": key, "burst_pos": pos, "error": str(err) if err else "",
                             "partition": msg.partition(), "offset": msg.offset(),
                             "late_ms": round((timing["produce"] - (start + plan[i][0])) * 1000, 3),
                             "produce_to_ack_ms": round((acked - timing["produce"]) * 1000, 3)})

        timing["produce"] = time.perf_counter()
        producer.produce(args.topic, key=key.encode(), value=payload, on_delivery=on_delivery)
    stop.set()
    reports.join()
    remaining = producer.flush(10)
    producer.poll(1.5)  # one more statistics callback

    rows.sort(key=lambda r: r["i"])
    with open(args.out_dir / f"{args.cell}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    with open(args.out_dir / f"{args.cell}.stats.jsonl", "w", encoding="utf-8") as f:
        f.write("\n".join(stats_lines) + "\n")

    kept = [r["produce_to_ack_ms"] for r in rows if r["i"] >= args.discard and not r["error"]]
    brokers = {}
    if stats_lines:
        last = json.loads(stats_lines[-1])
        for b in last.get("brokers", {}).values():
            if b.get("nodeid", -1) >= 0:
                brokers[b["name"]] = {k: {q: b[k].get(q) for q in ("p50", "p95", "avg", "max", "cnt")}
                                      for k in ("int_latency", "outbuf_latency", "rtt")}
    summary = {
        "cell": args.cell, "cadence": args.cadence, "platform": platform.platform(), "bootstrap": args.bootstrap,
        "linger_ms": args.linger_ms, "nagle_disable": args.nagle_disable, "timer_1ms": args.timer_1ms,
        "timer_resolution": timer, "sent": len(plan), "delivered": len(rows), "errors": sum(1 for r in rows if r["error"]),
        "undelivered": remaining, "n": len(kept),
        "p50": pct(kept, 50), "p95": pct(kept, 95), "min": min(kept), "max": max(kept),
        "mean": round(statistics.fmean(kept), 3),
        "max_late_ms": max(r["late_ms"] for r in rows),
        "librdkafka_last_window_us": brokers,
    }
    print(json.dumps(summary))
    with open(args.out_dir / "summary.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(summary) + "\n")
    return 0 if remaining == 0 and summary["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
