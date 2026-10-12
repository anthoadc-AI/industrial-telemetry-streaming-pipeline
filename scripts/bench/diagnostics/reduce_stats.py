"""Diagnostic (Task 3.3 STEP 0, disposable): reduce librdkafka statistics to one CSV per cell.

librdkafka's full statistics JSON (one document per second, every broker,
topic and partition) adds up to about 20 MB for the STEP 0 passes. This keeps,
per window and per broker with a node id, the fields analyze.py uses:
int_latency, outbuf_latency and rtt (p50, p95, avg, max, cnt; microseconds).
The full *.stats.jsonl files stay local (results/.gitignore).

    python scripts/bench/diagnostics/reduce_stats.py
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results"
FIELDS = ("int_latency", "outbuf_latency", "rtt")
STATS = ("p50", "p95", "avg", "max", "cnt")


def reduce(path: Path) -> int:
    out = path.with_name(path.name.replace(".stats.jsonl", ".stats.csv"))
    rows = []
    for window, line in enumerate(path.read_text(encoding="utf-8").splitlines()):
        if not line.strip():
            continue
        doc = json.loads(line)
        for b in doc["brokers"].values():
            if b["nodeid"] < 0:
                continue
            row = {"window": window, "ts_us": doc["ts"], "broker": b["name"]}
            for f in FIELDS:
                for s in STATS:
                    row[f"{f}_{s}"] = b[f][s]
            rows.append(row)
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["window", "ts_us", "broker"] + [f"{f}_{s}" for f in FIELDS for s in STATS])
        w.writeheader()
        w.writerows(rows)
    return len(rows)


if __name__ == "__main__":
    for p in sorted(RESULTS.rglob("*.stats.jsonl")):
        print(p.relative_to(RESULTS), reduce(p))
