"""Diagnostic (Task 3.3 STEP 0, disposable): tables for the STEP 0 report from the raw results.

    python scripts/bench/diagnostics/analyze.py

Prints, as Markdown:
- the matrix of pass 4 (run4-ac): p50, p95, min, max per cell, recomputed from
  the raw CSVs (first 20 messages discarded), and librdkafka's decomposition
  (median over the statistics windows after the first 25 s of the per-window
  p50 of int_latency, outbuf_latency and rtt, in ms);
- the bimodality diagnostic (bimodal-ac): per variant, the share of cells whose
  p50 is above SLOW_MS, and the p50 of each cell.
"""

from __future__ import annotations

import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results"
DISCARD = 20
SLOW_MS = 30.0


def pct(values, q):
    s = sorted(values)
    return s[max(0, min(len(s) - 1, int(-(-q * len(s) // 100)) - 1))]


def librdkafka(stats_path: Path) -> dict:
    """From <cell>.stats.csv (reduce_stats.py), windows after the first 25."""
    if not stats_path.exists():
        return {}
    acc = defaultdict(list)
    with open(stats_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if int(row["window"]) < 25:
                continue
            for k in ("int_latency", "outbuf_latency", "rtt"):
                if int(row[f"{k}_cnt"]):
                    acc[k].append(int(row[f"{k}_p50"]) / 1000)
    return {k: round(statistics.median(v), 2) for k, v in acc.items()}


def matrix(run: str) -> None:
    print(f"\n### {run}\n")
    print("| Celda | n | p50 | p95 | min | max | int_latency | outbuf | rtt |")
    print("|---|---|---|---|---|---|---|---|---|")
    order = [json.loads(line)["cell"] for line in (RESULTS / run / "cells.jsonl").read_text(encoding="utf-8").splitlines()]
    for cell in order:
        with open(RESULTS / run / f"{cell}.csv", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        kept = [float(r["produce_to_ack_ms"]) for r in rows if int(r["i"]) >= DISCARD]
        lk = librdkafka(RESULTS / run / f"{cell}.stats.csv")
        print(f"| {cell} | {len(kept)} | {pct(kept, 50):.1f} | {pct(kept, 95):.1f} | {min(kept):.1f} | {max(kept):.1f} | "
              f"{lk.get('int_latency', '—')} | {lk.get('outbuf_latency', '—')} | {lk.get('rtt', '—')} |")


def bimodal() -> None:
    path = RESULTS / "bimodal-ac" / "cells.jsonl"
    if not path.exists():
        return
    by = defaultdict(list)
    for line in path.read_text(encoding="utf-8").splitlines():
        r = json.loads(line)
        if r["valid"]:
            by[r["variant"]].append(r["p50"])
    print(f"\n### bimodal-ac (celda lenta: p50 > {SLOW_MS:g} ms)\n")
    print("| Variante | Celdas | Lentas | p50 de cada celda (ms) |")
    print("|---|---|---|---|")
    for variant, p50s in by.items():
        slow = sum(1 for v in p50s if v > SLOW_MS)
        print(f"| {variant} | {len(p50s)} | {slow} | {', '.join(f'{v:.1f}' for v in p50s)} |")


if __name__ == "__main__":
    matrix("run4-ac")
    bimodal()
