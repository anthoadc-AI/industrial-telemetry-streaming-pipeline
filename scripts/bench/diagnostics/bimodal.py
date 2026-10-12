"""Diagnostic (Task 3.3 STEP 0, disposable): how often the burst cadence lands in its slow mode.

The burst cells of the matrix came out either around 15 ms or around 55-60 ms
(p50), changing from one cell to the next. This runs short burst cells many
times, alternating the variants, so each variant's share of slow cells can be
compared instead of one cell each:

    host-burst-base       Nagle on (librdkafka's default), linger 5 ms
    host-burst-nagleoff   socket.nagle.disable=true
    host-burst-linger0    linger.ms=0, Nagle on
    host-burst-l0-nagleoff linger.ms=0, Nagle off
    ctr-burst-base        in a container on the compose network

Same guards as run_matrix.py (docker ps before every cell, AC power, generator
lateness). 110 messages per cell (22 bursts), the first 10 discarded.

    python scripts/bench/diagnostics/bimodal.py --rounds 8
"""

from __future__ import annotations

import argparse
import json
import sys
import time

import run_matrix as m

VARIANTS = [
    ("host-burst-base", "host", []),
    ("host-burst-nagleoff", "host", ["--nagle-disable"]),
    ("host-burst-linger0", "host", ["--linger-ms", "0"]),
    ("host-burst-l0-nagleoff", "host", ["--linger-ms", "0", "--nagle-disable"]),
    ("ctr-burst-base", "ctr", []),
]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--rounds", type=int, default=8)
    args = p.parse_args()
    m.N, m.DISCARD = 110, 10
    m.RESULTS = m.HERE / "results" / "bimodal-ac"
    m.RESULTS.mkdir(parents=True, exist_ok=True)
    if not m.power()["ac"]:
        print(json.dumps({"event": "not_on_ac_power"}))
        return 5
    for r in range(args.rounds):
        for name, where, extra in VARIANTS:
            foreign = m.guard()
            if foreign:
                print(json.dumps({"event": "foreign_containers_running", "containers": foreign}))
                return 3
            cell = f"{name}-r{r}"
            before, t0 = m.power(), time.time()
            result = m.probe(cell, where, "burst", extra)
            wall_s, after = time.time() - t0, m.power()
            problems = m.validity(result, before, after, wall_s, m.N / 5)
            line = {"variant": name, "round": r, "cell": cell, "valid": not problems, "problems": problems,
                    **{k: result.get(k) for k in ("n", "p50", "p95", "min", "max", "errors", "max_late_ms")}}
            with open(m.RESULTS / "cells.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(line) + "\n")
            print(json.dumps(line), flush=True)
            if problems:
                return 4
    return 0


if __name__ == "__main__":
    sys.exit(main())
