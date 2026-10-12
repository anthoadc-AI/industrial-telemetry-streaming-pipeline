"""Diagnostic (Task 3.3 STEP 0, disposable): offset between the host's wall clock and a container's.

Server side, in a container (answers each stdin line with its clocks):
    python clock_offset.py --serve
Client side, on the Windows host (starts or attaches to the containers):
    python scripts/bench/diagnostics/clock_offset.py --samples 200 --out-dir scripts/bench/diagnostics/results --label t0

For each exchange the host reads time.time_ns() before (t0) and after (t1)
asking; the container answers with its CLOCK_REALTIME and CLOCK_MONOTONIC.
offset = container_realtime - (t0 + t1) / 2, uncertain by +-(t1 - t0) / 2.
The estimate is the offset of the exchanges with the smallest round trip
(NTP's rule). Two containers are sampled interleaved; their offsets are
compared, and their CLOCK_MONOTONIC readings too, to check that they read
the same kernel clock.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path


def serve() -> int:
    for _ in sys.stdin:
        sys.stdout.write(f"{time.clock_gettime_ns(time.CLOCK_REALTIME)} {time.clock_gettime_ns(time.CLOCK_MONOTONIC)}\n")
        sys.stdout.flush()
    return 0


class Peer:
    def __init__(self, short: str, name: str, image: str):
        self.short = short
        self.name = name
        self.proc = subprocess.Popen(["docker", "run", "-i", "--rm", "--name", name, image,
                                      "python", "clock_offset.py", "--serve"],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)
        self.ask()  # first answer: the container is up

    def ask(self) -> tuple[int, int, int, int]:
        t0 = time.time_ns()
        self.proc.stdin.write("x\n")
        self.proc.stdin.flush()
        line = self.proc.stdout.readline()
        t1 = time.time_ns()
        real, mono = (int(v) for v in line.split())
        return t0, t1, real, mono

    def close(self) -> None:
        self.proc.stdin.close()
        self.proc.wait(timeout=30)


def best(samples: list[dict], key: str) -> dict:
    """Offset estimate from the 10 % of exchanges with the smallest round trip."""
    s = sorted(samples, key=lambda r: r["rtt_us"])
    top = s[: max(1, len(s) // 10)]
    return {"offset_ms": round(statistics.median(r[key] for r in top) / 1000, 3),
            "uncertainty_ms": round(max(r["rtt_us"] for r in top) / 2 / 1000, 3),
            "min_rtt_ms": round(s[0]["rtt_us"] / 1000, 3),
            "median_rtt_ms": round(statistics.median(r["rtt_us"] for r in s) / 1000, 3)}


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--serve", action="store_true")
    p.add_argument("--samples", type=int, default=200)
    p.add_argument("--image", default="bench-diag:local")
    p.add_argument("--label", default="t0")
    p.add_argument("--out-dir", type=Path)
    args = p.parse_args()
    if args.serve:
        return serve()

    print(json.dumps({"host_time_clock": str(time.get_clock_info("time"))}), file=sys.stderr)
    a = Peer("a", f"bench-diag-clock-a-{args.label}", args.image)
    b = Peer("b", f"bench-diag-clock-b-{args.label}", args.image)
    rows = []
    try:
        for i in range(args.samples):
            for peer in (a, b):
                t0, t1, real, mono = peer.ask()
                mid = (t0 + t1) // 2
                rows.append({"i": i, "peer": peer.short, "host_t0_ns": t0,
                             "host_t1_ns": t1, "container_realtime_ns": real, "container_monotonic_ns": mono,
                             "rtt_us": (t1 - t0) / 1000, "offset_us": (real - mid) / 1000,
                             "mono_minus_host_us": (mono - mid) / 1000})
            time.sleep(0.01)
    finally:
        a.close()
        b.close()

    by = {k: [r for r in rows if r["peer"] == k] for k in ("a", "b")}
    result = {"label": args.label, "samples_per_container": args.samples,
              "a": best(by["a"], "offset_us"), "b": best(by["b"], "offset_us"),
              # Same kernel: CLOCK_MONOTONIC minus the host's clock agrees across containers.
              "a_monotonic": best(by["a"], "mono_minus_host_us"), "b_monotonic": best(by["b"], "mono_minus_host_us")}
    result["realtime_a_minus_b_ms"] = round(result["a"]["offset_ms"] - result["b"]["offset_ms"], 3)
    result["monotonic_a_minus_b_ms"] = round(result["a_monotonic"]["offset_ms"] - result["b_monotonic"]["offset_ms"], 3)
    print(json.dumps(result))
    if args.out_dir:
        args.out_dir.mkdir(parents=True, exist_ok=True)
        with open(args.out_dir / f"clock-{args.label}.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        with open(args.out_dir / "clock_summary.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(result) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
