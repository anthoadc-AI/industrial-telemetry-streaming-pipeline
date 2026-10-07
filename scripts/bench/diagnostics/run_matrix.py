"""Diagnostic (Task 3.3 STEP 0, disposable): run the produce_to_ack_ms matrix, cell by cell.

Requires the core stack (`make up-core`) and the image bench-diag:local
(`docker build -t bench-diag:local scripts/bench/diagnostics`).

    python scripts/bench/diagnostics/run_matrix.py [--only CELL ...]

Before every cell, `docker ps` must show nothing but this project's
containers (compose project industrial-telemetry) and this script's own
(bench-diag-*); otherwise it stops with exit status 3 (owner instruction,
Task 3.3 STEP 0). The power source (AC or battery) is recorded per cell.

Cells:
  anchor-*     the real bridge: the simulator's burst cadence, and isolated
               messages from isolated_publisher.py; the bridge's own
               produce_to_ack_ms from its message_delivered lines
  host-*       produce_ack_probe.py on the Windows host, localhost:9092
               (Docker Desktop's port forwarding)
  ctr-*        produce_ack_probe.py in a container on the compose network,
               broker:29092 (no port forwarding)
"""

from __future__ import annotations

import argparse
import csv
import ctypes
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results" / "run4-ac"
PY = str(REPO / ".venv" / "Scripts" / "python.exe")
NETWORK = "industrial-telemetry_default"
N, DISCARD = 220, 20

CELLS = [
    # name, where, cadence, extra probe flags
    ("anchor-bridge-burst", "anchor", "burst", []),
    ("anchor-bridge-iso", "anchor", "isolated", []),
    ("host-burst-base", "host", "burst", []),
    ("host-iso-base", "host", "isolated", []),
    ("host-singles10-base", "host", "singles10", []),
    ("host-iso-nagleoff", "host", "isolated", ["--nagle-disable"]),
    ("host-iso-linger0", "host", "isolated", ["--linger-ms", "0"]),
    ("host-iso-timer1ms", "host", "isolated", ["--timer-1ms"]),
    ("host-burst-nagleoff", "host", "burst", ["--nagle-disable"]),
    ("host-burst-linger0", "host", "burst", ["--linger-ms", "0"]),
    ("ctr-burst-base", "ctr", "burst", []),
    ("ctr-iso-base", "ctr", "isolated", []),
    ("ctr-singles10-base", "ctr", "singles10", []),
    ("ctr-iso-nagleoff", "ctr", "isolated", ["--nagle-disable"]),
    ("ctr-iso-linger0", "ctr", "isolated", ["--linger-ms", "0"]),
]


class PowerStatus(ctypes.Structure):
    _fields_ = [("ACLineStatus", ctypes.c_byte), ("BatteryFlag", ctypes.c_byte), ("BatteryLifePercent", ctypes.c_byte),
                ("SystemStatusFlag", ctypes.c_byte), ("BatteryLifeTime", ctypes.c_ulong), ("BatteryFullLifeTime", ctypes.c_ulong)]


def power() -> dict:
    s = PowerStatus()
    ctypes.windll.kernel32.GetSystemPowerStatus(ctypes.byref(s))
    return {"ac": {0: False, 1: True}.get(s.ACLineStatus), "battery_pct": s.BatteryLifePercent}


def guard() -> list[str]:
    out = subprocess.run(["docker", "ps", "--format", '{{.Names}}|{{.Label "com.docker.compose.project"}}'],
                         capture_output=True, text=True, check=True).stdout.split()
    foreign = [line for line in out if not (line.split("|")[1] == "industrial-telemetry" or line.startswith("bench-diag"))]
    return foreign


def pct(values, q):
    s = sorted(values)
    return s[max(0, min(len(s) - 1, int(-(-q * len(s) // 100)) - 1))]


def anchor(cell: str, cadence: str) -> dict:
    """The real bridge; its message_delivered lines give produce_to_ack_ms."""
    log_path = RESULTS / f"{cell}.bridge.log"
    with open(log_path, "w", encoding="utf-8") as log:
        bridge = subprocess.Popen([PY, "-m", "bridge.mqtt_to_kafka", "--count", str(N), "--duration", "400"],
                                  cwd=REPO, stderr=log, stdout=subprocess.DEVNULL)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and "mqtt_subscribed" not in log_path.read_text(encoding="utf-8"):
            time.sleep(0.2)
        if cadence == "burst":
            pub = [PY, "-m", "edge.simulator", "--count", str(N), "--seed", "42"]
        else:
            pub = [PY, str(HERE / "isolated_publisher.py"), "--count", str(N), "--interval", "1.0"]
        subprocess.run(pub, cwd=REPO, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        code = bridge.wait(timeout=120)
    events = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines() if line.startswith("{")]
    delivered = [e for e in events if e.get("event") == "message_delivered"]
    delivered.sort(key=lambda e: e["receive_ts_ms"])
    rows = [{"i": i, "key": e["asset_id"], "seq": e["seq"], "partition": e["partition"], "offset": e["offset"],
             "produce_to_ack_ms": e["produce_to_ack_ms"], "receive_to_produce_ms": e["receive_to_produce_ms"],
             "event_to_receive_ms": e["event_to_receive_ms"]} for i, e in enumerate(delivered)]
    with open(RESULTS / f"{cell}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    kept = [r["produce_to_ack_ms"] for r in rows if r["i"] >= DISCARD]
    summary = {"cell": cell, "cadence": cadence, "bridge_exit": code, "errors": 0 if code == 0 else 1,
               "sent": N, "delivered": len(rows), "n": len(kept),
               "p50": pct(kept, 50), "p95": pct(kept, 95), "min": min(kept), "max": max(kept),
               "mean": round(statistics.fmean(kept), 3)}
    with open(RESULTS / "summary.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(summary) + "\n")
    return summary


def probe(cell: str, where: str, cadence: str, extra: list[str]) -> dict:
    flags = ["--cell", cell, "--cadence", cadence, "--n", str(N), "--discard", str(DISCARD), *extra]
    if where == "host":
        cmd = [PY, str(HERE / "produce_ack_probe.py"), *flags, "--out-dir", str(RESULTS)]
    else:
        cmd = ["docker", "run", "--rm", "--name", f"bench-diag-{cell}", "--network", NETWORK,
               "-v", f"{RESULTS}:/out", "bench-diag:local", "python", "produce_ack_probe.py", *flags,
               "--bootstrap", "broker:29092", "--out-dir", "/out"]
    out = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
    if out.returncode != 0:
        print(out.stderr, file=sys.stderr)
    return json.loads(out.stdout.strip().splitlines()[-1])


# A cell is valid only if nothing suspended or slowed the generator: every message
# left within MAX_LATE_MS of its scheduled time (probe cells), the machine was on
# AC power before and after every cell (owner decision, Task 3.3 STEP 0), and
# nothing failed. The first invalid cell stops the matrix, and the matrix does not
# start on battery. Earlier passes are kept, labelled, in results/ (README.md).
MAX_LATE_MS = 50.0
ES_CONTINUOUS, ES_SYSTEM_REQUIRED = 0x80000000, 0x00000001


def validity(result: dict, before: dict, after: dict, wall_s: float, expected_s: float) -> list[str]:
    problems = []
    if not (before["ac"] and after["ac"]):
        problems.append("not on AC power")
    if result.get("errors"):
        problems.append(f"errors={result['errors']}")
    if result.get("max_late_ms") is not None and result["max_late_ms"] > MAX_LATE_MS:
        problems.append(f"generator late by {result['max_late_ms']} ms")
    if wall_s > expected_s + 90:
        problems.append(f"took {wall_s:.0f} s, expected about {expected_s:.0f} s")
    return problems


def main() -> int:
    # Keep Windows from sleeping on idle while the matrix runs (a closed lid still suspends it).
    ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
    p = argparse.ArgumentParser()
    p.add_argument("--only", nargs="*")
    args = p.parse_args()
    RESULTS.mkdir(parents=True, exist_ok=True)
    if not power()["ac"]:
        print(json.dumps({"event": "not_on_ac_power", "power": power()}), flush=True)
        return 5
    for cell, where, cadence, extra in CELLS:
        if args.only and cell not in args.only:
            continue
        foreign = guard()
        if foreign:
            print(json.dumps({"event": "foreign_containers_running", "before_cell": cell, "containers": foreign}))
            return 3
        before = power()
        started = time.strftime("%Y-%m-%dT%H:%M:%S")
        t0 = time.time()
        result = anchor(cell, cadence) if where == "anchor" else probe(cell, where, cadence, extra)
        wall_s = time.time() - t0
        after = power()
        expected_s = {"isolated": N, "burst": N / 5, "singles10": N / 10}[cadence]
        problems = validity(result, before, after, wall_s, expected_s)
        line = {"cell": cell, "started": started, "wall_s": round(wall_s, 1), "power": before, "power_after": after,
                "valid": not problems, "problems": problems,
                **{k: result.get(k) for k in ("n", "p50", "p95", "min", "max", "mean", "delivered", "errors", "max_late_ms")}}
        with open(RESULTS / "cells.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(line) + "\n")
        print(json.dumps(line), flush=True)
        if problems:
            print(json.dumps({"event": "invalid_cell_stops_matrix", "cell": cell, "problems": problems}), flush=True)
            return 4
    return 0


if __name__ == "__main__":
    sys.exit(main())
