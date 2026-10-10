"""Diagnostic (Task 3.3 STEP 1, item 1.1.2, disposable): does a ThingsBoard WebSocket observer see every value?

With the full profile up (`make up`): creates a probe device of type pump
through the pipeline gateway (so it gets the pump device profile and the
PipelineSequential queue, like the pipeline's devices), subscribes to its
latest telemetry with ws_observer_poc.js, publishes --count values at --rate
msg/s through the gateway MQTT API (the sink's path, QoS 1), each stamped
with its scheduled instant and carrying seq = its index, then checks that the
observer received every seq exactly once with its exact ts. Deletes the probe
device at the end. Credentials come from the environment or .env (the import
script's setting()); none is printed or written.

    python scripts/bench/diagnostics/ws_poc.py --count 600 --rate 10
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
spec = importlib.util.spec_from_file_location("irc", REPO / "scripts" / "thingsboard" / "import_rule_chains.py")
irc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(irc)

DEVICE = "bench-ws-probe-01"


def pct(values, q):
    s = sorted(values)
    return s[max(0, min(len(s) - 1, int(-(-q * len(s) // 100)) - 1))]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=600)
    p.add_argument("--rate", type=float, default=10.0)
    p.add_argument("--out-dir", type=Path, default=HERE / "results" / "ws-poc")
    args = p.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    host = irc.setting("TB_HOST", "localhost")
    tb = irc.ThingsBoard(f"http://{host}:{irc.setting('TB_HTTP_PORT', '8080')}")
    tb.login(irc.setting("TB_TENANT_USERNAME"), irc.setting("TB_TENANT_PASSWORD"))

    acked = threading.Semaphore(0)
    gw = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="bench-ws-poc-gateway")
    gw.username_pw_set(irc.setting("TB_GATEWAY_TOKEN"))
    gw.on_publish = lambda *a: acked.release()
    gw.connect(host, int(irc.setting("TB_MQTT_PORT", "9883")), keepalive=60)
    gw.loop_start()
    gw.publish("v1/gateway/connect", json.dumps({"device": DEVICE, "type": "pump"}), qos=1).wait_for_publish(10)
    device_id = None
    for _ in range(30):
        found = tb.request("GET", "/api/tenant/devices", query={"deviceName": DEVICE}, allow_404=True)
        if found:
            device_id = found["id"]["id"]
            break
        time.sleep(1)
    if not device_id:
        print(json.dumps({"event": "probe_device_not_created"}))
        return 1

    obs_path = args.out_dir / "observer.jsonl"
    since_ms = int(time.time() * 1000) + 2000
    observer = subprocess.Popen(
        ["node", str(HERE / "ws_observer_poc.js"), "--url", f"ws://{host}:{irc.setting('TB_HTTP_PORT', '8080')}/api/ws",
         "--device", device_id, "--key", "seq", "--expect", str(args.count), "--since", str(since_ms), "--idle-s", "30"],
        stdout=subprocess.PIPE, text=True, env={**os.environ, "TB_JWT": tb.token})
    lines: list[str] = []
    subscribed = threading.Event()

    def read() -> None:
        for line in observer.stdout:
            lines.append(line)
            if '"subscribed"' in line:
                subscribed.set()

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    if not subscribed.wait(20):
        print(json.dumps({"event": "observer_not_subscribed", "observer_output": lines[-5:]}))
        observer.kill()
        return 1

    # Open loop: message i is due at t0 + i / rate on the monotonic clock and is
    # stamped with that scheduled instant on the wall clock.
    wall_t0_ms = max(since_ms, int(time.time() * 1000) + 500)
    mono_t0 = time.monotonic() + (wall_t0_ms - time.time() * 1000) / 1000
    late = []
    for i in range(args.count):
        due = mono_t0 + i / args.rate
        delay = due - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        late.append((time.monotonic() - due) * 1000)
        ts = wall_t0_ms + round(i * 1000 / args.rate)
        gw.publish("v1/gateway/telemetry", json.dumps({DEVICE: [{"ts": ts, "values": {"seq": i}}]}), qos=1)
    pubacks = sum(1 for _ in range(args.count) if acked.acquire(timeout=10))
    observer.wait(timeout=90)
    reader.join(timeout=5)

    obs = [json.loads(line) for line in lines if line.strip()]
    points = [o for o in obs if "seq" in o and o["ts"] >= since_ms]
    seqs = [o["seq"] for o in points]
    expected_ts = {i: wall_t0_ms + round(i * 1000 / args.rate) for i in range(args.count)}
    wrong_ts = [o for o in points if expected_ts.get(o["seq"]) != o["ts"]]
    latency = [o["arrival_ms"] - o["ts"] for o in points]
    summary = {
        "count": args.count, "rate": args.rate, "pubacks": pubacks, "received": len(points),
        "distinct_seq": len(set(seqs)), "missing": sorted(set(range(args.count)) - set(seqs))[:20],
        "missing_count": len(set(range(args.count)) - set(seqs)), "duplicates": len(seqs) - len(set(seqs)),
        "wrong_ts": len(wrong_ts), "in_order": seqs == sorted(seqs),
        "max_points_per_update": max((o.get("batch", 1) for o in points), default=0),
        "publisher_late_ms": {"p99": round(pct(late, 99), 3), "max": round(max(late), 3)},
        "arrival_minus_ts_ms": ({"p50": round(pct(latency, 50), 1), "p95": round(pct(latency, 95), 1),
                                 "min": round(min(latency), 1), "max": round(max(latency), 1)} if latency else None),
        "observer_end": next((o for o in obs if o.get("event") == "observer_finished"), None),
        "initial_points_before_run": sum(1 for o in obs if "seq" in o and o["ts"] < since_ms),
    }
    obs_path.write_text("".join(lines), encoding="utf-8")
    (args.out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary))

    gw.publish("v1/gateway/disconnect", json.dumps({"device": DEVICE}), qos=1).wait_for_publish(10)
    gw.disconnect()
    gw.loop_stop()
    tb.request("DELETE", f"/api/device/{device_id}")
    print(json.dumps({"event": "probe_device_deleted", "device": DEVICE}))
    ok = summary["received"] == summary["distinct_seq"] == args.count and summary["wrong_ts"] == 0
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
