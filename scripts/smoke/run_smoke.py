"""The Phase 1 smoke run (closure report, section 3) as one command: `make smoke`.

    python scripts/smoke/run_smoke.py [--count 200] [--log-dir smoke-logs]

Needs the stack up (`make up` or `make up-core`) with telemetry.raw empty,
which is how `make up` leaves it: the Kafka check reads the topic from the
earliest offset and expects exactly --count records, so a topic that already
holds records is refused before anything runs. Restart the stack to empty it.

Runs, from the repository root and with this Python:

1. the bridge, --count N --duration 180, waiting for mqtt_subscribed;
2. the Mosquitto probe (pahosubscribe.py --count N --duration 180 --quiet),
   waiting for its subscription;
3. the simulator, --count N --seed 42 --fault pump-01:imbalance:20;
4. once probe and bridge have finished, avro_consume.py --count N with the
   bridge log.

Component stderr (JSON lines) and stdout go to --log-dir, replaced on every
run. Each stage must reach N/N:

- simulator: exit 0, messages sent and acknowledged = N;
- Mosquitto probe: exit 0, N messages received;
- bridge: exit 0, received = produced = delivered = N, invalid 0, failed 0;
- Kafka: avro_consume.py exit 0, which means exactly N records, all decoded,
  keyed by asset_id, synthetic, seq contiguous per asset;
- Mosquitto: its dropped counter ($SYS, broker-wide) unchanged over the run,
  read after its next update, and no mqtt_messages_dropped from the bridge;
- logs: no non-JSON line on the stderr of the simulator or the bridge.

Exit status: 0 when every check passes, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import mosquitto_dropped
from confluent_kafka import Consumer, TopicPartition

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
from config.loader import load_config  # noqa: E402

PY = sys.executable
ENV = {**os.environ, "PYTHONIOENCODING": "utf-8"}
PORTS = {"Mosquitto": 1883, "Kafka": 9092, "Schema Registry": 8081}
DURATION = "180"


class Proc:
    """A component in the background, stdout and stderr each captured to a file."""

    def __init__(self, name: str, cmd: list[str], log_dir: Path):
        self.name = name
        self.out_path, self.err_path = log_dir / f"{name}.out.txt", log_dir / f"{name}.err.jsonl"
        self._out = self.out_path.open("w", encoding="utf-8")
        self._err = self.err_path.open("w", encoding="utf-8")
        self.p = subprocess.Popen(cmd, cwd=REPO_ROOT, env=ENV, stdout=self._out, stderr=self._err)

    def text(self, which: str = "err") -> str:
        return (self.err_path if which == "err" else self.out_path).read_text(encoding="utf-8")

    def events(self, name: str) -> list[dict]:
        found = []
        for line in self.text().splitlines():
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if entry.get("event") == name:
                found.append(entry)
        return found

    def non_json_lines(self) -> int:
        bad = 0
        for line in self.text().splitlines():
            try:
                json.loads(line)
            except ValueError:
                bad += 1
        return bad

    def wait_until(self, ready, what: str, timeout: float = 60) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if ready():
                return
            if self.p.poll() is not None:
                raise RuntimeError(f"{self.name} exited {self.p.returncode} before {what}")
            time.sleep(0.2)
        raise RuntimeError(f"{self.name}: no {what} within {timeout} s")

    def finish(self, timeout: float) -> int:
        try:
            return self.p.wait(timeout)
        finally:
            self.stop()

    def stop(self) -> None:
        if self.p.poll() is None:
            self.p.kill()
            self.p.wait()
        self._out.close()
        self._err.close()


def preflight(topic: str, bootstrap: str) -> list[str]:
    down = [f"{name} (port {port})" for name, port in PORTS.items() if not port_open(port)]
    if down:
        return [f"stack not reachable: {', '.join(down)}; start it with make up or make up-core"]
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"smoke-preflight-{os.getpid()}"})
    try:
        meta = c.list_topics(topic, timeout=10).topics.get(topic)
        if meta is None or meta.error is not None:
            return [f"{topic} does not exist; run make up or make up-core, which provision it"]
        held = 0
        for p in meta.partitions:
            low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            held += high - low
    finally:
        c.close()
    if held:
        return [f"{topic} already holds {held} record(s); the smoke run needs it empty: make down, then make up"]
    return []


def port_open(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("localhost", port)) == 0


def run(count: int, log_dir: Path) -> list[tuple[str, str, bool]]:
    """Run the pipeline once; return (stage, measured, passed) rows."""
    n = str(count)
    procs: list[Proc] = []
    dropped_before = mosquitto_dropped.dropped_total()
    try:
        bridge = Proc("bridge", [PY, "-m", "bridge.mqtt_to_kafka", "--count", n, "--duration", DURATION], log_dir)
        procs.append(bridge)
        bridge.wait_until(lambda: bridge.events("mqtt_subscribed"), "mqtt_subscribed")
        probe = Proc("pahosubscribe", [PY, "scripts/smoke/pahosubscribe.py", "--count", n, "--duration", DURATION,
                                       "--quiet"], log_dir)
        procs.append(probe)
        probe.wait_until(lambda: "subscribed to" in probe.text("out"), "subscription")
        simulator = Proc("simulator", [PY, "-m", "edge.simulator", "--count", n, "--seed", "42",
                                       "--fault", "pump-01:imbalance:20"], log_dir)
        procs.append(simulator)
        codes = {p.name: p.finish(timeout=float(DURATION) + 60) for p in (simulator, probe, bridge)}
    finally:
        for p in procs:
            p.stop()

    kafka = subprocess.run([PY, "scripts/smoke/avro_consume.py", "--count", n, "--bridge-log", str(bridge.err_path)],
                           cwd=REPO_ROOT, env=ENV, capture_output=True, text=True, timeout=120)
    (log_dir / "avro_consume.out.txt").write_text(kafka.stdout + kafka.stderr, encoding="utf-8")

    rows = []
    sim = (simulator.events("run_finished") or [{}])[0]
    sent, acked = sim.get("messages_sent"), sim.get("messages_acked")
    rows.append(("simulator sent / acknowledged", f"{sent} / {acked}, exit {codes['simulator']}",
                 codes["simulator"] == 0 and sent == acked == count))

    match = re.search(r"received (\d+) message", probe.text("out"))
    received = int(match.group(1)) if match else None
    rows.append(("Mosquitto probe received", f"{received}, exit {codes['pahosubscribe']}",
                 codes["pahosubscribe"] == 0 and received == count))

    b = (bridge.events("run_finished") or [{}])[0]
    figures = [b.get(k) for k in ("messages_received", "messages_invalid", "messages_produced", "messages_delivered",
                                  "messages_delivery_failed")]
    rows.append(("bridge received / invalid / produced / delivered / failed",
                 " / ".join(str(f) for f in figures) + f", exit {codes['bridge']}",
                 codes["bridge"] == 0 and figures == [count, 0, count, count, 0]))

    match = re.search(r"records decoded: (\d+)", kafka.stdout)
    decoded = int(match.group(1)) if match else None
    rows.append(("Kafka records decoded and checked", f"{decoded} / {count}, exit {kafka.returncode}",
                 kafka.returncode == 0 and decoded == count))

    # Mosquitto's dropped counter (DETENTE 3.3.h), broker-wide, after its next $SYS update (up to 10 s), and the
    # bridge's own record of it.
    dropped = mosquitto_dropped.dropped_total(after=time.monotonic()) - dropped_before
    logged = sum(e.get("delta", 0) for e in bridge.events("mqtt_messages_dropped"))
    rows.append(("Mosquitto dropped (broker-wide) / bridge mqtt_messages_dropped", f"{dropped} / {logged}",
                 dropped == 0 and logged == 0))

    bad = (simulator.non_json_lines(), bridge.non_json_lines())
    rows.append(("non-JSON stderr lines (simulator, bridge)", f"{bad[0]}, {bad[1]}", bad == (0, 0)))
    return rows


def main() -> int:
    p = argparse.ArgumentParser(description="Run the Phase 1 smoke test end to end and verify every stage.")
    p.add_argument("--count", type=int, default=200, help="messages to send (default 200, as in Phase 1)")
    p.add_argument("--log-dir", type=Path, default=REPO_ROOT / "smoke-logs",
                   help="where component output is written, replaced on every run (default smoke-logs/)")
    args = p.parse_args()

    topic = load_config().topics.telemetry_raw.name
    problems = preflight(topic, os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    if problems:
        for problem in problems:
            print(f"smoke: {problem}", file=sys.stderr)
        return 1

    shutil.rmtree(args.log_dir, ignore_errors=True)
    args.log_dir.mkdir(parents=True)
    started = time.monotonic()
    try:
        rows = run(args.count, args.log_dir)
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        print(f"smoke: {e}; logs in {args.log_dir}", file=sys.stderr)
        return 1

    width = max(len(stage) for stage, _, _ in rows)
    for stage, measured, passed in rows:
        print(f"{'PASS' if passed else 'FAIL'}  {stage:<{width}}  {measured}")
    ok = all(passed for _, _, passed in rows)
    print(f"smoke: {'PASS' if ok else 'FAIL'} in {time.monotonic() - started:.1f} s; logs in {args.log_dir}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
