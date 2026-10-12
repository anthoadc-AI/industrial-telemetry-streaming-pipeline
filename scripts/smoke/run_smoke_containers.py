"""The smoke run against the pipeline in containers: `make smoke-containers`.

    python scripts/smoke/run_smoke_containers.py [--count 200] [--log-dir smoke-logs-containers]

Needs `make up-pipeline-core` (or `make up-pipeline`): the stack provisioned,
telemetry.raw empty, and the bridge and alarm consumer containers running.
It is the containers' counterpart of `make smoke` (scripts/smoke/run_smoke.py):
the same simulator run, `--count N --seed 42 --fault pump-01:imbalance:20`,
here as a one-off container (`docker compose run --rm simulator`). The
bridge and the alarm consumer keep running, so their totals are read from
Kafka and from their container logs instead of from a run_finished line.

Each check must pass:

- containers: bridge and alarm-consumer running, the bridge subscribed to
  MQTT and the consumer started;
- simulator: exit 0, messages sent and acknowledged = N;
- Kafka: avro_consume.py exit 0, exactly N records in telemetry.raw, all
  decoded, keyed by asset_id, synthetic, seq contiguous per asset;
- bridge: N message_delivered lines, no dead letter;
- alarm consumer: its group has committed every telemetry.raw offset (it has
  processed all N records);
- alarms: at least one pump-01 alarm on telemetry.alarms, from the injected
  imbalance (read from the record keys, alarm_id = asset/channel/severity/ts);
- dead letters: none on telemetry.dlq;
- Mosquitto: its dropped counter ($SYS, broker-wide) unchanged over the run,
  read after its next update, and no mqtt_messages_dropped from the bridge;
- logs: no non-JSON line from the simulator, the bridge or the consumer.

Container output goes to --log-dir, replaced on every run. Exit status: 0
when every check passes, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
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
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CONSUMER_GROUP = os.environ.get("ALARM_CONSUMER_GROUP", "alarm-consumer")
SERVICES = ("bridge", "alarm-consumer")
CATCH_UP_S = 60.0


def compose(*args: str, timeout: float = 300) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "compose", *args], cwd=REPO_ROOT, capture_output=True, text=True,
                          encoding="utf-8", timeout=timeout)


def container_log(service: str) -> str:
    return compose("--profile", "pipeline", "logs", "--no-color", "--no-log-prefix", service).stdout


def events(text: str, name: str) -> list[dict]:
    found = []
    for line in text.splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if entry.get("event") == name:
            found.append(entry)
    return found


def non_json_lines(text: str) -> int:
    bad = 0
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            json.loads(line)
        except ValueError:
            bad += 1
    return bad


def running_services() -> set[str]:
    out = compose("--profile", "pipeline", "ps", "--status", "running", "--services").stdout
    return set(out.split())


def end_offsets(topic: str) -> dict[int, int]:
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"smoke-containers-{os.getpid()}"})
    try:
        meta = c.list_topics(topic, timeout=10).topics[topic]
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in meta.partitions}
    finally:
        c.close()


def committed(group: str, topic: str, partitions) -> dict[int, int]:
    # A consumer that never subscribes does not join the group: it only reads the group's committed offsets.
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "enable.auto.commit": False})
    try:
        return {tp.partition: tp.offset for tp in c.committed([TopicPartition(topic, p) for p in partitions], timeout=10)}
    finally:
        c.close()


def keys(topic: str) -> list[str]:
    """Every record key on a topic, from the earliest offset."""
    ends = end_offsets(topic)
    if not sum(ends.values()):
        return []
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"smoke-containers-keys-{os.getpid()}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    found = []
    try:
        c.assign([TopicPartition(topic, p, 0) for p in ends])
        deadline = time.monotonic() + 30
        while len(found) < sum(ends.values()) and time.monotonic() < deadline:
            msg = c.poll(0.5)
            if msg is not None and not msg.error():
                found.append(msg.key().decode("utf-8") if msg.key() else "")
    finally:
        c.close()
    return found


def preflight(topic: str) -> list[str]:
    missing = [s for s in SERVICES if s not in running_services()]
    if missing:
        return [f"container(s) not running: {', '.join(missing)}; start them with make up-pipeline-core or make up-pipeline"]
    try:
        held = sum(end_offsets(topic).values())
    except Exception as e:  # the client raises several types when Kafka is unreachable
        return [f"Kafka not reachable at {BOOTSTRAP}: {e}"]
    if held:
        return [f"{topic} already holds {held} record(s); the smoke run needs it empty: make down, then make up-pipeline-core"]
    return []


def wait_ready(timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if events(container_log("bridge"), "mqtt_subscribed") and events(container_log("alarm-consumer"), "run_started"):
            return
        time.sleep(0.5)
    raise RuntimeError("the bridge did not subscribe or the consumer did not start within 60 s")


def run(count: int, log_dir: Path, topics) -> list[tuple[str, str, bool]]:
    n = str(count)
    rows = []
    wait_ready()
    rows.append(("containers running and ready (bridge, alarm-consumer)", "yes", True))
    dropped_before = mosquitto_dropped.dropped_total()

    sim = compose("--profile", "simulator", "run", "--rm", "-T", "--no-deps", "simulator",
                  "--count", n, "--seed", "42", "--fault", "pump-01:imbalance:20", timeout=300)
    (log_dir / "simulator.log").write_text(sim.stdout + sim.stderr, encoding="utf-8")
    finished = (events(sim.stdout + sim.stderr, "run_finished") or [{}])[0]
    sent, acked = finished.get("messages_sent"), finished.get("messages_acked")
    rows.append(("simulator sent / acknowledged", f"{sent} / {acked}, exit {sim.returncode}",
                 sim.returncode == 0 and sent == acked == count))

    raw = topics.telemetry_raw.name
    deadline = time.monotonic() + CATCH_UP_S
    while sum(end_offsets(raw).values()) < count and time.monotonic() < deadline:
        time.sleep(0.5)
    bridge_log = log_dir / "bridge.log"
    bridge_log.write_text(container_log("bridge"), encoding="utf-8")
    kafka = subprocess.run([PY, "scripts/smoke/avro_consume.py", "--count", n, "--bridge-log", str(bridge_log)],
                           cwd=REPO_ROOT, env=ENV, capture_output=True, text=True, timeout=120)
    (log_dir / "avro_consume.out.txt").write_text(kafka.stdout + kafka.stderr, encoding="utf-8")
    match = re.search(r"records decoded: (\d+)", kafka.stdout)
    decoded = int(match.group(1)) if match else None
    rows.append(("Kafka records decoded and checked", f"{decoded} / {count}, exit {kafka.returncode}",
                 kafka.returncode == 0 and decoded == count))

    bridge_text = bridge_log.read_text(encoding="utf-8")
    delivered, dead = len(events(bridge_text, "message_delivered")), len(events(bridge_text, "message_dead_lettered"))
    rows.append(("bridge delivered / dead-lettered", f"{delivered} / {dead}", delivered == count and dead == 0))

    ends = end_offsets(raw)
    deadline = time.monotonic() + CATCH_UP_S
    done = committed(CONSUMER_GROUP, raw, ends)
    while done != ends and time.monotonic() < deadline:
        time.sleep(1)
        done = committed(CONSUMER_GROUP, raw, ends)
    behind = sum(ends[p] - max(done.get(p, 0), 0) for p in ends)
    rows.append(("alarm consumer records not yet committed", f"{behind} of {sum(ends.values())}", behind == 0))

    alarm_keys = keys(topics.kafka["telemetry_alarms"].name)
    pump = [k for k in alarm_keys if k.startswith("pump-01/")]
    kinds = sorted(set(k.rsplit("/", 1)[0] for k in pump))
    rows.append(("alarm events on telemetry.alarms (pump-01 / all)", f"{len(pump)} / {len(alarm_keys)}  {kinds}",
                 len(pump) >= 1))

    dlq = sum(end_offsets(topics.kafka["telemetry_dlq"].name).values())
    rows.append(("dead letters on telemetry.dlq", str(dlq), dlq == 0))

    # Mosquitto's dropped counter (DETENTE 3.3.h), broker-wide, after its next $SYS update (up to 10 s), and the
    # bridge's own record of it.
    dropped = mosquitto_dropped.dropped_total(after=time.monotonic()) - dropped_before
    logged = sum(e.get("delta", 0) for e in events(bridge_text, "mqtt_messages_dropped"))
    rows.append(("Mosquitto dropped (broker-wide) / bridge mqtt_messages_dropped", f"{dropped} / {logged}",
                 dropped == 0 and logged == 0))

    consumer_text = container_log("alarm-consumer")
    (log_dir / "alarm-consumer.log").write_text(consumer_text, encoding="utf-8")
    bad = (non_json_lines(sim.stdout + sim.stderr), non_json_lines(bridge_text), non_json_lines(consumer_text))
    rows.append(("non-JSON log lines (simulator, bridge, consumer)", ", ".join(map(str, bad)), bad == (0, 0, 0)))
    return rows


def main() -> int:
    p = argparse.ArgumentParser(description="Run the smoke test against the pipeline in containers.")
    p.add_argument("--count", type=int, default=200, help="messages to send (default 200, as make smoke)")
    p.add_argument("--log-dir", type=Path, default=REPO_ROOT / "smoke-logs-containers",
                   help="where container output is written, replaced on every run (default smoke-logs-containers/)")
    args = p.parse_args()

    topics = load_config().topics
    problems = preflight(topics.telemetry_raw.name)
    if problems:
        for problem in problems:
            print(f"smoke-containers: {problem}", file=sys.stderr)
        return 1

    shutil.rmtree(args.log_dir, ignore_errors=True)
    args.log_dir.mkdir(parents=True)
    started = time.monotonic()
    try:
        rows = run(args.count, args.log_dir, topics)
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        print(f"smoke-containers: {e}; logs in {args.log_dir}", file=sys.stderr)
        return 1

    width = max(len(stage) for stage, _, _ in rows)
    for stage, measured, passed in rows:
        print(f"{'PASS' if passed else 'FAIL'}  {stage:<{width}}  {measured}")
    ok = all(passed for _, _, passed in rows)
    print(f"smoke-containers: {'PASS' if ok else 'FAIL'} in {time.monotonic() - started:.1f} s; logs in {args.log_dir}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
