"""Helpers for the integration tests: compose, component processes, Kafka and Schema Registry access.

The pipeline components run exactly as a person runs them: separate
processes started from the repository root, logging JSON lines to stderr,
which is captured to a file per process.
"""

from __future__ import annotations

import io
import json
import os
import socket
import struct
import subprocess
import sys
import time
import uuid
import warnings
from pathlib import Path

import fastavro
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, AlterConfigOpType, ConfigEntry, ConfigResource

REPO_ROOT = Path(__file__).resolve().parents[2]
PY = sys.executable
BOOTSTRAP = "localhost:9092"
REGISTRY = "http://localhost:8081"
ENV = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONIOENCODING": "utf-8"}

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    _show, warnings.showwarning = warnings.showwarning, lambda *a, **k: None
    from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext
    warnings.showwarning = _show


def compose(*args: str, timeout: float = 900) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "compose", *args], cwd=REPO_ROOT, capture_output=True, text=True, timeout=timeout)


def port_open(port: int, host: str = "localhost") -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0


def volume_names() -> set[str]:
    out = subprocess.run(["docker", "volume", "ls", "-q"], capture_output=True, text=True, check=True).stdout
    return set(out.split())


def compose_config() -> dict:
    """docker-compose.yml as Compose resolves it (project name, volume names), all profiles included."""
    result = compose("--profile", "full", "config", "--format", "json")
    if result.returncode != 0:
        raise RuntimeError(f"docker compose config failed: {result.stderr[-500:]}")
    return json.loads(result.stdout)


def project_volume_names(config: dict) -> set[str]:
    """The named volumes the project declares, as Docker names them (<project>_<volume> unless the file sets a
    name). External volumes are left out: the project uses them but never creates them."""
    return {(spec or {}).get("name") or f"{config['name']}_{key}"
            for key, spec in (config.get("volumes") or {}).items() if not (spec or {}).get("external")}


def leaked_volumes(before: set[str], after: set[str], allowed: set[str]) -> set[str]:
    """Volumes that appeared during the session and are not the project's own named volumes.

    The project's named volumes (ThingsBoard's data and logs) are created by the first `up` on a machine and
    kept by `down` without -v, by design: on a clean machine, CI included, they appear once and are not a leak.
    Any other new volume, an anonymous one above all, is.
    """
    return (after - before) - allowed


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# --- component processes ----------------------------------------------------------


class Proc:
    """A pipeline component running in the background, stderr captured as JSON lines.

    Every Proc is registered in Proc.started. The autouse fixture in
    proc_isolation.py stops, after each test, whatever that test left running,
    whether it passed or failed: a failed assertion skips the test's own
    finish() calls, and a component left alive (a bridge still subscribed, a
    consumer still in its group) would otherwise act on the next test's data.
    """

    started: list["Proc"] = []

    def __init__(self, name: str, cmd: list[str], log_dir: Path):
        self.name = name
        self.log = log_dir / f"{name}.jsonl"
        self._file = self.log.open("w", encoding="utf-8")
        self.p = subprocess.Popen(cmd, cwd=REPO_ROOT, env=ENV, stdout=subprocess.DEVNULL, stderr=self._file)
        Proc.started.append(self)

    def stop(self) -> bool:
        """Kill the process if it is still running and close its log; True if it had to be killed."""
        running = self.p.poll() is None
        if running:
            self.p.kill()
            self.p.wait()
        if not self._file.closed:
            self._file.close()
        return running



    def events(self) -> list[dict]:
        if not self._file.closed:
            self._file.flush()
        out = []
        for line in self.log.read_text(encoding="utf-8").splitlines():
            try:
                out.append(json.loads(line))
            except ValueError:
                pass
        return out

    def find(self, event: str) -> list[dict]:
        return [e for e in self.events() if e.get("event") == event]

    def wait_for(self, event: str, timeout: float = 60) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            found = self.find(event)
            if found:
                return found[0]
            if self.p.poll() is not None:
                raise AssertionError(f"{self.name} exited {self.p.returncode} before logging {event}: {self.tail()}")
            time.sleep(0.2)
        raise AssertionError(f"{self.name} did not log {event} within {timeout} s: {self.tail()}")

    def finish(self, timeout: float = 120) -> int:
        try:
            code = self.p.wait(timeout)
        except subprocess.TimeoutExpired:
            self.p.kill()
            self.p.wait()
            raise AssertionError(f"{self.name} still running after {timeout} s: {self.tail()}")
        self._file.close()
        return code

    def non_json_lines(self) -> int:
        bad = 0
        for line in self.log.read_text(encoding="utf-8").splitlines():
            try:
                json.loads(line)
            except ValueError:
                bad += 1
        return bad

    def tail(self, n: int = 5) -> str:
        return " | ".join(self.log.read_text(encoding="utf-8").splitlines()[-n:])


def stop_leftover_processes() -> list[str]:
    """Stop every registered Proc still running; return the names of those stopped."""
    stopped = [proc.name for proc in Proc.started if proc.stop()]
    Proc.started.clear()
    return stopped


def simulator(log_dir: Path, name: str, count: int, *faults: str, seed: int = 1, rate: float = 10) -> Proc:
    cmd = [PY, "-m", "edge.simulator", "--count", str(count), "--rate", str(rate), "--seed", str(seed)]
    for f in faults:
        cmd += ["--fault", f]
    return Proc(name, cmd, log_dir)


def bridge(log_dir: Path, name: str, count: int, duration: float = 180, flush_timeout: float = 10) -> Proc:
    p = Proc(name, [PY, "-m", "bridge.mqtt_to_kafka", "--count", str(count), "--duration", str(duration),
                    "--client-id", unique("it-bridge"), "--flush-timeout", str(flush_timeout)], log_dir)
    p.wait_for("mqtt_subscribed", 60)
    return p


def alarm_consumer(log_dir: Path, name: str, group: str, count: int | None = None, duration: float = 180) -> Proc:
    cmd = ["node", "processing/alarm_consumer/index.js", "--group-id", group, "--duration", str(duration)]
    if count is not None:
        cmd += ["--count", str(count)]
    p = Proc(name, cmd, log_dir)
    p.wait_for("run_started", 90)
    return p


def thingsboard_sink(log_dir: Path, name: str, group: str, duration: float = 180, count: int | None = None) -> Proc:
    """The ThingsBoard sink in its own consumer group.

    The sink reads TB_GATEWAY_TOKEN from the environment only (no .env lookup, so it runs unchanged in a
    container); `make test-integration-full` exports .env into the environment.
    """
    cmd = [PY, "-m", "processing.thingsboard_sink", "--group-id", group, "--duration", str(duration),
           "--client-id", unique("it-sink")]
    if count is not None:
        cmd += ["--count", str(count)]
    p = Proc(name, cmd, log_dir)
    p.wait_for("run_started", 60)
    return p


def run_module(*args: str, timeout: float = 180) -> subprocess.CompletedProcess:
    return subprocess.run([PY, *args], cwd=REPO_ROOT, env=ENV, capture_output=True, text=True, timeout=timeout)


def json_events(stderr: str) -> list[dict]:
    return [json.loads(line) for line in stderr.splitlines() if line.startswith("{")]


# --- Kafka --------------------------------------------------------------------------


def admin() -> AdminClient:
    """A new AdminClient. Keep a reference until its futures resolve: a client
    that is garbage-collected fails them with _DESTROY ("handle is terminating")."""
    return AdminClient({"bootstrap.servers": BOOTSTRAP})


def describe_topic_configs(*topics: str) -> dict[str, dict[str, str]]:
    client = admin()
    resources = [ConfigResource(ConfigResource.Type.TOPIC, t) for t in topics]
    return {r.name: {k: e.value for k, e in f.result().items()} for r, f in client.describe_configs(resources).items()}


def alter_topic_configs(topic: str, set_: dict[str, str] | None = None, delete: tuple[str, ...] = ()) -> None:
    """Incrementally set and delete topic-level settings (deleting returns a setting to the broker default)."""
    entries = [ConfigEntry(k, v, incremental_operation=AlterConfigOpType.SET) for k, v in (set_ or {}).items()]
    entries += [ConfigEntry(k, None, incremental_operation=AlterConfigOpType.DELETE) for k in delete]
    client = admin()
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic, incremental_configs=entries)
    for future in client.incremental_alter_configs([resource]).values():
        future.result()


def watermarks(topic: str) -> dict[int, int]:
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": unique("it-wm"), "enable.auto.commit": False})
    try:
        meta = c.list_topics(topic, timeout=10).topics[topic]
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in meta.partitions}
    finally:
        c.close()


def read_since(topic: str, mark: dict[int, int], deserialize: bool = True, settle: float = 3.0) -> list[dict]:
    """Every record written to topic after mark, up to the end offsets at call time."""
    end = watermarks(topic)
    if all(end[p] <= mark.get(p, 0) for p in end):
        return []
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": unique("it-read"), "enable.auto.commit": False})
    c.assign([TopicPartition(topic, p, mark.get(p, 0)) for p in end])
    decoder = AvroDeserializer(SchemaRegistryClient({"url": REGISTRY})) if deserialize else None
    ctx = SerializationContext(topic, MessageField.VALUE)
    records, deadline = [], time.monotonic() + 30
    remaining = {p for p in end if end[p] > mark.get(p, 0)}
    try:
        while remaining and time.monotonic() < deadline:
            msg = c.poll(0.5)
            if msg is None or msg.error():
                continue
            records.append({
                "partition": msg.partition(), "offset": msg.offset(), "key": msg.key(), "raw": msg.value(),
                "value": decoder(msg.value(), ctx) if decoder and msg.value() else None,
            })
            if msg.offset() + 1 >= end[msg.partition()]:
                remaining.discard(msg.partition())
    finally:
        c.close()
    assert not remaining, f"could not read {topic} to its end offsets {end} within 30 s"
    return records


def committed(group: str, topic: str) -> dict[int, int]:
    """Committed offsets of group on topic (-1001 when nothing is committed)."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "enable.auto.commit": False})
    try:
        parts = [TopicPartition(topic, p) for p in watermarks(topic)]
        return {tp.partition: tp.offset for tp in c.committed(parts, timeout=10)}
    finally:
        c.close()


def position_group_at_end(group: str, topic: str) -> dict[int, int]:
    """Commit the current end offsets for a new group, so it starts after everything already written."""
    end = watermarks(topic)
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "enable.auto.commit": False})
    try:
        c.commit(offsets=[TopicPartition(topic, p, o) for p, o in end.items()], asynchronous=False)
    finally:
        c.close()
    return end


ROLL_MARKER_KEY = "it-roll-marker/{partition}"


def roll_every_partition(topic: str, segment_ms: int) -> int:
    """Make the broker roll the active segment of every partition of a compacted topic.

    The broker evaluates segment.ms only when it appends to a partition: a
    partition that receives no further write keeps its active segment open
    indefinitely, and the log cleaner never compacts an active segment. This
    waits past segment_ms, then appends one tombstone with a reserved key to
    each partition, so every record written before is in a closed, cleanable
    segment. Returns the number of markers written (one per partition).
    """
    time.sleep(segment_ms / 1000 + 1.0)
    partitions = sorted(watermarks(topic))
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    for partition in partitions:
        p.produce(topic, key=ROLL_MARKER_KEY.format(partition=partition).encode(), value=None, partition=partition)
    assert p.flush(10) == 0
    return len(partitions)


def is_roll_marker(record: dict) -> bool:
    return record["key"] is not None and record["key"].decode().startswith("it-roll-marker/")


def murmur2(data: bytes) -> int:
    """Kafka's Java murmur2, signed 32-bit, as kafkajs's DefaultPartitioner and the Java client compute it."""
    length, m = len(data), 0x5BD1E995
    h = (0x9747B28C ^ length) & 0xFFFFFFFF
    for i in range(length // 4):
        k = (int.from_bytes(data[i * 4:i * 4 + 4], "little") * m) & 0xFFFFFFFF
        k = ((k ^ (k >> 24)) * m) & 0xFFFFFFFF
        h = ((h * m) & 0xFFFFFFFF) ^ k
    tail = data[length - length % 4:]
    if len(tail) == 3:
        h ^= tail[2] << 16
    if len(tail) >= 2:
        h ^= tail[1] << 8
    if len(tail) >= 1:
        h = ((h ^ tail[0]) * m) & 0xFFFFFFFF
    h = ((h ^ (h >> 13)) * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h - (1 << 32) if h & 0x80000000 else h


def asset_partition(asset_id: str, partitions: int = 3) -> int:
    """The partition of telemetry.alarms the alarm consumer's AssetPartitioner gives an asset's records."""
    return (murmur2(asset_id.encode()) & 0x7FFFFFFF) % partitions


def produce_raw(topic: str, value: bytes | None, key: bytes | None = b"pump-01") -> None:
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    p.produce(topic, key=key, value=value)
    assert p.flush(10) == 0


def wait_topic_absent(topic: str, timeout: float = 60) -> None:
    client = admin()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if topic not in client.list_topics(timeout=10).topics:
            return
        time.sleep(0.5)
    raise AssertionError(f"{topic} still exists after {timeout} s")


def delete_topic(topic: str) -> None:
    client = admin()
    for future in client.delete_topics([topic], operation_timeout=30).values():
        future.result()
    wait_topic_absent(topic)


def provision() -> subprocess.CompletedProcess:
    """Run broker.kafka.provision, retrying while a just-deleted topic is still being removed."""
    for _ in range(20):
        result = run_module("-m", "broker.kafka.provision")
        if result.returncode == 0:
            return result
        time.sleep(1)
    raise AssertionError(f"provision failed: {result.stderr[-500:]}")


# --- Schema Registry and Avro -----------------------------------------------------------


def registry() -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": REGISTRY})


def latest_schema_id(subject: str) -> int:
    return registry().get_latest_version(subject).schema_id


def register(subject: str, schema: dict) -> int:
    return registry().register_schema(subject, Schema(json.dumps(schema), "AVRO"))


def framed(schema_id: int, schema: dict, record: dict) -> bytes:
    """Confluent wire format, encoded locally: no registry call is made."""
    buf = io.BytesIO()
    buf.write(b"\x00" + struct.pack(">I", schema_id))
    fastavro.schemaless_writer(buf, fastavro.parse_schema(schema), record)
    return buf.getvalue()


def telemetry_schema() -> dict:
    return json.loads((REPO_ROOT / "schemas" / "telemetry_v1.avsc").read_text(encoding="utf-8"))


# --- ThingsBoard --------------------------------------------------------------------


def import_script():
    """scripts/thingsboard/import_rule_chains.py as a module: its REST client, settings and paging."""
    import importlib.util

    name = "import_rule_chains_support"
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "scripts" / "thingsboard" / "import_rule_chains.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


def thingsboard():
    irc = import_script()
    tb = irc.ThingsBoard(f"http://localhost:{irc.setting('TB_HTTP_PORT', '8080')}")
    tb.login(irc.setting("TB_TENANT_USERNAME"), irc.setting("TB_TENANT_PASSWORD"))
    return tb


def active_pipeline_alarms(tb) -> list[dict]:
    """Active ThingsBoard alarms raised by the pipeline, in the scope of the import's reconciliation:
    devices of the configured asset types, alarms whose details carry an alarm_id."""
    from config.loader import load_config

    irc = import_script()
    found = []
    for asset_type in sorted({a.type for a in load_config(env={}).telemetry.assets}):
        for d in irc._pages(tb, "/api/tenant/devices", {"type": asset_type}):
            for a in irc._pages(tb, f"/api/alarm/DEVICE/{d['id']['id']}", {"searchStatus": "ACTIVE"}):
                if (a.get("details") or {}).get("alarm_id"):
                    found.append({"asset": d["name"], "type": a["type"], "alarm_id": a["details"]["alarm_id"],
                                  "status": a["status"]})
    return found


PIPELINE_QUEUE = "PipelineSequential"
# ThingsBoard logs a queue's statistics once a minute, for minutes with messages: wait this long after the
# last message before reading them, so its minute is in the log.
QUEUE_STATS_WAIT_S = 65
QUEUE_STATS_KEYS = ("totalMsgs", "successfulMsgs", "timeoutMsgs", "failedMsgs", "tmpTimeout", "tmpFailed")


def queue_stats_since(since: float, queue: str = PIPELINE_QUEUE) -> dict[str, int]:
    """Sum of ThingsBoard's "[<queue>] Stats:" log lines since `since` (epoch seconds).

    timeoutMsgs and failedMsgs count messages the queue gave up on (dropped); tmpTimeout and tmpFailed count
    attempts it retried. The dashboard cannot show these reliably (see build_dashboard.py), the log can.
    """
    import re
    from datetime import datetime, timezone

    iso = datetime.fromtimestamp(since, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out = compose("--profile", "full", "logs", "--no-color", "--since", iso, "thingsboard")
    totals = dict.fromkeys(QUEUE_STATS_KEYS, 0)
    for line in out.stdout.splitlines():
        if f"[{queue}] Stats:" in line:
            for key, value in re.findall(r"(\w+) = \[(\d+)\]", line):
                if key in totals:
                    totals[key] += int(value)
    return totals


def gateway_publish(entries: list[tuple[str, dict]]) -> None:
    """Publish (device, entry) pairs to ThingsBoard's gateway API at full speed, as the sink would without its rate
    limit: QoS 1, one at a time, each acknowledged before the next. One at a time, as the sink does, because
    ThingsBoard may hand messages in flight together to the rule engine in another order, and AlarmMapping then
    rightly drops an older event that arrives after a newer one (seen with 20 in flight: 191 of 200 alarms)."""
    import paho.mqtt.client as mqtt

    irc = import_script()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=unique("it-gateway"))
    client.username_pw_set(irc.setting("TB_GATEWAY_TOKEN"))
    client.connect(irc.setting("TB_HOST", "localhost"), int(irc.setting("TB_MQTT_PORT", "9883")))
    client.loop_start()
    try:
        for device, entry in entries:
            info = client.publish("v1/gateway/telemetry", json.dumps({device: [entry]}), qos=1)
            info.wait_for_publish(30)
            assert info.is_published()
    finally:
        client.loop_stop()
        client.disconnect()


def produce_telemetry(records: list[dict]) -> None:
    """telemetry_v1 records to telemetry.raw, keyed by asset as the bridge writes them."""
    from confluent_kafka.schema_registry.avro import AvroSerializer

    serialize = AvroSerializer(registry(), (REPO_ROOT / "schemas" / "telemetry_v1.avsc").read_text(encoding="utf-8"))
    ctx = SerializationContext("telemetry.raw", MessageField.VALUE)
    p = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    for r in records:
        p.produce("telemetry.raw", key=r["asset_id"].encode(), value=serialize(r, ctx))
        p.poll(0)
    assert p.flush(60) == 0


def load_config_value() -> float:
    """config/thingsboard.yaml: the sink's rate limit per asset."""
    from config.loader import load_config

    return load_config(env={}).thingsboard.sink_max_messages_per_s_per_asset


def mapping_failures_since(tb, since: float) -> list[dict]:
    """alarm_mapping_failed entries recorded on the pipeline gateway since `since` (epoch seconds)."""
    gw = tb.request("GET", "/api/tenant/devices", query={"deviceName": import_script().PIPELINE_GATEWAY}, allow_404=True)
    if gw is None:
        return []
    v = tb.request("GET", f"/api/plugins/telemetry/DEVICE/{gw['id']['id']}/values/timeseries",
                   query={"keys": "alarm_mapping_failed", "startTs": int(since * 1000), "endTs": int(time.time() * 1000) + 60_000,
                          "limit": 1000})
    return [json.loads(p["value"]) for p in v.get("alarm_mapping_failed", [])]
