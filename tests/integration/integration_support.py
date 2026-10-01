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


def volume_count() -> int:
    out = subprocess.run(["docker", "volume", "ls", "-q"], capture_output=True, text=True, check=True).stdout
    return len(out.split())


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# --- component processes ----------------------------------------------------------


class Proc:
    """A pipeline component running in the background, stderr captured as JSON lines."""

    def __init__(self, name: str, cmd: list[str], log_dir: Path):
        self.name = name
        self.log = log_dir / f"{name}.jsonl"
        self._file = self.log.open("w", encoding="utf-8")
        self.p = subprocess.Popen(cmd, cwd=REPO_ROOT, env=ENV, stdout=subprocess.DEVNULL, stderr=self._file)

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
