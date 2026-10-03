"""Set up ThingsBoard as the operations layer: rule chains, device profiles, gateway, dashboard.

    python scripts/thingsboard/import_rule_chains.py

Run by `make up` (full profile). Everything it writes is found by exact name
and updated in place, so re-running never duplicates anything.

Rule chains, from processing/thingsboard/rule-chains/*.json:
- Root Rule Chain: saves telemetry, then hands every saved telemetry message
  to AlarmMapping. Its "Firebase" REST node is the only Firebase sink kept
  from the coursework (owner decision, Task 3.2), off by default.
- AlarmMapping: turns the alarm lifecycle events the ThingsBoard sink sends
  into ThingsBoard alarms. It evaluates no threshold: the Node.js alarm
  consumer is the only alarm engine.
- Retired (RETIRED_CHAINS): CreateAndClearAlarms, the course chain with its
  own temperature threshold, and TempToFirebase and AlarmToFirebase, reached
  only through it. They are no longer imported and are deleted, by exact
  name, if an earlier import created them.
Placeholders in the exports:
- ``${FIREBASE_DB_URL}`` at the start of a Firebase REST node URL. With
  FIREBASE_ENABLED=true it is replaced by FIREBASE_DB_URL; that path is
  verified up to the URL written into ThingsBoard, never against a live
  database. Otherwise the Firebase leg is pruned before import
  (prune_firebase), so no node is left to fail on every message.
- ``${RULE_CHAIN_ID:<name>}`` in each rule-chain input node, replaced by the
  id that the named chain has in this ThingsBoard instance.

Device profiles: one per asset type in config/telemetry.yaml, on the
SequentialByOriginator queue. AlarmMapping reads and rewrites a device
attribute per event; that queue hands ThingsBoard a device's next message
only after the previous one has been fully processed, so those
read-modify-write steps never interleave (verified in Task 3.2: with the
default BURST queue the same events left wrong and duplicated alarms).
The ThingsBoard sink's gateway creates each asset's device with its type, so
the device gets its profile, and the queue, by name.

Devices: the pipeline gateway (PIPELINE_GATEWAY, flagged as a gateway, token
TB_GATEWAY_TOKEN) through which the sink publishes for every asset; and the
coursework demo device (DEMO_DEVICE, token TB_ACCESS_TOKEN).

Dashboard: processing/thingsboard/dashboards/operations.json, imported by
title. It names no device: its aliases select devices by the asset types.

Alarm reconciliation, last: every pipeline alarm active in ThingsBoard but not
in telemetry.alarms is cleared, so no alarm outlives the Kafka it came from
(see reconcile_alarms). It logs alarm_reconciled per alarm cleared and
alarm_reconciliation_done with the totals, also when they are 0.

Configuration comes from the environment, falling back to the repository's
.env file for anything unset: TB_HOST, TB_HTTP_PORT, TB_TENANT_USERNAME,
TB_TENANT_PASSWORD, TB_ACCESS_TOKEN, TB_GATEWAY_TOKEN, FIREBASE_ENABLED,
FIREBASE_DB_URL, KAFKA_BOOTSTRAP, SCHEMA_REGISTRY_URL. The script never prints the password, the session token,
a device token or a rendered Firebase URL.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaException, TopicPartition

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
from common import jsonlog  # noqa: E402
from config.loader import load_config  # noqa: E402

RULE_CHAIN_DIR = REPO_ROOT / "processing" / "thingsboard" / "rule-chains"
DASHBOARD_FILE = REPO_ROOT / "processing" / "thingsboard" / "dashboards" / "operations.json"

# Import order only matters for readability: all chains are created before
# any metadata is written, so every reference can be resolved.
CHAIN_FILES = [
    "alarm-mapping.json",
    "root-rule-chain.json",
]
# Chains earlier versions imported; deleted by exact name when present.
RETIRED_CHAINS = ("CreateAndClearAlarms", "TempToFirebase", "AlarmToFirebase")
ROOT_CHAIN = "Root Rule Chain"
DEMO_DEVICE = "DHT11 Demo Device"
PIPELINE_GATEWAY = "pipeline-gateway"
ALARM_QUEUE = "SequentialByOriginator"
FIREBASE_PLACEHOLDER = "${FIREBASE_DB_URL}"
CHAIN_REF = re.compile(r"^\$\{RULE_CHAIN_ID:(.+)\}$")
LOGIN_RETRY_S = 120

log = logging.getLogger("scripts.thingsboard.import_rule_chains")


class ImportFailed(Exception):
    pass


def setting(name: str, default: str | None = None) -> str | None:
    """Environment first, then the repository .env file, then the default."""
    if os.environ.get(name):
        return os.environ[name]
    env_file = REPO_ROOT / ".env"
    if env_file.is_file():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            key, sep, value = line.partition("=")
            if sep and key.strip() == name and not key.lstrip().startswith("#") and value.strip():
                return value.strip()
    return default


class ThingsBoard:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.token: str | None = None

    def request(self, method: str, path: str, body: dict | None = None, query: dict | None = None,
                allow_404: bool = False):
        url = self.base_url + path + ("?" + urllib.parse.urlencode(query) if query else "")
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.token:
            headers["X-Authorization"] = f"Bearer {self.token}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                raw = response.read()
        except urllib.error.HTTPError as e:
            if allow_404 and e.code == 404:
                return None
            detail = e.read().decode("utf-8", errors="replace")[:300]
            raise ImportFailed(f"{method} {path} -> HTTP {e.code}: {detail}") from None
        return json.loads(raw) if raw else None

    def login(self, username: str, password: str) -> None:
        deadline = time.monotonic() + LOGIN_RETRY_S
        while True:
            try:
                self.token = self.request("POST", "/api/auth/login", {"username": username, "password": password})["token"]
                log.info("thingsboard_logged_in", extra={"url": self.base_url, "username": username})
                return
            except (ImportFailed, urllib.error.URLError, OSError) as e:
                if isinstance(e, ImportFailed) and "HTTP 401" in str(e):
                    raise ImportFailed("login rejected: check TB_TENANT_USERNAME and TB_TENANT_PASSWORD") from None
                if time.monotonic() >= deadline:
                    raise ImportFailed(f"ThingsBoard not reachable at {self.base_url}: {e}") from None
                time.sleep(5)

    def find_rule_chain(self, name: str) -> dict | None:
        page = self.request("GET", "/api/ruleChains", query={"pageSize": 100, "page": 0, "type": "CORE", "textSearch": name})
        matches = [rc for rc in page["data"] if rc["name"] == name]
        if len(matches) > 1:
            raise ImportFailed(f"{len(matches)} rule chains named {name!r}; resolve by hand")
        return matches[0] if matches else None


def render_nodes(nodes: list[dict], chain_ids: dict[str, str], firebase_url: str | None) -> tuple[list[dict], int]:
    rendered, unresolved = [], 0
    for node in nodes:
        node = json.loads(json.dumps(node))  # deep copy
        cfg = node.get("configuration") or {}
        url = cfg.get("restEndpointUrlPattern")
        if isinstance(url, str) and url.startswith(FIREBASE_PLACEHOLDER):
            if firebase_url:
                cfg["restEndpointUrlPattern"] = firebase_url.rstrip("/") + url[len(FIREBASE_PLACEHOLDER):]
            else:
                unresolved += 1
        ref = cfg.get("ruleChainId")
        if isinstance(ref, str):
            m = CHAIN_REF.match(ref)
            if not m:
                raise ImportFailed(f"node {node['name']!r}: ruleChainId is not a ${{RULE_CHAIN_ID:<name>}} placeholder")
            if m.group(1) not in chain_ids:
                raise ImportFailed(f"node {node['name']!r} references unknown chain {m.group(1)!r}")
            cfg["ruleChainId"] = chain_ids[m.group(1)]
        rendered.append(node)
    return rendered, unresolved


REST_NODE = "org.thingsboard.rule.engine.rest.TbRestApiCallNode"
CHAIN_INPUT_NODE = "org.thingsboard.rule.engine.flow.TbRuleChainInputNode"


def _drop_nodes(name: str, metadata: dict, drop: set[int]) -> dict:
    """metadata without the nodes at the indices in drop, connections re-indexed, theirs removed."""
    keep = [i for i in range(len(metadata["nodes"])) if i not in drop]
    new_index = {old: new for new, old in enumerate(keep)}
    first = metadata.get("firstNodeIndex")
    if keep and first is not None and first not in new_index:
        raise ImportFailed(f"chain {name!r}: its first node would be removed")
    return {
        **metadata,
        "nodes": [metadata["nodes"][i] for i in keep],
        "connections": [{**c, "fromIndex": new_index[c["fromIndex"]], "toIndex": new_index[c["toIndex"]]}
                        for c in metadata.get("connections") or []
                        if c["fromIndex"] in new_index and c["toIndex"] in new_index],
        "firstNodeIndex": new_index.get(first) if keep else None,
    }


def prune_firebase(exports: list[dict]) -> tuple[list[dict], list[str], dict[str, int]]:
    """The exports as imported when Firebase is disabled.

    1. Every REST node whose URL starts with ${FIREBASE_DB_URL} is removed with
       its connections: disabling a node in place does not stop it running.
    2. A chain left with no node is not imported.
    3. Every rule-chain input node that points to a chain not imported is
       removed with its connections, and steps 2-3 repeat until nothing
       changes, so no reference is left dangling.
    Returns (exports to import, names of the chains left out, counts removed).
    Pure: the exports passed in are not modified.
    """
    chains = {e["ruleChain"]["name"]: json.loads(json.dumps(e)) for e in exports}
    removed = {"rest_nodes": 0, "rule_chain_input_nodes": 0}
    for name, export in chains.items():
        md = export["metadata"]
        drop = {i for i, n in enumerate(md["nodes"]) if n["type"] == REST_NODE
                and str((n.get("configuration") or {}).get("restEndpointUrlPattern", "")).startswith(FIREBASE_PLACEHOLDER)}
        removed["rest_nodes"] += len(drop)
        export["metadata"] = _drop_nodes(name, md, drop)

    skipped: list[str] = []
    while True:
        empty = [n for n, e in chains.items() if n not in skipped and not e["metadata"]["nodes"]]
        if not empty:
            break
        skipped += empty
        for name, export in chains.items():
            if name in skipped:
                continue
            md = export["metadata"]
            drop = set()
            for i, node in enumerate(md["nodes"]):
                m = CHAIN_REF.match(str((node.get("configuration") or {}).get("ruleChainId", "")))
                if node["type"] == CHAIN_INPUT_NODE and m and m.group(1) in skipped:
                    drop.add(i)
            removed["rule_chain_input_nodes"] += len(drop)
            export["metadata"] = _drop_nodes(name, md, drop)
    if ROOT_CHAIN in skipped:
        raise ImportFailed(f"pruning the Firebase leg would leave {ROOT_CHAIN!r} empty")
    kept = [e for n, e in chains.items() if n not in skipped]
    return kept, skipped, removed


def import_chains(tb: ThingsBoard, firebase_url: str | None) -> dict[str, str]:
    exports = [json.loads((RULE_CHAIN_DIR / f).read_text(encoding="utf-8")) for f in CHAIN_FILES]
    skipped: list[str] = []
    if not firebase_url:
        exports, skipped, removed = prune_firebase(exports)
        log.info("firebase_leg_pruned", extra={
            "reason": "FIREBASE_ENABLED is not true", "rest_nodes_removed": removed["rest_nodes"],
            "rule_chain_input_nodes_removed": removed["rule_chain_input_nodes"], "chains_not_imported": skipped,
        })

    chain_ids: dict[str, str] = {}
    for export in exports:
        rc = export["ruleChain"]
        existing = tb.find_rule_chain(rc["name"])
        if existing:
            chain_ids[rc["name"]] = existing["id"]["id"]
            log.info("rule_chain_exists", extra={"rule_chain": rc["name"], "id": existing["id"]["id"], "action": "update"})
        else:
            created = tb.request("POST", "/api/ruleChain", {
                "name": rc["name"], "type": rc.get("type", "CORE"), "debugMode": False,
                "configuration": rc.get("configuration"), "additionalInfo": rc.get("additionalInfo"),
            })
            chain_ids[rc["name"]] = created["id"]["id"]
            log.info("rule_chain_created", extra={"rule_chain": rc["name"], "id": created["id"]["id"]})

    for export in exports:
        name = export["ruleChain"]["name"]
        md = export["metadata"]
        nodes, unresolved = render_nodes(md["nodes"], chain_ids, firebase_url)
        if unresolved:  # prune_firebase removes every such node; one left would fail on every message
            raise ImportFailed(f"chain {name!r}: {unresolved} Firebase REST node(s) left unresolved")
        current = tb.request("GET", f"/api/ruleChain/{chain_ids[name]}/metadata")
        tb.request("POST", "/api/ruleChain/metadata", {
            "ruleChainId": {"entityType": "RULE_CHAIN", "id": chain_ids[name]},
            "version": current.get("version"),
            "firstNodeIndex": md["firstNodeIndex"],
            "nodes": nodes,
            "connections": md.get("connections") or [],
            "ruleChainConnections": None,
        }, query={"updateRelated": "true"})
        log.info("rule_chain_written", extra={"rule_chain": name, "nodes": len(nodes),
                                              "connections": len(md.get("connections") or [])})

    # Chains this import owns but left out (Firebase disabled) or retired are
    # deleted if an earlier import created them, matched by exact name only,
    # and only now: the chains that referenced them have just been rewritten
    # without those references.
    reasons = {**{name: "retired" for name in RETIRED_CHAINS}, **{name: "FIREBASE_ENABLED is not true" for name in skipped}}
    for name, reason in reasons.items():
        if name in chain_ids:
            continue
        existing = tb.find_rule_chain(name)
        if existing:
            tb.request("DELETE", f"/api/ruleChain/{existing['id']['id']}")
            log.info("rule_chain_deleted", extra={"rule_chain": name, "id": existing["id"]["id"], "reason": reason})

    tb.request("POST", f"/api/ruleChain/{chain_ids[ROOT_CHAIN]}/root")
    log.info("rule_chain_set_root", extra={"rule_chain": ROOT_CHAIN})
    return chain_ids


def ensure_device(tb: ThingsBoard, token: str, name: str = DEMO_DEVICE, gateway: bool = False) -> None:
    """The named device exists with this access token (and the gateway flag, for the pipeline gateway)."""
    device = tb.request("GET", "/api/tenant/devices", query={"deviceName": name}, allow_404=True)
    if device is None:
        body = {"name": name, **({"additionalInfo": {"gateway": True}} if gateway else {})}
        tb.request("POST", "/api/device", body, query={"accessToken": token})
        log.info("device_created", extra={"device": name, "gateway": gateway})
        return
    if gateway and not (device.get("additionalInfo") or {}).get("gateway"):
        device["additionalInfo"] = {**(device.get("additionalInfo") or {}), "gateway": True}
        tb.request("POST", "/api/device", device)
        log.info("device_gateway_flag_set", extra={"device": name})
    credentials = tb.request("GET", f"/api/device/{device['id']['id']}/credentials")
    if credentials.get("credentialsType") == "ACCESS_TOKEN" and credentials.get("credentialsId") == token:
        log.info("device_token_ok", extra={"device": name})
        return
    credentials.update({"credentialsType": "ACCESS_TOKEN", "credentialsId": token, "credentialsValue": None})
    tb.request("POST", "/api/device/credentials", credentials)
    log.info("device_token_updated", extra={"device": name})


def find_device_profile(tb: ThingsBoard, name: str) -> dict | None:
    page = tb.request("GET", "/api/deviceProfiles", query={"pageSize": 100, "page": 0, "textSearch": name})
    return next((p for p in page["data"] if p["name"] == name), None)


def ensure_device_profiles(tb: ThingsBoard, asset_types: list[str]) -> None:
    """One device profile per asset type, on the SequentialByOriginator queue (see the module docstring)."""
    base = None
    for name in asset_types:
        profile = find_device_profile(tb, name)
        if profile is None:
            if base is None:
                default_id = tb.request("GET", "/api/deviceProfileInfo/default")["id"]["id"]
                base = tb.request("GET", f"/api/deviceProfile/{default_id}")
            body = {k: v for k, v in base.items()
                    if k not in ("id", "createdTime", "default", "version", "tenantId", "externalId", "name")}
            body.update({"name": name, "default": False, "defaultQueueName": ALARM_QUEUE, "defaultRuleChainId": None,
                         "description": f"Pipeline asset type {name!r} (config/telemetry.yaml)."})
            created = tb.request("POST", "/api/deviceProfile", body)
            log.info("device_profile_created", extra={"device_profile": name, "id": created["id"]["id"], "queue": ALARM_QUEUE})
        elif profile.get("defaultQueueName") != ALARM_QUEUE:
            profile["defaultQueueName"] = ALARM_QUEUE
            tb.request("POST", "/api/deviceProfile", profile)
            log.info("device_profile_updated", extra={"device_profile": name, "queue": ALARM_QUEUE})
        else:
            log.info("device_profile_ok", extra={"device_profile": name, "queue": ALARM_QUEUE})


def import_dashboard(tb: ThingsBoard, path: Path = DASHBOARD_FILE) -> str:
    """Create the dashboard, or overwrite its content if one with the same title exists; return its id."""
    export = json.loads(path.read_text(encoding="utf-8"))
    title = export["title"]
    page = tb.request("GET", "/api/tenant/dashboards", query={"pageSize": 100, "page": 0, "textSearch": title})
    existing = next((d for d in page["data"] if d["title"] == title), None)
    body = {"title": title, "configuration": export["configuration"]}
    if existing:
        current = tb.request("GET", f"/api/dashboard/{existing['id']['id']}")
        body.update({"id": current["id"], "version": current.get("version")})
    saved = tb.request("POST", "/api/dashboard", body)
    log.info("dashboard_written", extra={"dashboard": title, "id": saved["id"]["id"],
                                         "action": "update" if existing else "create"})
    return saved["id"]["id"]


# --- alarm reconciliation ---------------------------------------------------------------------------------
#
# ThingsBoard keeps its alarms in a volume; Kafka starts empty at every `make up`. An alarm active in
# ThingsBoard whose CLEARED event was lost with Kafka would stay active forever (a "ghost") and absorb the
# next alarm of its type on that asset: ThingsBoard's Create Alarm updates an active alarm of the same type
# instead of creating one, so the new alarm would keep the ghost's start time. reconcile_alarms clears, through
# the REST API, every active pipeline alarm that is not active in telemetry.alarms, and records why in its
# details. Pipeline alarms: alarms on devices of the configured asset types whose details carry an alarm_id
# (set by AlarmMapping); anything else is left alone.
#
# It also keeps AlarmMapping's attribute alarm_last coherent: an entry still RAISED for an alarm that is no
# longer active becomes CLEARED, keeping its alarm_id and raised_ts. A later alarm of that type (larger
# raised_ts) is applied and creates a new ThingsBoard alarm; a replayed RAISED or CLEARED of the reconciled
# alarm (same raised_ts) is dropped as a duplicate instead of re-creating or re-clearing it. Deleting the
# entry would let such a replay re-create the alarm.
#
# make up runs it before any ThingsBoard sink starts. Run while a sink is writing, a rule-engine update of
# alarm_last could interleave with this script's read-modify-write of the same attribute.

RECONCILED_BY = "reconciliation"
UNKNOWN = "UNKNOWN"  # latest record of an alarm_id that could not be decoded: treated as active, never cleared
KAFKA_READ_TIMEOUT_S = 60


def read_alarm_topic(consumer, topic: str, decode, timeout: float = KAFKA_READ_TIMEOUT_S) -> list[tuple[str, str | None]]:
    """(alarm_id, state) of every record of the compacted alarm topic, up to its end offsets at the call.

    state is None for a tombstone and UNKNOWN for a value decode() rejects. Records of one alarm_id share a
    partition, so their order in the list is their order in the topic.
    """
    found = consumer.list_topics(topic, timeout=10).topics.get(topic)
    if found is None or found.error is not None:
        raise ImportFailed(f"Kafka topic {topic!r} not found; run `python -m broker.kafka.provision` first")
    ends = {}
    for p in sorted(found.partitions):
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        if high > low:
            ends[p] = high
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in ends])
    records: list[tuple[str, str | None]] = []
    deadline = time.monotonic() + timeout
    while ends:
        if time.monotonic() > deadline:
            raise ImportFailed(f"Kafka topic {topic!r}: end offsets not reached in {timeout:.0f} s")
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise ImportFailed(f"Kafka topic {topic!r}: {msg.error()}")
        if msg.key() is not None:
            key = msg.key().decode("utf-8", errors="replace")
            if msg.value() is None:
                records.append((key, None))
            else:
                try:
                    records.append((key, decode(msg.value())["state"]))
                except Exception as e:  # any decode failure: the alarm's state is unknown, so it is kept
                    log.warning("alarm_record_undecodable", extra={"topic": topic, "partition": msg.partition(),
                                                                   "offset": msg.offset(), "error": type(e).__name__})
                    records.append((key, UNKNOWN))
        if msg.partition() in ends and msg.offset() + 1 >= ends[msg.partition()]:
            del ends[msg.partition()]
    return records


def latest_alarm_states(records: list[tuple[str, str | None]]) -> dict[str, str]:
    """alarm_id -> state of its latest event; a tombstone removes the alarm_id."""
    states: dict[str, str] = {}
    for key, state in records:
        if state is None:
            states.pop(key, None)
        else:
            states[key] = state
    return states


def _pages(tb: ThingsBoard, path: str, query: dict):
    page = 0
    while True:
        data = tb.request("GET", path, query={**query, "pageSize": 100, "page": page})
        yield from data["data"]
        if not data.get("hasNext"):
            return
        page += 1


def _is_active(states: dict[str, str], alarm_id: str) -> bool:
    return states.get(alarm_id) in ("RAISED", UNKNOWN)


def reconcile_alarms(tb: ThingsBoard, asset_types: list[str], states: dict[str, str], topic: str) -> dict[str, int]:
    """Clear the pipeline alarms of ThingsBoard not active in the alarm topic; make alarm_last agree. Idempotent."""
    totals = {"devices": 0, "alarms_cleared": 0, "alarm_last_entries_cleared": 0}
    for asset_type in asset_types:
        for device in _pages(tb, "/api/tenant/devices", {"type": asset_type}):
            totals["devices"] += 1
            asset, device_id = device["name"], device["id"]["id"]
            # Listed in full first: clearing while paging through the active alarms would shift the pages.
            for alarm in list(_pages(tb, f"/api/alarm/DEVICE/{device_id}", {"searchStatus": "ACTIVE"})):
                alarm_id = (alarm.get("details") or {}).get("alarm_id")
                if not alarm_id or _is_active(states, alarm_id):
                    continue
                reason = (f"CLEARED in {topic}, not yet applied by the sink" if states.get(alarm_id) == "CLEARED"
                          else f"not in {topic}: Kafka starts empty at make up, its CLEARED event was lost")
                tb.request("POST", "/api/alarm", {**alarm, "details": {**alarm["details"], "cleared_by": RECONCILED_BY,
                                                                        "reconciliation_reason": reason}})
                tb.request("POST", f"/api/alarm/{alarm['id']['id']}/clear")
                totals["alarms_cleared"] += 1
                log.info("alarm_reconciled", extra={"asset": asset, "type": alarm["type"], "alarm_id": alarm_id,
                                                    "thingsboard_alarm_id": alarm["id"]["id"], "reason": reason})
            path = f"/api/plugins/telemetry/DEVICE/{device_id}"
            attrs = tb.request("GET", f"{path}/values/attributes/SERVER_SCOPE", query={"keys": "alarm_last"}) or []
            raw = next((a["value"] for a in attrs if a["key"] == "alarm_last"), None)
            if raw is None:
                continue
            last = json.loads(raw)
            stale = [t for t, e in last.items() if e.get("state") == "RAISED" and not _is_active(states, e.get("alarm_id"))]
            if stale:
                for t in stale:
                    last[t] = {**last[t], "state": "CLEARED"}
                    log.info("alarm_last_reconciled", extra={"asset": asset, "type": t, "alarm_id": last[t]["alarm_id"]})
                tb.request("POST", f"{path}/attributes/SERVER_SCOPE", {"alarm_last": json.dumps(last)})
                totals["alarm_last_entries_cleared"] += len(stale)
    log.info("alarm_reconciliation_done", extra={**totals, "alarms_active_in_topic": sum(
        1 for s in states.values() if s == "RAISED"), "topic": topic})
    return totals


# Real Kafka and Schema Registry clients for read_alarm_topic; exercised by the integration tests, like main().
def read_alarms(topic: str) -> list[tuple[str, str | None]]:  # pragma: no cover
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    deserializer = AvroDeserializer(SchemaRegistryClient({"url": setting("SCHEMA_REGISTRY_URL", "http://localhost:8081")}))
    context = SerializationContext(topic, MessageField.VALUE)
    consumer = Consumer({"bootstrap.servers": setting("KAFKA_BOOTSTRAP", "localhost:9092"),
                         "group.id": "thingsboard-import-reconciliation", "enable.auto.commit": False,
                         "logger": logging.getLogger("librdkafka")})
    try:
        return read_alarm_topic(consumer, topic, lambda value: deserializer(value, context))
    finally:
        consumer.close()


# Orchestration entry point: wires real MQTT, Kafka and Schema Registry
# clients and runs until stopped. Exercised by the integration tests
# (Task 2.5), not by unit tests; excluded from unit coverage by owner
# decision (Task 2.4), like read_alarms, the Kafka client it uses.
def main() -> int:  # pragma: no cover
    jsonlog.configure("INFO")
    username = setting("TB_TENANT_USERNAME")
    tenant_pw = setting("TB_TENANT_PASSWORD")
    token = setting("TB_ACCESS_TOKEN")
    gateway_token = setting("TB_GATEWAY_TOKEN")
    missing = [n for n, v in (("TB_TENANT_USERNAME", username), ("TB_TENANT_PASSWORD", tenant_pw),
                              ("TB_ACCESS_TOKEN", token), ("TB_GATEWAY_TOKEN", gateway_token)) if not v]
    if missing:
        log.error("settings_missing", extra={"settings": missing, "hint": "set them in .env (see .env.example)"})
        return 1

    firebase_enabled = (setting("FIREBASE_ENABLED", "false") or "").lower() == "true"
    firebase_url = setting("FIREBASE_DB_URL") if firebase_enabled else None
    if firebase_enabled and not firebase_url:
        log.error("firebase_url_missing", extra={"detail": "FIREBASE_ENABLED=true but FIREBASE_DB_URL is empty"})
        return 1

    tb = ThingsBoard(f"http://{setting('TB_HOST', 'localhost')}:{setting('TB_HTTP_PORT', '8080')}")
    try:
        tb.login(username, tenant_pw)
        cfg = load_config()
        asset_types = sorted({a.type for a in cfg.telemetry.assets})
        ensure_device_profiles(tb, asset_types)
        import_chains(tb, firebase_url)
        ensure_device(tb, token)
        ensure_device(tb, gateway_token, PIPELINE_GATEWAY, gateway=True)
        import_dashboard(tb)
        topic = cfg.topics.kafka["telemetry_alarms"].name
        reconcile_alarms(tb, asset_types, latest_alarm_states(read_alarms(topic)), topic)
    except (ImportFailed, KafkaException) as e:
        log.error("import_failed", extra={"error": str(e)})
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
