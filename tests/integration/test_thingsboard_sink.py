"""Integration, full profile only (not in CI): the ThingsBoard sink and AlarmMapping against a real ThingsBoard.

ThingsBoard keeps its data in a named volume across sessions, so every assertion is scoped to what this test
wrote: alarms by the alarm_id in their details, telemetry by time window, statistics by period timestamps.
Alarm timestamps generated here are in the future relative to any earlier run, because AlarmMapping applies an
alarm only if it is newer than the last one it applied for that device and type.

alarm_last is read with alarm_last_when, never once: AlarmMapping creates or clears the alarm first and saves
alarm_last after it (nodes "Create alarm" or "Clear alarm", then "Save alarm_last"), so an alarm can be visible
before alarm_last says so. Found in CI run 38486097882 on main: a test read alarm_last once, right after its alarm
became active, found no attribute yet and stopped halfway; the alarm it left active then failed the next test and
the session-end check.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import time
from datetime import datetime, timezone

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition

import integration_support as it
from config.loader import load_config

pytestmark = pytest.mark.full

spec = importlib.util.spec_from_file_location("import_rule_chains_sink_it",
                                              it.REPO_ROOT / "scripts/thingsboard/import_rule_chains.py")
irc = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = irc
spec.loader.exec_module(irc)

FAULTS = ("pump-01:imbalance:0",)
SEVERITY = {"WARNING": 4.5, "CRITICAL": 7.1}
VIBRATION = load_config(env={}).telemetry.channels["vibration_rms_mm_s"].label  # "Vibration RMS"


def alarm_type(severity: str) -> str:
    """The ThingsBoard alarm type the sink sets: "<channel label> high (warning|critical)"."""
    return f"{VIBRATION} high ({severity.lower()})"


@pytest.fixture(scope="module")
def tb(stack):
    client = irc.ThingsBoard(f"http://localhost:{irc.setting('TB_HTTP_PORT', '8080')}")
    client.login(irc.setting("TB_TENANT_USERNAME"), irc.setting("TB_TENANT_PASSWORD"))
    return client


def device(tb, name):
    return tb.request("GET", "/api/tenant/devices", query={"deviceName": name}, allow_404=True)


def alarms_of(tb, device_id, alarm_ids=None):
    out, page = [], 0
    while True:
        r = tb.request("GET", f"/api/alarm/DEVICE/{device_id}", query={"pageSize": 500, "page": page})
        out += r["data"]
        if not r["hasNext"]:
            break
        page += 1
    if alarm_ids is not None:
        out = [a for a in out if (a.get("details") or {}).get("alarm_id") in alarm_ids]
    return out


def wait_until(check, timeout=90, every=2.0):
    end = time.monotonic() + timeout
    while True:
        result = check()
        if result or time.monotonic() > end:
            return result
        time.sleep(every)


def fresh_sink_group(prefix="it-sink"):
    """A new sink group positioned at the end of both topics: it forwards only what the test writes next."""
    group = it.unique(prefix)
    for topic in ("telemetry.raw", "telemetry.alarms"):
        it.position_group_at_end(group, topic)
    return group


def sink_group_at(marks: dict[str, dict[int, int]], prefix: str) -> str:
    """A new sink group positioned at the given offsets of both topics."""
    group = it.unique(prefix)
    c = Consumer({"bootstrap.servers": it.BOOTSTRAP, "group.id": group, "enable.auto.commit": False})
    try:
        c.commit(offsets=[TopicPartition(t, p, o) for t, parts in marks.items() for p, o in parts.items()],
                 asynchronous=False)
    finally:
        c.close()
    return group


def test_sink_forwards_telemetry_with_the_sample_timestamps(stack, tb, log_dir):
    sink = it.thingsboard_sink(log_dir, "sink", fresh_sink_group())
    b = it.bridge(log_dir, "bridge", 50)
    started = int(time.time() * 1000)
    assert it.simulator(log_dir, "simulator", 50, seed=61).finish() == 0 and b.finish() == 0
    expected_types = {a.id: a.type for a in load_config(env={}).telemetry.assets}

    def all_arrived():
        rows = {}
        for asset, kind in expected_types.items():
            d = device(tb, asset)
            if not d:
                return None
            v = tb.request("GET", f"/api/plugins/telemetry/DEVICE/{d['id']['id']}/values/timeseries",
                           query={"keys": "vibration_rms_mm_s,seq", "startTs": started - 5000, "endTs": started + 120_000,
                                  "limit": 1000})
            if len(v.get("vibration_rms_mm_s", [])) < 10:
                return None
            rows[asset] = (d, v)
        return rows

    rows = wait_until(all_arrived)
    assert rows, "telemetry of all five assets did not arrive within 90 s"
    for asset, (d, values) in rows.items():
        profile = tb.request("GET", f"/api/deviceProfile/{d['deviceProfileId']['id']}")
        assert d["type"] == profile["name"] == expected_types[asset]
        assert profile["defaultQueueName"] == "PipelineSequential"
        assert len(values["vibration_rms_mm_s"]) == 10 and sorted(int(s["value"]) for s in values["seq"]) == list(range(10))
    assert sink.non_json_lines() == 0


def test_a_fault_raises_then_clears_the_alarm_in_thingsboard(stack, tb, log_dir, fresh_group):
    sink = it.thingsboard_sink(log_dir, "sink", fresh_sink_group())
    consumer = it.alarm_consumer(log_dir, "consumer", fresh_group())
    b = it.bridge(log_dir, "bridge", 200)
    assert it.simulator(log_dir, "sim-fault", 100, *FAULTS, seed=62).finish() == 0
    raised = wait_until(lambda: [e for e in consumer.find("alarm_raised") if e["asset_id"] == "pump-01"], timeout=60)
    assert raised, "the alarm consumer raised no alarm for pump-01"
    ids = {e["alarm_id"] for e in raised}
    pump = device(tb, "pump-01")
    active = wait_until(lambda: [a for a in alarms_of(tb, pump["id"]["id"], ids) if a["status"].startswith("ACTIVE")])
    assert active, f"no active ThingsBoard alarm for {ids}"
    for a in active:
        expected = next(e for e in raised if e["alarm_id"] == a["details"]["alarm_id"])
        assert (a["type"], a["severity"]) == (alarm_type(expected["severity"]), expected["severity"])
        assert a["details"]["threshold"] == SEVERITY[expected["severity"]]
        # Its own start: the RAISED sample's time, the alarm_id's last segment. An alarm absorbed into an older
        # active alarm of the same type (a ghost from an earlier session) would keep the older start.
        assert a["startTs"] == int(a["details"]["alarm_id"].rsplit("/", 1)[1])
    # An operator acknowledges the alarm in the dashboard (allowed there; clearing is not): the pipeline must
    # still clear it when the fault ends.
    tb.request("POST", f"/api/alarm/{active[0]['id']['id']}/ack")

    assert it.simulator(log_dir, "sim-normal", 100, seed=63).finish() == 0
    assert b.finish() == 0
    cleared = wait_until(lambda: all(a["status"].startswith("CLEARED") for a in alarms_of(tb, pump["id"]["id"], ids))
                         and len(alarms_of(tb, pump["id"]["id"], ids)) == len(ids))
    assert cleared, f"alarms not cleared: {[(a['type'], a['status']) for a in alarms_of(tb, pump['id']['id'], ids)]}"
    assert len(alarms_of(tb, pump["id"]["id"], ids)) == len(ids)  # one ThingsBoard alarm per alarm_id
    statuses = {a["id"]["id"]: a["status"] for a in alarms_of(tb, pump["id"]["id"], ids)}
    assert statuses[active[0]["id"]["id"]] == "CLEARED_ACK"  # acknowledged by hand, cleared by the pipeline
    assert all(a["startTs"] == int(a["details"]["alarm_id"].rsplit("/", 1)[1]) for a in alarms_of(tb, pump["id"]["id"], ids))
    assert all(p.non_json_lines() == 0 for p in (sink, consumer, b))


def alarm_event(alarm_id, asset, severity, state, raised_ms, ts_ms):
    return {"alarm_id": alarm_id, "asset_id": asset, "channel": "vibration_rms_mm_s", "severity": severity,
            "state": state, "ts": datetime.fromtimestamp(ts_ms / 1000, timezone.utc), "value": 9.0,
            "threshold": SEVERITY[severity], "hysteresis": 0.3, "persistence_samples": 5, "source_seq": 1,
            "synthetic": True}


def produce_alarm_events(events, extra=()):
    """Avro alarm_v1 records as the alarm consumer writes them, back to back, plus optional raw (key, value) records.

    Keyed by alarm_id and written to the asset's partition (AssetPartitioner in the consumer), so all of an
    asset's events keep their order; the extra records go to the same partition.
    """
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    schema = (it.REPO_ROOT / "schemas" / "alarm_v1.avsc").read_text(encoding="utf-8")
    serialize = AvroSerializer(it.registry(), schema)
    ctx = SerializationContext("telemetry.alarms", MessageField.VALUE)
    p = Producer({"bootstrap.servers": it.BOOTSTRAP, "enable.idempotence": True})
    partition = it.asset_partition(events[0]["asset_id"])
    for key, value in extra:
        p.produce("telemetry.alarms", key=key, value=value, partition=partition)
    for e in events:
        p.produce("telemetry.alarms", key=e["alarm_id"].encode(), value=serialize(e, ctx),
                  partition=it.asset_partition(e["asset_id"]))
    assert p.flush(30) == 0


def test_alarm_ordering_and_replay_leave_exactly_one_alarm_per_alarm_id(stack, tb, log_dir, offsets_mark):
    """Owner change 1: many RAISED/CLEARED pairs of one alarm type, back to back, then a full replay.

    40 WARNING alarms on pump-02, each RAISED then CLEARED, plus 8 CRITICAL alarms raised at the same
    timestamps as 8 of them; all produced to telemetry.alarms at once. A first sink forwards them; a second
    sink in a new group re-reads them all from the same offsets (a replay). Expected: exactly one ThingsBoard
    alarm per alarm_id, none active, alarm_last pointing at the newest alarm of each type, and both alarm
    keys kept in the time-series history where they share a timestamp.
    """
    # After any alarm an earlier run applied for pump-02. Coupling: these events stay in telemetry.alarms, and the
    # alarm consumer's replay guard (state_machine.js) takes the latest event of each alarm type as the point up to
    # which samples are already accounted for. An alarm consumer started later in the same session therefore ignores
    # pump-02 vibration samples stamped before base + ~40 s. No test sends pump-02 vibration alarms through the
    # consumer; one that does must use timestamps after that, or another asset or channel.
    base = int(time.time() * 1000) + 60_000
    events, ids, shared_ts = [], set(), []
    for i in range(40):
        r = base + i * 1000
        aid = f"pump-02/vibration_rms_mm_s/WARNING/{r}"
        events += [alarm_event(aid, "pump-02", "WARNING", "RAISED", r, r),
                   alarm_event(aid, "pump-02", "WARNING", "CLEARED", r, r + 500)]
        ids.add(aid)
        if i % 5 == 0:
            cid = f"pump-02/vibration_rms_mm_s/CRITICAL/{r}"
            events += [alarm_event(cid, "pump-02", "CRITICAL", "RAISED", r, r),
                       alarm_event(cid, "pump-02", "CRITICAL", "CLEARED", r, r + 400)]
            ids.add(cid)
            shared_ts.append(r)
    marker = (b"it-roll-marker/unknown-key", None)  # a tombstone with a key nothing knows: skipped by the sink
    produce_alarm_events(events, extra=[marker])

    # First pass, then a replay: two new groups, both starting at this test's mark on telemetry.alarms and at
    # the current end of telemetry.raw, so each forwards exactly the records produced above.
    marks = {"telemetry.alarms": offsets_mark["telemetry.alarms"], "telemetry.raw": it.watermarks("telemetry.raw")}
    pump = device(tb, "pump-02")

    def all_cleared():
        found = alarms_of(tb, pump["id"]["id"], ids)
        return len(found) == len(ids) and not any(a["status"].startswith("ACTIVE") for a in found)

    for name in ("first", "replay"):
        g = sink_group_at(marks, f"it-sink-{name}")
        sink = it.thingsboard_sink(log_dir, f"sink-{name}", g, count=len(events) + 1)
        assert sink.finish(180) == 0
        summary = sink.find("run_finished")[0]
        assert summary["alarm_events_forwarded"] == len(events) and summary["tombstones_skipped"] == 1
        assert summary.get("dead_lettered", 0) == 0
        # Checked after each pass, the first included: a replay must not hide what the first pass lost (Task 3.2
        # fix: before PipelineSequential, the replay made up for 236 messages ThingsBoard had dropped).
        settled = wait_until(all_cleared, timeout=120)
        alarms = alarms_of(tb, pump["id"]["id"], ids)
        active = sum(a["status"].startswith("ACTIVE") for a in alarms)
        assert settled, f"after the {name} pass: {len(alarms)} alarms for {len(ids)} alarm_ids, {active} active"
        assert sorted(a["details"]["alarm_id"] for a in alarms) == sorted(ids)  # exactly one per alarm_id
    newest = {"alarm_id": f"pump-02/vibration_rms_mm_s/WARNING/{base + 39_000}", "state": "CLEARED",
              "raised_ts": base + 39_000}
    last = alarm_last_when(tb, pump["id"]["id"], lambda v: v.get(alarm_type("WARNING")) == newest
                           and v.get(alarm_type("CRITICAL"), {}).get("raised_ts") == base + 35_000)
    assert last[alarm_type("WARNING")] == newest
    assert last[alarm_type("CRITICAL")]["raised_ts"] == base + 35_000
    for t in shared_ts:  # both alarm keys at the same timestamp: neither overwrote the other
        v = tb.request("GET", f"/api/plugins/telemetry/DEVICE/{pump['id']['id']}/values/timeseries",
                       query={"keys": "alarm_vibration_rms_mm_s_WARNING,alarm_vibration_rms_mm_s_CRITICAL",
                              "startTs": t, "endTs": t + 1, "limit": 10})
        assert {k: json.loads(v[k][0]["value"])["severity"] for k in v} == {
            "alarm_vibration_rms_mm_s_WARNING": "WARNING", "alarm_vibration_rms_mm_s_CRITICAL": "CRITICAL"}


KEYS = ["successfulMsgs", "failedMsgs", "tmpFailed", "timeoutMsgs", "ruleEngineException"]


def queue_stats(tb):
    """Latest rule-engine statistics of the running ThingsBoard container (one QUEUE_STATS entity per queue)."""
    container = it.compose("--profile", "full", "ps", "-q", "thingsboard").stdout.strip()
    hostname = subprocess.run(["docker", "inspect", "-f", "{{.Config.Hostname}}", container],
                              capture_output=True, text=True, check=True).stdout.strip()
    body = {"entityFilter": {"type": "entityType", "entityType": "QUEUE_STATS"},
            "entityFields": [{"type": "ENTITY_FIELD", "key": "name"}],
            "latestValues": [{"type": "TIME_SERIES", "key": k} for k in KEYS],
            "pageLink": {"pageSize": 200, "page": 0}}
    rows = tb.request("POST", "/api/entitiesQuery/find", body)["data"]
    out = {}
    for r in rows:
        name = r["latest"]["ENTITY_FIELD"]["name"]["value"]
        if name.endswith(f"_{hostname}"):
            series = r["latest"].get("TIME_SERIES", {})
            out[name] = {k: (series.get(k, {}).get("value"), int(series.get(k, {}).get("ts") or 0)) for k in KEYS}
    return out


def test_simulator_telemetry_causes_no_rule_engine_failure(stack, tb, log_dir):
    """Telemetry without a temperature field (the retired MaxTemp filter's risk) through the real Root and
    AlarmMapping: no failed message in a statistics period that lies inside the sending window.

    No alarm consumer runs here, so no alarm event is sent: the simulator's fault only drives the values, and
    AlarmMapping sees telemetry without alarm keys, the case it must pass through. Alarm events through the rule
    chains are covered by the other tests of this module."""
    sink = it.thingsboard_sink(log_dir, "sink", fresh_sink_group(), duration=150)
    b = it.bridge(log_dir, "bridge", 100_000, duration=140)
    started = int(time.time() * 1000)
    s = it.simulator(log_dir, "simulator", 100_000, *FAULTS, seed=64, rate=10)
    before = queue_stats(tb)
    period = wait_until(lambda: (lambda now: [v for v in now.values() if v["successfulMsgs"][1] > started + 1000
                                              and v["successfulMsgs"][0] not in (None, "", "0")] or None)(queue_stats(tb)),
                        timeout=130, every=5)
    assert period, f"no statistics period after {started} with processed messages; before: {before}"
    for stats in period:
        assert stats["failedMsgs"][0] in ("0", None) and stats["tmpFailed"][0] in ("0", None), stats
        assert stats["ruleEngineException"][1] < started, f"rule-engine exception during the run: {stats['ruleEngineException']}"
    for p in (s, b, sink):
        p.stop()


def test_alarm_start_time_is_the_event_time_not_the_arrival(stack, tb, log_dir, offsets_mark):
    """A RAISED event forwarded ten minutes late (a delayed replay): the ThingsBoard alarm starts at the event.

    startTs, which the dashboard shows as "Start time (event)", is the event's timestamp; createdTime is when
    ThingsBoard stored the alarm. Measured on 2026-10-03: startTs = event + 0.0 s, createdTime = event + 602 s.
    """
    event_ms = int(time.time() * 1000) - 600_000
    aid = f"motor-02/bearing_temperature_c/WARNING/{event_ms}"
    event = {**alarm_event(aid, "motor-02", "WARNING", "RAISED", event_ms, event_ms), "channel": "bearing_temperature_c"}
    produce_alarm_events([event])
    marks = {"telemetry.alarms": offsets_mark["telemetry.alarms"], "telemetry.raw": it.watermarks("telemetry.raw")}
    forwarded_at = int(time.time() * 1000)
    sink = it.thingsboard_sink(log_dir, "sink", sink_group_at(marks, "it-sink-late"), count=1)
    assert sink.finish(120) == 0
    motor = device(tb, "motor-02")
    [alarm] = wait_until(lambda: alarms_of(tb, motor["id"]["id"], {aid}) or None)
    assert alarm["startTs"] == event_ms
    assert alarm["createdTime"] >= forwarded_at  # stored on arrival, ten minutes after the event
    tb.request("DELETE", f"/api/alarm/{alarm['id']['id']}")  # leave no active test alarm in the shared volume


def alarm_last(tb, device_id):
    """The device's alarm_last attribute as it is now: {} if AlarmMapping has not saved one yet."""
    attr = tb.request("GET", f"/api/plugins/telemetry/DEVICE/{device_id}/values/attributes/SERVER_SCOPE",
                      query={"keys": "alarm_last"})
    return json.loads(attr[0]["value"]) if attr else {}


def alarm_last_when(tb, device_id, check, timeout=60):
    """alarm_last once `check` holds for it, polled: AlarmMapping saves it after the alarm (module docstring)."""
    last = wait_until(lambda: (lambda v: v if check(v) else None)(alarm_last(tb, device_id)), timeout=timeout)
    assert last, f"alarm_last of {device_id} did not reach the expected value in {timeout} s: {alarm_last(tb, device_id)}"
    return last


def forward(log_dir, name, events):
    """Produce alarm events to telemetry.alarms and forward exactly those with a new sink group."""
    marks = {"telemetry.alarms": it.watermarks("telemetry.alarms"), "telemetry.raw": it.watermarks("telemetry.raw")}
    produce_alarm_events(events)
    sink = it.thingsboard_sink(log_dir, name, sink_group_at(marks, f"it-sink-{name}"), count=len(events))
    assert sink.finish(120) == 0
    assert sink.find("run_finished")[0]["alarm_events_forwarded"] == len(events)


def test_make_up_reconciliation_clears_an_alarm_lost_with_kafka(stack, tb, log_dir):
    """Owner decision, Task 3.2 (ghost alarms, option A): the import run by make up reconciles ThingsBoard with
    telemetry.alarms.

    (a) X is raised through Kafka and the sink. (b) Kafka loses telemetry.alarms, as at every make up: the topic
    is recreated empty, and Y, another alarm of the asset, is raised on it. (c) The import: X is cleared with the
    reason in its details and one alarm_reconciled event; Y stays active; alarm_last says X is CLEARED. (d) A
    replay of X is dropped, and Z, a new alarm of X's type, is a new ThingsBoard alarm with its own start time
    (without reconciliation it would update X and keep X's start). Y and Z are then cleared through the pipeline.
    """
    asset, warning, critical = "motor-01", alarm_type("WARNING"), alarm_type("CRITICAL")
    now = int(time.time() * 1000)
    x, y, z = (f"{asset}/vibration_rms_mm_s/WARNING/{now}", f"{asset}/vibration_rms_mm_s/CRITICAL/{now + 1000}",
               f"{asset}/vibration_rms_mm_s/WARNING/{now + 2000}")
    motor = device(tb, asset)["id"]["id"]

    def one_active(alarm_id):
        return wait_until(lambda: [a for a in alarms_of(tb, motor, {alarm_id}) if a["status"].startswith("ACTIVE")] or None)

    # (a)
    forward(log_dir, "recon-a", [alarm_event(x, asset, "WARNING", "RAISED", now, now)])
    [ghost] = one_active(x)
    # (b)
    it.delete_topic("telemetry.alarms")
    it.provision()
    forward(log_dir, "recon-b", [alarm_event(y, asset, "CRITICAL", "RAISED", now + 1000, now + 1000)])
    [alarm_y] = one_active(y)
    # The import reads and rewrites alarm_last: let AlarmMapping save Y's entry first.
    alarm_last_when(tb, motor, lambda v: v.get(critical, {}).get("alarm_id") == y)
    # (c)
    result = it.run_module("scripts/thingsboard/import_rule_chains.py")
    assert result.returncode == 0, result.stderr[-800:]
    events = it.json_events(result.stderr)
    reconciled = [e for e in events if e["event"] == "alarm_reconciled"]
    assert [(e["asset"], e["type"], e["alarm_id"], e["thingsboard_alarm_id"]) for e in reconciled] == [
        (asset, warning, x, ghost["id"]["id"])]
    assert reconciled[0]["reason"] == "not in telemetry.alarms: Kafka starts empty at make up, its CLEARED event was lost"
    [done] = [e for e in events if e["event"] == "alarm_reconciliation_done"]
    assert (done["alarms_cleared"], done["alarms_active_in_topic"]) == (1, 1)
    [cleared] = alarms_of(tb, motor, {x})
    assert cleared["status"] == "CLEARED_UNACK" and cleared["details"]["cleared_by"] == "reconciliation"
    assert [a["status"] for a in alarms_of(tb, motor, {y})] == ["ACTIVE_UNACK"]
    last = alarm_last(tb, motor)
    assert last[warning] == {"alarm_id": x, "state": "CLEARED", "raised_ts": now}
    assert last[critical] == {"alarm_id": y, "state": "RAISED", "raised_ts": now + 1000}
    # (d) a replay of X (RAISED, CLEARED) is dropped as a duplicate; Z starts a new alarm of X's type
    forward(log_dir, "recon-d", [alarm_event(x, asset, "WARNING", "RAISED", now, now),
                                 alarm_event(x, asset, "WARNING", "CLEARED", now, now + 500),
                                 alarm_event(z, asset, "WARNING", "RAISED", now + 2000, now + 2000)])
    [alarm_z] = one_active(z)
    assert alarm_z["id"] != ghost["id"] and alarm_z["type"] == warning and alarm_z["startTs"] == now + 2000
    [x_after] = alarms_of(tb, motor, {x})  # still one alarm for X, cleared, with its own start
    assert (x_after["id"], x_after["status"], x_after["startTs"]) == (ghost["id"], "CLEARED_UNACK", now)
    alarm_last_when(tb, motor, lambda v: v.get(warning) == {"alarm_id": z, "state": "RAISED", "raised_ts": now + 2000})
    # Leave nothing active: Y and Z end through the pipeline, as any fault does.
    forward(log_dir, "recon-end", [alarm_event(y, asset, "CRITICAL", "CLEARED", now + 1000, now + 3000),
                                   alarm_event(z, asset, "WARNING", "CLEARED", now + 2000, now + 3000)])
    assert wait_until(lambda: all(a["status"] == "CLEARED_UNACK" for a in alarms_of(tb, motor, {y, z}))), \
        alarms_of(tb, motor, {y, z})
    assert not [a for a in it.active_pipeline_alarms(tb) if a["asset"] == asset]
