"""Integration, full profile only: delivery into ThingsBoard's rule engine (Task 3.2 fix).

CI run 37168739715 lost alarm events inside ThingsBoard: one device's messages are processed one at a time, about
150 ms each with ThingsBoard's default SQL batch delays, and the SequentialByOriginator queue gave a batch 2 s and
dropped what was left after three retries. The fix: the PipelineSequential queue (60 s per batch, 20 retries),
SQL batch delays at 25 ms (about 50 ms per message), the sink's rate limit per asset (config/thingsboard.yaml),
and Failure branches in AlarmMapping. These tests check each part against a real ThingsBoard, with drops and
retries read from ThingsBoard's own queue statistics (its log).

Each test uses an alarm type no other test uses, with timestamps of the current time, so AlarmMapping's
"newer than the last applied" rule never makes one test depend on another.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone

import pytest

import integration_support as it
from test_thingsboard_sink import alarm_event, alarm_last, alarms_of, device, produce_alarm_events, sink_group_at, \
    wait_until

pytestmark = pytest.mark.full

LABELS = {"bearing_temperature_c": "Bearing temperature", "motor_current_a": "Motor current", "speed_rpm": "Speed",
          "vibration_rms_mm_s": "Vibration RMS"}  # config/telemetry.yaml
THRESHOLD = 4.5


@pytest.fixture(scope="module")
def tb(stack):
    return it.thingsboard()


def event(asset, channel, raised_ms, state, ts_ms, severity="WARNING"):
    aid = f"{asset}/{channel}/{severity}/{raised_ms}"
    return {**alarm_event(aid, asset, severity, state, raised_ms, ts_ms), "channel": channel, "threshold": THRESHOLD}


def gateway_entry(e, severity=None):
    """The sink's gateway entry for an alarm event (processing/thingsboard_sink.alarm_entry)."""
    body = {"alarm_id": e["alarm_id"], "state": e["state"], "severity": severity or e["severity"], "channel": e["channel"],
            "value": e["value"], "threshold": e["threshold"], "raised_ts": int(e["alarm_id"].rsplit("/", 1)[1]),
            "type": f"{LABELS[e['channel']]} high ({e['severity'].lower()})"}
    ts = int(e["ts"].timestamp() * 1000)
    return e["asset_id"], {"ts": ts, "values": {f"alarm_{e['channel']}_{e['severity']}": json.dumps(body)}}


def pairs(asset, channel, n, start_ms):
    events, ids = [], []
    for i in range(n):
        r = start_ms + i * 1000
        e = event(asset, channel, r, "RAISED", r)
        events += [e, event(asset, channel, r, "CLEARED", r + 500)]
        ids.append(e["alarm_id"])
    return events, ids


def settled(tb, asset, ids, timeout=180):
    """All alarms of ids exist, once each, and none is active."""
    dev = device(tb, asset)["id"]["id"]

    def check():
        found = alarms_of(tb, dev, set(ids))
        return found if len(found) == len(ids) and not any(a["status"].startswith("ACTIVE") for a in found) else None
    return wait_until(check, timeout=timeout, every=3) or alarms_of(tb, dev, set(ids))


def stats_after(since):
    time.sleep(it.QUEUE_STATS_WAIT_S)
    return it.queue_stats_since(since)


def test_a_burst_straight_to_thingsboard_loses_nothing(stack, tb):
    """The queue as the safety net: 400 alarm events of one asset published at full speed, bypassing the sink's rate
    limit, forwarded once (no replay to hide a loss). With SequentialByOriginator (2 s per batch, 3 retries) this
    lost most of them on any machine; PipelineSequential must apply all, with no drop and no retried timeout."""
    since = time.time()
    events, ids = pairs("motor-01", "motor_current_a", 200, int(since * 1000) - 600_000)
    it.gateway_publish([gateway_entry(e) for e in events])
    found = settled(tb, "motor-01", ids)
    assert len(found) == len(ids) and sorted(a["details"]["alarm_id"] for a in found) == sorted(ids)
    assert not any(a["status"].startswith("ACTIVE") for a in found)
    stats = stats_after(since)
    assert stats["successfulMsgs"] >= len(events)
    assert (stats["timeoutMsgs"], stats["failedMsgs"], stats["tmpTimeout"], stats["tmpFailed"]) == (0, 0, 0, 0), stats


def test_a_mapping_failure_is_recorded_and_does_not_hold_back_the_asset(stack, tb):
    """A message AlarmMapping cannot apply (a severity Create alarm rejects, published straight to the gateway: the
    sink's schema would refuse it) goes to the Failure branch: one alarm_mapping_failed entry on the pipeline gateway,
    no retry, and the asset's next events applied."""
    since = time.time()
    now = int(since * 1000) - 60_000
    bad = event("pump-01", "speed_rpm", now, "RAISED", now)
    good = [event("pump-01", "speed_rpm", now + 1000, "RAISED", now + 1000),
            event("pump-01", "speed_rpm", now + 1000, "CLEARED", now + 1500)]
    gw = device(tb, it.import_script().PIPELINE_GATEWAY)["id"]["id"]
    path = f"/api/plugins/telemetry/DEVICE/{gw}"
    try:
        it.gateway_publish([gateway_entry(bad, severity="BOGUS"), *(gateway_entry(e) for e in good)])
        found = settled(tb, "pump-01", [good[0]["alarm_id"]], timeout=60)
        assert [a["status"] for a in found] == ["CLEARED_UNACK"]  # applied after the failure
        assert alarms_of(tb, device(tb, "pump-01")["id"]["id"], {bad["alarm_id"]}) == []
        window = {"startTs": int(since * 1000), "endTs": int(time.time() * 1000) + 60_000}
        points = tb.request("GET", f"{path}/values/timeseries", query={"keys": "alarm_mapping_failed", **window, "limit": 10})
        [failure] = [json.loads(p["value"]) for p in points.get("alarm_mapping_failed", [])]
        assert (failure["asset"], failure["alarm_id"], failure["alarm_type"]) == \
            ("pump-01", bad["alarm_id"], "Speed high (warning)")
        stats = stats_after(since)
        assert (stats["failedMsgs"], stats["tmpFailed"], stats["timeoutMsgs"]) == (0, 0, 0), stats  # handled, not retried
    finally:
        # This failure was provoked: remove it, pass or fail, so the session's check of mapping failures only
        # sees failures nobody provoked.
        window = {"startTs": int(since * 1000), "endTs": int(time.time() * 1000) + 60_000}
        tb.request("DELETE", f"{path}/timeseries/delete",
                   query={"keys": "alarm_mapping_failed", "deleteAllDataForKeys": "false", **window})


def test_an_event_applied_again_after_a_partial_run_is_absorbed(stack, tb, log_dir, offsets_mark):
    """Design item 5: a retry that re-runs an event whose alarm was created but whose alarm_last was not saved.
    AlarmMapping applies it again; Create alarm updates the active alarm of that type: still one alarm."""
    now = int(time.time() * 1000) - 60_000
    raised = event("compressor-01", "speed_rpm", now, "RAISED", now)
    marks = {"telemetry.alarms": offsets_mark["telemetry.alarms"], "telemetry.raw": it.watermarks("telemetry.raw")}
    produce_alarm_events([raised])
    assert it.thingsboard_sink(log_dir, "sink-1", sink_group_at(marks, "it-sink-partial"), count=1).finish(120) == 0
    dev = device(tb, "compressor-01")["id"]["id"]
    active = lambda: [a for a in alarms_of(tb, dev, {raised["alarm_id"]}) if a["status"].startswith("ACTIVE")] or None  # noqa: E731
    [first] = wait_until(active)
    # The partial run: alarm created, alarm_last not saved. Put alarm_last back as it was before this event.
    last = alarm_last(tb, dev)
    before = {k: v for k, v in last.items() if k != "Speed high (warning)"}
    tb.request("POST", f"/api/plugins/telemetry/DEVICE/{dev}/attributes/SERVER_SCOPE", {"alarm_last": json.dumps(before)})
    it.gateway_publish([gateway_entry(raised)])  # the retry
    assert wait_until(lambda: alarm_last(tb, dev).get("Speed high (warning)", {}).get("alarm_id") == raised["alarm_id"])
    again = alarms_of(tb, dev, {raised["alarm_id"]})
    assert [a["id"] for a in again] == [first["id"]] and again[0]["status"].startswith("ACTIVE")  # one alarm, the same
    it.gateway_publish([gateway_entry(event("compressor-01", "speed_rpm", now, "CLEARED", now + 500))])
    assert [a["status"] for a in settled(tb, "compressor-01", [raised["alarm_id"]], timeout=60)] == ["CLEARED_UNACK"]


def test_a_backlog_of_one_asset_reaches_thingsboard_without_a_drop(stack, tb, log_dir, offsets_mark):
    """Owner condition (Task 3.2 fix): 3,000 telemetry records and 100 alarm pairs of one asset waiting in Kafka, as
    after a ThingsBoard outage, forwarded by one sink with the configured rate limit. Everything arrives, with no
    message dropped and no timeout retried: this fails if config/thingsboard.yaml's limit and the SQL batch delays
    in docker-compose.yml stop being a safe pair."""
    asset, n = "pump-01", 3000
    start = datetime.now(timezone.utc) - timedelta(seconds=n + 600)
    # seq values of this run only (the current millisecond onward): the time window also holds pump-01 telemetry
    # other tests and sessions wrote at real time (local run 1 of the series: 1,328 such points).
    base = int(time.time() * 1000)
    records = [{"asset_id": asset, "asset_type": "pump", "seq": base + i, "ts": start + timedelta(seconds=i), "synthetic": True,
                "bearing_temperature_c": 53.0, "vibration_rms_mm_s": 2.6, "motor_current_a": 25.0, "speed_rpm": 2962.0,
                "injected_fault": None} for i in range(n)]
    events, ids = pairs(asset, "bearing_temperature_c", 100, int(time.time() * 1000) - 300_000)
    marks = {"telemetry.alarms": offsets_mark["telemetry.alarms"], "telemetry.raw": it.watermarks("telemetry.raw")}
    it.produce_telemetry(records)
    produce_alarm_events(events)
    since = time.time()
    sink = it.thingsboard_sink(log_dir, "sink-backlog", sink_group_at(marks, "it-sink-backlog"), duration=900,
                               count=n + len(events))
    assert sink.finish(900) == 0
    elapsed = time.time() - since
    [summary] = sink.find("run_finished")
    assert (summary["telemetry_forwarded"], summary["alarm_events_forwarded"]) == (n, len(events))
    limit = it.load_config_value()  # messages per second per asset, config/thingsboard.yaml
    assert summary["rate_limited"] > 0 and elapsed >= (n + len(events)) / limit - 5  # it was paced
    dev = device(tb, asset)["id"]["id"]
    span = {"startTs": int(start.timestamp() * 1000), "endTs": int((start + timedelta(seconds=n)).timestamp() * 1000)}

    def mine():
        v = tb.request("GET", f"/api/plugins/telemetry/DEVICE/{dev}/values/timeseries",
                       query={"keys": "seq", **span, "limit": 20_000})
        return sorted(int(float(p["value"])) for p in v.get("seq", []) if base <= int(float(p["value"])) < base + n)
    assert wait_until(lambda: mine() == list(range(base, base + n)), timeout=120, every=5), \
        f"{len(mine())} of the backlog's {n} telemetry points in ThingsBoard"
    found = settled(tb, asset, ids)
    assert len(found) == len(ids) and not any(a["status"].startswith("ACTIVE") for a in found)
    stats = stats_after(since)
    print(f"backlog of {n + len(events)} messages: {elapsed:.0f} s at {limit:g} msg/s per asset; queue {stats}")
    assert (stats["timeoutMsgs"], stats["failedMsgs"], stats["tmpTimeout"], stats["tmpFailed"]) == (0, 0, 0, 0), stats
