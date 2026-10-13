"""Integration: a restored ACTIVE alarm meets a replay of the samples that preceded its raise (Task 3.4, H3).

The alarm consumer's offsets are committed by kafkajs after the handlers of a fetched batch have returned
(processing/alarm_consumer/consumer.js, runConsumer, autoCommit). A stop between the RAISED send and that commit
(a crash, a kill, or the documented stop on a failed send later in the batch) leaves the RAISED event in
telemetry.alarms and the committed offset before the samples that raised it. On restart the consumer restores
the alarm as ACTIVE with its counters at zero (state_machine.js, restoreActive) and processes those samples again,
starting with any normal samples of the same batch that came before the raise.

The test builds that state without timing a crash: consumer A processes NORMAL normal samples and then PERSISTENCE
high samples of one asset and raises one alarm; the group's committed offset on telemetry.raw is then moved back
to before those samples, which is what an uncommitted batch leaves; consumer B starts in the same group.

Observed (2026-10-12) and pinned here as a known defect: consumer B emits a CLEARED for the restored alarm_id,
stamped with a sample that precedes the raise, and then the same RAISED again. telemetry.alarms ends with RAISED
as the latest record of that key, so a later restore is right; ThingsBoard is not (see the full-profile test):
AlarmMapping applies the CLEARED, because it has the raised_ts of the alarm it holds, and drops the second RAISED
as a replay, so the alarm shows cleared while the condition still holds. When the consumer is fixed, these tests
fail and must change with docs/ARCHITECTURE.md (section "Alarm logic") and the backlog item.

Asset and alarm type: pump-02, bearing temperature WARNING, used by no other test, with timestamps of the current
time, so AlarmMapping's "newer than the last applied" rule does not couple this test to another.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest
from confluent_kafka import Consumer, TopicPartition

import integration_support as it
from config.loader import load_config
from test_thingsboard_sink import alarms_of, device, sink_group_at, wait_until

ASSET, ASSET_TYPE, CHANNEL, SEVERITY = "pump-02", "pump", "bearing_temperature_c", "WARNING"
RULE = load_config(env={}).alarms.rule(ASSET, CHANNEL)
PERSISTENCE = RULE.persistence_samples
NORMAL = PERSISTENCE + 2  # enough normal samples before the raise to meet the clear condition on their own
LOW = RULE.warning - RULE.hysteresis - 20.0  # below the clear level
HIGH = RULE.warning + 5.0  # at or above the warning threshold, below critical
assert HIGH < RULE.critical


def samples(base_seq: int, base_ms: int, values: list[float]) -> list[dict]:
    """telemetry_v1 records of ASSET, one second apart; only CHANNEL leaves its normal range."""
    return [{"asset_id": ASSET, "asset_type": ASSET_TYPE, "seq": base_seq + i,
             "ts": datetime.fromtimestamp((base_ms + i * 1000) / 1000, timezone.utc), "synthetic": True,
             CHANNEL: v, "vibration_rms_mm_s": 2.0, "motor_current_a": 25.0, "speed_rpm": 2950.0, "injected_fault": None}
            for i, v in enumerate(values)]


def commit_at(group: str, topic: str, offsets: dict[int, int]) -> None:
    """Commit offsets for a group with no member: what a stop before the batch commit leaves behind."""
    c = Consumer({"bootstrap.servers": it.BOOTSTRAP, "group.id": group, "enable.auto.commit": False})
    try:
        c.commit(offsets=[TopicPartition(topic, p, o) for p, o in offsets.items()], asynchronous=False)
    finally:
        c.close()


def transitions(proc) -> list[dict]:
    events = proc.find("alarm_raised") + proc.find("alarm_cleared")
    return sorted((e for e in events if (e["asset_id"], e["channel"], e["severity"]) == (ASSET, CHANNEL, SEVERITY)),
                  key=lambda e: int(e["source_offset"]))


def alarm_records(mark: dict[int, int]) -> list[tuple[str, str, int]]:
    """(alarm_id, state, source_seq) of this asset's alarm type written to telemetry.alarms after mark, in order."""
    prefix = f"{ASSET}/{CHANNEL}/{SEVERITY}/"
    records = [r for r in it.read_since("telemetry.alarms", mark) if r["key"] and r["key"].decode().startswith(prefix)]
    return [(r["value"]["alarm_id"], r["value"]["state"], r["value"]["source_seq"])
            for r in sorted(records, key=lambda r: r["offset"])]


def raise_then_replay(log_dir, group: str, base_seq: int, base_ms: int) -> tuple[str, list[dict]]:
    """Consumer A raises one alarm; the offset goes back before its samples; consumer B restores and replays.

    Returns the alarm_id and consumer B's transitions."""
    records = samples(base_seq, base_ms, [LOW] * NORMAL + [HIGH] * PERSISTENCE)
    before = it.watermarks("telemetry.raw")
    a = it.alarm_consumer(log_dir, "consumer-a", group, count=len(records), duration=300)
    it.produce_telemetry(records)
    assert a.finish(120) == 0
    raised = transitions(a)
    alarm_id = f"{ASSET}/{CHANNEL}/{SEVERITY}/{base_ms + (NORMAL + PERSISTENCE - 1) * 1000}"
    assert [(e["event"], e["alarm_id"]) for e in raised] == [("alarm_raised", alarm_id)]
    assert it.committed(group, "telemetry.raw") == it.watermarks("telemetry.raw")

    commit_at(group, "telemetry.raw", before)
    b = it.alarm_consumer(log_dir, "consumer-b", group, count=len(records), duration=300)
    assert alarm_id in b.find("alarms_restored")[0]["alarm_ids"]
    assert b.finish(120) == 0
    assert all(p.non_json_lines() == 0 for p in (a, b))
    return alarm_id, transitions(b)


def clear(log_dir, group: str, base_seq: int, base_ms: int, alarm_id: str) -> None:
    """Normal samples after the replay: a new consumer in the group clears the alarm, leaving no active alarm behind."""
    records = samples(base_seq + NORMAL + PERSISTENCE, base_ms + (NORMAL + PERSISTENCE) * 1000, [LOW] * PERSISTENCE)
    c = it.alarm_consumer(log_dir, "consumer-c", group, count=len(records), duration=300)
    it.produce_telemetry(records)
    assert c.finish(120) == 0
    assert [(e["event"], e["alarm_id"]) for e in transitions(c)] == [("alarm_cleared", alarm_id)]


def expected_replay(alarm_id: str, base_seq: int) -> list[tuple[str, str, int]]:
    """The known defect: a CLEARED stamped with the PERSISTENCE-th normal sample, then the same RAISED again."""
    return [(alarm_id, "CLEARED", base_seq + PERSISTENCE - 1), (alarm_id, "RAISED", base_seq + NORMAL + PERSISTENCE - 1)]


def test_replay_before_the_raise_emits_a_spurious_clear(stack, offsets_mark, fresh_group, log_dir):
    group = fresh_group()
    base_seq, base_ms = int(time.time() * 1000), int(time.time() * 1000)
    alarm_id, replayed = raise_then_replay(log_dir, group, base_seq, base_ms)

    assert [(e["alarm_id"], "CLEARED" if e["event"] == "alarm_cleared" else "RAISED", e["source_seq"])
            for e in replayed] == expected_replay(alarm_id, base_seq)
    # The spurious CLEARED is stamped before the alarm's own raise.
    assert int(replayed[0]["sample_ts"]) < int(alarm_id.rsplit("/", 1)[1])
    written = alarm_records(offsets_mark["telemetry.alarms"])
    assert written == [(alarm_id, "RAISED", base_seq + NORMAL + PERSISTENCE - 1)] + expected_replay(alarm_id, base_seq)
    # The latest record of the key is RAISED, so the next restore still finds the alarm active.
    assert written[-1][1] == "RAISED"

    clear(log_dir, group, base_seq, base_ms, alarm_id)


@pytest.mark.full
def test_after_the_replay_thingsboard_shows_the_alarm_cleared_while_it_is_active(stack, offsets_mark, fresh_group,
                                                                                  log_dir):
    tb = it.thingsboard()
    group = fresh_group()
    base_seq, base_ms = int(time.time() * 1000), int(time.time() * 1000)
    alarms_mark = offsets_mark["telemetry.alarms"]
    alarm_id, replayed = raise_then_replay(log_dir, group, base_seq, base_ms)
    assert len(replayed) == 2

    def forward(name: str, count: int) -> None:
        marks = {"telemetry.alarms": alarms_mark, "telemetry.raw": it.watermarks("telemetry.raw")}
        sink = it.thingsboard_sink(log_dir, name, sink_group_at(marks, f"it-sink-{name}"), count=count)
        assert sink.finish(180) == 0
        assert sink.non_json_lines() == 0

    # RAISED, the spurious CLEARED, RAISED again: ThingsBoard applies the first two and drops the third.
    forward("sink-replay", 3)
    pump = device(tb, ASSET)["id"]["id"]
    settled = wait_until(lambda: [a for a in alarms_of(tb, pump, {alarm_id}) if a["status"].startswith("CLEARED")])
    assert [a["status"] for a in settled] == ["CLEARED_UNACK"], alarms_of(tb, pump, {alarm_id})
    time.sleep(5)  # the second RAISED has been processed by now; it must not have reopened the alarm
    assert [a["status"] for a in alarms_of(tb, pump, {alarm_id})] == ["CLEARED_UNACK"]

    # The alarm consumer still holds the alarm ACTIVE: its real clear arrives later and is dropped as a replay.
    alarms_mark = it.watermarks("telemetry.alarms")
    clear(log_dir, group, base_seq, base_ms, alarm_id)
    forward("sink-clear", 1)
    time.sleep(5)
    [alarm] = alarms_of(tb, pump, {alarm_id})
    assert alarm["status"] == "CLEARED_UNACK"
