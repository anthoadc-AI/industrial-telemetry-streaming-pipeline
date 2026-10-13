"""Integration: a replay of samples that an alarm's latest event already accounts for (Task 3.4, H3, and its fix).

The alarm consumer's offsets are committed by kafkajs after the handlers of a fetched batch have returned
(processing/alarm_consumer/consumer.js, runConsumer, autoCommit). A stop between an alarm send and that commit
(a crash, a kill, or the documented stop on a failed send later in the batch) leaves the event in telemetry.alarms
and the committed offset before the samples that caused it. On restart the consumer restores each level alarm from
its latest event and processes those samples again.

The tests build that state without timing a crash: a first consumer processes the samples and emits the event; the
group's committed offset on telemetry.raw is then moved back to before them, which is what an uncommitted batch
leaves; a second consumer starts in the same group.

Before the fix (found in the architecture review, 2026-10-12, and while fixing it, 2026-10-13):
- H3: a restored ACTIVE alarm met the normal samples before its raise and emitted a CLEARED stamped before the raise,
  then the same RAISED again. ThingsBoard applied the CLEARED and dropped the rest: the alarm showed cleared while the
  condition still held.
- Phantom alarm: an alarm raised in a committed batch and cleared in an uncommitted one was restored NORMAL; the
  replayed high samples raised it again under a new alarm_id, then cleared it.

The fix (state_machine.js, replay guard): a sample stamped at or before an alarm's latest event, RAISED or CLEARED,
does not count for that alarm. These tests require the correct behaviour; they fail against the code before the fix.

Asset and alarm type: pump-02, bearing temperature WARNING, used by no other test. Each test stamps its samples after
the latest event of that alarm type already in telemetry.alarms (start_ms), and never before the current time: the
replay guard ignores samples at or before that event, and AlarmMapping applies only alarms newer than the last one.
The samples are one second apart and produced at once, so a test writes events up to about ten seconds ahead of the
clock; the next test must start after them, not merely after the current time.
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
GUARD_KEY = f"{ASSET}/{CHANNEL}"


def start_ms() -> int:
    """A first sample time after every event of this alarm type in telemetry.alarms, and not before now."""
    c = Consumer({"bootstrap.servers": it.BOOTSTRAP, "group.id": it.unique("it-wm"), "enable.auto.commit": False})
    try:
        meta = c.list_topics("telemetry.alarms", timeout=10).topics["telemetry.alarms"]
        low = {p: c.get_watermark_offsets(TopicPartition("telemetry.alarms", p), timeout=10)[0] for p in meta.partitions}
    finally:
        c.close()
    prefix = f"{ASSET}/{CHANNEL}/{SEVERITY}/"
    stamps = [int(r["value"]["ts"].timestamp() * 1000) for r in it.read_since("telemetry.alarms", low)
              if r["key"] and r["key"].decode().startswith(prefix) and r["value"]]
    return max([int(time.time() * 1000), *(s + 1000 for s in stamps)])


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


def consume(log_dir, name: str, group: str, records: list[dict], produce: bool = True):
    """One alarm consumer in group, stopped after len(records) messages; produces the records first if asked."""
    proc = it.alarm_consumer(log_dir, name, group, count=len(records), duration=300)
    if produce:
        it.produce_telemetry(records)
    assert proc.finish(120) == 0
    assert proc.non_json_lines() == 0
    return proc


def skipped(proc) -> int:
    """Samples of this asset and channel that the consumer's replay guard skipped (run_finished)."""
    [finished] = proc.find("run_finished")
    return finished["samples_skipped_by_replay_guard"].get(GUARD_KEY, 0)


def raise_then_replay(log_dir, group: str, base_seq: int, base_ms: int) -> tuple[str, object]:
    """Consumer A raises one alarm; the offset goes back before its samples; consumer B restores and replays.

    Returns the alarm_id and consumer B."""
    records = samples(base_seq, base_ms, [LOW] * NORMAL + [HIGH] * PERSISTENCE)
    before = it.watermarks("telemetry.raw")
    a = consume(log_dir, "consumer-a", group, records)
    alarm_id = f"{ASSET}/{CHANNEL}/{SEVERITY}/{base_ms + (NORMAL + PERSISTENCE - 1) * 1000}"
    assert [(e["event"], e["alarm_id"]) for e in transitions(a)] == [("alarm_raised", alarm_id)]
    assert it.committed(group, "telemetry.raw") == it.watermarks("telemetry.raw")

    commit_at(group, "telemetry.raw", before)
    b = it.alarm_consumer(log_dir, "consumer-b", group, count=len(records), duration=300)
    assert alarm_id in b.find("alarms_restored")[0]["alarm_ids"]
    assert b.finish(120) == 0
    assert b.non_json_lines() == 0
    return alarm_id, b


def clear(log_dir, group: str, base_seq: int, base_ms: int, alarm_id: str) -> None:
    """Normal samples after the replay: a new consumer in the group clears the alarm, leaving no active alarm behind."""
    records = samples(base_seq + NORMAL + PERSISTENCE, base_ms + (NORMAL + PERSISTENCE) * 1000, [LOW] * PERSISTENCE)
    c = consume(log_dir, "consumer-c", group, records)
    assert [(e["event"], e["alarm_id"]) for e in transitions(c)] == [("alarm_cleared", alarm_id)]


def test_replay_before_the_raise_does_not_clear_the_restored_alarm(stack, offsets_mark, fresh_group, log_dir):
    group = fresh_group()
    base_ms = start_ms()
    base_seq = base_ms
    raised_seq = base_seq + NORMAL + PERSISTENCE - 1
    alarm_id, b = raise_then_replay(log_dir, group, base_seq, base_ms)

    # The replay emits nothing: every replayed sample is at or before the restored RAISED.
    assert transitions(b) == []
    assert skipped(b) == NORMAL + PERSISTENCE
    assert alarm_records(offsets_mark["telemetry.alarms"]) == [(alarm_id, "RAISED", raised_seq)]

    # The real clear comes later, with the restored alarm_id and a sample after the raise.
    clear(log_dir, group, base_seq, base_ms, alarm_id)
    cleared_seq = base_seq + NORMAL + 2 * PERSISTENCE - 1
    assert alarm_records(offsets_mark["telemetry.alarms"]) == [(alarm_id, "RAISED", raised_seq),
                                                               (alarm_id, "CLEARED", cleared_seq)]


def test_replay_of_a_clear_does_not_raise_a_phantom_alarm(stack, offsets_mark, fresh_group, log_dir):
    group = fresh_group()
    base_ms = start_ms()
    base_seq = base_ms
    # A committed batch raises the alarm.
    raising = samples(base_seq, base_ms, [HIGH] * PERSISTENCE)
    a = consume(log_dir, "consumer-a", group, raising)
    alarm_id = f"{ASSET}/{CHANNEL}/{SEVERITY}/{base_ms + (PERSISTENCE - 1) * 1000}"
    assert [(e["event"], e["alarm_id"]) for e in transitions(a)] == [("alarm_raised", alarm_id)]

    # The next batch keeps the value high, then clears the alarm; its offsets are then moved back.
    before = it.watermarks("telemetry.raw")
    clearing = samples(base_seq + PERSISTENCE, base_ms + PERSISTENCE * 1000, [HIGH] * (PERSISTENCE + 1) + [LOW] * PERSISTENCE)
    a2 = consume(log_dir, "consumer-a2", group, clearing)
    assert [(e["event"], e["alarm_id"]) for e in transitions(a2)] == [("alarm_cleared", alarm_id)]
    commit_at(group, "telemetry.raw", before)

    # The replay starts from the CLEARED: the high samples before it do not raise a new alarm.
    b = consume(log_dir, "consumer-b", group, clearing, produce=False)
    assert alarm_id not in b.find("alarms_restored")[0]["alarm_ids"]
    assert transitions(b) == []
    assert skipped(b) == len(clearing)
    cleared_seq = base_seq + 2 * PERSISTENCE + PERSISTENCE
    assert alarm_records(offsets_mark["telemetry.alarms"]) == [(alarm_id, "RAISED", base_seq + PERSISTENCE - 1),
                                                               (alarm_id, "CLEARED", cleared_seq)]


@pytest.mark.full
def test_after_the_replay_thingsboard_keeps_the_alarm_active_until_the_real_clear(stack, offsets_mark, fresh_group,
                                                                                    log_dir):
    tb = it.thingsboard()
    group = fresh_group()
    base_ms = start_ms()
    base_seq = base_ms
    alarms_mark = offsets_mark["telemetry.alarms"]
    alarm_id, _ = raise_then_replay(log_dir, group, base_seq, base_ms)

    def forward(name: str, mark: dict[int, int]) -> None:
        """A sink forwards every event of this alarm type written after mark (and nothing of telemetry.raw)."""
        count = len(alarm_records(mark))
        marks = {"telemetry.alarms": mark, "telemetry.raw": it.watermarks("telemetry.raw")}
        sink = it.thingsboard_sink(log_dir, name, sink_group_at(marks, f"it-sink-{name}"), count=count)
        assert sink.finish(180) == 0
        assert sink.non_json_lines() == 0

    # Everything the raise and the replay wrote reaches ThingsBoard: the alarm is active and stays active.
    forward("sink-replay", alarms_mark)
    pump = device(tb, ASSET)["id"]["id"]
    settled = wait_until(lambda: alarms_of(tb, pump, {alarm_id}) or None)
    assert [a["status"] for a in settled] == ["ACTIVE_UNACK"], settled
    time.sleep(5)  # anything else forwarded has been processed by now; it must not have cleared the alarm
    assert [a["status"] for a in alarms_of(tb, pump, {alarm_id})] == ["ACTIVE_UNACK"]

    # The real clear: ThingsBoard clears the alarm only now.
    alarms_mark = it.watermarks("telemetry.alarms")
    clear(log_dir, group, base_seq, base_ms, alarm_id)
    forward("sink-clear", alarms_mark)
    cleared = wait_until(lambda: [a for a in alarms_of(tb, pump, {alarm_id}) if a["status"].startswith("CLEARED")] or None)
    assert [a["status"] for a in cleared] == ["CLEARED_UNACK"], alarms_of(tb, pump, {alarm_id})
