"""Integration: state restore from telemetry.alarms after the broker has really compacted it.

The topic's settings are tightened for this test only (short segments, a
cleaner that runs at any dirty ratio) so the log cleaner compacts within the
test; the finalizer deletes those overrides, returning them to the broker
defaults and leaving cleanup.policy=compact as config/ sets it.

Sequence, the Task 2.2 F1-F3 verification on a compacted topic:
  1. faults  -> consumer A raises alarms (episode 1)
  2. normal  -> consumer A clears them
  3. faults  -> consumer A raises new alarms (episode 2)
  4. roll every partition, then wait until the cleaner has removed episode 1's
     RAISED records
  5. consumer B starts: it must restore exactly episode 2's alarms
  6. faults  -> consumer B must emit nothing
  7. normal  -> consumer B clears episode 2 with the same alarm_ids

Step 4, why the roll: the broker evaluates segment.ms only when it appends to
a partition, and the cleaner never compacts the active segment. An episode-1
alarm can only be compacted once its CLEARED record sits in a closed segment,
which needs a later write to that same partition. Alarms are keyed by
alarm_id, which contains a timestamp, so which partitions episode 2 writes to
is chance: when it wrote to none of episode 1's partitions, nothing was ever
compacted and the test failed (CI run 37070295558: episode 1 all on partition
1, episode 2 on 2 and 0). roll_every_partition() appends one tombstone with a
reserved key to every partition after segment.ms, so every alarm record is in
a closed segment. The compaction itself is still the broker's, unassisted.
"""

from __future__ import annotations

import time

import integration_support as it

FAULTS = ("pump-01:imbalance:0", "compressor-01:imbalance:0")
FAST_COMPACTION = {"segment.ms": "1000", "min.cleanable.dirty.ratio": "0.001",
                   "delete.retention.ms": "1000", "min.compaction.lag.ms": "0"}


def transitions(proc, kind=None):
    events = proc.find("alarm_raised") + proc.find("alarm_cleared")
    return [e for e in events if kind is None or e["event"] == kind]


def test_restore_after_real_compaction(stack, offsets_mark, fresh_group, log_dir, request):
    request.addfinalizer(lambda: it.alter_topic_configs("telemetry.alarms", delete=tuple(FAST_COMPACTION)))
    it.alter_topic_configs("telemetry.alarms", set_=FAST_COMPACTION)
    group = fresh_group()
    b = it.bridge(log_dir, "bridge", 350, duration=600)

    a = it.alarm_consumer(log_dir, "consumer-a", group, count=250, duration=600)
    assert it.simulator(log_dir, "sim-1-faults", 100, *FAULTS, seed=31).finish() == 0
    assert it.simulator(log_dir, "sim-2-normal", 50, seed=32).finish() == 0
    # Episode 1 must be fully written (every RAISED cleared) before episode 2.
    deadline = time.monotonic() + 60
    def episode1_written():
        raised = a.find("alarm_raised")
        return raised and len(a.find("alarm_cleared")) == len(raised)
    while time.monotonic() < deadline and not episode1_written():
        time.sleep(0.3)
    time.sleep(2.5)  # a gap between the episodes, as in a real run
    assert it.simulator(log_dir, "sim-3-faults", 100, *FAULTS, seed=33).finish() == 0
    assert a.finish(120) == 0

    raised_a = transitions(a, "alarm_raised")
    cleared_a = transitions(a, "alarm_cleared")
    episode1 = {e["alarm_id"] for e in cleared_a}
    episode2 = {e["alarm_id"] for e in raised_a} - episode1
    assert episode1 and episode2 and len(raised_a) == len(episode1) + len(episode2)
    produced = len(raised_a) + len(cleared_a)
    markers = it.roll_every_partition("telemetry.alarms", int(FAST_COMPACTION["segment.ms"]))

    # Wait for the cleaner: compaction drops episode 1's RAISED records, leaving gaps.
    started = time.monotonic()
    deadline = started + 120
    while True:
        remaining = [r for r in it.read_since("telemetry.alarms", offsets_mark["telemetry.alarms"])
                     if not it.is_roll_marker(r)]
        if len(remaining) < produced or time.monotonic() > deadline:
            break
        time.sleep(3)
    assert len(remaining) < produced, f"no compaction within 120 s ({len(remaining)} of {produced} records remain)"
    print(f"compaction observed after {time.monotonic() - started:.1f} s")
    states = {}
    for r in remaining:
        states[r["key"].decode()] = r["value"]["state"]
    assert {k: v for k, v in states.items() if k in episode1} == dict.fromkeys(episode1, "CLEARED")

    consumer_b = it.alarm_consumer(log_dir, "consumer-b", group, count=100, duration=600)
    restored = consumer_b.find("alarms_restored")[0]
    assert set(restored["alarm_ids"]) == episode2
    assert restored["records_read"] < produced + markers  # it read the compacted log (markers included)

    assert it.simulator(log_dir, "sim-4-faults", 50, *FAULTS, seed=34).finish() == 0
    time.sleep(3)
    assert transitions(consumer_b) == []  # values still high: nothing re-emitted
    assert it.simulator(log_dir, "sim-5-normal", 50, seed=35).finish() == 0
    assert consumer_b.finish(120) == 0
    assert b.finish(120) == 0

    cleared_b = transitions(consumer_b, "alarm_cleared")
    assert {e["alarm_id"] for e in cleared_b} == episode2 and transitions(consumer_b, "alarm_raised") == []
    assert all(p.non_json_lines() == 0 for p in (a, consumer_b, b))
