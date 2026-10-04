"""Integration: the alarm consumer writes all of an asset's alarm events to one partition of telemetry.alarms.

Key alarm_id (for compaction), partition from the asset (for order): see AssetPartitioner in
processing/alarm_consumer/consumer.js. Faults on two assets raise several alarms each (pump-01 WARNING,
compressor-01 WARNING and CRITICAL), then normal values clear them; every record of an asset must sit in a
single partition, whatever its alarm_id.
"""

from __future__ import annotations

import time
from collections import defaultdict

import integration_support as it

FAULTS = ("pump-01:imbalance:0", "compressor-01:imbalance:0")


def test_alarms_of_an_asset_share_one_partition(stack, offsets_mark, fresh_group, log_dir):
    consumer = it.alarm_consumer(log_dir, "consumer", fresh_group())
    b = it.bridge(log_dir, "bridge", 200)
    assert it.simulator(log_dir, "sim-fault", 100, *FAULTS, seed=81).finish() == 0
    assert it.simulator(log_dir, "sim-normal", 100, seed=82).finish() == 0
    assert b.finish() == 0
    expected = len(consumer.find("alarm_raised")) + len(consumer.find("alarm_cleared"))
    assert expected >= 4, f"only {expected} alarm events"
    deadline = time.monotonic() + 30
    while True:
        records = it.read_since("telemetry.alarms", offsets_mark["telemetry.alarms"])
        if len(records) >= expected or time.monotonic() > deadline:
            break
        time.sleep(1)
    assert len(records) == expected
    by_asset = defaultdict(set)
    alarm_ids = defaultdict(set)
    for r in records:
        asset = r["key"].decode().split("/")[0]
        by_asset[asset].add(r["partition"])
        alarm_ids[asset].add(r["key"].decode())
    assert set(by_asset) == {"pump-01", "compressor-01"}
    # compressor-01 raises WARNING and CRITICAL: two alarm_ids, so two different keys, one partition.
    assert len(alarm_ids["compressor-01"]) >= 2, dict(alarm_ids)
    assert {a: len(p) for a, p in by_asset.items()} == {"pump-01": 1, "compressor-01": 1}, dict(by_asset)
    assert consumer.non_json_lines() == 0
