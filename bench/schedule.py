"""Open-loop schedule, sample values and expected alarms: the generator's logic, free of I/O.

Open loop (protocol 5.1.1): message i is due at t0 + i / rate, whatever happened to the messages before it, and its
ts is that scheduled instant, never the time it was actually sent. The schedule runs on the same clock as ts, the
VM's wall clock: the protocol said monotonic, but in the Docker Desktop VM CLOCK_MONOTONIC runs about 100 ppm off
CLOCK_REALTIME (measured at STEP 0, section 2, and again in the STEP 1 trial: -94 ppm), so a monotonic schedule
with wall-clock ts sent messages ever earlier than their ts (4 ms by the end of a 60 s trial). Every other time
stamp (the bridge's receive time, CreateTime, the meter, the alarm consumer) is on that wall clock too; a jump
of it shows as generator lateness and invalidates the run.
A generator that falls behind therefore sends the late messages at once, still stamped with their scheduled
instants, so the delay counts as latency downstream (no coordinated omission), and its own lateness (actual send
minus due) is recorded apart, for the saturated-generator rule.

Messages go round-robin over the assets, so each asset gets rate / len(assets) messages per second, with its
own seq counting from 0.

Fault pattern (F2): an asset's pattern channel is high_value for high_s out of every period_s, on the scheduled
time line, with each asset's phase shifted by index / len(assets) of a period so the alarms of many assets do
not all fire in the same second. Every other channel with an alarm rule sits at half its warning threshold.
expected_alarms() replays the same samples through the alarm semantics of config/alarms.yaml to predict the
RAISED and CLEARED events the alarm consumer must emit.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone

from config.loader import AlarmRule, Config, FaultPattern


@dataclass(frozen=True)
class Plan:
    rate_per_s: float
    assets: tuple[str, ...]
    count: int  # messages in the whole run (warm-up included)
    t0_wall_ms: int  # the scheduled instant of message 0, epoch milliseconds

    def due_offset_s(self, i: int) -> float:
        return i / self.rate_per_s

    def asset_of(self, i: int) -> str:
        return self.assets[i % len(self.assets)]

    def seq_of(self, i: int) -> int:
        return i // len(self.assets)

    def ts_ms(self, i: int) -> int:
        """Scheduled instant of message i in epoch ms, truncated as the simulator truncates its ts."""
        return self.t0_wall_ms + math.floor(i * 1000 / self.rate_per_s)


def plan_for(rate_per_s: float, assets: tuple[str, ...], duration_s: float, t0_wall_ms: int) -> Plan:
    if rate_per_s <= 0 or not assets or duration_s <= 0:
        raise ValueError("rate, assets and duration must be positive")
    return Plan(rate_per_s, tuple(assets), math.floor(rate_per_s * duration_s), t0_wall_ms)


def is_high(plan: Plan, pattern: FaultPattern, i: int) -> bool:
    phase = (i % len(plan.assets)) / len(plan.assets) * pattern.period_s
    return (plan.due_offset_s(i) + phase) % pattern.period_s < pattern.high_s


def normal_values(config: Config, asset_id: str) -> dict[str, float]:
    """Half the warning threshold of every channel the asset has a rule for; 1000.0 for the others."""
    asset = config.telemetry.asset(asset_id)
    values = {}
    for channel in asset.channels:
        rule = config.alarms.rule(asset_id, channel)
        values[channel] = round(rule.warning / 2, 3) if rule else 1000.0
    return values


def sample(plan: Plan, pattern: FaultPattern, config: Config, i: int, values: dict[str, float] | None = None) -> dict:
    """The telemetry_v1 message of index i, as the bridge validates it (ts in ISO 8601 UTC, milliseconds)."""
    asset_id = plan.asset_of(i)
    asset = config.telemetry.asset(asset_id)
    values = dict(values if values is not None else normal_values(config, asset_id))
    if pattern.channel in values:
        values[pattern.channel] = pattern.high_value if is_high(plan, pattern, i) else pattern.normal_value
    ts = datetime.fromtimestamp(plan.ts_ms(i) / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    message = {"asset_id": asset_id, "asset_type": asset.type, "seq": plan.seq_of(i), "ts": ts, "synthetic": True}
    for channel in config.telemetry.channels:
        message[channel] = values.get(channel)
    message["injected_fault"] = None
    return message


def run_schedule(plan: Plan, t0: float, now: Callable[[], float], sleep: Callable[[float], None],
                 send: Callable[[int], None], late_ms: Callable[[float], None], stop: Callable[[], bool] = lambda: False) -> int:
    """Send every message of the plan at its due time; returns how many were sent.

    t0 is message 0's due time on the clock now() reads (the wall clock, in seconds, in the generator); now/sleep
    are injected so tests can run on a fake clock. A message already
    late is sent at once (open loop: nothing waits for the messages before it to be acknowledged), and its
    lateness, actual minus due in ms, goes to late_ms.
    """
    sent = 0
    for i in range(plan.count):
        if stop():
            break
        due = t0 + plan.due_offset_s(i)
        delay = due - now()
        if delay > 0:
            sleep(delay)
        late_ms(max(0.0, (now() - due) * 1000))
        send(i)
        sent += 1
    return sent


# --- expected alarms -------------------------------------------------------------


def _replay(values: list[float], rule: AlarmRule, threshold: float) -> list[tuple[int, str]]:
    """(sample index, RAISED|CLEARED) for one level alarm, direction high (processing/alarm_consumer semantics)."""
    events, active, above, below = [], False, 0, 0
    for n, v in enumerate(values):
        if not active:
            above = above + 1 if v >= threshold else 0
            if above >= rule.persistence_samples:
                active, above, below = True, 0, 0
                events.append((n, "RAISED"))
        else:
            below = below + 1 if v < threshold - rule.hysteresis else 0
            if below >= rule.persistence_samples:
                active, above, below = False, 0, 0
                events.append((n, "CLEARED"))
    return events


def expected_alarms(plan: Plan, pattern: FaultPattern, config: Config, measured_from_ms: int | None = None,
                    measured_until_ms: int | None = None) -> dict[str, int]:
    """RAISED and CLEARED counts the plan must produce, over the whole run or for samples whose ts is in the window."""
    per_asset: dict[str, list[tuple[int, float]]] = {a: [] for a in plan.assets}
    for i in range(plan.count):
        value = pattern.high_value if is_high(plan, pattern, i) else pattern.normal_value
        per_asset[plan.asset_of(i)].append((plan.ts_ms(i), value))
    counts = {"RAISED": 0, "CLEARED": 0}
    for asset_id, rows in per_asset.items():
        rule = config.alarms.rule(asset_id, pattern.channel)
        if rule is None:
            continue
        values = [v for _, v in rows]
        for threshold in (rule.warning, rule.critical):
            for n, state in _replay(values, rule, threshold):
                ts = rows[n][0]
                if (measured_from_ms is None or ts >= measured_from_ms) and (measured_until_ms is None or ts < measured_until_ms):
                    counts[state] += 1
    return counts
