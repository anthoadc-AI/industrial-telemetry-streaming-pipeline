"""Throughput criterion (owner decision D8) and the ladder (amendment E2), from a run's recorded results.

A run is sustainable when all three hold over its measured interval:
- p99 end to end below criteria.p99_ms, read as "at least 99 % of samples below it" from the fixed histogram,
  exact at a bucket edge (1 s is one);
- bounded lag: the larger of the two lags (messages scheduled but not yet in telemetry.raw, and telemetry.raw
  records the alarm consumer has not processed), in seconds of messages at the run's rate, never above
  criteria.max_lag_s, and its least-squares slope at most criteria.max_lag_slope_per_s (seconds of lag gained
  per second);
- no loss: messages sent minus distinct messages seen by the meter at most criteria.max_lost.

A run whose generator saturated (its own lateness above generator.max_late_p99_ms or max_late_max_ms) is
invalid, and on the ladder the step it was at reports "limited by the generator": the pipeline was not shown to
fail there, the generator could not drive it (owner, DETENTE 3.3.b). Every input is kept in the results, so the
cut can be recomputed with other thresholds without new runs.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from bench.histogram import Histogram


@dataclass(frozen=True)
class Thresholds:
    p99_ms: float
    max_lag_s: float
    max_lag_slope_per_s: float
    max_lost: int
    max_late_p99_ms: float
    max_late_max_ms: float


@dataclass
class RunInput:
    rate_per_s: float
    e2e: Histogram  # end to end, measured interval
    generator_late: Histogram
    lag_series: list[tuple[float, float]]  # (seconds since the measured interval began, lag in messages)
    sent: int
    seen_distinct: int
    environment_problems: list[str] = field(default_factory=list)  # power, suspend, foreign containers, CPU, ...


@dataclass
class Verdict:
    valid: bool
    sustainable: bool
    generator_saturated: bool
    reasons: list[str]


def slope(points: list[tuple[float, float]]) -> float:
    """Least-squares slope of y over x; 0.0 with fewer than two distinct x."""
    n = len(points)
    if n < 2:
        return 0.0
    mx = sum(x for x, _ in points) / n
    my = sum(y for _, y in points) / n
    sxx = sum((x - mx) ** 2 for x, _ in points)
    if sxx == 0:
        return 0.0
    return sum((x - mx) * (y - my) for x, y in points) / sxx


def generator_saturated(late: Histogram, t: Thresholds) -> bool:
    if late.total == 0:
        return False
    return late.percentile(99) > t.max_late_p99_ms or late.max_ms > t.max_late_max_ms


def evaluate(run: RunInput, t: Thresholds) -> Verdict:
    reasons = list(run.environment_problems)
    saturated = generator_saturated(run.generator_late, t)
    if saturated:
        late = run.generator_late
        reasons.append(f"generator saturated (lateness p99 {late.percentile(99)} ms, max {late.max_ms} ms)")
    valid = not reasons
    sustainable = False
    if valid:
        below = run.e2e.fraction_below(t.p99_ms)
        if below is None or below < 0.99:
            reasons.append(f"p99 not below {t.p99_ms:g} ms ({'no samples' if below is None else f'{below:.4%} below'})")
        lag_s = [(x, y / run.rate_per_s) for x, y in run.lag_series]
        peak = max((y for _, y in lag_s), default=0.0)
        if peak > t.max_lag_s:
            reasons.append(f"lag reached {peak:.3f} s of messages (limit {t.max_lag_s:g} s)")
        trend = slope(lag_s)
        if trend > t.max_lag_slope_per_s:
            reasons.append(f"lag grows {trend:.5f} s per s (limit {t.max_lag_slope_per_s:g})")
        lost = run.sent - run.seen_distinct
        if lost > t.max_lost:
            reasons.append(f"{lost} message(s) lost (limit {t.max_lost})")
        sustainable = not reasons
    return Verdict(valid, sustainable, saturated, reasons)


@dataclass
class LadderResult:
    max_sustainable_rate: float | None
    limited_by_generator: bool
    first_failure: tuple[float, list[str]] | None
    steps: list[tuple[float, Verdict]]


def ladder(runs: list[RunInput], t: Thresholds) -> LadderResult:
    """Walk the steps upward (one run each) and stop at the first that is not sustainable.

    The result is the highest sustainable step below that failure: the candidate for the repetitions (E2).
    If the failing step's generator saturated, the ladder is reported as limited by the generator.
    """
    steps, best = [], None
    for run in sorted(runs, key=lambda r: r.rate_per_s):
        v = evaluate(run, t)
        steps.append((run.rate_per_s, v))
        if not v.sustainable:
            return LadderResult(best, v.generator_saturated, (run.rate_per_s, v.reasons), steps)
        best = run.rate_per_s
    return LadderResult(best, False, None, steps)


def confirmed(repetitions: list[RunInput], t: Thresholds) -> tuple[bool, list[list[str]]]:
    """E2: a candidate step stands only if every one of its repetitions is valid and sustainable."""
    verdicts = [evaluate(r, t) for r in repetitions]
    return bool(verdicts) and all(v.sustainable for v in verdicts), [v.reasons for v in verdicts]
