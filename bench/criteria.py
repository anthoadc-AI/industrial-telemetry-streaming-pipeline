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
    If the failing step was not measured validly because an instrument was at its limit (the generator saturated,
    or a CPU-bound instrument), the ladder is limited by the measuring tools: it ends there, with no intermediate
    step (owner, after DETENTE 3.3.f: F3 closed at 2000 msg/s, the generator unable to drive 3000 validly).
    """
    steps, best = [], None
    for run in sorted(runs, key=lambda r: r.rate_per_s):
        v = evaluate(run, t)
        steps.append((run.rate_per_s, v))
        if not v.sustainable:
            by_tools = v.generator_saturated or any(r.startswith("CPU-bound instrument(s)") for r in v.reasons)
            return LadderResult(best, by_tools, (run.rate_per_s, v.reasons), steps)
        best = run.rate_per_s
    return LadderResult(best, False, None, steps)


def intermediate_step(last_pass: float, first_fail: float) -> float:
    """The step run between the last passing and the first failing step, before E2 (owner, DETENTE 3.3.d).

    Their midpoint, rounded to a multiple of 50 msg/s when that stays strictly between them.
    """
    if not last_pass < first_fail:
        raise ValueError("last_pass must be below first_fail")
    mid = (last_pass + first_fail) / 2
    rounded = round(mid / 50) * 50
    return float(rounded) if last_pass < rounded < first_fail else mid


def intermediate_needed(result: LadderResult, ladder_per_s: tuple[float, ...]) -> float | None:
    """The intermediate step still to run: the ladder stopped between two consecutive configured steps."""
    if result.max_sustainable_rate is None or result.first_failure is None or result.limited_by_generator:
        return None
    best, failed = result.max_sustainable_rate, result.first_failure[0]
    steps = sorted(ladder_per_s)
    if best in steps and failed in steps and steps.index(failed) == steps.index(best) + 1:
        return intermediate_step(best, failed)
    return None


def cap_ladder(ladder_per_s: tuple[float, ...], max_rate: float | None) -> tuple[tuple[float, ...], bool]:
    """The ladder limited to the calibrated maximum of the generator and the meter (owner, DETENTE 3.3.d)."""
    if max_rate is None:
        return tuple(ladder_per_s), False
    kept = tuple(r for r in ladder_per_s if r <= max_rate)
    return kept, len(kept) < len(ladder_per_s)


# A run's reasons that mean an instrument (the generator or the meter) was at its limit, not the pipeline.
INSTRUMENT_REASONS = ("CPU-bound instrument(s)", "generator saturated")


def instrument_only(reasons: list[str]) -> bool:
    """Every reason is an instrument at its limit: nothing lost, no Mosquitto drop, no environment problem (those
    are reasons of their own)."""
    return bool(reasons) and all(r.startswith(INSTRUMENT_REASONS) for r in reasons)


def confirmed(repetitions: list[RunInput], t: Thresholds) -> tuple[bool, list[list[str]]]:
    """E2: a candidate step stands only if every one of its repetitions is valid and sustainable."""
    verdicts = [evaluate(r, t) for r in repetitions]
    return bool(verdicts) and all(v.sustainable for v in verdicts), [v.reasons for v in verdicts]


# --- calibration (owner, DETENTE 3.3.e) --------------------------------------------------------------------------


@dataclass
class CalibrationVerdict:
    valid: bool
    sustained: bool
    reasons: list[str]  # why invalid when not valid, else why not sustained


def generator_calibration(planned: int, sent: int, acknowledged: int, stopped_early: bool, late: Histogram,
                          cpu_peak_percent: float | None, environment_problems: list[str], t: Thresholds,
                          max_core_fraction: float) -> CalibrationVerdict:
    """The generator alone at one rate, judged by the ladder's own criteria (owner, DETENTE 3.3.e).

    Not sustained (a valid answer: the generator cannot drive that rate): its lateness above the limits, or its CPU
    above max_core_fraction of a core, the rule that flags a client in a pipeline run. Acknowledgements still pending
    when its wait ends are part of that saturation, not a failure of the run. Invalid only for environment or
    integrity failures: a problem of the host, the generator not sending what it planned, or acknowledgements
    missing although the generator was not saturated.
    """
    invalid = list(environment_problems)
    if stopped_early or sent != planned:
        invalid.append(f"sent {sent} of {planned}" + (" (stopped early)" if stopped_early else ""))
    saturation = []
    if generator_saturated(late, t):
        saturation.append(f"lateness p99 {late.percentile(99)} ms, max {late.max_ms:.1f} ms "
                          f"(limits {t.max_late_p99_ms:g} and {t.max_late_max_ms:g} ms)")
    if cpu_peak_percent is not None and cpu_peak_percent > max_core_fraction * 100:
        saturation.append(f"generator at {cpu_peak_percent:.0f} % of a core (limit {max_core_fraction * 100:.0f} %)")
    pending = sent - acknowledged
    if pending > 0:
        if saturation:
            saturation.append(f"{pending} acknowledgements still pending when its wait ended")
        else:
            invalid.append(f"{pending} acknowledgements missing, the generator not saturated")
    if invalid:
        return CalibrationVerdict(False, False, invalid)
    return CalibrationVerdict(True, not saturation, saturation)


def meter_calibration_cap(records_per_s: float, cpu_peak_percent: float | None, max_core_fraction: float) -> float:
    """The highest rate the meter reads within max_core_fraction of a core (owner, DETENTE 3.3.e: one criterion).

    The calibration reads as fast as the meter can, so it runs at a full core; scaled to the limit, assuming its CPU
    proportional to the rate it reads. Without a CPU sample, or below the limit, its measured rate.
    """
    if not cpu_peak_percent or cpu_peak_percent <= max_core_fraction * 100:
        return records_per_s
    return round(records_per_s * max_core_fraction * 100 / cpu_peak_percent, 1)


def calibrated_cap(generator: list[tuple[float, CalibrationVerdict]],
                   meter_caps: list[float]) -> tuple[float | None, float | None, float | None, str]:
    """(generator's cap, meter's cap, ladder cap, note) from every calibration run (owner, DETENTE 3.3.e).

    The generator's cap is the highest rate sustained with every lower rate sustained: at each rate, ascending, a
    valid run that sustained it counts; a rate with valid runs and none sustained ends the walk; a rate with no
    valid run ends it too, undetermined. The meter's is the lowest of its valid runs. The ladder is capped only when
    both are calibrated.
    """
    gen_cap, note = None, ""
    for rate in sorted({r for r, _ in generator}):
        verdicts = [v for r, v in generator if r == rate]
        valid = [v for v in verdicts if v.valid]
        if not valid:
            note = f"no valid generator run at {rate:g}/s"
            break
        if not any(v.sustained for v in valid):
            break
        gen_cap = rate
    meter_cap = min(meter_caps) if meter_caps else None
    missing = [name for name, cap in (("generator", gen_cap), ("meter", meter_cap)) if cap is None]
    if missing:
        return gen_cap, meter_cap, None, f"no ladder cap: {' and '.join(missing)} not calibrated" + (f" ({note})" if note else "")
    return gen_cap, meter_cap, min(gen_cap, meter_cap), note
