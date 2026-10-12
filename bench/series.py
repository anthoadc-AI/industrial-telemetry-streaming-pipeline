"""The re-measurement series on the final configuration, as one process (owner, DETENTE 3.3.i).

    python -m bench.series run [--dry-run]

Blocks, in order, each committed and pushed when it ends (bench/results/ only):
1. latency: every latency.rates_per_s, latency.repetitions each, bridge at INFO;
2. ladder: throughput.ladder_per_s up to the calibration's cap, one run per step, bridge at INFO; when a step is
   not sustainable right above a sustainable one, the intermediate step between them (criteria.intermediate_step);
3. E2: runs at the candidate until it has throughput.repetitions_at_candidate, its ladder run included; the same at
   the next step when the ladder measured it validly as not sustainable (a step lost to an instrument at its limit
   cannot be measured validly and gets no repetitions, as at DETENTE 3.3.f);
4. sensitivity: the bridge at bridge.log_level_sensitivity at SENSITIVITY_COMPARISON (publishable comparison) and,
   as a diagnostic only (never publishable), at SENSITIVITY_DIAGNOSTIC.

Every run: SERIES_COOLDOWN_S of cooldown first, then AC power, no foreign container and host free memory checked
(environment.min_host_free_memory_gb); the harness's own checks and verdict apply, Mosquitto dropped = 0 included.

Stop rules (owner): an environment check failing before a run; a harness exit other than 0 (valid) or 1 (invalid);
two invalid runs in a row at the same point (an invalid run is kept and repeated once), except at a ladder step
where both runs were invalid with an instrument at its limit among the reasons, which ends the ladder there
("limited by the measuring tools", criteria.ladder, DETENTE 3.3.f); an ambiguous candidate: no sustainable step,
a valid E2 run at the candidate that is not sustainable, or a valid E2 run at the next step that is sustainable.
The diagnostic is run once and never repeated. Every decision goes to bench/results/series-<start>.log (JSON
lines) and into the block's commit.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from bench import criteria

SERIES_COOLDOWN_S = 120
SENSITIVITY_COMPARISON = 2000.0  # owner, DETENTE 3.3.i
SENSITIVITY_DIAGNOSTIC = 3000.0
INSTRUMENT_REASONS = ("CPU-bound instrument(s)", "generator saturated")


class Stop(Exception):
    """A stop rule fired: the series ends here, with what it has."""


@dataclass
class Point:
    mode: str  # latency | throughput
    rate: float
    repetition: int = 1
    sensitivity: bool = False
    diagnostic: bool = False

    def label(self) -> str:
        kind = self.mode + (" sensitivity" if self.sensitivity else "") + (" diagnostic" if self.diagnostic else "")
        return f"{kind} {self.rate:g}/s r{self.repetition}"


@dataclass
class Outcome:
    point: Point
    exit_code: int
    manifest: dict

    @property
    def valid(self) -> bool:
        return self.exit_code == 0 and bool(self.manifest.get("valid"))

    @property
    def sustainable(self) -> bool:
        return self.valid and bool(self.manifest.get("sustainable"))

    @property
    def problems(self) -> list[str]:
        return list(self.manifest.get("problems") or self.manifest.get("run_problems") or [])


def instrument_limited(outcome: Outcome) -> bool:
    """Invalid with an instrument (the generator or the meter) at its limit among the reasons: the rule by which
    criteria.ladder ends the ladder "limited by the measuring tools" (DETENTE 3.3.f: the 3000 msg/s runs had losses
    and the generator at 97-98 % of a core, and F3 closed at 2000)."""
    return not outcome.valid and any(p.startswith(INSTRUMENT_REASONS) for p in outcome.problems)


@dataclass
class Series:
    """The series' decisions. run(point) runs one point and returns its Outcome; environment() returns the
    environment problems before a run (empty when fine); commit(block, outcomes, notes) records a finished block;
    log(event, **fields) records a decision."""

    latency_rates: tuple[float, ...]
    latency_repetitions: int
    ladder: tuple[float, ...]
    repetitions_at_candidate: int
    run: Callable[[Point], Outcome]
    environment: Callable[[], list[str]]
    commit: Callable[[str, list[Outcome], list[str]], None]
    log: Callable[..., None]
    outcomes: list[Outcome] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    # --- one point, repeated once if invalid ------------------------------------------------------------

    def attempt(self, point: Point) -> Outcome:
        problems = self.environment()
        if problems:
            raise Stop(f"environment check failed before {point.label()}: {'; '.join(problems)}")
        outcome = self.run(point)
        self.outcomes.append(outcome)
        self.log("run", point=point.label(), exit_code=outcome.exit_code, valid=outcome.valid,
                 sustainable=outcome.manifest.get("sustainable"), problems=outcome.problems,
                 run_id=outcome.manifest.get("run_id"))
        if outcome.exit_code not in (0, 1):
            raise Stop(f"harness exit {outcome.exit_code} at {point.label()}")
        return outcome

    def measure(self, point: Point, tools_limit_ends: bool = False) -> Outcome:
        """A valid outcome, or the second invalid one when tools_limit_ends and both were instrument-limited."""
        first = self.attempt(point)
        if first.valid or point.diagnostic:
            return first
        second = self.attempt(point)
        if second.valid:
            return second
        if tools_limit_ends and instrument_limited(first) and instrument_limited(second):
            return second
        raise Stop(f"two invalid runs in a row at {point.label()}: {first.problems} / {second.problems}")

    def note(self, text: str) -> None:
        self.notes.append(text)
        self.log("decision", text=text)

    def block(self, name: str, body: Callable[[], None]) -> None:
        start, self.notes = len(self.outcomes), []
        try:
            body()
        finally:
            self.commit(name, self.outcomes[start:], list(self.notes))

    # --- the blocks ---------------------------------------------------------------------------------------

    def latency_block(self) -> None:
        for rate in self.latency_rates:
            for rep in range(1, self.latency_repetitions + 1):
                self.measure(Point("latency", rate, rep))

    def ladder_block(self) -> tuple[float, float | None, bool]:
        """(candidate, next step or None, next step measured validly as not sustainable)."""
        last_pass: float | None = None
        for step in self.ladder:
            outcome = self.measure(Point("throughput", step), tools_limit_ends=True)
            if outcome.sustainable:
                last_pass = step
                continue
            if not outcome.valid:
                self.note(f"ladder ends at {step:g}/s: not measurable validly, an instrument at its limit "
                          f"({'; '.join(outcome.problems)}); limited by the measuring tools, no intermediate step")
                return self.candidate(last_pass), step, False
            self.note(f"{step:g}/s not sustainable: {'; '.join(outcome.problems)}")
            # The ladder stops at its first failure, so the step below it, when there is one, is the last pass.
            mid = criteria.intermediate_step(self.candidate(last_pass), step)
            self.note(f"intermediate step {mid:g}/s, between {last_pass:g} and {step:g}")
            middle = self.measure(Point("throughput", mid), tools_limit_ends=True)
            if middle.sustainable:
                return mid, step, True
            if not middle.valid:
                self.note(f"intermediate step {mid:g}/s not measurable validly: limited by the measuring tools")
                return self.candidate(last_pass), mid, False
            self.note(f"intermediate step {mid:g}/s not sustainable: {'; '.join(middle.problems)}")
            return self.candidate(last_pass), mid, True
        self.note(f"every ladder step up to {self.ladder[-1]:g}/s sustainable: the result is at least that, limited "
                  "by the ladder's cap")
        return self.candidate(last_pass), None, False

    def candidate(self, rate: float | None) -> float:
        if rate is None:
            raise Stop("ambiguous candidate: no sustainable ladder step")
        return rate

    def e2_block(self, candidate: float, next_step: float | None, next_measured: bool) -> None:
        self.note(f"E2 at the candidate {candidate:g}/s: {self.repetitions_at_candidate} runs, its ladder run included")
        for rep in range(2, self.repetitions_at_candidate + 1):
            outcome = self.measure(Point("throughput", candidate, rep))
            if not outcome.sustainable:
                raise Stop(f"ambiguous candidate: E2 run r{rep} at {candidate:g}/s valid but not sustainable "
                           f"({'; '.join(outcome.problems)})")
        if next_step is None or not next_measured:
            self.note("no E2 at the next step: " + ("none above the candidate" if next_step is None else
                                                   f"{next_step:g}/s cannot be measured validly"))
            return
        self.note(f"E2 at the next step {next_step:g}/s: {self.repetitions_at_candidate} runs, its ladder run included")
        for rep in range(2, self.repetitions_at_candidate + 1):
            outcome = self.measure(Point("throughput", next_step, rep))
            if outcome.sustainable:
                raise Stop(f"ambiguous candidate: E2 run r{rep} at the next step {next_step:g}/s is sustainable")

    def sensitivity_block(self) -> None:
        self.measure(Point("throughput", SENSITIVITY_COMPARISON, 1, sensitivity=True))
        diagnostic = self.measure(Point("throughput", SENSITIVITY_DIAGNOSTIC, 1, sensitivity=True, diagnostic=True))
        self.note(f"sensitivity diagnostic at {SENSITIVITY_DIAGNOSTIC:g}/s (never publishable): "
                  f"{'valid' if diagnostic.valid else 'invalid: ' + '; '.join(diagnostic.problems)}")

    def run_all(self) -> str:
        """Every block; returns how the series ended."""
        try:
            self.block("latency", self.latency_block)
            result: list = []
            self.block("ladder", lambda: result.extend(self.ladder_block()))
            candidate, next_step, next_measured = result
            self.block("E2", lambda: self.e2_block(candidate, next_step, next_measured))
            self.block("sensitivity", self.sensitivity_block)
        except Stop as e:
            self.log("stop", reason=str(e))
            return f"STOPPED: {e}"
        self.log("done")
        return "DONE"


def commit_message(block: str, outcomes: list[Outcome], notes: list[str]) -> str:
    """The results commit of one block: what ran, each verdict, and the decisions taken."""
    lines = [f"bench(results): final configuration, block {block} ({len(outcomes)} runs)", "",
             "Run by bench/series.py (owner, DETENTE 3.3.i); harness revision "
             f"{(outcomes[0].manifest.get('harness_revision') if outcomes else '-')!r}.", ""]
    for o in outcomes:
        verdict = ("valid" if o.valid else "INVALID") + (
            "" if o.manifest.get("sustainable") is None else ", sustainable" if o.sustainable else ", not sustainable")
        lines.append(f"- {o.manifest.get('run_id', '?')}: {verdict}" + (f" ({'; '.join(o.problems)})" if o.problems else ""))
    if notes:
        lines += ["", *notes]
    return "\n".join(lines) + "\n"


# --- the process (Docker, git) --------------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:  # pragma: no cover - orchestration on the host
    from bench import harness as h
    from bench import report
    from config.loader import REPO_ROOT, load_benchmark_config

    p = argparse.ArgumentParser(prog="python -m bench.series")
    p.add_argument("command", choices=["run"])
    p.add_argument("--dry-run", action="store_true", help="print the configuration and the first point, run nothing")
    args = p.parse_args(argv)
    bench = load_benchmark_config()
    results = REPO_ROOT / "bench" / "results"
    started = datetime.now(timezone.utc)
    log_path = results / f"series-{started.strftime('%Y%m%dT%H%M%SZ')}.log"
    cap, _ = report.calibration([report.load_run(d) for d in sorted(results.iterdir()) if (d / "manifest.json").is_file()],
                                bench.max_client_core_fraction,
                                criteria.Thresholds(bench.criteria_p99_ms, bench.criteria_max_lag_s,
                                                    bench.criteria_max_lag_slope_per_s, bench.criteria_max_lost,
                                                    bench.max_late_p99_ms, bench.max_late_max_ms))
    ladder, _ = criteria.cap_ladder(bench.ladder_per_s, cap)

    def log(event: str, **fields) -> None:
        line = json.dumps({"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), "event": event, **fields})
        print(line, flush=True)
        if not args.dry_run:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def git(*a: str, check: bool = True) -> subprocess.CompletedProcess:
        out = subprocess.run(["git", *a], cwd=REPO_ROOT, capture_output=True, text=True, encoding="utf-8")
        if check and out.returncode != 0:
            raise RuntimeError(f"git {' '.join(a)}: {out.stderr.strip()}")
        return out

    def environment() -> list[str]:
        problems = []
        if not h.on_ac_power():
            problems.append("not on AC power")
        free = h.free_memory_gb()
        if free < bench.min_host_free_memory_gb:
            problems.append(f"host free memory {free:.2f} GB < {bench.min_host_free_memory_gb:g}")
        foreign = h.foreign_containers()
        if foreign:
            problems.append(f"foreign containers: {foreign}")
        return problems

    def run(point: Point) -> Outcome:
        log("cooldown", seconds=SERIES_COOLDOWN_S, next=point.label())
        time.sleep(SERIES_COOLDOWN_S)
        before = {d.name for d in results.iterdir()}
        cmd = [sys.executable, "-m", "bench.harness", "run", "--mode", point.mode, "--rate", f"{point.rate:g}",
               "--repetition", str(point.repetition)]
        cmd += ["--sensitivity"] * point.sensitivity + ["--diagnostic", "--full-window"] * point.diagnostic
        log("start", point=point.label(), cmd=" ".join(cmd[1:]))
        done = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, encoding="utf-8")
        new = sorted(d for d in results.iterdir() if d.name not in before and (d / "manifest.json").is_file())
        manifest = json.loads((new[-1] / "manifest.json").read_text(encoding="utf-8")) if new else {}
        if not new:
            log("no_run_directory", point=point.label(), stderr=done.stderr[-2000:])
        return Outcome(point, done.returncode, manifest)

    def commit(block: str, outcomes: list[Outcome], notes: list[str]) -> None:
        if not outcomes:
            return
        if git("diff", "--cached", "--name-only").stdout.strip():
            log("commit_skipped", block=block, reason="something else is staged")
            return
        git("add", "bench/results/")
        if not git("diff", "--cached", "--name-only").stdout.strip():
            return
        git("commit", "-q", "-m", commit_message(block, outcomes, notes))
        pushed = git("push", "-q", check=False)
        log("committed", block=block, commit=git("rev-parse", "--short", "HEAD").stdout.strip(),
            pushed=pushed.returncode == 0, push_error=pushed.stderr.strip()[-300:] or None)

    series = Series(bench.latency_rates_per_s, bench.latency_repetitions, ladder, bench.repetitions_at_candidate,
                    run, environment, commit, log)
    log("series_start", ladder=list(ladder), latency=list(bench.latency_rates_per_s),
        repetitions=bench.latency_repetitions, repetitions_at_candidate=bench.repetitions_at_candidate,
        commit=git("rev-parse", "--short", "HEAD").stdout.strip(), environment=environment())
    if args.dry_run:
        return 0
    ended = series.run_all()
    log("series_end", result=ended)
    # The series log itself, with the last block's results already committed.
    git("add", str(log_path.relative_to(REPO_ROOT)))
    git("commit", "-q", "-m", f"bench(results): series log, {ended[:60]}", check=False)
    git("push", "-q", check=False)
    return 0 if ended == "DONE" else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
