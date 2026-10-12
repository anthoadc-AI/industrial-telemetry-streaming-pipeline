"""bench/series.py: the re-measurement series' decisions and stop rules (owner, DETENTE 3.3.i), with a scripted runner."""

from __future__ import annotations

import pytest

from bench import series
from bench.series import Outcome, Point, Series

OK = (0, {"valid": True, "sustainable": True})
SLOW = (0, {"valid": True, "sustainable": False, "problems": ["p99 not below 1000 ms (98.0000% below)"]})
TOOLS = (1, {"valid": False, "problems": ["meter saw 9 distinct of 10 sent, 1 gap(s)",
                                          "CPU-bound instrument(s): generator at 98 % of a core"]})
BROKEN = (1, {"valid": False, "problems": ["Mosquitto dropped 12 message(s) ($SYS, broker-wide)"]})
LATENCY_OK = (0, {"valid": True, "sustainable": None})


class Script:
    """Results per point (mode, rate, sensitivity, diagnostic), consumed in order; OK when none is scripted."""

    def __init__(self, **by_point):
        self.by_point = {k: list(v) for k, v in by_point.items()}
        self.ran: list[str] = []
        self.commits: list[tuple[str, int, list[str]]] = []
        self.events: list[tuple[str, dict]] = []
        self.env: list[str] = []

    def run(self, point: Point) -> Outcome:
        self.ran.append(point.label())
        key = f"{point.mode[0]}{point.rate:g}" + ("s" if point.sensitivity else "") + ("d" if point.diagnostic else "")
        queue = self.by_point.get(key)
        default = LATENCY_OK if point.mode == "latency" else OK
        code, manifest = queue.pop(0) if queue else default
        return Outcome(point, code, {"run_id": f"run-{len(self.ran)}", "harness_revision": "final", **manifest})

    def series(self, ladder=(250.0, 500.0, 1000.0, 2000.0, 3000.0)) -> Series:
        return Series((5.0, 50.0), 2, ladder, 3, self.run, lambda: list(self.env),
                      lambda block, outcomes, notes: self.commits.append((block, len(outcomes), notes)),
                      lambda event, **fields: self.events.append((event, fields)))


def stopped(script: Script) -> str:
    return next(f["reason"] for e, f in script.events if e == "stop")


def test_the_expected_series_ends_the_ladder_at_the_instrument_limit():
    """As at DETENTE 3.3.f: 3000/s invalid twice with the generator at its limit ends the ladder; E2 at 2000 only."""
    s = Script(t3000=[TOOLS, TOOLS], t3000sd=[TOOLS])
    assert s.series().run_all() == "DONE"
    assert s.ran == ["latency 5/s r1", "latency 5/s r2", "latency 50/s r1", "latency 50/s r2",
                     "throughput 250/s r1", "throughput 500/s r1", "throughput 1000/s r1", "throughput 2000/s r1",
                     "throughput 3000/s r1", "throughput 3000/s r1",
                     "throughput 2000/s r2", "throughput 2000/s r3",
                     "throughput sensitivity 2000/s r1", "throughput sensitivity diagnostic 3000/s r1"]
    assert [(b, n) for b, n, _ in s.commits] == [("latency", 4), ("ladder", 6), ("E2", 2), ("sensitivity", 2)]
    ladder_notes, e2_notes, sensitivity_notes = s.commits[1][2], s.commits[2][2], s.commits[3][2]
    assert ladder_notes[0].startswith("ladder ends at 3000/s: not measurable validly, an instrument at its limit")
    assert e2_notes == ["E2 at the candidate 2000/s: 3 runs, its ladder run included",
                        "no E2 at the next step: 3000/s cannot be measured validly"]
    assert sensitivity_notes[0].startswith("sensitivity diagnostic at 3000/s (never publishable): invalid: meter saw")
    assert s.events[-1] == ("done", {})


def test_an_invalid_run_is_repeated_once_and_the_series_goes_on():
    s = Script(l50=[BROKEN, LATENCY_OK])
    assert s.series().run_all() == "DONE"
    assert s.ran[2:4] == ["latency 50/s r1", "latency 50/s r1"]
    assert s.commits[0][:2] == ("latency", 5)


def test_two_invalid_runs_at_one_point_stop_the_series_and_its_block_is_still_committed():
    s = Script(t1000=[BROKEN, BROKEN])
    assert s.series().run_all().startswith("STOPPED: two invalid runs in a row at throughput 1000/s r1")
    assert "Mosquitto dropped 12" in stopped(s)
    assert [(b, n) for b, n, _ in s.commits] == [("latency", 4), ("ladder", 4)]


def test_two_invalid_ladder_runs_not_both_at_an_instrument_limit_stop():
    s = Script(t3000=[TOOLS, BROKEN])
    assert s.series().run_all().startswith("STOPPED: two invalid runs in a row at throughput 3000/s r1")


def test_an_environment_failure_stops_before_the_run():
    s = Script()
    sr = s.series()
    s.env = ["not on AC power"]
    assert sr.run_all() == "STOPPED: environment check failed before latency 5/s r1: not on AC power"
    assert s.ran == [] and s.commits == [("latency", 0, [])]  # the real commit skips an empty block


def test_a_harness_error_stops():
    s = Script(l5=[(2, {})])
    assert s.series().run_all() == "STOPPED: harness exit 2 at latency 5/s r1"


def test_a_pipeline_failure_gets_the_intermediate_step_and_e2_at_both():
    s = Script(t3000=[SLOW, SLOW, SLOW])  # the ladder run, then two of the next step's E2 runs
    assert s.series().run_all() == "DONE"
    assert s.ran[8:] == ["throughput 3000/s r1", "throughput 2500/s r1",
                         "throughput 2500/s r2", "throughput 2500/s r3", "throughput 3000/s r2", "throughput 3000/s r3",
                         "throughput sensitivity 2000/s r1", "throughput sensitivity diagnostic 3000/s r1"]
    assert "intermediate step 2500/s, between 2000 and 3000" in s.commits[1][2]
    assert s.commits[2][2][-1] == "E2 at the next step 3000/s: 3 runs, its ladder run included"


def test_an_intermediate_step_that_fails_becomes_the_next_step():
    s = Script(t3000=[SLOW], t2500=[SLOW, SLOW, SLOW])
    assert s.series().run_all() == "DONE"
    assert s.ran[10:14] == ["throughput 2000/s r2", "throughput 2000/s r3", "throughput 2500/s r2", "throughput 2500/s r3"]
    assert s.commits[1][2][-1].startswith("intermediate step 2500/s not sustainable")


def test_an_intermediate_step_at_the_instrument_limit_ends_the_ladder():
    s = Script(t3000=[SLOW], t2500=[TOOLS, TOOLS])
    assert s.series().run_all() == "DONE"
    assert s.commits[1][2][-1] == "intermediate step 2500/s not measurable validly: limited by the measuring tools"
    assert s.commits[2][2][-1] == "no E2 at the next step: 2500/s cannot be measured validly"


def test_the_intermediate_step_is_rounded_to_50():
    s = Script(t500=[SLOW, SLOW, SLOW])
    assert s.series(ladder=(250.0, 500.0, 1000.0)).run_all() == "DONE"
    assert "throughput 400/s r1" in s.ran and "throughput 1000/s r1" not in s.ran  # 375 rounded to 400


def test_ambiguous_candidates_stop():
    no_step = Script(t250=[SLOW])
    assert no_step.series().run_all() == "STOPPED: ambiguous candidate: no sustainable ladder step"
    e2_fails = Script(t3000=[TOOLS, TOOLS], t2000=[OK, SLOW])
    assert e2_fails.series().run_all().startswith("STOPPED: ambiguous candidate: E2 run r2 at 2000/s valid but not sustainable")
    next_passes = Script(t3000=[SLOW])
    assert next_passes.series().run_all() == ("STOPPED: ambiguous candidate: E2 run r2 at the next step 3000/s is "
                                              "sustainable")


def test_every_step_sustainable_is_a_result_at_least_the_cap():
    s = Script()
    assert s.series().run_all() == "DONE"
    assert s.commits[1][2] == ["every ladder step up to 3000/s sustainable: the result is at least that, limited by "
                               "the ladder's cap"]
    assert s.commits[2][2][-1] == "no E2 at the next step: none above the candidate"


def test_a_diagnostic_is_never_repeated():
    s = Script(t3000=[TOOLS, TOOLS], t3000sd=[BROKEN])
    assert s.series().run_all() == "DONE"
    assert s.ran.count("throughput sensitivity diagnostic 3000/s r1") == 1


def test_outcome_and_instrument_limit():
    assert series.instrument_limited(Outcome(Point("throughput", 1), *TOOLS))
    assert not series.instrument_limited(Outcome(Point("throughput", 1), *BROKEN))
    assert not series.instrument_limited(Outcome(Point("throughput", 1), *OK))
    assert not Outcome(Point("throughput", 1), 1, {"valid": True}).valid  # the exit code counts too
    assert Outcome(Point("latency", 1), 0, {"valid": True, "run_problems": ["x"]}).problems == ["x"]


def test_commit_message():
    outcomes = [Outcome(Point("throughput", 2000), *OK), Outcome(Point("throughput", 3000), *TOOLS),
                Outcome(Point("latency", 5), *LATENCY_OK)]
    for i, o in enumerate(outcomes):
        o.manifest.update(run_id=f"r{i}", harness_revision="final configuration (after 3.3.h and 3.3.i)")
    text = series.commit_message("ladder", outcomes, ["ladder ends at 3000/s"])
    assert text.splitlines()[0] == "bench(results): final configuration, block ladder (3 runs)"
    assert "harness revision 'final configuration (after 3.3.h and 3.3.i)'." in text
    assert "- r0: valid, sustainable\n" in text
    assert "- r1: INVALID (meter saw 9 distinct of 10 sent, 1 gap(s); CPU-bound instrument(s): generator at 98 % of a core)" \
        in text
    assert "- r2: valid\n" in text and text.endswith("ladder ends at 3000/s\n")
    assert series.commit_message("x", [], []).startswith("bench(results): final configuration, block x (0 runs)")


@pytest.mark.parametrize("point, label", [
    (Point("latency", 50, 3), "latency 50/s r3"),
    (Point("throughput", 3000, 1, sensitivity=True, diagnostic=True), "throughput sensitivity diagnostic 3000/s r1"),
])
def test_point_labels(point, label):
    assert point.label() == label


def test_a_run_without_results_is_a_harness_error_not_an_invalid_run():
    """Owner, DETENTE 3.3.g: the first WARNING runs crashed before measuring (exit 1, no results directory)."""
    s = Script(t2000s=[(1, {})])  # the harness crashed: exit 1
    sr = s.series()

    def run(point):  # the scripted manifest always gains run_id and harness_revision: drop them, as a crash would
        outcome = Script.run(s, point)
        if point.sensitivity:
            outcome.manifest = {}
        return outcome

    sr.run = run
    assert sr.run_all(only_sensitivity=True) == "STOPPED: harness error at throughput sensitivity 2000/s r1: no results " \
        "directory (exit 1)"
    assert s.ran == ["throughput sensitivity 2000/s r1"]


def test_the_sensitivity_block_alone():
    s = Script(t3000sd=[TOOLS])
    assert s.series().run_all(only_sensitivity=True) == "DONE"
    assert s.ran == ["throughput sensitivity 2000/s r1", "throughput sensitivity diagnostic 3000/s r1"]
    assert [(b, n) for b, n, _ in s.commits] == [("sensitivity", 2)]
