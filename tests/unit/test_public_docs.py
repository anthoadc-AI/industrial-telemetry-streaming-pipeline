"""README.md and docs/CASE_STUDY.md must agree with the benchmark and the repository (Task 3.5, DETENTE 3.5.a).

- The README's figures table and its throughput statement are compared with `make bench-report`, rendered here
  from bench/results/ exactly as bench/report.py's main() renders it.
- Every relative link and image in both documents exists, and every anchor into a Markdown file matches one of
  its headings (GitHub's slug rule).
- Every repository path cited in backticks exists (the rule of test_architecture_doc.py).
- The README's architecture diagram is the one in docs/ARCHITECTURE.md, section 2.
- The synthetic-data notice comes before any figure.
- Every line citation (`path:line`) in the three public documents points at a line that still says what it was
  cited for: CITED_LINES gives a short expected substring per citation (owner decision, DETENTE 3.6.a). A line
  count alone let an edit above the cited line move the citation without failing.

Out of scope: the CI figures of the README's "Verification" section (test counts, coverage). They come from a CI
run's log and cannot be recomputed offline; docs/internal/phase3_task3.5_traceability.md gives their source.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from test_architecture_doc import REPO_ROOT, cited_paths, table_under

from bench import criteria
from bench.report import load_run, render
from config.loader import load_benchmark_config

README = REPO_ROOT / "README.md"
CASE_STUDY = REPO_ROOT / "docs" / "CASE_STUDY.md"
ARCHITECTURE = REPO_ROOT / "docs" / "ARCHITECTURE.md"
LINK = re.compile(r"!?\[[^\]]*\]\(([^)\s]+)\)")
MERMAID = re.compile(r"```mermaid\n(.*?)```", re.S)
NOTICE = "**All data in this repository is synthetic.**"

# README figures-table row -> (Mode, Rate/s, Figure) of the report's "Repetitions" table.
FIGURE_ROWS = {
    ("Sample to record read from Kafka (F1)", "50 msg/s"): ("latency", "50", "F1 e2e"),
    ("Sample to the alarm event acknowledged by Kafka (F2)", "50 msg/s"): ("latency", "50", "F2 alarm"),
    ("Sample to record read from Kafka (F1), at the sustained rate", "2000 msg/s"): ("throughput", "2000", "F1 e2e"),
}

# (cited path, line) -> a substring the cited line must contain. Every line citation in the public documents has
# an entry, and every entry is cited; for a range, the line is its last (cited_paths).
CITED_LINES = {
    ("docs/internal/phase3_backlog.md", 14): "A simulator QoS 1 acknowledgement means Mosquitto accepted the message",
    ("docs/internal/phase0_diagnostic_report.md", 152): "**Confluent Platform version: 6.1.1**",
    ("docs/internal/phase0_diagnostic_report.md", 218): "ZooKeeper reloaded a persisted `/brokers/ids/1` ephemeral znode",
    ("docs/internal/phase0_diagnostic_report.md", 320): "**ThingsBoard version (resolves Q6)** | **v4.2.1.1**",
    ("docs/internal/phase0_diagnostic_report.md", 473): "`node-rdkafka` incompatible with Node.js v24.",
    ("docs/internal/claude_code_briefings/phase1_briefing_solid_base.md", 62): "Image pins: `thingsboard/tb-postgres:4.2.1.1`",
    ("docs/internal/claude_code_briefings/phase2_briefing_engineering_quality.md", 29): "**Decision:** Node.js",
    ("docs/internal/phase2_backlog.md", 14): "the alarm consumer is built in **Node.js** (kafkajs)",
    ("docs/internal/phase2_backlog.md", 35): "**kafkajs maintenance risk**",
    ("docs/internal/claude_code_briefings/phase3_briefing_quantified_evidence.md", 76): "**one alarm engine.**",
}


def bench_report() -> str:
    """The output of `make bench-report`: bench/report.py's main() with its defaults."""
    bench = load_benchmark_config()
    results = REPO_ROOT / "bench" / "results"
    runs = [load_run(d) for d in sorted(results.iterdir()) if (d / "manifest.json").is_file()]
    t = criteria.Thresholds(bench.criteria_p99_ms, bench.criteria_max_lag_s, bench.criteria_max_lag_slope_per_s,
                            bench.criteria_max_lost, bench.max_late_p99_ms, bench.max_late_max_ms)
    return render(runs, t, bench.ladder_per_s, bench.max_client_core_fraction, bench.repetitions_at_candidate)


def slug(heading: str) -> str:
    """GitHub's anchor for a Markdown heading: lower case, punctuation dropped, spaces to hyphens."""
    return re.sub(r"[^\w\- ]", "", heading.strip().lower()).replace(" ", "-")


def anchors(path: Path) -> set[str]:
    text = re.sub(r"^```.*?^```", "", path.read_text(encoding="utf-8"), flags=re.S | re.M)
    return {slug(m) for m in re.findall(r"^#{1,6} +(.+?) *$", text, re.M)}


def broken_links(path: Path) -> list[str]:
    broken = []
    for target in LINK.findall(path.read_text(encoding="utf-8")):
        if re.match(r"[a-z]+:", target):
            continue
        file, _, anchor = target.partition("#")
        resolved = (path.parent / file).resolve() if file else path
        if not resolved.exists():
            broken.append(target)
        elif anchor and resolved.suffix == ".md" and anchor not in anchors(resolved):
            broken.append(target)
    return broken


@pytest.fixture(scope="module")
def readme() -> str:
    return README.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def report() -> str:
    return bench_report()


def check_figures(readme: str, report: str) -> None:
    published = {(r["Mode"], r["Rate/s"], r["Figure"]): r
                 for r in table_under(report, "## Repetitions (median and range across valid runs, whole ms)")}
    rows = table_under(readme, "## Measured figures")
    assert {(r["Figure"], r["Rate"]) for r in rows} == set(FIGURE_ROWS)
    for row in rows:
        source = published[FIGURE_ROWS[(row["Figure"], row["Rate"])]]
        for column in ("p50", "p95", "p99"):
            assert row[f"{column} ms"] == source[column], (row["Figure"], row["Rate"], column)
        assert row["Runs"] == source["Runs"], row["Figure"]
        assert row["Samples per run"] == source["n per run"], row["Figure"]


def check_throughput(readme: str, report: str) -> None:
    m = re.search(r"^Candidate: (\d+)/s; runs (\d+) of the (\d+) E2 needs; confirmed\.$", report, re.M)
    assert m, "the report confirms no candidate"
    rate, runs, needed = m.groups()
    assert f"**Sustained throughput (F3): {rate} msg/s**, confirmed in {runs} of {needed} runs" in readme
    above = re.search(r"the step above the candidate, (\d+)/s, could not be measured validly", report)
    assert above and f"The next step, {above[1]} msg/s, could not be measured validly" in readme


def test_readme_figures_match_the_benchmark_report(readme, report):
    check_figures(readme, report)


def test_readme_throughput_matches_the_benchmark_report(readme, report):
    check_throughput(readme, report)


@pytest.mark.parametrize("path", [README, CASE_STUDY], ids=["README", "CASE_STUDY"])
def test_every_link_and_anchor_exists(path):
    assert LINK.findall(path.read_text(encoding="utf-8"))
    assert broken_links(path) == []


@pytest.mark.parametrize("path", [README, CASE_STUDY], ids=["README", "CASE_STUDY"])
def test_every_cited_path_exists(path):
    for cited, line in cited_paths(path.read_text(encoding="utf-8")):
        target = REPO_ROOT / cited
        assert target.exists(), cited
        if line is not None:
            assert len(target.read_text(encoding="utf-8").splitlines()) >= line, f"{cited}:{line}"


def line_citations(documents: list[Path]) -> set[tuple[str, int]]:
    return {(cited, line) for doc in documents for cited, line in cited_paths(doc.read_text(encoding="utf-8"))
            if line is not None}


def stale_citations(expected: dict[tuple[str, int], str]) -> list[str]:
    """Citations whose line no longer contains its expected substring (or no longer exists)."""
    stale = []
    for (cited, line), substring in expected.items():
        lines = (REPO_ROOT / cited).read_text(encoding="utf-8").splitlines()
        if len(lines) < line or substring not in lines[line - 1]:
            stale.append(f"{cited}:{line}")
    return stale


def test_every_line_citation_has_an_expected_substring():
    assert line_citations([README, CASE_STUDY, ARCHITECTURE]) == set(CITED_LINES)


def test_every_cited_line_says_what_it_was_cited_for():
    assert stale_citations(CITED_LINES) == []


def test_the_readme_diagram_is_the_architecture_diagram(readme):
    (diagram,) = MERMAID.findall(readme)
    assert diagram == MERMAID.findall(ARCHITECTURE.read_text(encoding="utf-8"))[0]


@pytest.mark.parametrize("path", [README, CASE_STUDY], ids=["README", "CASE_STUDY"])
def test_the_synthetic_notice_comes_before_any_figure(path):
    text = path.read_text(encoding="utf-8")
    first_heading = re.search(r"^## ", text, re.M).start()
    assert 0 <= text.find(NOTICE) < first_heading


# --- the checks themselves catch drift ------------------------------------------------


def test_a_changed_figure_is_caught(readme, report):
    check_figures(readme, report)  # the unchanged pair passes, so the failures below come from the drift
    drifted = readme.replace("| 35 (34-36) |", "| 34 (34-36) |")
    assert drifted != readme
    with pytest.raises(AssertionError):
        check_figures(drifted, report)
    drifted = report.replace("| latency | 50 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F1 e2e | 7 (7-8) |",
                             "| latency | 50 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F1 e2e | 8 (7-8) |")
    assert drifted != report
    with pytest.raises(AssertionError):
        check_figures(readme, drifted)


def test_a_changed_throughput_is_caught(readme, report):
    check_throughput(readme, report)
    drifted = report.replace("Candidate: 2000/s", "Candidate: 1000/s")
    assert drifted != report
    with pytest.raises(AssertionError):
        check_throughput(readme, drifted)


def test_a_moved_citation_is_caught():
    assert stale_citations(CITED_LINES) == []  # the unchanged pairs pass, so the failure below comes from the move
    (cited, line), substring = next(iter(CITED_LINES.items()))
    assert stale_citations({(cited, line + 1): substring}) == [f"{cited}:{line + 1}"]
    assert stale_citations({(cited, 10**6): substring}) == [f"{cited}:{10**6}"]


def test_slug_follows_github(tmp_path):
    assert slug("13. Known limitations") == "13-known-limitations"
    assert slug("14. Firebase (optional, inherited from the course)") == "14-firebase-optional-inherited-from-the-course"
    assert slug("Kafka to the alarm consumer and `telemetry.alarms`") == "kafka-to-the-alarm-consumer-and-telemetryalarms"
    doc = tmp_path / "a.md"
    doc.write_text("# A\n## Two words\n```\n# not a heading\n```\n[ok](a.md#two-words) [bad](a.md#not-a-heading) "
                   "[gone](missing.md) [web](https://example.org/x) ![img](a.md)\n", encoding="utf-8")
    assert broken_links(doc) == ["a.md#not-a-heading", "missing.md"]
