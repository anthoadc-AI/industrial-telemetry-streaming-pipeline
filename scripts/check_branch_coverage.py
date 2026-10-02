"""Fail when Python branch coverage is below the threshold in pyproject.toml.

    python scripts/check_branch_coverage.py

Run after `pytest --cov` (make test and make coverage do). coverage.py's own
fail_under applies to one total that mixes statements and branches; it has
no threshold for branches alone. This script reads the branch totals of the
.coverage data file the run just wrote, with the configuration in
pyproject.toml, and compares them with [tool.check_branch_coverage]
fail_under (owner decision, Phase 2 Task 2.8: branches >= 80 per cent,
enforced, in both languages; Jest enforces its own in jest.config.js).

Exit status: 0 when the threshold is met, 1 when it is not or when there is
no branch data to check.
"""

from __future__ import annotations

import json
import sys
import tempfile
import tomllib
from pathlib import Path

import coverage

REPO_ROOT = Path(__file__).resolve().parents[1]


def threshold(pyproject: Path) -> float:
    with pyproject.open("rb") as f:
        return float(tomllib.load(f)["tool"]["check_branch_coverage"]["fail_under"])


def branch_totals(root: Path) -> tuple[int, int]:
    """(covered, total) branches from root/.coverage, measured with root/pyproject.toml."""
    cov = coverage.Coverage(data_file=str(root / ".coverage"), config_file=str(root / "pyproject.toml"))
    cov.load()
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "coverage.json"
        cov.json_report(outfile=str(out))
        totals = json.loads(out.read_text(encoding="utf-8"))["totals"]
    return totals["covered_branches"], totals["num_branches"]


def evaluate(covered: int, total: int, fail_under: float) -> tuple[bool, str]:
    if total == 0:
        return False, "check-branch-coverage: no branch data; run pytest --cov with branch = true first"
    percent = 100.0 * covered / total
    ok = percent >= fail_under
    verdict = "reached" if ok else "NOT reached"
    return ok, f"check-branch-coverage: {covered}/{total} branches, {percent:.2f} %; required {fail_under:g} % {verdict}"


def main() -> int:
    try:
        covered, total = branch_totals(REPO_ROOT)
    except coverage.exceptions.NoDataError as e:
        print(f"check-branch-coverage: {e}; run pytest --cov first", file=sys.stderr)
        return 1
    ok, message = evaluate(covered, total, threshold(REPO_ROOT / "pyproject.toml"))
    print(message, file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
