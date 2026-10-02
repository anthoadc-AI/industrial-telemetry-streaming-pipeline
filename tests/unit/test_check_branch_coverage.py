"""scripts/check_branch_coverage.py: the threshold, the verdict and the exit status.

branch_totals() reads the real .coverage file; make test runs it on every
call. Here it is replaced, so no second coverage measurement is nested
inside pytest-cov's.
"""

from __future__ import annotations

import importlib.util
import sys

import coverage
import pytest

from conftest import REPO_ROOT

spec = importlib.util.spec_from_file_location("check_branch_coverage", REPO_ROOT / "scripts" / "check_branch_coverage.py")
cbc = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = cbc
spec.loader.exec_module(cbc)


def test_threshold_is_read_from_pyproject():
    assert cbc.threshold(REPO_ROOT / "pyproject.toml") == 80.0


@pytest.mark.parametrize("covered, total, ok", [(80, 100, True), (79, 100, False), (300, 314, True), (0, 5, False)])
def test_evaluate_compares_the_branch_percentage(covered, total, ok):
    passed, message = cbc.evaluate(covered, total, 80.0)
    assert passed is ok
    assert f"{covered}/{total} branches" in message and ("NOT reached" in message) is not ok


def test_no_branch_data_fails():
    passed, message = cbc.evaluate(0, 0, 80.0)
    assert not passed and "no branch data" in message


@pytest.mark.parametrize("totals, code, stream", [((90, 100), 0, "out"), ((70, 100), 1, "err")])
def test_main_exit_status(monkeypatch, capsys, totals, code, stream):
    monkeypatch.setattr(cbc, "branch_totals", lambda root: totals)
    assert cbc.main() == code
    assert "check-branch-coverage:" in getattr(capsys.readouterr(), stream)


def test_main_without_a_data_file(monkeypatch, capsys):
    def no_data(root):
        raise coverage.exceptions.NoDataError("No data to report.")
    monkeypatch.setattr(cbc, "branch_totals", no_data)
    assert cbc.main() == 1
    assert "run pytest --cov first" in capsys.readouterr().err
