"""The process-isolation fixture stops a failed test's components; without it they outlive the test.

Each case runs a separate pytest on a generated test that starts a component
through integration_support.Proc (a Python process that sleeps for two
minutes, standing in for a bridge) and then fails. The parent checks whether
that process is still alive once the inner pytest has exited:
  - without proc_isolation: it is (the leak behind CI run 37070295558, where a
    failed test's bridge produced the next test's messages a second time);
  - with proc_isolation: it is not.
Needs no stack.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

INTEGRATION_DIR = Path(__file__).resolve().parent

INNER_TEST = textwrap.dedent("""
    import sys
    from pathlib import Path

    import integration_support as it


    def test_starts_a_component_then_fails(tmp_path):
        proc = it.Proc("stand-in", [sys.executable, "-c", "import time; time.sleep(120)"], tmp_path)
        Path(__file__).with_name("pid.txt").write_text(str(proc.p.pid))
        assert False, "this test fails on purpose"
""")


def alive(pid: int) -> bool:
    if os.name == "nt":
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"], capture_output=True, text=True).stdout
        return str(pid) in out
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def kill(pid: int) -> None:
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True)
    else:
        os.kill(pid, 9)


def run_inner(tmp_path: Path, with_fixture: bool) -> int:
    conftest = f"import sys\nsys.path.insert(0, {str(INTEGRATION_DIR)!r})\n"
    if with_fixture:
        conftest += "from proc_isolation import stop_leftover_processes  # noqa: F401\n"
    (tmp_path / "conftest.py").write_text(conftest)
    (tmp_path / "test_inner.py").write_text(INNER_TEST)
    result = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-o", "addopts=",
                             "--rootdir", str(tmp_path), str(tmp_path / "test_inner.py")],
                            cwd=tmp_path, capture_output=True, text=True, timeout=120)
    assert result.returncode == 1 and "1 failed" in result.stdout, result.stdout[-1500:]
    return int((tmp_path / "pid.txt").read_text())


@pytest.mark.parametrize("with_fixture", [False, True], ids=["without-fixture", "with-fixture"])
def test_a_failed_tests_processes_do_not_outlive_it(tmp_path, with_fixture):
    pid = run_inner(tmp_path, with_fixture)
    try:
        time.sleep(0.5)
        if with_fixture:
            assert not alive(pid), "the fixture should have stopped the failed test's process"
        else:
            assert alive(pid), "control: without the fixture the process outlives the failed test"
    finally:
        if alive(pid):
            kill(pid)
