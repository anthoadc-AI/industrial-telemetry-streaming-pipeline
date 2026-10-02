"""Autouse fixture: no component process outlives the test that started it.

Loaded by tests/integration/conftest.py; kept in its own module so
test_proc_isolation.py can load it into a separate pytest run and show that
it works when a test fails.
"""

from __future__ import annotations

import pytest

import integration_support as it


@pytest.fixture(autouse=True)
def stop_leftover_processes():
    yield
    stopped = it.stop_leftover_processes()
    if stopped:
        # Visible in the output of a failed test: these were still running.
        print(f"stopped leftover processes: {', '.join(stopped)}")
