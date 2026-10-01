"""Integration, POSIX only: SIGTERM stops the simulator and the bridge cleanly, with nothing lost.

Windows cannot deliver SIGTERM to a native process from outside (Phase 0
found this; the handlers share the SIGBREAK path tested by hand), so these
tests run where SIGTERM exists: Linux CI.
"""

from __future__ import annotations

import os
import signal
import time

import pytest

import integration_support as it

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SIGTERM cannot be sent to a native Windows process; runs on Linux (CI)")


def test_sigterm_flushes_simulator_and_bridge(stack, offsets_mark, log_dir):
    b = it.bridge(log_dir, "bridge", 100000, duration=300)
    s = it.simulator(log_dir, "simulator", 100000, seed=41)
    s.wait_for("run_started", 30)
    time.sleep(3)
    s.p.send_signal(signal.SIGTERM)
    assert s.finish(30) == 0
    sim = s.find("run_finished")[0]
    assert sim["reason"] == "signal" and sim["messages_sent"] == sim["messages_acked"] > 0

    time.sleep(2)
    b.p.send_signal(signal.SIGTERM)
    assert b.finish(30) == 0
    run = b.find("run_finished")[0]
    assert run["reason"] == "signal"
    assert run["messages_received"] == run["messages_delivered"] == sim["messages_sent"]
    assert len(it.read_since("telemetry.raw", offsets_mark["telemetry.raw"], deserialize=False)) == sim["messages_sent"]
