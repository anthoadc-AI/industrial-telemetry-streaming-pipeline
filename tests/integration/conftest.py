"""Integration-test fixtures: one stack per session, offset marks and unique consumer groups per test.

The session fixture refuses to run if a stack (or anything on its ports) is
already up, starts the stack with the repository's docker-compose.yml (core
profile, or full with ThingsBoard), provisions it, and stops it at the end
with `docker compose --profile full down`, never -v. It lists Docker volumes
before and after and fails the session if any leaked: a new volume other than
the named volumes docker-compose.yml declares (those are created by the first
`up` on a clean machine and kept by `down`, by design). Before stopping, it
saves `docker compose logs` as docker-compose.log in pytest's base temp
directory, beside the component logs. With the full profile it also fails the
session if any pipeline alarm is still active in ThingsBoard at the end
(owner, Task 3.2): every test must leave the alarms it raised cleared, since
ThingsBoard's volume outlives the session and its Kafka.

Options:
  --stack-profile core|full   core: Mosquitto, ZooKeeper, Kafka, Schema Registry
                              full: core plus ThingsBoard and the rule-chain import
  --reverse-order             run the tests in reverse, to show they do not depend
                              on order
"""

from __future__ import annotations

import time

import pytest

import integration_support as it
from proc_isolation import stop_leftover_processes  # noqa: F401  (autouse fixture)

TOPICS = ("telemetry.raw", "telemetry.alarms", "telemetry.dlq")


def pytest_addoption(parser):
    parser.addoption("--stack-profile", choices=["core", "full"], default="core")
    parser.addoption("--reverse-order", action="store_true", default=False)


def pytest_configure(config):
    config.addinivalue_line("markers", "full: needs the full profile (ThingsBoard)")
    config.stack_report = {}


def pytest_collection_modifyitems(config, items):
    if config.getoption("--stack-profile") != "full":
        skip = pytest.mark.skip(reason="needs --stack-profile=full (ThingsBoard)")
        for item in items:
            if "full" in item.keywords:
                item.add_marker(skip)
    if config.getoption("--reverse-order"):
        items.reverse()


@pytest.fixture(scope="session")
def stack(request, tmp_path_factory):
    profile = request.config.getoption("--stack-profile")
    ports = [1883, 9092, 8081] + ([8080, 9883] if profile == "full" else [])
    busy = [p for p in ports if it.port_open(p)]
    if busy or it.compose("--profile", "full", "ps", "-q").stdout.strip():
        pytest.exit(f"a stack is already running (ports in use: {busy}); stop it with `make down` first", returncode=3)

    report = request.config.stack_report
    report["profile"] = profile
    volumes_before = it.volume_names()
    allowed = it.project_volume_names(it.compose_config())
    report["volumes_before"] = len(volumes_before)
    profile_args = ["--profile", "full"] if profile == "full" else []
    started = time.monotonic()
    up = it.compose(*profile_args, "up", "-d", "--wait", "--wait-timeout", "600", "--force-recreate")
    if up.returncode != 0:
        it.compose("--profile", "full", "down")
        pytest.exit(f"docker compose up failed: {up.stderr[-800:]}", returncode=3)
    report["up_s"] = round(time.monotonic() - started, 1)
    it.provision()
    if profile == "full":
        result = it.run_module("scripts/thingsboard/import_rule_chains.py")
        assert result.returncode == 0, result.stderr[-800:]
    report["ready_s"] = round(time.monotonic() - started, 1)

    yield profile

    leftover: list[dict] | str = []
    if profile == "full":
        try:
            leftover = it.active_pipeline_alarms(it.thingsboard())
        except Exception as e:  # reported below, after the stack is stopped
            leftover = f"could not query ThingsBoard: {type(e).__name__}: {e}"
        report["active_pipeline_alarms"] = leftover if isinstance(leftover, str) else len(leftover)

    # Container logs are gone after `down`; keep them next to the component
    # logs (pytest's base temp directory, which CI uploads on failure).
    logs = it.compose("--profile", "full", "logs", "--no-color", "--timestamps")
    (tmp_path_factory.getbasetemp() / "docker-compose.log").write_text(logs.stdout + logs.stderr, encoding="utf-8")
    started = time.monotonic()
    down = it.compose("--profile", "full", "down")
    report["down_s"] = round(time.monotonic() - started, 1)
    volumes_after = it.volume_names()
    report["volumes_after"] = len(volumes_after)
    new = sorted(volumes_after - volumes_before)
    if new:
        report["volumes_new"] = new
    assert down.returncode == 0, down.stderr[-800:]
    leaked = it.leaked_volumes(volumes_before, volumes_after, allowed)
    assert not leaked, f"volumes leaked: {sorted(leaked)} (project volumes allowed: {sorted(allowed)}); {report}"
    assert not leftover, f"pipeline alarms still active in ThingsBoard at the end of the session: {leftover}"


@pytest.fixture
def offsets_mark(stack):
    """End offsets of the three pipeline topics at the start of the test."""
    return {topic: it.watermarks(topic) for topic in TOPICS}


@pytest.fixture
def fresh_group(stack):
    """A consumer group id used by this test only, positioned at the end of telemetry.raw."""
    def make() -> str:
        group = it.unique("it-alarms")
        it.position_group_at_end(group, "telemetry.raw")
        return group
    return make


@pytest.fixture
def log_dir(tmp_path):
    return tmp_path


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    report = getattr(config, "stack_report", None)
    if report:
        terminalreporter.write_sep("-", "integration stack")
        terminalreporter.write_line(
            f"profile={report.get('profile')} up={report.get('up_s')}s ready={report.get('ready_s')}s "
            f"down={report.get('down_s')}s volumes before={report.get('volumes_before')} after={report.get('volumes_after')}"
            + (f" new={report['volumes_new']}" if "volumes_new" in report else "")
            + (f" active_pipeline_alarms={report['active_pipeline_alarms']}" if "active_pipeline_alarms" in report else ""))
