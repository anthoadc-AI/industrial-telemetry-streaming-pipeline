"""The integration session's volume-leak check (tests/integration/integration_support.py).

CI run 37167373432 failed the first full-profile session on a clean runner: ThingsBoard's named volumes were
created by `up` and kept by `down`, and the check, which compared counts, called them a leak. The check now
allows the named volumes docker-compose.yml declares, and nothing else.
"""

from __future__ import annotations

import sys

import yaml

from conftest import REPO_ROOT

sys.path.insert(0, str(REPO_ROOT / "tests" / "integration"))
import integration_support as it  # noqa: E402

# The shape `docker compose config --format json` gives: the project name and each volume's resolved name.
CONFIG = {"name": "industrial-telemetry",
          "volumes": {"tb-data": {"name": "industrial-telemetry_tb-data"},
                      "tb-logs": {"name": "industrial-telemetry_tb-logs"},
                      "shared": {"name": "shared-cache", "external": True}}}
ALLOWED = it.project_volume_names(CONFIG)
BEFORE = {"mytb-data", "0f3c9a"}


def test_allowed_volumes_are_the_projects_own_named_volumes():
    assert ALLOWED == {"industrial-telemetry_tb-data", "industrial-telemetry_tb-logs"}  # external: not created by up


def test_allowed_volumes_follow_docker_compose_yml():
    """Derived from the file, not fixed in code: its project name and its top-level named volumes."""
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    names = it.project_volume_names(compose)  # unresolved specs: named <project>_<volume>, as Compose does
    assert names == {f"{compose['name']}_{v}" for v in compose["volumes"]}
    assert "industrial-telemetry_tb-data" in names


def test_a_project_volume_that_appears_passes():
    """A clean machine: the first `up` creates ThingsBoard's volumes, `down` keeps them."""
    assert it.leaked_volumes(BEFORE, BEFORE | ALLOWED, ALLOWED) == set()


def test_an_anonymous_or_foreign_volume_that_appears_fails():
    anonymous = "3b1e5d0c7a2f4e6b9d8c1a0f2e3d4c5b6a7f8e9d0c1b2a3f4e5d6c7b8a9f0e1d"
    after = BEFORE | {"industrial-telemetry_tb-data", anonymous, "other-project_data"}
    assert it.leaked_volumes(BEFORE, after, ALLOWED) == {anonymous, "other-project_data"}


def test_no_change_passes():
    assert it.leaked_volumes(BEFORE, set(BEFORE), ALLOWED) == set()
    assert it.leaked_volumes(BEFORE | ALLOWED, BEFORE | ALLOWED, ALLOWED) == set()  # already there before
