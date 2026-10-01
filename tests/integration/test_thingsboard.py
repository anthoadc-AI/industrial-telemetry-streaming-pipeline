"""Integration, full profile only: the rule-chain import against a real ThingsBoard."""

from __future__ import annotations

import importlib.util
import sys

import pytest

import integration_support as it

spec = importlib.util.spec_from_file_location("import_rule_chains_it", it.REPO_ROOT / "scripts" / "thingsboard" / "import_rule_chains.py")
irc = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = irc
spec.loader.exec_module(irc)

pytestmark = pytest.mark.full


def import_ids() -> dict[str, str]:
    result = it.run_module("scripts/thingsboard/import_rule_chains.py")
    assert result.returncode == 0, result.stderr[-800:]
    events = it.json_events(result.stderr)
    return {e["rule_chain"]: e["id"] for e in events if e["event"] in ("rule_chain_exists", "rule_chain_created")}


def test_rule_chains_imported_and_linked(stack):
    tb = irc.ThingsBoard(f"http://localhost:{irc.setting('TB_HTTP_PORT', '8080')}")
    tb.login(irc.setting("TB_TENANT_USERNAME"), irc.setting("TB_TENANT_PASSWORD"))
    chains = {c["name"]: c for c in tb.request("GET", "/api/ruleChains", query={"pageSize": 100, "page": 0})["data"]}
    names = {c: chains[c]["id"]["id"] for c in ("TempToFirebase", "AlarmToFirebase", "CreateAndClearAlarms", "Root Rule Chain")}
    assert chains["Root Rule Chain"]["root"] is True
    by_id = {v: k for k, v in names.items()}
    counts, refs = {}, {}
    for name, chain_id in names.items():
        md = tb.request("GET", f"/api/ruleChain/{chain_id}/metadata")
        counts[name] = (len(md["nodes"]), len(md["connections"] or []))
        refs[name] = sorted(by_id.get(n["configuration"]["ruleChainId"], "UNRESOLVED")
                            for n in md["nodes"] if n["type"].endswith("TbRuleChainInputNode"))
    assert counts == {"TempToFirebase": (1, 0), "AlarmToFirebase": (1, 0), "CreateAndClearAlarms": (5, 4), "Root Rule Chain": (9, 8)}
    assert refs == {"TempToFirebase": [], "AlarmToFirebase": [], "CreateAndClearAlarms": ["AlarmToFirebase", "TempToFirebase"],
                    "Root Rule Chain": ["CreateAndClearAlarms"]}


def test_import_is_idempotent(stack):
    first, second = import_ids(), import_ids()
    assert first == second and len(first) == 4
