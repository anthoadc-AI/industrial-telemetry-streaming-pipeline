"""Integration, full profile only: the rule-chain import against a real ThingsBoard."""

from __future__ import annotations

import importlib.util
import sys

import pytest

import integration_support as it

SCRIPT = it.REPO_ROOT / "scripts" / "thingsboard" / "import_rule_chains.py"
spec = importlib.util.spec_from_file_location("import_rule_chains_it", SCRIPT)
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
    """With FIREBASE_ENABLED=false (.env.example): the Firebase leg pruned, nothing left to fail.

    The two Firebase chains do not exist, Root Rule Chain has no REST node,
    and CreateAndClearAlarms has no rule-chain input node (Task 3.1-B).
    """
    assert (irc.setting("FIREBASE_ENABLED", "false") or "").lower() != "true", "these assertions are for the disabled mode"
    tb = irc.ThingsBoard(f"http://localhost:{irc.setting('TB_HTTP_PORT', '8080')}")
    tb.login(irc.setting("TB_TENANT_USERNAME"), irc.setting("TB_TENANT_PASSWORD"))
    chains = {c["name"]: c for c in tb.request("GET", "/api/ruleChains", query={"pageSize": 100, "page": 0})["data"]}
    assert "TempToFirebase" not in chains and "AlarmToFirebase" not in chains
    names = {c: chains[c]["id"]["id"] for c in ("CreateAndClearAlarms", "Root Rule Chain")}
    assert chains["Root Rule Chain"]["root"] is True
    by_id = {v: k for k, v in names.items()}
    counts, refs, rest = {}, {}, 0
    for name, chain_id in names.items():
        md = tb.request("GET", f"/api/ruleChain/{chain_id}/metadata")
        counts[name] = (len(md["nodes"]), len(md["connections"] or []))
        refs[name] = sorted(by_id.get(n["configuration"]["ruleChainId"], "UNRESOLVED")
                            for n in md["nodes"] if n["type"].endswith("TbRuleChainInputNode"))
        rest += sum(n["type"].endswith("TbRestApiCallNode") for n in md["nodes"])
    assert counts == {"CreateAndClearAlarms": (3, 2), "Root Rule Chain": (8, 7)}
    assert refs == {"CreateAndClearAlarms": [], "Root Rule Chain": ["CreateAndClearAlarms"]}
    assert rest == 0


def test_import_is_idempotent(stack):
    first, second = import_ids(), import_ids()
    assert first == second and sorted(first) == ["CreateAndClearAlarms", "Root Rule Chain"]
