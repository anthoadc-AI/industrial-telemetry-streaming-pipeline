"""Integration, full profile only (not in CI): the ThingsBoard import against a real ThingsBoard."""

from __future__ import annotations

import importlib.util
import sys

import pytest

import integration_support as it
from config.loader import load_config

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


def tb_client():
    tb = irc.ThingsBoard(f"http://localhost:{irc.setting('TB_HTTP_PORT', '8080')}")
    tb.login(irc.setting("TB_TENANT_USERNAME"), irc.setting("TB_TENANT_PASSWORD"))
    return tb


def test_rule_chains_imported_and_linked(stack):
    """With FIREBASE_ENABLED=false (.env.example): Root and AlarmMapping only, nothing left to fail.

    The retired course chains do not exist; Root Rule Chain hands saved telemetry to AlarmMapping and has no
    REST node (its Firebase node pruned); AlarmMapping has no rule-chain input node.
    """
    assert (irc.setting("FIREBASE_ENABLED", "false") or "").lower() != "true", "these assertions are for the disabled mode"
    tb = tb_client()
    chains = {c["name"]: c for c in tb.request("GET", "/api/ruleChains", query={"pageSize": 100, "page": 0})["data"]}
    assert not set(irc.RETIRED_CHAINS) & set(chains)
    names = {c: chains[c]["id"]["id"] for c in ("AlarmMapping", "Root Rule Chain")}
    assert chains["Root Rule Chain"]["root"] is True
    by_id = {v: k for k, v in names.items()}
    counts, refs, rest = {}, {}, 0
    for name, chain_id in names.items():
        md = tb.request("GET", f"/api/ruleChain/{chain_id}/metadata")
        counts[name] = (len(md["nodes"]), len(md["connections"] or []))
        refs[name] = sorted(by_id.get(n["configuration"]["ruleChainId"], "UNRESOLVED")
                            for n in md["nodes"] if n["type"].endswith("TbRuleChainInputNode"))
        rest += sum(n["type"].endswith("TbRestApiCallNode") for n in md["nodes"])
    assert counts == {"AlarmMapping": (10, 18), "Root Rule Chain": (8, 7)}
    assert refs == {"AlarmMapping": [], "Root Rule Chain": ["AlarmMapping"]}
    assert rest == 0


def test_profiles_gateway_and_dashboard_imported(stack):
    tb = tb_client()
    types = sorted({a.type for a in load_config(env={}).telemetry.assets})
    for name in types:
        profile = irc.find_device_profile(tb, name)
        assert profile and profile["defaultQueueName"] == irc.ALARM_QUEUE == "PipelineSequential", name
    gateway = tb.request("GET", "/api/tenant/devices", query={"deviceName": irc.PIPELINE_GATEWAY})
    assert gateway["additionalInfo"]["gateway"] is True
    dashboards = tb.request("GET", "/api/tenant/dashboards",
                            query={"pageSize": 100, "page": 0, "textSearch": "Pipeline operations"})
    [dashboard] = [d for d in dashboards["data"] if d["title"] == "Pipeline operations"]
    full = tb.request("GET", f"/api/dashboard/{dashboard['id']['id']}")
    alias = next(a for a in full["configuration"]["entityAliases"].values() if a["filter"]["type"] == "deviceType")
    assert sorted(alias["filter"]["deviceTypes"]) == types


def test_import_is_idempotent(stack):
    first, second = import_ids(), import_ids()
    assert first == second and sorted(first) == ["AlarmMapping", "Root Rule Chain"]
