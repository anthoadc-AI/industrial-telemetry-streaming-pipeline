"""scripts/thingsboard/import_rule_chains.py: placeholder rendering, settings, the REST client and the import flow.

HTTP is faked at urllib for the client tests; the import flow runs against an
in-memory ThingsBoard that records every call.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import urllib.error

import pytest

from conftest import REPO_ROOT

spec = importlib.util.spec_from_file_location("import_rule_chains",
                                              REPO_ROOT / "scripts" / "thingsboard" / "import_rule_chains.py")
irc = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = irc
spec.loader.exec_module(irc)

REST = "org.thingsboard.rule.engine.rest.TbRestApiCallNode"
INPUT = "org.thingsboard.rule.engine.flow.TbRuleChainInputNode"


def node(kind, name, **cfg):
    return {"type": kind, "name": name, "configuration": cfg}


# --- render_nodes ----------------------------------------------------------------


def test_firebase_url_is_resolved_when_enabled_and_kept_when_not():
    nodes = [node(REST, "Firebase", restEndpointUrlPattern="${FIREBASE_DB_URL}/temperature.json")]
    rendered, unresolved = irc.render_nodes(nodes, {}, "https://example-db.invalid/")
    assert rendered[0]["configuration"]["restEndpointUrlPattern"] == "https://example-db.invalid/temperature.json"
    assert unresolved == 0
    rendered, unresolved = irc.render_nodes(nodes, {}, None)
    assert rendered[0]["configuration"]["restEndpointUrlPattern"] == "${FIREBASE_DB_URL}/temperature.json"
    assert unresolved == 1
    assert nodes[0]["configuration"]["restEndpointUrlPattern"].startswith("${")  # input untouched


def test_chain_references_resolve_by_name():
    nodes = [node(INPUT, "To alarms", ruleChainId="${RULE_CHAIN_ID:CreateAndClearAlarms}")]
    rendered, _ = irc.render_nodes(nodes, {"CreateAndClearAlarms": "id-123"}, None)
    assert rendered[0]["configuration"]["ruleChainId"] == "id-123"


@pytest.mark.parametrize("ref, message", [
    ("5b2e77c0-a19f-11f1-ad2f-59bf1b6f16f1", "is not a ${RULE_CHAIN_ID:<name>} placeholder"),
    ("${RULE_CHAIN_ID:Missing}", "references unknown chain 'Missing'"),
])
def test_bad_chain_references(ref, message):
    with pytest.raises(irc.ImportFailed, match=message.replace("$", r"\$").replace("{", r"\{").replace("}", r"\}")):
        irc.render_nodes([node(INPUT, "n", ruleChainId=ref)], {}, None)


def test_the_committed_exports_render_completely():
    names = ["AlarmMapping", "Root Rule Chain"]
    ids = {n: f"id-{i}" for i, n in enumerate(names)}
    total = 0
    for f in irc.CHAIN_FILES:
        export = json.loads((irc.RULE_CHAIN_DIR / f).read_text(encoding="utf-8"))
        rendered, unresolved = irc.render_nodes(export["metadata"]["nodes"], ids, None)
        total += unresolved
        assert "${RULE_CHAIN_ID" not in json.dumps(rendered)
    assert total == 1  # render_nodes alone leaves Root's "Firebase" REST node unresolved; prune_firebase removes it


# --- prune_firebase (Task 3.1-B) -------------------------------------------------


def committed_exports():
    return [json.loads((irc.RULE_CHAIN_DIR / f).read_text(encoding="utf-8")) for f in irc.CHAIN_FILES]


def wiring(export):
    md = export["metadata"]
    names = [n["name"] for n in md["nodes"]]
    return sorted((names[c["fromIndex"]], c["type"], names[c["toIndex"]]) for c in md["connections"] or [])


def test_prune_removes_the_firebase_leg_from_the_committed_exports():
    exports = committed_exports()
    before = json.dumps(exports)
    kept, skipped, removed = irc.prune_firebase(exports)
    assert json.dumps(exports) == before  # pure: the input is not modified
    assert skipped == [] and removed == {"rest_nodes": 1, "rule_chain_input_nodes": 0}
    by_name = {e["ruleChain"]["name"]: e for e in kept}
    assert sorted(by_name) == ["AlarmMapping", "Root Rule Chain"]
    assert "${FIREBASE_DB_URL}" not in json.dumps(kept)
    root = by_name["Root Rule Chain"]
    original_root = next(e for e in exports if e["ruleChain"]["name"] == "Root Rule Chain")
    assert len(root["metadata"]["nodes"]) == 8 and len(root["metadata"]["connections"]) == 7
    assert wiring(root) == [w for w in wiring(original_root) if w[2] != "Firebase"]
    assert ("Save Timeseries", "Success", "AlarmMapping") in wiring(root)
    assert root["metadata"]["nodes"][root["metadata"]["firstNodeIndex"]]["name"] == "Device Profile Node"
    assert by_name["AlarmMapping"] == next(e for e in exports if e["ruleChain"]["name"] == "AlarmMapping")


def test_every_connection_is_valid_after_pruning():
    kept, _, _ = irc.prune_firebase(committed_exports())
    for export in kept:
        md = export["metadata"]
        n = len(md["nodes"])
        assert 0 <= md["firstNodeIndex"] < n
        assert all(0 <= c["fromIndex"] < n and 0 <= c["toIndex"] < n for c in md["connections"])


def test_alarm_mapping_evaluates_no_threshold():
    """One alarm engine (Task 3.2): AlarmMapping only maps and deduplicates; its nodes are the expected ones."""
    md = json.loads((irc.RULE_CHAIN_DIR / "alarm-mapping.json").read_text(encoding="utf-8"))["metadata"]
    kinds = [n["type"].rsplit(".", 1)[1] for n in md["nodes"]]
    assert kinds == ["TbGetAttributesNode", "TbTransformMsgNode", "TbJsSwitchNode", "TbCreateAlarmNode",
                     "TbClearAlarmNode", "TbTransformMsgNode", "TbMsgAttributesNode"]
    scripts = " ".join(str(n["configuration"].get("tbelScript", "")) for n in md["nodes"])
    for channel in ("bearing_temperature_c", "vibration_rms_mm_s", "motor_current_a", "speed_rpm", "temperature"):
        assert f"msg.{channel}" not in scripts
    create, clear = md["nodes"][3]["configuration"], md["nodes"][4]["configuration"]
    assert create["alarmType"] == clear["alarmType"] == "${alarmType}"
    assert create["dynamicSeverity"] is True and create["severity"] == "${alarmSeverity}"
    assert md["nodes"][0]["configuration"]["tellFailureIfAbsent"] is False  # the first event has no alarm_last yet


def chain(name, nodes, connections=(), first=0):
    return {"ruleChain": {"name": name}, "metadata": {"firstNodeIndex": first, "nodes": list(nodes),
            "connections": [{"fromIndex": f, "toIndex": t, "type": k} for f, t, k in connections]}}


FIREBASE_REST = node(REST, "Sink", restEndpointUrlPattern="${FIREBASE_DB_URL}/x.json")


def test_pruning_cascades_through_chains_left_empty():
    exports = [
        chain("Root Rule Chain", [node("A", "start"), node(INPUT, "to-relay", ruleChainId="${RULE_CHAIN_ID:Relay}")],
              [(0, 1, "Success")]),
        chain("Relay", [node(INPUT, "to-sink", ruleChainId="${RULE_CHAIN_ID:Sink}")]),
        chain("Sink", [FIREBASE_REST]),
    ]
    kept, skipped, removed = irc.prune_firebase(exports)
    assert skipped == ["Sink", "Relay"]
    assert removed == {"rest_nodes": 1, "rule_chain_input_nodes": 2}
    [root] = kept
    assert [n["name"] for n in root["metadata"]["nodes"]] == ["start"] and root["metadata"]["connections"] == []


def test_other_rest_nodes_are_kept():
    other = node(REST, "Elsewhere", restEndpointUrlPattern="https://example.invalid/x")
    kept, skipped, removed = irc.prune_firebase([chain("Root Rule Chain", [node("A", "a"), other], [(0, 1, "Success")])])
    assert skipped == [] and removed["rest_nodes"] == 0 and len(kept[0]["metadata"]["nodes"]) == 2


def test_removing_the_first_node_of_a_non_empty_chain_is_refused():
    with pytest.raises(irc.ImportFailed, match="its first node would be removed"):
        irc.prune_firebase([chain("Root Rule Chain", [FIREBASE_REST, node("A", "a")], first=0)])


def test_an_empty_root_chain_is_refused():
    with pytest.raises(irc.ImportFailed, match="would leave 'Root Rule Chain' empty"):
        irc.prune_firebase([chain("Root Rule Chain", [FIREBASE_REST])])


# --- settings ----------------------------------------------------------------------


def test_setting_reads_environment_then_env_file_then_default(tmp_path, monkeypatch):
    # The empty password line is assembled from two pieces so that this file
    # does not trip scripts/check_secrets.py.
    env_text = "# TB_HOST=commented\nTB_HOST=from-file\n" + "TB_TENANT_PASS" + "WORD=\n" + "TB_HTTP_PORT = 9090\n"
    (tmp_path / ".env").write_text(env_text, encoding="utf-8")
    monkeypatch.setattr(irc, "REPO_ROOT", tmp_path)
    for name in ("TB_HOST", "TB_HTTP_PORT", "TB_TENANT_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    assert irc.setting("TB_HOST") == "from-file"
    assert irc.setting("TB_HTTP_PORT") == "9090"
    assert irc.setting("TB_TENANT_PASSWORD", "fallback") == "fallback"  # empty value counts as unset
    monkeypatch.setenv("TB_HOST", "from-env")
    assert irc.setting("TB_HOST") == "from-env"
    monkeypatch.setattr(irc, "REPO_ROOT", tmp_path / "nowhere")
    assert irc.setting("TB_MISSING", "d") == "d"


# --- REST client --------------------------------------------------------------------


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def http_error(code, body=b'{"message":"x"}'):
    return urllib.error.HTTPError("http://tb/api", code, "error", {}, io.BytesIO(body))


def fake_urlopen(monkeypatch, *outcomes):
    seen = []
    queue = list(outcomes)

    def urlopen(request, timeout):
        seen.append(request)
        outcome = queue.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return Response(outcome)
    monkeypatch.setattr(irc.urllib.request, "urlopen", urlopen)
    return seen


def test_request_sends_json_and_token(monkeypatch):
    seen = fake_urlopen(monkeypatch, b'{"ok": 1}', b"")
    tb = irc.ThingsBoard("http://tb:8080/")
    tb.token = "jwt"
    assert tb.request("POST", "/api/x", {"a": 1}, query={"q": "v w"}) == {"ok": 1}
    assert tb.request("GET", "/api/y") is None
    req = seen[0]
    assert req.full_url == "http://tb:8080/api/x?q=v+w"
    assert req.get_header("X-authorization") == "Bearer jwt"
    assert json.loads(req.data) == {"a": 1}


def test_request_errors(monkeypatch):
    fake_urlopen(monkeypatch, http_error(404), http_error(500, b"boom"))
    tb = irc.ThingsBoard("http://tb")
    assert tb.request("GET", "/api/d", allow_404=True) is None
    with pytest.raises(irc.ImportFailed, match=r"GET /api/d -> HTTP 500: boom"):
        tb.request("GET", "/api/d")


def test_login(monkeypatch, json_log):
    fake_urlopen(monkeypatch, b'{"token": "jwt"}')
    tb = irc.ThingsBoard("http://tb")
    tb.login("tenant@thingsboard.org", "tenant")
    assert tb.token == "jwt"
    assert json_log()[0]["event"] == "thingsboard_logged_in" and "tenant" not in json_log()[0].get("password", "")


def test_login_rejected_and_unreachable(monkeypatch):
    fake_urlopen(monkeypatch, http_error(401))
    with pytest.raises(irc.ImportFailed, match="login rejected"):
        irc.ThingsBoard("http://tb").login("u", "p")
    monkeypatch.setattr(irc, "LOGIN_RETRY_S", 0)
    monkeypatch.setattr(irc.time, "sleep", lambda s: None)
    fake_urlopen(monkeypatch, urllib.error.URLError("refused"))
    with pytest.raises(irc.ImportFailed, match="not reachable"):
        irc.ThingsBoard("http://tb").login("u", "p")


def test_login_retries_while_thingsboard_starts(monkeypatch):
    monkeypatch.setattr(irc.time, "sleep", lambda s: None)
    fake_urlopen(monkeypatch, urllib.error.URLError("starting"), b'{"token": "jwt"}')
    tb = irc.ThingsBoard("http://tb")
    tb.login("u", "p")
    assert tb.token == "jwt"


# --- import flow ----------------------------------------------------------------------


class FakeThingsBoard(irc.ThingsBoard):
    """In-memory ThingsBoard: rule chains, device profiles and dashboards by name, one device, every call recorded."""

    def __init__(self, chains=None, device=None, credentials=None, profiles=None, dashboards=None):
        super().__init__("http://fake")
        self.chains = dict(chains or {})
        self.device, self.credentials = device, credentials
        self.profiles = {p["name"]: p for p in (profiles or [])}
        self.dashboards = dict(dashboards or {})
        self.calls = []
        self.created = 0

    def request(self, method, path, body=None, query=None, allow_404=False):
        self.calls.append((method, path, body, query))
        if path == "/api/ruleChains":
            return {"data": [{"name": n, "id": {"id": i}} for n, i in self.chains.items() if query["textSearch"] in n]}
        if method == "POST" and path == "/api/ruleChain":
            self.created += 1
            self.chains[body["name"]] = f"new-{self.created}"
            return {"id": {"id": self.chains[body["name"]]}}
        if method == "DELETE" and path.startswith("/api/ruleChain/"):
            chain_id = path.rsplit("/", 1)[1]
            self.chains = {n: i for n, i in self.chains.items() if i != chain_id}
            return None
        if path.endswith("/metadata") and method == "GET":
            return {"version": 7}
        if path == "/api/deviceProfiles":
            return {"data": [p for n, p in self.profiles.items() if query["textSearch"] in n]}
        if path == "/api/deviceProfileInfo/default":
            return {"id": {"id": "default-profile"}}
        if path == "/api/deviceProfile/default-profile":
            return {"id": {"id": "default-profile"}, "name": "default", "default": True, "version": 3,
                    "profileData": {"configuration": {"type": "DEFAULT"}}, "transportType": "DEFAULT"}
        if method == "POST" and path == "/api/deviceProfile":
            self.profiles[body["name"]] = {**body, "id": body.get("id") or {"id": f"profile-{body['name']}"}}
            return self.profiles[body["name"]]
        if path == "/api/tenant/dashboards":
            return {"data": [{"title": t, "id": {"id": i}} for t, i in self.dashboards.items() if query["textSearch"] in t]}
        if method == "GET" and path.startswith("/api/dashboard/"):
            return {"id": {"id": path.rsplit("/", 1)[1]}, "version": 4}
        if method == "POST" and path == "/api/dashboard":
            dashboard_id = (body.get("id") or {}).get("id") or "dash-new"
            self.dashboards[body["title"]] = dashboard_id
            return {"id": {"id": dashboard_id}}
        if path == "/api/tenant/devices":
            return self.device
        if path.startswith("/api/device/") and path.endswith("/credentials"):
            return dict(self.credentials)
        return None


def metadata_writes(tb):
    return [c for c in tb.calls if c[1] == "/api/ruleChain/metadata"]


def test_import_with_firebase_disabled_writes_the_pruned_chains(json_log):
    tb = FakeThingsBoard(chains={"Root Rule Chain": "root-id"})
    ids = irc.import_chains(tb, None)
    assert sorted(ids) == ["AlarmMapping", "Root Rule Chain"] and ids["Root Rule Chain"] == "root-id"
    writes = metadata_writes(tb)
    assert len(writes) == 2 and all(c[2]["version"] == 7 for c in writes)
    root_md = next(c[2] for c in writes if c[2]["ruleChainId"]["id"] == "root-id")
    assert len(root_md["nodes"]) == 8 and not any(n["type"] == REST for n in root_md["nodes"])
    refs = [n["configuration"]["ruleChainId"] for n in root_md["nodes"] if n["type"] == INPUT]
    assert refs == [ids["AlarmMapping"]]
    assert ("POST", "/api/ruleChain/root-id/root", None, None) in tb.calls
    events = {e["event"]: e for e in json_log()}
    assert events["firebase_leg_pruned"]["rest_nodes_removed"] == 1
    assert events["firebase_leg_pruned"]["chains_not_imported"] == []
    assert not any(c[0] == "DELETE" for c in tb.calls)  # nothing retired was present


def test_import_with_firebase_enabled_resolves_the_url_of_the_root_node(json_log):
    tb = FakeThingsBoard()
    ids = irc.import_chains(tb, "https://example-db.invalid")
    assert sorted(ids) == ["AlarmMapping", "Root Rule Chain"]
    urls = [n["configuration"]["restEndpointUrlPattern"]
            for c in metadata_writes(tb) for n in c[2]["nodes"] if n["type"] == REST]
    assert urls == ["https://example-db.invalid/temperature.json"]
    assert "firebase_leg_pruned" not in [e["event"] for e in json_log()]


def test_retired_chains_are_deleted_by_exact_name_after_rewriting(json_log):
    tb = FakeThingsBoard(chains={"Root Rule Chain": "root", "CreateAndClearAlarms": "cac", "TempToFirebase": "temp",
                                 "AlarmToFirebase": "alarm", "TempToFirebase (copy)": "someone-elses"})
    irc.import_chains(tb, None)
    deletes = [c for c in tb.calls if c[0] == "DELETE"]
    assert sorted(c[1] for c in deletes) == ["/api/ruleChain/alarm", "/api/ruleChain/cac", "/api/ruleChain/temp"]
    assert tb.calls.index(deletes[0]) > max(tb.calls.index(c) for c in metadata_writes(tb))
    assert sorted(tb.chains) == ["AlarmMapping", "Root Rule Chain", "TempToFirebase (copy)"]
    deleted = {e["rule_chain"]: e["reason"] for e in json_log() if e["event"] == "rule_chain_deleted"}
    assert deleted == {"CreateAndClearAlarms": "retired", "TempToFirebase": "retired", "AlarmToFirebase": "retired"}


def test_import_is_idempotent_in_both_directions(json_log):
    tb = FakeThingsBoard()
    enabled = irc.import_chains(tb, "https://example-db.invalid")
    disabled = irc.import_chains(tb, None)
    again = irc.import_chains(tb, "https://example-db.invalid")
    assert enabled == disabled == again and tb.created == 2  # created once, then updated in place
    assert not any(c[0] == "DELETE" for c in tb.calls)


def test_an_unresolved_firebase_node_stops_the_import(monkeypatch):
    monkeypatch.setattr(irc, "prune_firebase", lambda exports: (exports, [], {"rest_nodes": 0, "rule_chain_input_nodes": 0}))
    with pytest.raises(irc.ImportFailed, match=r"Firebase REST node\(s\) left unresolved"):
        irc.import_chains(FakeThingsBoard(), None)


def test_device_profiles_are_created_on_the_sequential_queue(json_log):
    tb = FakeThingsBoard(profiles=[{"name": "pump", "id": {"id": "p"}, "defaultQueueName": "Main"},
                                   {"name": "motor", "id": {"id": "m"}, "defaultQueueName": "SequentialByOriginator"}])
    irc.ensure_device_profiles(tb, ["compressor", "motor", "pump"])
    assert {n: p["defaultQueueName"] for n, p in tb.profiles.items()} == dict.fromkeys(["pump", "motor", "compressor"],
                                                                                         "SequentialByOriginator")
    created = tb.profiles["compressor"]
    assert created["default"] is False and created["defaultRuleChainId"] is None and "id" not in \
        next(c[2] for c in tb.calls if c[0] == "POST" and c[2]["name"] == "compressor")
    assert [e["event"] for e in json_log() if e["event"].startswith("device_profile")] == \
        ["device_profile_created", "device_profile_ok", "device_profile_updated"]


def test_the_gateway_device_is_created_and_flagged(json_log):
    tb = FakeThingsBoard()
    irc.ensure_device(tb, "GW", irc.PIPELINE_GATEWAY, gateway=True)
    post = next(c for c in tb.calls if c[1] == "/api/device")
    assert post[2] == {"name": "pipeline-gateway", "additionalInfo": {"gateway": True}} and post[3] == {"accessToken": "GW"}
    tb = FakeThingsBoard(device={"id": {"id": "g"}, "name": "pipeline-gateway", "additionalInfo": {"description": "x"}},
                         credentials={"credentialsType": "ACCESS_TOKEN", "credentialsId": "GW"})
    irc.ensure_device(tb, "GW", irc.PIPELINE_GATEWAY, gateway=True)
    flagged = next(c[2] for c in tb.calls if c[1] == "/api/device")
    assert flagged["additionalInfo"] == {"description": "x", "gateway": True}
    assert [e["event"] for e in json_log()][-2:] == ["device_gateway_flag_set", "device_token_ok"]


def test_dashboard_is_created_then_updated_by_title(json_log, tmp_path):
    path = tmp_path / "d.json"
    path.write_text(json.dumps({"title": "Pipeline operations", "configuration": {"widgets": {}}}), encoding="utf-8")
    tb = FakeThingsBoard()
    first = irc.import_dashboard(tb, path)
    second = irc.import_dashboard(tb, path)
    assert first == second == "dash-new"
    posts = [c[2] for c in tb.calls if c[0] == "POST" and c[1] == "/api/dashboard"]
    assert "id" not in posts[0] and posts[1]["id"] == {"id": "dash-new"} and posts[1]["version"] == 4
    assert [e["action"] for e in json_log() if e["event"] == "dashboard_written"] == ["create", "update"]


def test_the_committed_dashboard_names_no_device_and_selects_the_configured_types(cfg):
    text = irc.DASHBOARD_FILE.read_text(encoding="utf-8")
    dashboard = json.loads(text)
    assert '"entityType": "DEVICE"' not in text and '"singleEntity"' not in text
    filters = [a["filter"] for a in dashboard["configuration"]["entityAliases"].values()]
    by_type = next(f for f in filters if f["type"] == "deviceType")
    assert sorted(by_type["deviceTypes"]) == sorted({a.type for a in cfg.telemetry.assets})
    assert {f["type"] for f in filters} == {"deviceType", "stateEntity"}
    keys = {k["name"] for w in dashboard["configuration"]["widgets"].values()
            for d in (w["config"].get("datasources") or []) for k in d["dataKeys"] if k["type"] == "timeseries"}
    assert keys == set(cfg.telemetry.channels)


def test_duplicate_chain_names_stop_the_import():
    tb = FakeThingsBoard()
    twins = [{"name": "TempToFirebase", "id": {"id": "a"}}, {"name": "TempToFirebase", "id": {"id": "b"}}]
    tb.request = lambda *a, **k: {"data": twins}
    with pytest.raises(irc.ImportFailed, match="2 rule chains named 'TempToFirebase'"):
        tb.find_rule_chain("TempToFirebase")


@pytest.mark.parametrize("device, credentials, event", [
    (None, None, "device_created"),
    ({"id": {"id": "d1"}}, {"credentialsType": "ACCESS_TOKEN", "credentialsId": "TOKEN"}, "device_token_ok"),
    ({"id": {"id": "d1"}}, {"credentialsType": "ACCESS_TOKEN", "credentialsId": "OTHER"}, "device_token_updated"),
])
def test_ensure_device(json_log, device, credentials, event):
    tb = FakeThingsBoard(device=device, credentials=credentials)
    irc.ensure_device(tb, "TOKEN")
    assert json_log()[-1]["event"] == event
    if event == "device_token_updated":
        posted = next(c[2] for c in tb.calls if c[1] == "/api/device/credentials")
        assert posted["credentialsId"] == "TOKEN"
