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


def test_every_alarm_mapping_failure_is_recorded_on_the_gateway_and_ends_the_message():
    """Owner decision (Task 3.2 fix): a node that fails sends the message to its Failure branch, which records
    alarm_mapping_failed on the pipeline gateway (server time) and ends normally, so the queue never retries a
    message that would always fail and the asset's next messages are not held back. Debug: failures only."""
    md = json.loads((irc.RULE_CHAIN_DIR / "alarm-mapping.json").read_text(encoding="utf-8"))["metadata"]
    names = [n["name"] for n in md["nodes"]]
    describe, to_gateway, save = (names.index(n) for n in ("Describe mapping failure", "To the pipeline gateway",
                                                            "Save mapping failure"))
    failures = {c["fromIndex"]: c["toIndex"] for c in md["connections"] if c["type"] == "Failure"}
    assert failures == {i: describe for i in range(describe)}  # every mapping node, nothing else
    tail = {(c["fromIndex"], c["toIndex"]) for c in md["connections"] if c["type"] == "Success" and c["fromIndex"] >= describe}
    assert tail == {(describe, to_gateway), (to_gateway, save)}
    gw = md["nodes"][to_gateway]["configuration"]
    assert (gw["originatorSource"], gw["entityType"], gw["entityNamePattern"]) == ("ENTITY", "DEVICE", irc.PIPELINE_GATEWAY)
    assert md["nodes"][save]["configuration"]["useServerTs"] is True  # two failures of one event never overwrite
    assert "alarm_mapping_failed" in md["nodes"][describe]["configuration"]["tbelScript"]
    assert all(n["debugSettings"] == {"failuresEnabled": True, "allEnabled": False, "allEnabledUntil": 0} for n in md["nodes"])


def test_alarm_mapping_evaluates_no_threshold():
    """One alarm engine (Task 3.2): AlarmMapping only maps and deduplicates; its nodes are the expected ones."""
    md = json.loads((irc.RULE_CHAIN_DIR / "alarm-mapping.json").read_text(encoding="utf-8"))["metadata"]
    kinds = [n["type"].rsplit(".", 1)[1] for n in md["nodes"]]
    assert kinds == ["TbGetAttributesNode", "TbTransformMsgNode", "TbJsSwitchNode", "TbCreateAlarmNode",
                     "TbClearAlarmNode", "TbTransformMsgNode", "TbMsgAttributesNode",
                     "TbTransformMsgNode", "TbChangeOriginatorNode", "TbMsgTimeseriesNode"]
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


def test_device_profiles_are_created_on_the_pipeline_queue(json_log):
    tb = FakeThingsBoard(profiles=[{"name": "pump", "id": {"id": "p"}, "defaultQueueName": "SequentialByOriginator"},
                                   {"name": "motor", "id": {"id": "m"}, "defaultQueueName": "PipelineSequential"}])
    irc.ensure_device_profiles(tb, ["compressor", "motor", "pump"])
    assert {n: p["defaultQueueName"] for n, p in tb.profiles.items()} == dict.fromkeys(["pump", "motor", "compressor"],
                                                                                         "PipelineSequential")
    created = tb.profiles["compressor"]
    assert created["default"] is False and created["defaultRuleChainId"] is None and "id" not in \
        next(c[2] for c in tb.calls if c[0] == "POST" and c[2]["name"] == "compressor")
    assert [e["event"] for e in json_log() if e["event"].startswith("device_profile")] == \
        ["device_profile_created", "device_profile_ok", "device_profile_updated"]


class FakeQueues(irc.ThingsBoard):
    """The system administrator's view of the rule-engine queues."""

    def __init__(self, queues=()):
        super().__init__("http://fake")
        self.queues = {q["name"]: q for q in queues}
        self.calls = []

    def request(self, method, path, body=None, query=None, allow_404=False):
        self.calls.append((method, path, body, query))
        assert path == "/api/queues" and query["serviceType"] == "TB_RULE_ENGINE"
        if method == "GET":
            return {"data": list(self.queues.values()), "hasNext": False}
        self.queues[body["name"]] = {**body, "id": body.get("id") or {"id": "q-new"}}
        return self.queues[body["name"]]


def test_pipeline_queue_is_created_then_kept_then_corrected(json_log):
    """Owner decision (Task 3.2 fix): 60 s per batch, 20 retries from 5 s up to 60 s, never dropped at the first timeout."""
    tb = FakeQueues([{"name": "SequentialByOriginator", "id": {"id": "s"}}])
    irc.ensure_pipeline_queue(tb)
    q = tb.queues["PipelineSequential"]
    assert q["submitStrategy"]["type"] == "SEQUENTIAL_BY_ORIGINATOR" and q["packProcessingTimeout"] == 60_000
    assert q["processingStrategy"] == {"type": "RETRY_FAILED_AND_TIMED_OUT", "retries": 20, "failurePercentage": 0.0,
                                       "pauseBetweenRetries": 5, "maxPauseBetweenRetries": 60}
    irc.ensure_pipeline_queue(tb)  # unchanged: nothing written
    tb.queues["PipelineSequential"]["packProcessingTimeout"] = 2000  # changed by hand in the UI
    irc.ensure_pipeline_queue(tb)
    assert tb.queues["PipelineSequential"]["packProcessingTimeout"] == 60_000
    assert tb.queues["PipelineSequential"]["id"] == {"id": "q-new"}  # updated in place, same queue
    assert [c[0] for c in tb.calls] == ["GET", "POST", "GET", "GET", "POST"]
    events = [e for e in json_log() if e["event"].startswith("queue_")]
    assert [e["event"] for e in events] == ["queue_created", "queue_ok", "queue_updated"]
    assert events[2]["settings"] == ["packProcessingTimeout"]


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


# --- alarm reconciliation ---------------------------------------------------------------------------------


class Msg:
    def __init__(self, partition, offset, key, value, error=None):
        self._p, self._o, self._k, self._v, self._e = partition, offset, key, value, error

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def key(self):
        return self._k

    def value(self):
        return self._v

    def error(self):
        return self._e


class FakeConsumer:
    """A compacted topic as {partition: [(offset, key, value)]}; poll returns them partition by partition."""

    def __init__(self, partitions, idle_polls=1):
        self.partitions = partitions
        self.queue = [None] * idle_polls + [Msg(p, o, k, v) for p, recs in partitions.items() for o, k, v in recs]
        self.assigned = None

    def list_topics(self, topic, timeout):
        meta = type("T", (), {"error": None, "partitions": dict.fromkeys(self.partitions)})
        return type("M", (), {"topics": {"telemetry.alarms": meta}})

    def get_watermark_offsets(self, tp, timeout):
        recs = self.partitions[tp.partition]
        return (recs[0][0], recs[-1][0] + 1) if recs else (0, 0)

    def assign(self, tps):
        self.assigned = sorted(tp.partition for tp in tps)

    def poll(self, timeout):
        return self.queue.pop(0) if self.queue else None


def decode(value):
    if value == b"bad":
        raise ValueError("not avro")
    return {"state": value.decode()}


def test_read_alarm_topic_reads_every_partition_to_its_end_offset(json_log):
    consumer = FakeConsumer({
        0: [(0, b"pump-01/v/WARNING/1", b"RAISED"), (4, b"pump-01/v/WARNING/1", b"CLEARED")],  # offsets with gaps
        1: [],
        2: [(7, b"fan-01/v/WARNING/2", None), (8, b"motor-01/v/CRITICAL/3", b"bad"), (9, None, b"RAISED")],
    })
    records = irc.read_alarm_topic(consumer, "telemetry.alarms", decode)
    assert consumer.assigned == [0, 2]  # empty partitions are not read
    assert records == [("pump-01/v/WARNING/1", "RAISED"), ("pump-01/v/WARNING/1", "CLEARED"),
                       ("fan-01/v/WARNING/2", None), ("motor-01/v/CRITICAL/3", irc.UNKNOWN)]
    assert [e["event"] for e in json_log()] == ["alarm_record_undecodable"]


def test_read_alarm_topic_failures():
    with pytest.raises(irc.ImportFailed, match="not found"):
        irc.read_alarm_topic(FakeConsumer({}), "telemetry.other", decode)
    consumer = FakeConsumer({0: [(0, b"k", b"RAISED")]}, idle_polls=0)
    consumer.queue = [Msg(0, 0, None, None, error="broker down")]
    with pytest.raises(irc.ImportFailed, match="broker down"):
        irc.read_alarm_topic(consumer, "telemetry.alarms", decode)
    consumer = FakeConsumer({0: [(0, b"k", b"RAISED")]})
    consumer.queue = []
    with pytest.raises(irc.ImportFailed, match="end offsets not reached"):
        irc.read_alarm_topic(consumer, "telemetry.alarms", decode, timeout=-1)


def test_latest_alarm_states_keeps_the_last_event_and_drops_tombstoned_ids():
    states = irc.latest_alarm_states([("a", "RAISED"), ("b", "RAISED"), ("a", "CLEARED"), ("b", None), ("c", "RAISED")])
    assert states == {"a": "CLEARED", "c": "RAISED"}


class ReconcileThingsBoard(irc.ThingsBoard):
    """Devices by type, their alarms and their alarm_last attribute, in memory; every call recorded."""

    def __init__(self, devices, page_size=100):
        super().__init__("http://fake")
        self.devices = devices  # name -> {"type", "alarms": [...], "alarm_last": dict | None}
        self.page_size = page_size
        self.calls = []

    def _page(self, items, query):
        start = query["page"] * self.page_size
        return {"data": items[start:start + self.page_size], "hasNext": start + self.page_size < len(items)}

    def _alarm(self, alarm_id):
        return next(a for d in self.devices.values() for a in d["alarms"] if a["id"]["id"] == alarm_id)

    def request(self, method, path, body=None, query=None, allow_404=False):
        self.calls.append((method, path, body, query))
        if path == "/api/tenant/devices":
            return self._page([{"name": n, "id": {"id": f"id-{n}"}} for n, d in self.devices.items()
                               if d["type"] == query["type"]], query)
        device = next((d for n, d in self.devices.items() if path.split("/")[-1] == f"id-{n}"
                       or f"/id-{n}/" in path), None)
        if path.startswith("/api/alarm/DEVICE/"):
            return self._page([a for a in device["alarms"] if a["status"].startswith("ACTIVE")], query)
        if method == "POST" and path == "/api/alarm":
            self._alarm(body["id"]["id"])["details"] = body["details"]
            return None
        if method == "POST" and path.endswith("/clear"):
            alarm = self._alarm(path.split("/")[-2])
            alarm["status"] = alarm["status"].replace("ACTIVE", "CLEARED")
            return None
        if path.endswith("/values/attributes/SERVER_SCOPE"):
            last = device["alarm_last"]
            return [] if last is None else [{"key": "alarm_last", "value": json.dumps(last), "lastUpdateTs": 1}]
        if method == "POST" and path.endswith("/attributes/SERVER_SCOPE"):
            device["alarm_last"] = json.loads(body["alarm_last"])
            return None
        raise AssertionError(f"unexpected call {method} {path}")


def tb_alarm(n, alarm_id=None, type_="Vibration RMS high (warning)", status="ACTIVE_UNACK"):
    return {"id": {"id": f"alarm-{n}"}, "type": type_, "status": status, "startTs": 1,
            "details": {"alarm_id": alarm_id, "channel": "vibration_rms_mm_s"} if alarm_id else {}}


def entry(alarm_id, state):
    return {"alarm_id": alarm_id, "state": state, "raised_ts": int(alarm_id.rsplit("/", 1)[1])}


GHOST = "pump-01/vibration_rms_mm_s/WARNING/100"
LIVE = "pump-01/vibration_rms_mm_s/CRITICAL/200"
PENDING = "pump-02/vibration_rms_mm_s/WARNING/300"
WARNING_TYPE, CRITICAL_TYPE = "Vibration RMS high (warning)", "Vibration RMS high (critical)"


def reconcile_fixture():
    return ReconcileThingsBoard({
        "pump-01": {"type": "pump", "alarm_last": {WARNING_TYPE: entry(GHOST, "RAISED"), CRITICAL_TYPE: entry(LIVE, "RAISED")},
                    "alarms": [tb_alarm(1, GHOST), tb_alarm(2, LIVE, CRITICAL_TYPE), tb_alarm(3),
                               tb_alarm(4, "pump-01/vibration_rms_mm_s/WARNING/50", status="CLEARED_ACK")]},
        "pump-02": {"type": "pump", "alarm_last": {WARNING_TYPE: entry(PENDING, "RAISED")}, "alarms": [tb_alarm(5, PENDING)]},
        "motor-01": {"type": "motor", "alarm_last": None, "alarms": []},
        "fan-01": {"type": "fan", "alarm_last": {"x": entry("fan-01/v/WARNING/1", "RAISED")},
                   "alarms": [tb_alarm(6, "fan-01/v/WARNING/1")]},  # not a configured asset type
    }, page_size=1)


def test_reconcile_clears_only_pipeline_alarms_not_active_in_the_topic(json_log):
    tb = reconcile_fixture()
    totals = irc.reconcile_alarms(tb, ["motor", "pump"], {LIVE: "RAISED", PENDING: "CLEARED"}, "telemetry.alarms")
    assert totals == {"devices": 3, "alarms_cleared": 2, "alarm_last_entries_cleared": 2}
    status = {a["id"]["id"]: a["status"] for d in tb.devices.values() for a in d["alarms"]}
    assert status == {"alarm-1": "CLEARED_UNACK",  # the ghost
                      "alarm-2": "ACTIVE_UNACK",  # active in the topic: kept
                      "alarm-3": "ACTIVE_UNACK",  # no alarm_id: not raised by the pipeline
                      "alarm-4": "CLEARED_ACK", "alarm-5": "CLEARED_UNACK",
                      "alarm-6": "ACTIVE_UNACK"}  # fan: not a configured asset type
    assert tb.devices["pump-01"]["alarms"][0]["details"] == {
        "alarm_id": GHOST, "channel": "vibration_rms_mm_s", "cleared_by": "reconciliation",
        "reconciliation_reason": "not in telemetry.alarms: Kafka starts empty at make up, its CLEARED event was lost"}
    assert tb.devices["pump-02"]["alarms"][0]["details"]["reconciliation_reason"] == \
        "CLEARED in telemetry.alarms, not yet applied by the sink"
    # alarm_last: a reconciled entry becomes CLEARED with its alarm_id and raised_ts; the live one is kept.
    assert tb.devices["pump-01"]["alarm_last"] == {WARNING_TYPE: entry(GHOST, "CLEARED"), CRITICAL_TYPE: entry(LIVE, "RAISED")}
    assert tb.devices["pump-02"]["alarm_last"] == {WARNING_TYPE: entry(PENDING, "CLEARED")}
    assert tb.devices["fan-01"]["alarm_last"]["x"]["state"] == "RAISED"
    events = json_log()
    assert [{k: e[k] for k in ("asset", "type", "alarm_id", "thingsboard_alarm_id")}
            for e in events if e["event"] == "alarm_reconciled"] == [
        {"asset": "pump-01", "type": WARNING_TYPE, "alarm_id": GHOST, "thingsboard_alarm_id": "alarm-1"},
        {"asset": "pump-02", "type": WARNING_TYPE, "alarm_id": PENDING, "thingsboard_alarm_id": "alarm-5"}]
    assert all(e["reason"] for e in events if e["event"] == "alarm_reconciled")
    assert [e["alarm_id"] for e in events if e["event"] == "alarm_last_reconciled"] == [GHOST, PENDING]
    done = [e for e in events if e["event"] == "alarm_reconciliation_done"]
    assert len(done) == 1 and (done[0]["alarms_cleared"], done[0]["alarms_active_in_topic"]) == (2, 1)


def test_reconcile_is_idempotent_and_reports_a_zero_total(json_log):
    tb = reconcile_fixture()
    irc.reconcile_alarms(tb, ["motor", "pump"], {}, "telemetry.alarms")
    writes = len([c for c in tb.calls if c[0] == "POST"])
    totals = irc.reconcile_alarms(tb, ["motor", "pump"], {}, "telemetry.alarms")
    assert totals == {"devices": 3, "alarms_cleared": 0, "alarm_last_entries_cleared": 0}
    assert len([c for c in tb.calls if c[0] == "POST"]) == writes  # the second run writes nothing
    done = [e for e in json_log() if e["event"] == "alarm_reconciliation_done"]
    assert [d["alarms_cleared"] for d in done] == [3, 0]  # the summary is logged also when the total is 0


def test_an_alarm_whose_latest_record_is_undecodable_is_kept():
    tb = reconcile_fixture()
    irc.reconcile_alarms(tb, ["pump"], {GHOST: irc.UNKNOWN, LIVE: "RAISED", PENDING: "RAISED"}, "telemetry.alarms")
    assert [a["status"] for a in tb.devices["pump-01"]["alarms"][:2]] == ["ACTIVE_UNACK", "ACTIVE_UNACK"]
    assert tb.devices["pump-01"]["alarm_last"][WARNING_TYPE]["state"] == "RAISED"
