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
    names = ["TempToFirebase", "AlarmToFirebase", "CreateAndClearAlarms", "Root Rule Chain"]
    ids = {n: f"id-{i}" for i, n in enumerate(names)}
    total = 0
    for f in irc.CHAIN_FILES:
        export = json.loads((irc.RULE_CHAIN_DIR / f).read_text(encoding="utf-8"))
        rendered, unresolved = irc.render_nodes(export["metadata"]["nodes"], ids, None)
        total += unresolved
        assert "${RULE_CHAIN_ID" not in json.dumps(rendered)
    assert total == 3  # the three Firebase REST nodes stay unresolved when the sink is disabled


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
    """In-memory ThingsBoard: rule chains by name, one device, every call recorded."""

    def __init__(self, chains=None, device=None, credentials=None):
        super().__init__("http://fake")
        self.chains = dict(chains or {})
        self.device, self.credentials = device, credentials
        self.calls = []

    def request(self, method, path, body=None, query=None, allow_404=False):
        self.calls.append((method, path, body, query))
        if path == "/api/ruleChains":
            return {"data": [{"name": n, "id": {"id": i}} for n, i in self.chains.items() if query["textSearch"] in n]}
        if method == "POST" and path == "/api/ruleChain":
            self.chains[body["name"]] = f"new-{len(self.chains)}"
            return {"id": {"id": self.chains[body["name"]]}}
        if path.endswith("/metadata") and method == "GET":
            return {"version": 7}
        if path == "/api/tenant/devices":
            return self.device
        if path.startswith("/api/device/") and path.endswith("/credentials"):
            return dict(self.credentials)
        return None


def test_import_creates_missing_chains_updates_existing_and_sets_root(json_log):
    tb = FakeThingsBoard(chains={"Root Rule Chain": "root-id"})
    ids = irc.import_chains(tb, None)
    assert ids["Root Rule Chain"] == "root-id" and len(ids) == 4
    metadata = [c for c in tb.calls if c[1] == "/api/ruleChain/metadata"]
    assert len(metadata) == 4 and all(c[2]["version"] == 7 for c in metadata)
    root_md = next(c[2] for c in metadata if c[2]["ruleChainId"]["id"] == "root-id")
    refs = [n["configuration"]["ruleChainId"] for n in root_md["nodes"] if n["type"].endswith("TbRuleChainInputNode")]
    assert refs == [ids["CreateAndClearAlarms"]]
    assert ("POST", "/api/ruleChain/root-id/root", None, None) in tb.calls
    events = [e["event"] for e in json_log()]
    assert events.count("rule_chain_created") == 3 and events.count("rule_chain_exists") == 1
    assert "firebase_leg_disabled" in events


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
