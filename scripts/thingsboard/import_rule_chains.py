"""Import the four project rule chains into ThingsBoard and create the demo device.

    python scripts/thingsboard/import_rule_chains.py

Reads processing/thingsboard/rule-chains/*.json, which hold two kinds of
placeholder:

- ``${FIREBASE_DB_URL}`` at the start of each Firebase REST node URL. When
  FIREBASE_ENABLED=true it is replaced by FIREBASE_DB_URL. Otherwise it is
  imported unresolved, the Firebase leg cannot reach anything, and the script
  says so. ThingsBoard itself treats ``${...}`` in a URL as a message-metadata
  lookup and leaves an unknown key as written.
- ``${RULE_CHAIN_ID:<name>}`` in each rule-chain input node, replaced by the
  id that the named chain has in this ThingsBoard instance.

Steps: log in as the tenant, make sure all four chains exist (matched by
name), write each chain's nodes and connections with the placeholders
resolved, set "Root Rule Chain" as the root chain, and make sure the demo
device exists with the access token TB_ACCESS_TOKEN.

Idempotent: chains and the device are found by name and updated in place, so
re-running never duplicates them.

Configuration comes from the environment, falling back to the repository's
.env file for anything unset: TB_HOST, TB_HTTP_PORT, TB_TENANT_USERNAME,
TB_TENANT_PASSWORD, TB_ACCESS_TOKEN, FIREBASE_ENABLED, FIREBASE_DB_URL.
The script never prints the password, the session token or a rendered
Firebase URL.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
from common import jsonlog  # noqa: E402

RULE_CHAIN_DIR = REPO_ROOT / "processing" / "thingsboard" / "rule-chains"

# Import order only matters for readability: all chains are created before
# any metadata is written, so every reference can be resolved.
CHAIN_FILES = [
    "temp-to-firebase.json",
    "alarm-to-firebase.json",
    "create-and-clear-alarms.json",
    "root-rule-chain.json",
]
ROOT_CHAIN = "Root Rule Chain"
DEMO_DEVICE = "DHT11 Demo Device"
FIREBASE_PLACEHOLDER = "${FIREBASE_DB_URL}"
CHAIN_REF = re.compile(r"^\$\{RULE_CHAIN_ID:(.+)\}$")
LOGIN_RETRY_S = 120

log = logging.getLogger("scripts.thingsboard.import_rule_chains")


class ImportFailed(Exception):
    pass


def setting(name: str, default: str | None = None) -> str | None:
    """Environment first, then the repository .env file, then the default."""
    if os.environ.get(name):
        return os.environ[name]
    env_file = REPO_ROOT / ".env"
    if env_file.is_file():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            key, sep, value = line.partition("=")
            if sep and key.strip() == name and not key.lstrip().startswith("#") and value.strip():
                return value.strip()
    return default


class ThingsBoard:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.token: str | None = None

    def request(self, method: str, path: str, body: dict | None = None, query: dict | None = None,
                allow_404: bool = False):
        url = self.base_url + path + ("?" + urllib.parse.urlencode(query) if query else "")
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.token:
            headers["X-Authorization"] = f"Bearer {self.token}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                raw = response.read()
        except urllib.error.HTTPError as e:
            if allow_404 and e.code == 404:
                return None
            detail = e.read().decode("utf-8", errors="replace")[:300]
            raise ImportFailed(f"{method} {path} -> HTTP {e.code}: {detail}") from None
        return json.loads(raw) if raw else None

    def login(self, username: str, password: str) -> None:
        deadline = time.monotonic() + LOGIN_RETRY_S
        while True:
            try:
                self.token = self.request("POST", "/api/auth/login", {"username": username, "password": password})["token"]
                log.info("thingsboard_logged_in", extra={"url": self.base_url, "username": username})
                return
            except (ImportFailed, urllib.error.URLError, OSError) as e:
                if isinstance(e, ImportFailed) and "HTTP 401" in str(e):
                    raise ImportFailed("login rejected: check TB_TENANT_USERNAME and TB_TENANT_PASSWORD") from None
                if time.monotonic() >= deadline:
                    raise ImportFailed(f"ThingsBoard not reachable at {self.base_url}: {e}") from None
                time.sleep(5)

    def find_rule_chain(self, name: str) -> dict | None:
        page = self.request("GET", "/api/ruleChains", query={"pageSize": 100, "page": 0, "type": "CORE", "textSearch": name})
        matches = [rc for rc in page["data"] if rc["name"] == name]
        if len(matches) > 1:
            raise ImportFailed(f"{len(matches)} rule chains named {name!r}; resolve by hand")
        return matches[0] if matches else None


def render_nodes(nodes: list[dict], chain_ids: dict[str, str], firebase_url: str | None) -> tuple[list[dict], int]:
    rendered, unresolved = [], 0
    for node in nodes:
        node = json.loads(json.dumps(node))  # deep copy
        cfg = node.get("configuration") or {}
        url = cfg.get("restEndpointUrlPattern")
        if isinstance(url, str) and url.startswith(FIREBASE_PLACEHOLDER):
            if firebase_url:
                cfg["restEndpointUrlPattern"] = firebase_url.rstrip("/") + url[len(FIREBASE_PLACEHOLDER):]
            else:
                unresolved += 1
        ref = cfg.get("ruleChainId")
        if isinstance(ref, str):
            m = CHAIN_REF.match(ref)
            if not m:
                raise ImportFailed(f"node {node['name']!r}: ruleChainId is not a ${{RULE_CHAIN_ID:<name>}} placeholder")
            if m.group(1) not in chain_ids:
                raise ImportFailed(f"node {node['name']!r} references unknown chain {m.group(1)!r}")
            cfg["ruleChainId"] = chain_ids[m.group(1)]
        rendered.append(node)
    return rendered, unresolved


def import_chains(tb: ThingsBoard, firebase_url: str | None) -> dict[str, str]:
    exports = [json.loads((RULE_CHAIN_DIR / f).read_text(encoding="utf-8")) for f in CHAIN_FILES]

    chain_ids: dict[str, str] = {}
    for export in exports:
        rc = export["ruleChain"]
        existing = tb.find_rule_chain(rc["name"])
        if existing:
            chain_ids[rc["name"]] = existing["id"]["id"]
            log.info("rule_chain_exists", extra={"rule_chain": rc["name"], "id": existing["id"]["id"], "action": "update"})
        else:
            created = tb.request("POST", "/api/ruleChain", {
                "name": rc["name"], "type": rc.get("type", "CORE"), "debugMode": False,
                "configuration": rc.get("configuration"), "additionalInfo": rc.get("additionalInfo"),
            })
            chain_ids[rc["name"]] = created["id"]["id"]
            log.info("rule_chain_created", extra={"rule_chain": rc["name"], "id": created["id"]["id"]})

    total_unresolved = 0
    for export in exports:
        name = export["ruleChain"]["name"]
        md = export["metadata"]
        nodes, unresolved = render_nodes(md["nodes"], chain_ids, firebase_url)
        total_unresolved += unresolved
        current = tb.request("GET", f"/api/ruleChain/{chain_ids[name]}/metadata")
        tb.request("POST", "/api/ruleChain/metadata", {
            "ruleChainId": {"entityType": "RULE_CHAIN", "id": chain_ids[name]},
            "version": current.get("version"),
            "firstNodeIndex": md["firstNodeIndex"],
            "nodes": nodes,
            "connections": md.get("connections") or [],
            "ruleChainConnections": None,
        }, query={"updateRelated": "true"})
        log.info("rule_chain_written", extra={"rule_chain": name, "nodes": len(nodes),
                                              "connections": len(md.get("connections") or [])})

    tb.request("POST", f"/api/ruleChain/{chain_ids[ROOT_CHAIN]}/root")
    log.info("rule_chain_set_root", extra={"rule_chain": ROOT_CHAIN})
    if total_unresolved:
        log.info("firebase_leg_disabled", extra={
            "reason": "FIREBASE_ENABLED is not true", "rest_nodes_unresolved": total_unresolved,
            "placeholder": FIREBASE_PLACEHOLDER, "detail": "these nodes cannot reach any database",
        })
    return chain_ids


def ensure_device(tb: ThingsBoard, token: str) -> None:
    device = tb.request("GET", "/api/tenant/devices", query={"deviceName": DEMO_DEVICE}, allow_404=True)
    if device is None:
        device = tb.request("POST", "/api/device", {"name": DEMO_DEVICE}, query={"accessToken": token})
        log.info("device_created", extra={"device": DEMO_DEVICE})
        return
    credentials = tb.request("GET", f"/api/device/{device['id']['id']}/credentials")
    if credentials.get("credentialsType") == "ACCESS_TOKEN" and credentials.get("credentialsId") == token:
        log.info("device_token_ok", extra={"device": DEMO_DEVICE})
        return
    credentials.update({"credentialsType": "ACCESS_TOKEN", "credentialsId": token, "credentialsValue": None})
    tb.request("POST", "/api/device/credentials", credentials)
    log.info("device_token_updated", extra={"device": DEMO_DEVICE})


def main() -> int:
    jsonlog.configure("INFO")
    username = setting("TB_TENANT_USERNAME")
    tenant_pw = setting("TB_TENANT_PASSWORD")
    token = setting("TB_ACCESS_TOKEN")
    missing = [n for n, v in (("TB_TENANT_USERNAME", username), ("TB_TENANT_PASSWORD", tenant_pw),
                              ("TB_ACCESS_TOKEN", token)) if not v]
    if missing:
        log.error("settings_missing", extra={"settings": missing, "hint": "set them in .env (see .env.example)"})
        return 1

    firebase_enabled = (setting("FIREBASE_ENABLED", "false") or "").lower() == "true"
    firebase_url = setting("FIREBASE_DB_URL") if firebase_enabled else None
    if firebase_enabled and not firebase_url:
        log.error("firebase_url_missing", extra={"detail": "FIREBASE_ENABLED=true but FIREBASE_DB_URL is empty"})
        return 1

    tb = ThingsBoard(f"http://{setting('TB_HOST', 'localhost')}:{setting('TB_HTTP_PORT', '8080')}")
    try:
        tb.login(username, tenant_pw)
        import_chains(tb, firebase_url)
        ensure_device(tb, token)
    except ImportFailed as e:
        log.error("import_failed", extra={"error": str(e)})
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
