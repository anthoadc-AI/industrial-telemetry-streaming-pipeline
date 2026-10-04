"""Build processing/thingsboard/dashboards/operations.json, the operations dashboard, from config/ and ThingsBoard.

    python scripts/thingsboard/build_dashboard.py      (full stack up; tenant settings as for the import)

The dashboard JSON is the version-controlled artifact the import script loads; this script is how it is
made, so it can be regenerated rather than edited by hand. Each widget starts from its own default
configuration as this ThingsBoard serves it (GET /api/widgetType?fqn=...); only datasources, data keys,
titles, alarm filters and the row-click action are set here. Asset types, channels and channel labels come
from config/telemetry.yaml. No device is named: one alias selects devices by the asset types, the other is the
entity a user selects. Ids are uuid5 of fixed names, so regenerating gives the same file.

Content (owner review, Task 3.2): the plant view fits one 1920x1080 screen (the layout fills the window height),
in dashboard fullscreen and in a maximized window with ThingsBoard's side menu, also at 125 % display scaling
(about 1536x730 CSS pixels; measured in headless Edge): the five assets with their latest values, no scrolling
or paging, beside the counters of active critical and warning alarms, and the table of active alarms below, its
type and status on one line. No dashboard title: the toolbar's breadcrumb names the view. A row click opens the
asset view: a header with the asset's name and a "Back to all assets" button, which returns to the plant view
without stacking states, and one time-series chart per channel titled with the asset.
The alarms table cannot clear an alarm: the pipeline's alarm consumer is the only alarm engine, so a manual
clear would leave ThingsBoard and the pipeline disagreeing. Acknowledging and assigning stay: they annotate an
alarm without changing its state. Its time column is the alarm's start time, the event's own timestamp
(verified: a replay delayed by ten minutes keeps startTs at the event and createdTime at the arrival).
"""

from __future__ import annotations

import copy
import json
import sys
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config.loader import TelemetryConfig, load_config  # noqa: E402

OUTPUT = REPO_ROOT / "processing" / "thingsboard" / "dashboards" / "operations.json"
TITLE = "Pipeline operations"
FQNS = ("system.time_series_chart", "system.cards.entities_table", "system.alarm_widgets.alarms_table",
        "system.alarm_count", "system.cards.markdown_card")
# How a unit in config/telemetry.yaml is written on screen; units not listed are shown as they are.
UNIT_DISPLAY = {"degC": "°C"}
# Table rows of 36 px instead of 52: the five assets and several alarms fit one screen also at 125 % display
# scaling (a maximized window on a 1920x1080 screen shows about 1536x730 CSS pixels with ThingsBoard's menu).
COMPACT_ROWS = (".mat-mdc-header-row { height: 40px !important; min-height: 40px; }\n"
                ".mat-mdc-row { height: 36px !important; min-height: 36px; }")
COLORS = ["#2196f3", "#4caf50", "#f44336", "#ff9800", "#9c27b0"]
NS = uuid.UUID("6f0e2c1a-3b7d-4c55-9a51-0d2f6a7c9e10")


def uid(name: str) -> str:
    return str(uuid.uuid5(NS, name))


def _key(name: str, kind: str, label: str, i: int = 0) -> dict:
    return {"name": name, "type": kind, "label": label, "color": COLORS[i % len(COLORS)], "settings": {},
            "_hash": round(0.1 + i / 100, 4)}


def build(defaults: dict, telemetry: TelemetryConfig) -> dict:
    """The dashboard as ThingsBoard's REST API takes it; defaults maps each widget fqn to its descriptor."""
    types = sorted({a.type for a in telemetry.assets})
    channels = list(telemetry.channels)
    # Labels come from config/telemetry.yaml, the same source as the ThingsBoard alarm types.
    LABELS = {name: f"{c.label} ({UNIT_DISPLAY.get(c.unit, c.unit)})" for name, c in telemetry.channels.items()}
    assets, selected = uid("alias-pipeline-assets"), uid("alias-selected-asset")
    aliases = {
        assets: {"id": assets, "alias": "Pipeline assets",
                 "filter": {"type": "deviceType", "resolveMultiple": True, "deviceTypes": types, "deviceNameFilter": ""}},
        selected: {"id": selected, "alias": "Selected asset",
                   "filter": {"type": "stateEntity", "resolveMultiple": False, "stateEntityParamName": None,
                              "defaultStateEntity": None}},
    }
    widgets: dict = {}
    layouts: dict = {"default": {}, "asset": {}}

    def place(state, fqn, name, config, size, row, col):
        w = {"typeFullFqn": fqn, "type": defaults[fqn]["type"], "sizeX": size[0], "sizeY": size[1], "config": config,
             "id": uid(name)}
        widgets[w["id"]] = w
        layouts[state][w["id"]] = {"sizeX": size[0], "sizeY": size[1], "row": row, "col": col}

    c = copy.deepcopy(defaults["system.cards.entities_table"]["defaultConfig"])
    c.update({"title": "Assets, latest values (select a row for its history)", "showTitle": True,
              "datasources": [{"type": "entity", "name": "", "entityAliasId": assets, "filterId": None,
                               "dataKeys": [_key("name", "entityField", "Asset", 0), _key("type", "entityField", "Type", 1)]
                               + [_key(ch, "timeseries", LABELS[ch], i + 2) for i, ch in enumerate(channels)]}],
              "actions": {"rowClick": [{"name": "Open asset", "icon": "show_chart", "type": "openDashboardState",
                                        "targetDashboardStateId": "asset", "setEntityId": True,
                                        "stateEntityParamName": None, "openRightLayout": False,
                                        "openInSeparateDialog": False, "openInPopover": False,
                                        "id": uid("action-open-asset")}]}})
    c["settings"] = {**c["settings"], "entitiesTitle": "Assets", "defaultSortOrder": "name", "displayPagination": False,
                     "defaultPageSize": 10}
    c["widgetCss"] = COMPACT_ROWS
    place("default", "system.cards.entities_table", "w-assets", c, (18, 8), 0, 0)

    for i, sev in enumerate(("CRITICAL", "WARNING")):
        c = copy.deepcopy(defaults["system.alarm_count"]["defaultConfig"])
        c.update({"title": f"Active {sev.lower()} alarms", "showTitle": False,
                  "datasources": [{"type": "alarmCount", "name": "", "entityAliasId": assets, "filterId": None,
                                   "dataKeys": [_key("count", "count", "count", i)],
                                   "alarmFilterConfig": {"statusList": ["ACTIVE"], "severityList": [sev], "typeList": [],
                                                         "searchPropagatedAlarms": False, "assignedToCurrentUser": False,
                                                         "assigneeId": None}}]})
        c["settings"] = {**c["settings"], "label": f"Active {sev.lower()}"}
        place("default", "system.alarm_count", f"w-count-{sev}", c, (6, 4), i * 4, 18)

    c = copy.deepcopy(defaults["system.alarm_widgets.alarms_table"]["defaultConfig"])
    # Widths that keep the longest type ("Bearing temperature high (critical)") and status ("Active Unacknowledged")
    # on one line at 1536 px.
    widths = {"type": "240px", "status": "190px"}
    alarm_keys = [{**k, "settings": {**k.get("settings", {}), "columnWidth": widths[k["name"]]}} if k["name"] in widths else k
                  for k in c["alarmSource"]["dataKeys"] if k["name"] != "createdTime"]
    start = {**next(k for k in c["alarmSource"]["dataKeys"] if k["name"] == "createdTime"),
             "name": "startTime", "label": "Start time (event)"}
    c["alarmSource"] = {**c["alarmSource"], "type": "entity", "name": "", "entityAliasId": assets, "filterId": None,
                        "dataKeys": [start, *alarm_keys]}
    c["settings"] = {**c["settings"], "allowClear": False, "allowAcknowledgment": True, "allowAssign": True,
                     "alarmsTitle": "Active alarms"}
    # The table filters alarms by time; its default (one day) would hide an alarm active for longer.
    c["timewindow"] = {**c["timewindow"], "realtime": {**c["timewindow"]["realtime"], "timewindowMs": 30 * 86_400_000}}
    c.update({"title": "Active alarms", "showTitle": True, "alarmStatusList": ["ACTIVE"], "alarmSeverityList": [],
              "alarmTypeList": [], "searchPropagatedAlarms": False, "displayTimewindow": False, "widgetCss": COMPACT_ROWS})
    if c.get("alarmFilterConfig"):
        c["alarmFilterConfig"] = {**c["alarmFilterConfig"], "statusList": ["ACTIVE"]}
    place("default", "system.alarm_widgets.alarms_table", "w-alarms", c, (24, 8), 8, 0)

    c = copy.deepcopy(defaults["system.cards.markdown_card"]["defaultConfig"])
    # The header: the asset's name as the title, the way back, one line of text, no scrolling. "Back" resets the
    # state stack to the plant view; opening the default state would push it ("Pipeline operations > pump-01 >
    # Pipeline operations").
    c.update({"title": "${entityName}", "showTitle": True, "titleStyle": {"fontSize": "20px", "fontWeight": 500},
              "datasources": [{"type": "entity", "name": "", "entityAliasId": selected, "filterId": None,
                               "dataKeys": [_key("name", "entityField", "Asset", 0), _key("type", "entityField", "Type", 1)]}],
              "actions": {"headerButton": [{"name": "Back to all assets", "icon": "arrow_back", "type": "custom",
                                            "customFunction": "widgetContext.stateController.resetState();",
                                            "showIcon": True, "buttonType": "stroked", "id": uid("action-back")}]}})
    # The row height scales with the window: the line is centred in whatever height the header gets, never
    # scrolled. ThingsBoard scopes this CSS to the widget's own elements, so it styles the container only (the
    # paragraph rendered from the markdown is not reached).
    c["settings"] = {**c["settings"], "useMarkdownTextFunction": False,
                     "markdownTextPattern": "Type: ${Type}. Charts follow the dashboard's time window (top right).",
                     "markdownCss": ".tb-markdown-view { overflow: hidden !important; height: 100%; display: flex;"
                                    " align-items: center; padding: 0 5px !important; }"}
    place("asset", "system.cards.markdown_card", "w-asset-header", c, (24, 2), 0, 0)

    for i, ch in enumerate(channels):
        c = copy.deepcopy(defaults["system.time_series_chart"]["defaultConfig"])
        c.update({"title": "${entityName}: " + LABELS[ch], "showTitle": True, "useDashboardTimewindow": True,
                  "datasources": [{"type": "entity", "name": "", "entityAliasId": selected, "filterId": None,
                                   "dataKeys": [_key(ch, "timeseries", LABELS[ch], i)]}]})
        place("asset", "system.time_series_chart", f"w-chart-{ch}", c, (12, 6), 2 + (i // 2) * 6, (i % 2) * 12)

    # autoFillHeight: rows stretch so the whole layout fits the window height (1920x1080 included).
    grid = {"backgroundColor": "#eeeeee", "columns": 24, "margin": 10, "outerMargin": True, "backgroundSizeMode": "100%",
            "autoFillHeight": True, "mobileAutoFillHeight": False}
    return {
        "title": TITLE,
        "configuration": {
            "description": "Operations view of the synthetic industrial telemetry pipeline: latest values and history "
                           "per asset, active alarms raised by the pipeline's alarm consumer. All data is synthetic.",
            "widgets": widgets,
            "states": {
                "default": {"name": TITLE, "root": True,
                            "layouts": {"main": {"widgets": layouts["default"], "gridSettings": grid}}},
                "asset": {"name": "${entityName}", "root": False,
                          "layouts": {"main": {"widgets": layouts["asset"], "gridSettings": grid}}},
            },
            "entityAliases": aliases,
            "filters": {},
            "timewindow": {"displayValue": "", "selectedTab": 0,
                           "realtime": {"realtimeType": 0, "interval": 5000, "timewindowMs": 1800000},
                           "aggregation": {"type": "NONE", "limit": 50000}},
            # No dashboard title: the toolbar's state breadcrumb already names the view, and the height is the table's.
            "settings": {"stateControllerId": "entity", "showTitle": False, "showDashboardsSelect": False,
                         "showEntitiesSelect": False, "showDashboardTimewindow": True, "showDashboardExport": False,
                         "toolbarAlwaysOpen": True},
        },
    }


def write(dashboard: dict, path: Path = OUTPUT) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")


# Reads widget descriptors from a live ThingsBoard; excluded from unit coverage like the import's main().
def main() -> int:  # pragma: no cover
    import import_rule_chains as irc

    tb = irc.ThingsBoard(f"http://{irc.setting('TB_HOST', 'localhost')}:{irc.setting('TB_HTTP_PORT', '8080')}")
    tb.login(irc.setting("TB_TENANT_USERNAME"), irc.setting("TB_TENANT_PASSWORD"))
    defaults = {}
    for fqn in FQNS:
        descriptor = tb.request("GET", "/api/widgetType", query={"fqn": fqn})["descriptor"]
        defaults[fqn] = {"type": descriptor["type"], "defaultConfig": json.loads(descriptor["defaultConfig"])}
    write(build(defaults, load_config().telemetry))
    print(f"written {OUTPUT.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
