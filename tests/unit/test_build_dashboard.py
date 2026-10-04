"""scripts/thingsboard/build_dashboard.py: the operations dashboard built from config/ and widget defaults."""

from __future__ import annotations

import dataclasses
import importlib.util
import json
import sys


from conftest import REPO_ROOT

spec = importlib.util.spec_from_file_location("build_dashboard", REPO_ROOT / "scripts" / "thingsboard" / "build_dashboard.py")
bd = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = bd
spec.loader.exec_module(bd)

DEFAULTS = {
    "system.time_series_chart": {"type": "timeseries", "defaultConfig": {"datasources": [], "showLegend": True}},
    "system.cards.entities_table": {"type": "latest", "defaultConfig": {"datasources": [], "settings": {"enableSearch": True}}},
    "system.alarm_widgets.alarms_table": {"type": "alarm", "defaultConfig": {"alarmSource": {"type": "function", "dataKeys": []},
                                                                           "alarmFilterConfig": {"statusList": []}}},
    "system.alarm_count": {"type": "latest", "defaultConfig": {"datasources": [], "settings": {"label": "Total"}}},
    "system.cards.markdown_card": {"type": "latest",
                                   "defaultConfig": {"datasources": [], "settings": {"markdownTextPattern": "x"}}},
    "system.cards.horizontal_value_card": {"type": "latest", "defaultConfig": {
        "datasources": [], "units": "°C",
        "settings": {"icon": "thermostat", "iconColor": {"type": "function", "color": "#5469FF"},
                     "valueColor": {"type": "function", "color": "#000"}}}},
}
DEFAULTS["system.alarm_widgets.alarms_table"]["defaultConfig"].update({
    "alarmSource": {"type": "function", "dataKeys": [{"name": "createdTime", "type": "alarm", "label": "Created time"},
                                                     {"name": "originator", "type": "alarm", "label": "Originator"},
                                                     {"name": "type", "type": "alarm", "label": "Type", "settings": {}},
                                                     {"name": "status", "type": "alarm", "label": "Status"}]},
    "settings": {"allowClear": True, "allowAcknowledgment": True, "allowAssign": True, "alarmsTitle": "Alarms"},
    "timewindow": {"realtime": {"interval": 1000, "timewindowMs": 86_400_000}},
})


def test_build_selects_assets_by_type_and_charts_every_channel(cfg):
    d = bd.build(DEFAULTS, cfg.telemetry)
    c = d["configuration"]
    assert d["title"] == "Pipeline operations"
    [by_type] = [a["filter"] for a in c["entityAliases"].values() if a["filter"]["type"] == "deviceType"]
    assert by_type["deviceTypes"] == sorted({a.type for a in cfg.telemetry.assets})
    asset_state = c["states"]["asset"]["layouts"]["main"]["widgets"]
    charts = [c["widgets"][w] for w in asset_state if c["widgets"][w]["typeFullFqn"] == "system.time_series_chart"]
    assert [w["config"]["datasources"][0]["dataKeys"][0]["name"] for w in charts] == list(cfg.telemetry.channels)
    counts = [w for w in c["widgets"].values() if w["typeFullFqn"] == "system.alarm_count"]
    severities = sorted(w["config"]["datasources"][0]["alarmFilterConfig"]["severityList"][0] for w in counts)
    assert severities == ["CRITICAL", "WARNING"]
    [table] = [w for w in c["widgets"].values() if w["typeFullFqn"] == "system.cards.entities_table"]
    assert table["config"]["actions"]["rowClick"][0]["targetDashboardStateId"] == "asset"
    assert table["config"]["settings"]["enableSearch"] is True  # widget defaults are kept
    [alarms] = [w for w in c["widgets"].values() if w["typeFullFqn"] == "system.alarm_widgets.alarms_table"]
    assert alarms["config"]["alarmStatusList"] == ["ACTIVE"] and alarms["config"]["alarmFilterConfig"]["statusList"] == ["ACTIVE"]


def test_build_is_deterministic_and_matches_the_committed_layout(cfg, tmp_path):
    first, second = bd.build(DEFAULTS, cfg.telemetry), bd.build(DEFAULTS, cfg.telemetry)
    assert json.dumps(first) == json.dumps(second)
    committed = json.loads(bd.OUTPUT.read_text(encoding="utf-8"))["configuration"]
    assert first["configuration"]["states"] == committed["states"]  # same widget ids and layout as the export
    bd.write(first, tmp_path / "x" / "d.json")
    assert json.loads((tmp_path / "x" / "d.json").read_text(encoding="utf-8")) == first


def test_labels_come_from_config(cfg):
    """One source: a label changed in config/telemetry.yaml changes the dashboard (and, in the sink, the alarm type)."""
    channels = dict(cfg.telemetry.channels)
    channels["vibration_rms_mm_s"] = dataclasses.replace(channels["vibration_rms_mm_s"], label="Casing vibration")
    d = bd.build(DEFAULTS, dataclasses.replace(cfg.telemetry, channels=channels))
    labels = {k["name"]: k["label"] for w in d["configuration"]["widgets"].values()
              for ds in (w["config"].get("datasources") or []) for k in ds["dataKeys"] if k["type"] == "timeseries"}
    assert labels["vibration_rms_mm_s"] == "Casing vibration (mm/s)"
    assert labels["bearing_temperature_c"] == "Bearing temperature (°C)"  # degC shown as °C


def widgets_of(dashboard, fqn):
    return [w for w in dashboard["configuration"]["widgets"].values() if w["typeFullFqn"] == fqn]


def test_alarm_table_cannot_clear_and_shows_the_event_time(cfg):
    """Owner review: one alarm engine, so no manual clear; acknowledge and assign stay; time = startTs (the event's)."""
    [alarms] = widgets_of(bd.build(DEFAULTS, cfg.telemetry), "system.alarm_widgets.alarms_table")
    settings = alarms["config"]["settings"]
    assert (settings["allowClear"], settings["allowAcknowledgment"], settings["allowAssign"]) == (False, True, True)
    assert settings["alarmsTitle"] == "Active alarms"
    keys = [k["name"] for k in alarms["config"]["alarmSource"]["dataKeys"]]
    assert keys[0] == "startTime" and "createdTime" not in keys and "originator" in keys
    assert alarms["config"]["timewindow"]["realtime"]["timewindowMs"] == 30 * 86_400_000
    widths = {k["name"]: k.get("settings", {}).get("columnWidth") for k in alarms["config"]["alarmSource"]["dataKeys"]}
    assert (widths["type"], widths["status"]) == ("240px", "190px")  # the longest type and status on one line


def test_charts_default_to_the_last_ten_minutes(cfg):
    """Owner decision: 10 minutes by default, the toolbar's time selector kept to widen it."""
    c = bd.build(DEFAULTS, cfg.telemetry)["configuration"]
    assert c["timewindow"]["realtime"]["timewindowMs"] == 600_000 and c["settings"]["showDashboardTimewindow"] is True
    charts = widgets_of({"configuration": c}, "system.time_series_chart")
    assert charts and all(w["config"]["useDashboardTimewindow"] for w in charts)


def test_alarm_mapping_failures_are_counted_on_the_pipeline_gateway(cfg):
    """Owner decision (Task 3.2 fix): a counter, not an alarm. AlarmMapping records each failure on the gateway;
    the card counts the entries of the current month (an aggregating card takes only calendar intervals)."""
    d = bd.build(DEFAULTS, cfg.telemetry)
    [card] = widgets_of(d, "system.cards.horizontal_value_card")
    [key] = card["config"]["datasources"][0]["dataKeys"]
    assert (key["name"], key["type"], key["aggregationType"]) == ("alarm_mapping_failed", "timeseries", "COUNT")
    alias = d["configuration"]["entityAliases"][card["config"]["datasources"][0]["entityAliasId"]]["filter"]
    assert (alias["type"], alias["entityType"], alias["entityNameFilter"]) == ("entityName", "DEVICE", "pipeline-gateway")
    window = card["config"]["timewindow"]["realtime"]
    assert card["config"]["useDashboardTimewindow"] is False
    assert (window["realtimeType"], window["quickInterval"]) == (1, "CURRENT_MONTH")
    assert (card["config"]["units"], card["config"]["settings"]["valueColor"]["type"]) == ("", "constant")
    main = d["configuration"]["states"]["default"]["layouts"]["main"]["widgets"]
    counts = sorted((main[w["id"]]["row"], main[w["id"]]["sizeY"]) for w in widgets_of(d, "system.alarm_count"))
    place = main[card["id"]]
    assert counts == [(0, 3), (3, 3)] and (place["row"], place["sizeY"], place["col"]) == (6, 2, 18)


def test_plant_view_fits_one_screen_and_shows_every_asset(cfg):
    d = bd.build(DEFAULTS, cfg.telemetry)
    main = d["configuration"]["states"]["default"]["layouts"]["main"]
    assert main["gridSettings"]["autoFillHeight"] is True
    [table] = widgets_of(d, "system.cards.entities_table")
    assert table["config"]["settings"]["displayPagination"] is False
    assert table["config"]["settings"]["defaultPageSize"] >= len(cfg.telemetry.assets)
    rows = {w: (p["row"], p["row"] + p["sizeY"]) for w, p in main["widgets"].items()}
    [alarms] = widgets_of(d, "system.alarm_widgets.alarms_table")
    assert rows[alarms["id"]][0] >= rows[table["id"]][1]  # alarms below the assets, counters beside them
    assert max(end for _, end in rows.values()) <= 16
    # Height for the tables at 125 % scaling with the side menu (owner review): compact rows, no dashboard title
    # (the breadcrumb names the view), no time-window line inside the alarms table.
    assert table["config"]["widgetCss"] == alarms["config"]["widgetCss"] == bd.COMPACT_ROWS
    assert d["configuration"]["settings"]["showTitle"] is False and alarms["config"]["displayTimewindow"] is False


def test_asset_view_names_the_asset_and_offers_a_way_back(cfg):
    d = bd.build(DEFAULTS, cfg.telemetry)
    [header] = widgets_of(d, "system.cards.markdown_card")
    assert header["config"]["title"] == "${entityName}" and "${Type}" in header["config"]["settings"]["markdownTextPattern"]
    assert "overflow: hidden" in header["config"]["settings"]["markdownCss"]  # no scrollbar of its own
    [back] = header["config"]["actions"]["headerButton"]
    # Back resets the state stack to the plant view; opening the default state would stack it on the asset view.
    assert (back["type"], back["customFunction"]) == ("custom", "widgetContext.stateController.resetState();")
    charts = widgets_of(d, "system.time_series_chart")
    assert len(charts) == len(cfg.telemetry.channels) and all(c["config"]["title"].startswith("${entityName}: ") for c in charts)
    assert header["id"] in d["configuration"]["states"]["asset"]["layouts"]["main"]["widgets"]


def test_the_gateway_name_matches_the_import_script():
    import importlib.util
    path = REPO_ROOT / "scripts" / "thingsboard" / "import_rule_chains.py"
    spec = importlib.util.spec_from_file_location("irc_for_dashboard", path)
    irc = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(irc)
    assert bd.PIPELINE_GATEWAY == irc.PIPELINE_GATEWAY
