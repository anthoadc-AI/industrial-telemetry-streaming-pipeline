"""scripts/thingsboard/build_dashboard.py: the operations dashboard built from config/ and widget defaults."""

from __future__ import annotations

import dataclasses
import importlib.util
import json
import sys

import pytest

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
}


def test_build_selects_assets_by_type_and_charts_every_channel(cfg):
    d = bd.build(DEFAULTS, cfg.telemetry)
    c = d["configuration"]
    assert d["title"] == "Pipeline operations"
    [by_type] = [a["filter"] for a in c["entityAliases"].values() if a["filter"]["type"] == "deviceType"]
    assert by_type["deviceTypes"] == sorted({a.type for a in cfg.telemetry.assets})
    asset_state = c["states"]["asset"]["layouts"]["main"]["widgets"]
    charts = [c["widgets"][w] for w in asset_state]
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


def test_a_channel_without_a_label_is_refused(cfg):
    telemetry = dataclasses.replace(cfg.telemetry, channels=(*cfg.telemetry.channels, "oil_pressure_bar"))
    with pytest.raises(ValueError, match="oil_pressure_bar"):
        bd.build(DEFAULTS, telemetry)
