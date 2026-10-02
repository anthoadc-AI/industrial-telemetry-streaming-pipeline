"""config/loader.py: the real configuration, and one rejection per validation rule.

Each broken case edits a copy of config/ (or of the telemetry schema) and
asserts the exact ConfigError message, so a rule cannot weaken silently.
"""

from __future__ import annotations

import dataclasses
import json
import shutil
from pathlib import Path

import pytest

from config import loader
from config.loader import ConfigError, load_config, validate_fault
from conftest import REPO_ROOT


def test_real_configuration(cfg):
    t = cfg.telemetry
    assert [a.id for a in t.assets] == ["motor-01", "motor-02", "pump-01", "pump-02", "compressor-01"]
    assert list(t.channels) == ["bearing_temperature_c", "vibration_rms_mm_s", "motor_current_a", "speed_rpm"]
    assert set(t.faults) == {"bearing_overheating", "imbalance"}
    assert t.asset("pump-02").degradation == {"bearing_temperature_c": 1.5, "vibration_rms_mm_s": 0.6}
    assert len(cfg.alarms.rules) == 15
    assert cfg.alarms.rule("motor-01", "motor_current_a").warning == 57.75
    assert cfg.alarms.rule("motor-01", "speed_rpm") is None
    assert cfg.topics.mqtt_telemetry.for_asset("pump-01") == "plant/pump-01/telemetry"
    assert cfg.topics.telemetry_raw.name == "telemetry.raw"
    assert dict(cfg.topics.kafka["telemetry_alarms"].configs) == {"cleanup.policy": "compact"}
    assert dict(cfg.topics.kafka["telemetry_dlq"].configs) == {"cleanup.policy": "delete", "retention.ms": "604800000"}


def test_unknown_asset_lookup_raises_key_error(cfg):
    with pytest.raises(KeyError):
        cfg.telemetry.asset("pump-99")


@pytest.mark.parametrize("env, expected", [
    ({"TELEMETRY_TOPIC": "x.y"}, "x.y"),
    ({"TELEMETRY_TOPIC": ""}, "telemetry.raw"),
    ({}, "telemetry.raw"),
])
def test_telemetry_topic_override(env, expected):
    assert load_config(env=env).topics.telemetry_raw.name == expected


def test_validate_fault(cfg):
    assert validate_fault(cfg.telemetry, "pump-01", "imbalance", 5.0, "w").start_s == 5.0
    for args, message in [
        (("pump-99", "imbalance", 1.0), "w: unknown asset 'pump-99'"),
        (("pump-01", "seizure", 1.0), "w: unknown fault mode 'seizure'; known: ['bearing_overheating', 'imbalance']"),
        (("pump-01", "imbalance", -1.0), "w: start_s must be >= 0"),
    ]:
        with pytest.raises(ConfigError) as info:
            validate_fault(cfg.telemetry, *args, "w")
        assert str(info.value) == message


def test_fault_on_a_channel_the_asset_lacks(cfg):
    asset = dataclasses.replace(cfg.telemetry.asset("pump-01"), channels=("speed_rpm",))
    telemetry = dataclasses.replace(cfg.telemetry, assets=(asset,))
    with pytest.raises(ConfigError, match="does not report 'vibration_rms_mm_s'"):
        validate_fault(telemetry, "pump-01", "imbalance", 1.0, "w")


def broken(tmp_path: Path, file: str, old: str, new: str) -> Path:
    target = tmp_path / "config"
    shutil.copytree(REPO_ROOT / "config", target, ignore=shutil.ignore_patterns("__pycache__", "*.py"))
    text = (target / file).read_text(encoding="utf-8")
    assert old in text, f"fixture out of date: {old!r}"
    (target / file).write_text(text.replace(old, new, 1), encoding="utf-8")
    return target


CASES = [
    # telemetry.yaml
    ("telemetry.yaml", "sample_interval_s: 1.0", "sample_interval_s: 1.0\nextra: 1", "telemetry.yaml: unknown key(s) ['extra']"),
    ("telemetry.yaml", "sample_interval_s: 1.0\n", "", "telemetry.yaml: missing key(s) ['sample_interval_s']"),
    ("telemetry.yaml", "sample_interval_s: 1.0", "sample_interval_s: 0", "telemetry.yaml.sample_interval_s: must be > 0"),
    (
        "telemetry.yaml",
        "sample_interval_s: 1.0",
        "sample_interval_s: true",
        "telemetry.yaml.sample_interval_s: expected a finite number",
    ),
    (
        "telemetry.yaml",
        "ambient_temperature_c: 25.0",
        "ambient_temperature_c: .nan",
        "telemetry.yaml.ambient_temperature_c: expected a finite number",
    ),
    ("telemetry.yaml", "  - id: motor-01\n", "  - id: ''\n", "telemetry.yaml.assets[0].id: expected a non-empty string"),
    ("telemetry.yaml", "  - id: motor-02\n", "  - id: motor-01\n", "telemetry.yaml.assets: duplicate asset id"),
    (
        "telemetry.yaml",
        "    load: {base: 0.75,",
        "    load: {base: 1.5,",
        "telemetry.yaml.assets[motor-01].load.base: must be <= 1.2",
    ),
    (
        "telemetry.yaml",
        "rated_slip: 0.020",
        "rated_slip: -0.1",
        "telemetry.yaml.assets[motor-01].electrical.rated_slip: must be >= 0.0",
    ),
    (
        "telemetry.yaml",
        "time_constant_s: 600}",
        "time_constant_s: 0}",
        "telemetry.yaml.assets[motor-01].thermal.time_constant_s: must be > 0",
    ),
    ("telemetry.yaml", "noise_std: {bearing_temperature_c: 0.2, vibration_rms_mm_s: 0.08, motor_current_a: 0.4, speed_rpm: 1.0}",
     "noise_std: {bearing_temperature_c: 0.2}",
     "telemetry.yaml.assets[motor-01].noise_std: missing channel(s) ['motor_current_a', 'speed_rpm', 'vibration_rms_mm_s']"),
    ("telemetry.yaml", "degradation: {bearing_temperature_c: 1.5,", "degradation: {oil_c: 1.5,",
     "telemetry.yaml.assets[pump-02].degradation: channel(s) ['oil_c'] not reported by this asset"),
    ("telemetry.yaml",
     "    channels: [bearing_temperature_c, vibration_rms_mm_s, motor_current_a, speed_rpm]\n    load: {base: 0.75",
     "    channels: [speed_rpm, speed_rpm]\n    load: {base: 0.75",
     "telemetry.yaml.assets[motor-01].channels: duplicate channel"),
    ("telemetry.yaml",
     "    channels: [bearing_temperature_c, vibration_rms_mm_s, motor_current_a, speed_rpm]\n    load: {base: 0.75",
     "    channels: []\n    load: {base: 0.75", "telemetry.yaml.assets[motor-01].channels: expected a non-empty list"),
    ("telemetry.yaml", "    kind: step", "    kind: pulse", "telemetry.yaml.faults.imbalance.kind: must be 'step' or 'ramp'"),
    (
        "telemetry.yaml",
        "    ramp_s: 180",
        "    ramp_s: 0",
        "telemetry.yaml.faults.bearing_overheating.ramp_s: must be > 0 for a ramp",
    ),
    (
        "telemetry.yaml",
        "    channel: vibration_rms_mm_s\n    kind: step",
        "    channel: oil_c\n    kind: step",
        "telemetry.yaml.faults.imbalance.channel: unknown channel 'oil_c'",
    ),
    # The next three were accepted before commit 7a29126 (an empty container
    # of the wrong type passed as "absent").
    ("telemetry.yaml", "fault_schedule: []", "fault_schedule: {}", "telemetry.yaml.fault_schedule: expected a list"),
    ("telemetry.yaml", "degradation: {bearing_temperature_c: 1.5, vibration_rms_mm_s: 0.6}", "degradation: []",
     "telemetry.yaml.assets[pump-02].degradation: expected a mapping, got list"),
    ("topics.yaml", "    configs:\n      cleanup.policy: delete\n      retention.ms: 604800000", "    configs: []",
     "topics.yaml.kafka.telemetry_dlq.configs: expected a mapping, got list"),
    (
        "telemetry.yaml",
        "fault_schedule: []",
        "fault_schedule:\n  - {asset: pump-01, mode: imbalance}",
        "telemetry.yaml.fault_schedule[0]: missing key(s) ['start_s']",
    ),
    (
        "telemetry.yaml",
        "channels:\n  bearing_temperature_c:",
        "channels:\n  bearing_temperature_c: 3\n  x:",
        "telemetry.yaml.channels.bearing_temperature_c: expected a mapping, got int",
    ),
    ("telemetry.yaml", "sample_interval_s: 1.0", "sample_interval_s: [", None),  # invalid YAML
    # alarms.yaml
    (
        "alarms.yaml",
        "    direction: high\n    warning: 80.0",
        "    direction: low\n    warning: 80.0",
        "alarms.yaml rule for motor-01/bearing_temperature_c.direction: only 'high' is supported",
    ),
    ("alarms.yaml", "  motor-01:\n    motor_current_a: {warning: 57.75, critical: 66.0}",
     "  motor-01:\n    speed_rpm: {warning: 1.0, critical: 2.0}",
     "alarms.yaml.asset_overrides.motor-01.speed_rpm: channel has no default rule in alarms.yaml.channels"),
    (
        "alarms.yaml",
        "{warning: 57.75, critical: 66.0}",
        "{warning: 57.75, critical: 66.0, bogus: 1}",
        "alarms.yaml.asset_overrides.motor-01.motor_current_a: unknown key(s) ['bogus']",
    ),
    # topics.yaml
    (
        "topics.yaml",
        "template: plant/{asset_id}/telemetry",
        "template: plant/telemetry",
        "topics.yaml.mqtt.telemetry.template: must contain {asset_id}",
    ),
    ("topics.yaml", "qos: 1", "qos: 3", "topics.yaml.mqtt.telemetry.qos: must be 0, 1 or 2"),
    ("topics.yaml", "partitions: 1", "partitions: 0", "topics.yaml.kafka.telemetry_dlq.partitions: expected an integer >= 1"),
    (
        "topics.yaml",
        "value_schema: schemas/dlq_v1.avsc",
        "value_schema: schemas/missing.avsc",
        "topics.yaml.kafka.telemetry_dlq.value_schema: file not found: schemas/missing.avsc",
    ),
    (
        "topics.yaml",
        "      cleanup.policy: compact",
        "      cleanup.policy: [compact]",
        "topics.yaml.kafka.telemetry_alarms.configs.cleanup.policy: expected a string or number",
    ),
    (
        "topics.yaml",
        "    value_schema: schemas/telemetry_v1.avsc",
        "    value_schema: null",
        "topics.yaml.kafka.telemetry_raw.value_schema: required",
    ),
]


@pytest.mark.parametrize("file, old, new, message", CASES)
def test_each_rule_rejects(tmp_path, file, old, new, message):
    with pytest.raises(ConfigError) as info:
        load_config(broken(tmp_path, file, old, new), env={})
    if message is None:
        assert str(info.value).startswith(f"{file}: invalid YAML (")
    else:
        assert str(info.value) == message


def test_null_optional_keys_mean_absent(tmp_path):
    target = broken(tmp_path, "telemetry.yaml", "fault_schedule: []", "fault_schedule:")
    assert load_config(target, env={}).telemetry.fault_schedule == ()


def test_fault_schedule_entries_are_validated(tmp_path):
    target = broken(tmp_path, "telemetry.yaml", "fault_schedule: []",
                    "fault_schedule:\n  - {asset: pump-01, mode: imbalance, start_s: 30}")
    [fault] = load_config(target, env={}).telemetry.fault_schedule
    assert (fault.asset, fault.mode, fault.start_s) == ("pump-01", "imbalance", 30.0)


def test_missing_file(tmp_path):
    target = broken(tmp_path, "alarms.yaml", "channels:", "channels:")
    (target / "alarms.yaml").unlink()
    with pytest.raises(ConfigError, match=r"^alarms\.yaml: cannot read"):
        load_config(target, env={})


def test_yaml_that_is_not_a_mapping(tmp_path):
    target = broken(tmp_path, "alarms.yaml", "channels:", "channels:")
    (target / "alarms.yaml").write_text("- a\n- b\n", encoding="utf-8")
    with pytest.raises(ConfigError, match=r"^alarms\.yaml: expected a mapping, got list$"):
        load_config(target, env={})


def test_channel_must_be_in_the_telemetry_schema(cfg, tmp_path):
    schema = json.loads(cfg.topics.telemetry_raw.value_schema.read_text(encoding="utf-8"))
    schema["fields"] = [f for f in schema["fields"] if f["name"] != "speed_rpm"]
    path = tmp_path / "no_speed.avsc"
    path.write_text(json.dumps(schema), encoding="utf-8")
    with pytest.raises(ConfigError, match="speed_rpm: not a nullable double field of no_speed.avsc"):
        loader._check_channels_in_schema(cfg.telemetry, dataclasses.replace(cfg.topics.telemetry_raw, value_schema=path))
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(ConfigError, match="cannot parse"):
        loader._check_channels_in_schema(cfg.telemetry, dataclasses.replace(cfg.topics.telemetry_raw, value_schema=path))
    with pytest.raises(ConfigError, match="value_schema: required"):
        loader._check_channels_in_schema(cfg.telemetry, dataclasses.replace(cfg.topics.telemetry_raw, value_schema=None))
