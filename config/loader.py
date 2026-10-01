"""Load and validate the pipeline configuration.

The YAML files in this directory are the single source of truth for assets,
channels, alarm thresholds and topic names. Every Python component obtains
them through ``load_config()``, never by reading the files itself.

Validation is strict: unknown keys, missing keys, wrong types, out-of-range
values and cross-file references that do not resolve all raise
``ConfigError`` with the file and key path, so a typo fails at start-up
rather than silently changing behaviour.
"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

import yaml

CONFIG_DIR = Path(__file__).resolve().parent
REPO_ROOT = CONFIG_DIR.parent

TELEMETRY_TOPIC_ENV = "TELEMETRY_TOPIC"


class ConfigError(ValueError):
    """Raised when a configuration file is missing, malformed or inconsistent."""


# --- telemetry.yaml ---------------------------------------------------------


@dataclass(frozen=True)
class Channel:
    name: str
    unit: str
    description: str


@dataclass(frozen=True)
class LoadModel:
    base: float
    amplitude: float
    period_s: float
    noise_std: float


@dataclass(frozen=True)
class ElectricalModel:
    synchronous_speed_rpm: float
    rated_slip: float
    rated_current_a: float
    no_load_fraction: float


@dataclass(frozen=True)
class ThermalModel:
    rise_at_full_load_c: float
    time_constant_s: float


@dataclass(frozen=True)
class VibrationModel:
    baseline_mm_s: float
    load_coefficient_mm_s: float


@dataclass(frozen=True)
class Asset:
    id: str
    type: str
    description: str
    channels: tuple[str, ...]
    load: LoadModel
    electrical: ElectricalModel
    thermal: ThermalModel
    vibration: VibrationModel
    noise_std: Mapping[str, float]
    degradation: Mapping[str, float]  # channel -> drift per hour


@dataclass(frozen=True)
class FaultMode:
    name: str
    channel: str
    kind: str  # "step" or "ramp"
    magnitude: float
    ramp_s: float
    description: str


@dataclass(frozen=True)
class ScheduledFault:
    asset: str
    mode: str
    start_s: float


@dataclass(frozen=True)
class TelemetryConfig:
    sample_interval_s: float
    ambient_temperature_c: float
    channels: Mapping[str, Channel]
    assets: tuple[Asset, ...]
    faults: Mapping[str, FaultMode]
    fault_schedule: tuple[ScheduledFault, ...]

    def asset(self, asset_id: str) -> Asset:
        for a in self.assets:
            if a.id == asset_id:
                return a
        raise KeyError(asset_id)


# --- alarms.yaml ------------------------------------------------------------


@dataclass(frozen=True)
class AlarmRule:
    channel: str
    direction: str
    warning: float
    critical: float
    hysteresis: float
    persistence_samples: int


@dataclass(frozen=True)
class AlarmsConfig:
    # (asset_id, channel) -> effective rule, overrides already applied.
    rules: Mapping[tuple[str, str], AlarmRule]

    def rule(self, asset_id: str, channel: str) -> AlarmRule | None:
        return self.rules.get((asset_id, channel))


# --- topics.yaml ------------------------------------------------------------


@dataclass(frozen=True)
class MqttTopic:
    template: str
    subscription: str
    qos: int

    def for_asset(self, asset_id: str) -> str:
        return self.template.format(asset_id=asset_id)


@dataclass(frozen=True)
class KafkaTopic:
    key: str
    name: str
    description: str
    partitions: int
    replication_factor: int
    value_schema: Path | None
    configs: Mapping[str, str]  # topic-level settings, applied at creation


@dataclass(frozen=True)
class TopicsConfig:
    mqtt_telemetry: MqttTopic
    kafka: Mapping[str, KafkaTopic]

    @property
    def telemetry_raw(self) -> KafkaTopic:
        return self.kafka["telemetry_raw"]


@dataclass(frozen=True)
class Config:
    telemetry: TelemetryConfig
    alarms: AlarmsConfig
    topics: TopicsConfig


# --- validation helpers -----------------------------------------------------


def _mapping(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigError(f"{where}: expected a mapping, got {type(value).__name__}")
    return value


def _keys(m: Mapping[str, Any], where: str, required: set[str], optional: set[str] = frozenset()) -> None:
    missing = required - m.keys()
    unknown = m.keys() - required - optional
    if missing:
        raise ConfigError(f"{where}: missing key(s) {sorted(missing)}")
    if unknown:
        raise ConfigError(f"{where}: unknown key(s) {sorted(unknown)}")


def _str(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{where}: expected a non-empty string")
    return value


def _num(value: Any, where: str, *, min_: float | None = None, max_: float | None = None, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ConfigError(f"{where}: expected a finite number")
    value = float(value)
    if positive and value <= 0:
        raise ConfigError(f"{where}: must be > 0")
    if min_ is not None and value < min_:
        raise ConfigError(f"{where}: must be >= {min_}")
    if max_ is not None and value > max_:
        raise ConfigError(f"{where}: must be <= {max_}")
    return value


def _int(value: Any, where: str, *, min_: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < min_:
        raise ConfigError(f"{where}: expected an integer >= {min_}")
    return value


def _read_yaml(path: Path) -> Mapping[str, Any]:
    try:
        with path.open(encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except OSError as e:
        raise ConfigError(f"{path.name}: cannot read ({e})") from e
    except yaml.YAMLError as e:
        raise ConfigError(f"{path.name}: invalid YAML ({e})") from e
    return _mapping(data, path.name)


def _frozen(d: dict) -> Mapping:
    return MappingProxyType(d)


# --- parsers ----------------------------------------------------------------


def _parse_channel_values(value: Any, where: str, channels: tuple[str, ...], *, min_: float | None) -> Mapping[str, float]:
    m = _mapping(value, where)
    unknown = m.keys() - set(channels)
    if unknown:
        raise ConfigError(f"{where}: channel(s) {sorted(unknown)} not reported by this asset")
    return _frozen({k: _num(v, f"{where}.{k}", min_=min_) for k, v in m.items()})


def _parse_telemetry(data: Mapping[str, Any]) -> TelemetryConfig:
    w = "telemetry.yaml"
    _keys(data, w, {"sample_interval_s", "ambient_temperature_c", "channels", "assets", "faults", "fault_schedule"})

    channels = {}
    for name, c in _mapping(data["channels"], f"{w}.channels").items():
        cw = f"{w}.channels.{name}"
        c = _mapping(c, cw)
        _keys(c, cw, {"unit", "description"})
        channels[name] = Channel(name, _str(c["unit"], f"{cw}.unit"), _str(c["description"], f"{cw}.description"))
    if not channels:
        raise ConfigError(f"{w}.channels: at least one channel is required")

    assets = []
    if not isinstance(data["assets"], list) or not data["assets"]:
        raise ConfigError(f"{w}.assets: expected a non-empty list")
    for i, a in enumerate(data["assets"]):
        aw = f"{w}.assets[{i}]"
        a = _mapping(a, aw)
        _keys(a, aw, {"id", "type", "description", "channels", "load", "electrical", "thermal", "vibration", "noise_std"}, {"degradation"})
        asset_id = _str(a["id"], f"{aw}.id")
        aw = f"{w}.assets[{asset_id}]"

        if not isinstance(a["channels"], list) or not a["channels"]:
            raise ConfigError(f"{aw}.channels: expected a non-empty list")
        asset_channels = tuple(_str(c, f"{aw}.channels") for c in a["channels"])
        for c in asset_channels:
            if c not in channels:
                raise ConfigError(f"{aw}.channels: unknown channel {c!r}")
        if len(set(asset_channels)) != len(asset_channels):
            raise ConfigError(f"{aw}.channels: duplicate channel")

        ld = _mapping(a["load"], f"{aw}.load")
        _keys(ld, f"{aw}.load", {"base", "amplitude", "period_s", "noise_std"})
        load = LoadModel(
            base=_num(ld["base"], f"{aw}.load.base", min_=0.0, max_=1.2),
            amplitude=_num(ld["amplitude"], f"{aw}.load.amplitude", min_=0.0, max_=1.0),
            period_s=_num(ld["period_s"], f"{aw}.load.period_s", positive=True),
            noise_std=_num(ld["noise_std"], f"{aw}.load.noise_std", min_=0.0),
        )

        el = _mapping(a["electrical"], f"{aw}.electrical")
        _keys(el, f"{aw}.electrical", {"synchronous_speed_rpm", "rated_slip", "rated_current_a", "no_load_fraction"})
        electrical = ElectricalModel(
            synchronous_speed_rpm=_num(el["synchronous_speed_rpm"], f"{aw}.electrical.synchronous_speed_rpm", positive=True),
            rated_slip=_num(el["rated_slip"], f"{aw}.electrical.rated_slip", min_=0.0, max_=0.2),
            rated_current_a=_num(el["rated_current_a"], f"{aw}.electrical.rated_current_a", positive=True),
            no_load_fraction=_num(el["no_load_fraction"], f"{aw}.electrical.no_load_fraction", min_=0.0, max_=1.0),
        )

        th = _mapping(a["thermal"], f"{aw}.thermal")
        _keys(th, f"{aw}.thermal", {"rise_at_full_load_c", "time_constant_s"})
        thermal = ThermalModel(
            rise_at_full_load_c=_num(th["rise_at_full_load_c"], f"{aw}.thermal.rise_at_full_load_c", min_=0.0),
            time_constant_s=_num(th["time_constant_s"], f"{aw}.thermal.time_constant_s", positive=True),
        )

        vb = _mapping(a["vibration"], f"{aw}.vibration")
        _keys(vb, f"{aw}.vibration", {"baseline_mm_s", "load_coefficient_mm_s"})
        vibration = VibrationModel(
            baseline_mm_s=_num(vb["baseline_mm_s"], f"{aw}.vibration.baseline_mm_s", min_=0.0),
            load_coefficient_mm_s=_num(vb["load_coefficient_mm_s"], f"{aw}.vibration.load_coefficient_mm_s", min_=0.0),
        )

        noise = _parse_channel_values(a["noise_std"], f"{aw}.noise_std", asset_channels, min_=0.0)
        missing_noise = set(asset_channels) - noise.keys()
        if missing_noise:
            raise ConfigError(f"{aw}.noise_std: missing channel(s) {sorted(missing_noise)}")
        degradation = _parse_channel_values(a.get("degradation") or {}, f"{aw}.degradation", asset_channels, min_=None)

        assets.append(Asset(asset_id, _str(a["type"], f"{aw}.type"), _str(a["description"], f"{aw}.description"),
                            asset_channels, load, electrical, thermal, vibration, noise, degradation))
    ids = [a.id for a in assets]
    if len(set(ids)) != len(ids):
        raise ConfigError(f"{w}.assets: duplicate asset id")

    faults = {}
    for name, f in _mapping(data["faults"], f"{w}.faults").items():
        fw = f"{w}.faults.{name}"
        f = _mapping(f, fw)
        _keys(f, fw, {"channel", "kind", "magnitude", "ramp_s", "description"})
        channel = _str(f["channel"], f"{fw}.channel")
        if channel not in channels:
            raise ConfigError(f"{fw}.channel: unknown channel {channel!r}")
        kind = _str(f["kind"], f"{fw}.kind")
        if kind not in ("step", "ramp"):
            raise ConfigError(f"{fw}.kind: must be 'step' or 'ramp'")
        ramp_s = _num(f["ramp_s"], f"{fw}.ramp_s", min_=0.0)
        if kind == "ramp" and ramp_s <= 0:
            raise ConfigError(f"{fw}.ramp_s: must be > 0 for a ramp")
        faults[name] = FaultMode(name, channel, kind, _num(f["magnitude"], f"{fw}.magnitude"), ramp_s,
                                 _str(f["description"], f"{fw}.description"))

    schedule_raw = data["fault_schedule"] or []
    if not isinstance(schedule_raw, list):
        raise ConfigError(f"{w}.fault_schedule: expected a list")
    telemetry = TelemetryConfig(
        sample_interval_s=_num(data["sample_interval_s"], f"{w}.sample_interval_s", positive=True),
        ambient_temperature_c=_num(data["ambient_temperature_c"], f"{w}.ambient_temperature_c"),
        channels=_frozen(channels),
        assets=tuple(assets),
        faults=_frozen(faults),
        fault_schedule=(),
    )
    schedule = []
    for i, s in enumerate(schedule_raw):
        sw = f"{w}.fault_schedule[{i}]"
        s = _mapping(s, sw)
        _keys(s, sw, {"asset", "mode", "start_s"})
        schedule.append(validate_fault(telemetry, _str(s["asset"], f"{sw}.asset"), _str(s["mode"], f"{sw}.mode"),
                                       _num(s["start_s"], f"{sw}.start_s", min_=0.0), sw))
    return TelemetryConfig(**{**telemetry.__dict__, "fault_schedule": tuple(schedule)})


def validate_fault(telemetry: TelemetryConfig, asset: str, mode: str, start_s: float, where: str) -> ScheduledFault:
    """Check a fault injection against the configured assets and fault modes."""
    if asset not in {a.id for a in telemetry.assets}:
        raise ConfigError(f"{where}: unknown asset {asset!r}")
    if mode not in telemetry.faults:
        raise ConfigError(f"{where}: unknown fault mode {mode!r}; known: {sorted(telemetry.faults)}")
    if telemetry.faults[mode].channel not in telemetry.asset(asset).channels:
        raise ConfigError(f"{where}: asset {asset!r} does not report {telemetry.faults[mode].channel!r}")
    if start_s < 0:
        raise ConfigError(f"{where}: start_s must be >= 0")
    return ScheduledFault(asset, mode, start_s)


_RULE_FIELDS = {"direction", "warning", "critical", "hysteresis", "persistence_samples"}


def _parse_alarms(data: Mapping[str, Any], telemetry: TelemetryConfig) -> AlarmsConfig:
    w = "alarms.yaml"
    _keys(data, w, {"channels"}, {"asset_overrides"})

    defaults: dict[str, Mapping[str, Any]] = {}
    for channel, rule in _mapping(data["channels"], f"{w}.channels").items():
        cw = f"{w}.channels.{channel}"
        if channel not in telemetry.channels:
            raise ConfigError(f"{cw}: unknown channel")
        rule = _mapping(rule, cw)
        _keys(rule, cw, {"direction", "hysteresis", "persistence_samples"}, {"warning", "critical"})
        defaults[channel] = rule

    overrides = _mapping(data.get("asset_overrides") or {}, f"{w}.asset_overrides")
    for asset_id, per_channel in overrides.items():
        ow = f"{w}.asset_overrides.{asset_id}"
        try:
            asset = telemetry.asset(asset_id)
        except KeyError:
            raise ConfigError(f"{ow}: unknown asset") from None
        for channel, rule in _mapping(per_channel, ow).items():
            if channel not in defaults:
                raise ConfigError(f"{ow}.{channel}: channel has no default rule in {w}.channels")
            if channel not in asset.channels:
                raise ConfigError(f"{ow}.{channel}: asset does not report this channel")
            _keys(_mapping(rule, f"{ow}.{channel}"), f"{ow}.{channel}", set(), _RULE_FIELDS)

    rules = {}
    for asset in telemetry.assets:
        for channel in asset.channels:
            if channel not in defaults:
                continue  # channel not alarmed
            merged = {**defaults[channel], **((overrides.get(asset.id) or {}).get(channel) or {})}
            rw = f"{w} rule for {asset.id}/{channel}"
            if "warning" not in merged or "critical" not in merged:
                raise ConfigError(f"{rw}: warning and critical must be set by the channel default or an asset override")
            direction = _str(merged["direction"], f"{rw}.direction")
            if direction != "high":
                raise ConfigError(f"{rw}.direction: only 'high' is supported")
            warning = _num(merged["warning"], f"{rw}.warning")
            critical = _num(merged["critical"], f"{rw}.critical")
            if critical <= warning:
                raise ConfigError(f"{rw}: critical must be above warning")
            hysteresis = _num(merged["hysteresis"], f"{rw}.hysteresis", min_=0.0)
            if hysteresis >= critical - warning:
                raise ConfigError(f"{rw}: hysteresis must be smaller than the warning-to-critical band")
            rules[(asset.id, channel)] = AlarmRule(
                channel, direction, warning, critical, hysteresis,
                _int(merged["persistence_samples"], f"{rw}.persistence_samples", min_=1),
            )
    return AlarmsConfig(_frozen(rules))


def _parse_topics(data: Mapping[str, Any], env: Mapping[str, str]) -> TopicsConfig:
    w = "topics.yaml"
    _keys(data, w, {"mqtt", "kafka"})

    mqtt = _mapping(data["mqtt"], f"{w}.mqtt")
    _keys(mqtt, f"{w}.mqtt", {"telemetry"})
    t = _mapping(mqtt["telemetry"], f"{w}.mqtt.telemetry")
    _keys(t, f"{w}.mqtt.telemetry", {"template", "subscription", "qos"})
    template = _str(t["template"], f"{w}.mqtt.telemetry.template")
    if "{asset_id}" not in template:
        raise ConfigError(f"{w}.mqtt.telemetry.template: must contain {{asset_id}}")
    qos = _int(t["qos"], f"{w}.mqtt.telemetry.qos", min_=0)
    if qos > 2:
        raise ConfigError(f"{w}.mqtt.telemetry.qos: must be 0, 1 or 2")
    mqtt_telemetry = MqttTopic(template, _str(t["subscription"], f"{w}.mqtt.telemetry.subscription"), qos)

    kafka = {}
    required_topics = {"telemetry_raw", "telemetry_alarms", "telemetry_dlq"}
    k = _mapping(data["kafka"], f"{w}.kafka")
    _keys(k, f"{w}.kafka", required_topics)
    for key, topic in k.items():
        tw = f"{w}.kafka.{key}"
        topic = _mapping(topic, tw)
        _keys(topic, tw, {"name", "description", "partitions", "replication_factor", "value_schema"}, {"configs"})
        schema = topic["value_schema"]
        schema_path = None
        if schema is not None:
            schema_path = REPO_ROOT / _str(schema, f"{tw}.value_schema")
            if not schema_path.is_file():
                raise ConfigError(f"{tw}.value_schema: file not found: {schema}")
        name = _str(topic["name"], f"{tw}.name")
        if key == "telemetry_raw" and env.get(TELEMETRY_TOPIC_ENV):
            name = env[TELEMETRY_TOPIC_ENV]
        configs = {}
        for k, v in _mapping(topic.get("configs") or {}, f"{tw}.configs").items():
            if isinstance(v, bool) or not isinstance(v, (str, int, float)):
                raise ConfigError(f"{tw}.configs.{k}: expected a string or number")
            configs[_str(k, f"{tw}.configs")] = str(v)
        kafka[key] = KafkaTopic(key, name, _str(topic["description"], f"{tw}.description"),
                                _int(topic["partitions"], f"{tw}.partitions", min_=1),
                                _int(topic["replication_factor"], f"{tw}.replication_factor", min_=1),
                                schema_path, _frozen(configs))
    return TopicsConfig(mqtt_telemetry, _frozen(kafka))


def load_config(config_dir: Path | None = None, env: Mapping[str, str] | None = None) -> Config:
    """Load, validate and cross-check telemetry.yaml, alarms.yaml and topics.yaml."""
    config_dir = config_dir or CONFIG_DIR
    env = os.environ if env is None else env
    telemetry = _parse_telemetry(_read_yaml(config_dir / "telemetry.yaml"))
    alarms = _parse_alarms(_read_yaml(config_dir / "alarms.yaml"), telemetry)
    topics = _parse_topics(_read_yaml(config_dir / "topics.yaml"), env)
    _check_channels_in_schema(telemetry, topics.telemetry_raw)
    return Config(telemetry, alarms, topics)


def _check_channels_in_schema(telemetry: TelemetryConfig, topic: KafkaTopic) -> None:
    """Every configured channel must be a nullable double field of the telemetry schema."""
    if topic.value_schema is None:
        raise ConfigError("topics.yaml.kafka.telemetry_raw.value_schema: required")
    try:
        schema = json.loads(topic.value_schema.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise ConfigError(f"{topic.value_schema.name}: cannot parse ({e})") from e
    fields = {f["name"]: f["type"] for f in schema.get("fields", [])}
    for channel in telemetry.channels:
        if fields.get(channel) != ["null", "double"]:
            raise ConfigError(
                f"telemetry.yaml.channels.{channel}: not a nullable double field of {topic.value_schema.name}"
            )
