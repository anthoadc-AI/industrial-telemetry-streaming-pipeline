'use strict';

/**
 * Load the alarm consumer's configuration from config/: the resolved alarm
 * rules, and the topic names and alarm schema it needs.
 *
 * config/loader.py is the authoritative loader. This module resolves alarm
 * rules exactly as config/loader.py does (channel defaults, then per-asset
 * overrides field by field) and applies the same checks to alarms.yaml, with
 * the same error messages. tests/unit/test_alarm_config_equivalence.py
 * asserts that both loaders resolve identical rules.
 *
 * Of telemetry.yaml it validates only what the consumer reads: the top-level
 * keys, the channel names, and each asset's id and channel list. The model
 * parameters are validated by the Python loader, which the simulator and
 * `make up` run.
 *
 * Every problem raises ConfigError naming the file and key path.
 */

const fs = require('node:fs');
const path = require('node:path');
const YAML = require('yaml');

const REPO_ROOT = path.resolve(__dirname, '..', '..');
const CONFIG_DIR = path.join(REPO_ROOT, 'config');
const TELEMETRY_TOPIC_ENV = 'TELEMETRY_TOPIC';

class ConfigError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ConfigError';
  }
}

// Python's repr of a sorted list of strings, so messages match config/loader.py.
function pyList(items) {
  return `[${[...items].sort().map((s) => `'${s}'`).join(', ')}]`;
}

function isMapping(v) {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

function mapping(value, where) {
  if (!isMapping(value)) {
    const got = value === null ? 'NoneType' : Array.isArray(value) ? 'list' : typeof value;
    throw new ConfigError(`${where}: expected a mapping, got ${got}`);
  }
  return value;
}

function keys(m, where, required, optional = []) {
  const present = Object.keys(m);
  const missing = required.filter((k) => !present.includes(k));
  const unknown = present.filter((k) => !required.includes(k) && !optional.includes(k));
  if (missing.length) throw new ConfigError(`${where}: missing key(s) ${pyList(missing)}`);
  if (unknown.length) throw new ConfigError(`${where}: unknown key(s) ${pyList(unknown)}`);
}

function str(value, where) {
  if (typeof value !== 'string' || value === '') throw new ConfigError(`${where}: expected a non-empty string`);
  return value;
}

function num(value, where, { min } = {}) {
  if (typeof value !== 'number' || !Number.isFinite(value)) throw new ConfigError(`${where}: expected a finite number`);
  // Python writes a float minimum as 0.0; match it.
  if (min !== undefined && value < min) throw new ConfigError(`${where}: must be >= ${Number.isInteger(min) ? min.toFixed(1) : min}`);
  return value;
}

function int(value, where, { min }) {
  if (!Number.isInteger(value) || value < min) throw new ConfigError(`${where}: expected an integer >= ${min}`);
  return value;
}

function readYaml(dir, name) {
  let text;
  try {
    text = fs.readFileSync(path.join(dir, name), 'utf8');
  } catch (e) {
    throw new ConfigError(`${name}: cannot read (${e.message})`);
  }
  let data;
  try {
    data = YAML.parse(text);
  } catch (e) {
    throw new ConfigError(`${name}: invalid YAML (${e.message.split('\n')[0]})`);
  }
  return mapping(data, name);
}

function parseTelemetry(data) {
  const w = 'telemetry.yaml';
  keys(data, w, ['sample_interval_s', 'ambient_temperature_c', 'channels', 'assets', 'faults', 'fault_schedule']);
  const channels = Object.keys(mapping(data.channels, `${w}.channels`));
  if (!channels.length) throw new ConfigError(`${w}.channels: at least one channel is required`);
  if (!Array.isArray(data.assets) || !data.assets.length) throw new ConfigError(`${w}.assets: expected a non-empty list`);
  const assets = data.assets.map((a, i) => {
    let aw = `${w}.assets[${i}]`;
    mapping(a, aw);
    const id = str(a.id, `${aw}.id`);
    aw = `${w}.assets[${id}]`;
    if (!Array.isArray(a.channels) || !a.channels.length) throw new ConfigError(`${aw}.channels: expected a non-empty list`);
    for (const c of a.channels) {
      str(c, `${aw}.channels`);
      if (!channels.includes(c)) throw new ConfigError(`${aw}.channels: unknown channel '${c}'`);
    }
    if (new Set(a.channels).size !== a.channels.length) throw new ConfigError(`${aw}.channels: duplicate channel`);
    return { id, channels: [...a.channels] };
  });
  if (new Set(assets.map((a) => a.id)).size !== assets.length) throw new ConfigError(`${w}.assets: duplicate asset id`);
  return { channels, assets };
}

const RULE_FIELDS = ['direction', 'warning', 'critical', 'hysteresis', 'persistence_samples'];

function parseAlarms(data, telemetry) {
  const w = 'alarms.yaml';
  keys(data, w, ['channels'], ['asset_overrides']);

  const defaults = {};
  for (const [channel, rule] of Object.entries(mapping(data.channels, `${w}.channels`))) {
    const cw = `${w}.channels.${channel}`;
    if (!telemetry.channels.includes(channel)) throw new ConfigError(`${cw}: unknown channel`);
    keys(mapping(rule, cw), cw, ['direction', 'hysteresis', 'persistence_samples'], ['warning', 'critical']);
    defaults[channel] = rule;
  }

  const overrides = mapping(data.asset_overrides ?? {}, `${w}.asset_overrides`);
  for (const [assetId, perChannel] of Object.entries(overrides)) {
    const ow = `${w}.asset_overrides.${assetId}`;
    const asset = telemetry.assets.find((a) => a.id === assetId);
    if (!asset) throw new ConfigError(`${ow}: unknown asset`);
    for (const [channel, rule] of Object.entries(mapping(perChannel, ow))) {
      if (!(channel in defaults)) throw new ConfigError(`${ow}.${channel}: channel has no default rule in ${w}.channels`);
      if (!asset.channels.includes(channel)) throw new ConfigError(`${ow}.${channel}: asset does not report this channel`);
      keys(mapping(rule, `${ow}.${channel}`), `${ow}.${channel}`, [], RULE_FIELDS);
    }
  }

  const rules = [];
  for (const asset of telemetry.assets) {
    for (const channel of asset.channels) {
      if (!(channel in defaults)) continue; // channel not alarmed
      const merged = { ...defaults[channel], ...((overrides[asset.id] ?? {})[channel] ?? {}) };
      const rw = `${w} rule for ${asset.id}/${channel}`;
      if (!('warning' in merged) || !('critical' in merged)) {
        throw new ConfigError(`${rw}: warning and critical must be set by the channel default or an asset override`);
      }
      const direction = str(merged.direction, `${rw}.direction`);
      if (direction !== 'high') throw new ConfigError(`${rw}.direction: only 'high' is supported`);
      const warning = num(merged.warning, `${rw}.warning`);
      const critical = num(merged.critical, `${rw}.critical`);
      if (critical <= warning) throw new ConfigError(`${rw}: critical must be above warning`);
      const hysteresis = num(merged.hysteresis, `${rw}.hysteresis`, { min: 0 });
      if (hysteresis >= critical - warning) {
        throw new ConfigError(`${rw}: hysteresis must be smaller than the warning-to-critical band`);
      }
      rules.push({
        asset_id: asset.id,
        channel,
        direction,
        warning,
        critical,
        hysteresis,
        persistence_samples: int(merged.persistence_samples, `${rw}.persistence_samples`, { min: 1 }),
      });
    }
  }
  return rules;
}

function parseTopics(data, env) {
  const w = 'topics.yaml';
  keys(data, w, ['mqtt', 'kafka']);
  const k = mapping(data.kafka, `${w}.kafka`);
  keys(k, `${w}.kafka`, ['telemetry_raw', 'telemetry_alarms', 'telemetry_dlq']);
  const topics = {};
  for (const [key, topic] of Object.entries(k)) {
    const tw = `${w}.kafka.${key}`;
    keys(mapping(topic, tw), tw, ['name', 'description', 'partitions', 'replication_factor', 'value_schema'], ['configs']);
    let name = str(topic.name, `${tw}.name`);
    if (key === 'telemetry_raw' && env[TELEMETRY_TOPIC_ENV]) name = env[TELEMETRY_TOPIC_ENV];
    let schemaPath = null;
    if (topic.value_schema !== null) {
      schemaPath = path.join(REPO_ROOT, str(topic.value_schema, `${tw}.value_schema`));
      if (!fs.existsSync(schemaPath)) throw new ConfigError(`${tw}.value_schema: file not found: ${topic.value_schema}`);
    }
    const configs = {};
    for (const [ck, cv] of Object.entries(mapping(topic.configs ?? {}, `${tw}.configs`))) {
      if (typeof cv !== 'string' && typeof cv !== 'number') throw new ConfigError(`${tw}.configs.${ck}: expected a string or number`);
      configs[ck] = String(cv);
    }
    topics[key] = { name, valueSchemaPath: schemaPath, configs };
  }
  return topics;
}

/**
 * Load and validate the configuration the alarm consumer needs.
 * Returns { rules, topics, alarmSchema, dlqSchema } where rules is the ordered
 * list of resolved rules (one per alarmed asset/channel), and alarmSchema and
 * dlqSchema the text of the telemetry_alarms and telemetry_dlq value schemas.
 */
function loadConfig({ configDir = CONFIG_DIR, env = process.env } = {}) {
  const telemetry = parseTelemetry(readYaml(configDir, 'telemetry.yaml'));
  const rules = parseAlarms(readYaml(configDir, 'alarms.yaml'), telemetry);
  const topics = parseTopics(readYaml(configDir, 'topics.yaml'), env);
  for (const key of ['telemetry_alarms', 'telemetry_dlq']) {
    if (!topics[key].valueSchemaPath) throw new ConfigError(`topics.yaml.kafka.${key}.value_schema: required`);
  }
  const alarmSchema = fs.readFileSync(topics.telemetry_alarms.valueSchemaPath, 'utf8');
  const dlqSchema = fs.readFileSync(topics.telemetry_dlq.valueSchemaPath, 'utf8');
  return { rules, topics, alarmSchema, dlqSchema };
}

module.exports = { loadConfig, ConfigError, CONFIG_DIR };
