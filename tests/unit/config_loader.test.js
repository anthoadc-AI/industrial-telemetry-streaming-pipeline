'use strict';

// processing/alarm_consumer/config_loader.js: the real configuration and one
// rejection per rule, in-process so Jest measures it. Agreement with the
// Python loader is asserted separately by test_alarm_config_equivalence.py.

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { loadConfig, ConfigError, CONFIG_DIR } = require('../../processing/alarm_consumer/config_loader');

function brokenCopy(file, oldText, newText) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-'));
  for (const f of fs.readdirSync(CONFIG_DIR).filter((n) => n.endsWith('.yaml'))) {
    fs.copyFileSync(path.join(CONFIG_DIR, f), path.join(dir, f));
  }
  const target = path.join(dir, file);
  const text = fs.readFileSync(target, 'utf8');
  if (!text.includes(oldText)) throw new Error(`fixture out of date: ${oldText}`);
  fs.writeFileSync(target, text.replace(oldText, newText));
  return dir;
}

function rejection(file, oldText, newText) {
  try {
    loadConfig({ configDir: brokenCopy(file, oldText, newText), env: {} });
  } catch (e) {
    expect(e).toBeInstanceOf(ConfigError);
    return e.message;
  }
  throw new Error('configuration was accepted');
}

test('the real configuration', () => {
  const c = loadConfig({ env: {} });
  expect(c.rules).toHaveLength(15);
  expect(c.rules[0]).toEqual({
    asset_id: 'motor-01', channel: 'bearing_temperature_c', direction: 'high',
    warning: 80, critical: 95, hysteresis: 3, persistence_samples: 3,
  });
  expect(c.rules.find((r) => r.asset_id === 'compressor-01' && r.channel === 'motor_current_a')).toMatchObject({ warning: 126, critical: 144 });
  expect(c.topics.telemetry_alarms).toMatchObject({ name: 'telemetry.alarms', configs: { 'cleanup.policy': 'compact' } });
  expect(c.topics.telemetry_dlq.configs).toEqual({ 'cleanup.policy': 'delete', 'retention.ms': '604800000' });
  expect(JSON.parse(c.alarmSchema).name).toBe('Alarm');
  expect(JSON.parse(c.dlqSchema).name).toBe('DeadLetter');
});

test.each([[{ TELEMETRY_TOPIC: 'x.y' }, 'x.y'], [{ TELEMETRY_TOPIC: '' }, 'telemetry.raw'], [{}, 'telemetry.raw']])(
  'TELEMETRY_TOPIC %p', (env, expected) => {
    expect(loadConfig({ env }).topics.telemetry_raw.name).toBe(expected);
  },
);

test.each([
  ['alarms.yaml', 'channels:\n', 'channels: [unclosed\n', /^alarms\.yaml: invalid YAML \(/],
  ['alarms.yaml', '  motor-01:\n', '  motor-99:\n', 'alarms.yaml.asset_overrides.motor-99: unknown asset'],
  ['alarms.yaml', 'critical: 95.0', 'critical: 70.0', 'alarms.yaml rule for motor-01/bearing_temperature_c: critical must be above warning'],
  ['alarms.yaml', 'hysteresis: 3.0', 'hysteresis: 20.0', 'alarms.yaml rule for motor-01/bearing_temperature_c: hysteresis must be smaller than the warning-to-critical band'],
  ['alarms.yaml', 'hysteresis: 0.3', 'hysteresis: -0.3', 'alarms.yaml rule for motor-01/vibration_rms_mm_s.hysteresis: must be >= 0.0'],
  ['alarms.yaml', 'persistence_samples: 3', 'persistence_samples: 1.5', 'alarms.yaml rule for motor-01/bearing_temperature_c.persistence_samples: expected an integer >= 1'],
  ['alarms.yaml', '    direction: high\n    warning: 80.0', '    direction: low\n    warning: 80.0', "alarms.yaml rule for motor-01/bearing_temperature_c.direction: only 'high' is supported"],
  ['alarms.yaml', '    direction: high\n    warning: 80.0', '    direction: ""\n    warning: 80.0', 'alarms.yaml rule for motor-01/bearing_temperature_c.direction: expected a non-empty string'],
  ['alarms.yaml', 'critical: 95.0', 'critical: "high"', 'alarms.yaml rule for motor-01/bearing_temperature_c.critical: expected a finite number'],
  ['alarms.yaml', '    persistence_samples: 3\n', '    persistence_samples: 3\n    persistance: 4\n', "alarms.yaml.channels.bearing_temperature_c: unknown key(s) ['persistance']"],
  ['alarms.yaml', '    hysteresis: 3.0\n', '', "alarms.yaml.channels.bearing_temperature_c: missing key(s) ['hysteresis']"],
  ['alarms.yaml', '  vibration_rms_mm_s:\n    direction: high', '  oil_bar:\n    direction: high', 'alarms.yaml.channels.oil_bar: unknown channel'],
  ['alarms.yaml', '  pump-02:\n    motor_current_a: {warning: 31.5, critical: 36.0}\n', '',
    'alarms.yaml rule for pump-02/motor_current_a: warning and critical must be set by the channel default or an asset override'],
  ['alarms.yaml', '  motor-01:\n    motor_current_a: {warning: 57.75, critical: 66.0}', '  motor-01:\n    speed_rpm: {warning: 1, critical: 2}',
    'alarms.yaml.asset_overrides.motor-01.speed_rpm: channel has no default rule in alarms.yaml.channels'],
  ['alarms.yaml', 'channels:\n', 'extra: 1\nchannels:\n', "alarms.yaml: unknown key(s) ['extra']"],
  ['alarms.yaml', 'channels:\n  bearing_temperature_c:', 'channels:\n  bearing_temperature_c: 3\n  x:', 'alarms.yaml.channels.bearing_temperature_c: expected a mapping, got number'],
  ['telemetry.yaml', '  - id: motor-02\n', '  - id: motor-01\n', 'telemetry.yaml.assets: duplicate asset id'],
  ['telemetry.yaml', '    channels: [bearing_temperature_c, vibration_rms_mm_s, motor_current_a, speed_rpm]\n    load: {base: 0.75',
    '    channels: [speed_rpm, oil_bar]\n    load: {base: 0.75', "telemetry.yaml.assets[motor-01].channels: unknown channel 'oil_bar'"],
  ['telemetry.yaml', '    channels: [bearing_temperature_c, vibration_rms_mm_s, motor_current_a, speed_rpm]\n    load: {base: 0.75',
    '    channels: [speed_rpm, speed_rpm]\n    load: {base: 0.75', 'telemetry.yaml.assets[motor-01].channels: duplicate channel'],
  ['telemetry.yaml', '    channels: [bearing_temperature_c, vibration_rms_mm_s, motor_current_a, speed_rpm]\n    load: {base: 0.75',
    '    channels: []\n    load: {base: 0.75', 'telemetry.yaml.assets[motor-01].channels: expected a non-empty list'],
  ['telemetry.yaml', 'sample_interval_s: 1.0\n', '', "telemetry.yaml: missing key(s) ['sample_interval_s']"],
  ['telemetry.yaml', 'assets:\n  - id: motor-01', 'assets: []\nunused:\n  - id: motor-01', "telemetry.yaml: unknown key(s) ['unused']"],
  ['telemetry.yaml', 'channels:\n  bearing_temperature_c:\n    label: Bearing temperature\n    unit: degC\n    description: Drive-end bearing housing temperature.\n  vibration_rms_mm_s:\n    label: Vibration RMS\n    unit: mm/s\n    description: Casing vibration, velocity RMS.\n  motor_current_a:\n    label: Motor current\n    unit: A\n    description: Motor phase current, RMS.\n  speed_rpm:\n    label: Speed\n    unit: rpm\n    description: Shaft speed.\n',
    'channels: {}\n', 'telemetry.yaml.channels: at least one channel is required'],
  ['topics.yaml', 'value_schema: schemas/dlq_v1.avsc', 'value_schema: schemas/missing.avsc', 'topics.yaml.kafka.telemetry_dlq.value_schema: file not found: schemas/missing.avsc'],
  ['topics.yaml', '      cleanup.policy: compact', '      cleanup.policy: [compact]', 'topics.yaml.kafka.telemetry_alarms.configs.cleanup.policy: expected a string or number'],
  ['topics.yaml', '    value_schema: schemas/dlq_v1.avsc', '    value_schema: null', 'topics.yaml.kafka.telemetry_dlq.value_schema: required'],
  ['topics.yaml', '    name: telemetry.dlq', '    name: ""', 'topics.yaml.kafka.telemetry_dlq.name: expected a non-empty string'],
])('%s: %j -> rejected', (file, oldText, newText, message) => {
  const got = rejection(file, oldText, newText);
  if (message instanceof RegExp) expect(got).toMatch(message);
  else expect(got).toBe(message);
});

test('a missing file', () => {
  const dir = brokenCopy('alarms.yaml', 'channels:', 'channels:');
  fs.unlinkSync(path.join(dir, 'alarms.yaml'));
  expect(() => loadConfig({ configDir: dir, env: {} })).toThrow(/^alarms\.yaml: cannot read/);
});

test.each([['- a\n- b\n', 'list'], ['', 'NoneType']])('a YAML document that is not a mapping (%j)', (text, got) => {
  const dir = brokenCopy('alarms.yaml', 'channels:', 'channels:');
  fs.writeFileSync(path.join(dir, 'alarms.yaml'), text);
  expect(() => loadConfig({ configDir: dir, env: {} })).toThrow(`alarms.yaml: expected a mapping, got ${got}`);
});

test('telemetry.yaml assets must be a non-empty list of mappings with ids', () => {
  expect(rejection('telemetry.yaml', '  - id: motor-01\n', '  - id: ""\n')).toBe('telemetry.yaml.assets[0].id: expected a non-empty string');
  const dir = brokenCopy('telemetry.yaml', 'channels:', 'channels:');
  const text = fs.readFileSync(path.join(dir, 'telemetry.yaml'), 'utf8');
  fs.writeFileSync(path.join(dir, 'telemetry.yaml'), text.replace(/^assets:[\s\S]*?^faults:/m, 'assets: []\n\nfaults:'));
  expect(() => loadConfig({ configDir: dir, env: {} })).toThrow('telemetry.yaml.assets: expected a non-empty list');
});
