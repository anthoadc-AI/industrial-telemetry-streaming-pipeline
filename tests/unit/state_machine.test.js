'use strict';

// processing/alarm_consumer/state_machine.js: every transition rule, and the
// Task 2.2 F1-F3 verification replayed in memory.

const { AlarmEngine, LevelAlarm, alarmId } = require('../../processing/alarm_consumer/state_machine');
const { loadConfig } = require('../../processing/alarm_consumer/config_loader');

const RULE = {
  asset_id: 'pump-01', channel: 'vibration_rms_mm_s', direction: 'high',
  warning: 4.5, critical: 7.1, hysteresis: 0.3, persistence_samples: 5,
};

function feeder(engine, assetId = 'pump-01', channel = 'vibration_rms_mm_s') {
  let seq = 0;
  let ts = 1000;
  return (value, { gap = false, seqOverride } = {}) => {
    if (gap) seq += 1;
    const sample = { asset_id: assetId, seq: seqOverride ?? seq, ts: (ts += 1000), synthetic: true, [channel]: value };
    seq = sample.seq + 1;
    return engine.process(sample);
  };
}

const states = (events) => events.map((e) => `${e.state} ${e.severity}`);

describe('LevelAlarm', () => {
  test('raises after persistence consecutive samples at or above the threshold', () => {
    const feed = feeder(new AlarmEngine([RULE]));
    for (let i = 0; i < 4; i += 1) expect(feed(4.5)).toEqual([]);
    const [event] = feed(4.6);
    expect(event).toMatchObject({
      state: 'RAISED', severity: 'WARNING', value: 4.6, threshold: 4.5, hysteresis: 0.3,
      persistence_samples: 5, source_seq: 4, synthetic: true, asset_id: 'pump-01', channel: 'vibration_rms_mm_s',
    });
    expect(event.alarm_id).toBe(alarmId('pump-01', 'vibration_rms_mm_s', 'WARNING', event.ts));
  });

  test('a value below the threshold resets the raise counter', () => {
    const feed = feeder(new AlarmEngine([RULE]));
    [4.6, 4.6, 4.6, 4.6, 4.4].forEach((v) => expect(feed(v)).toEqual([]));
    [4.6, 4.6, 4.6, 4.6].forEach((v) => expect(feed(v)).toEqual([]));
    expect(states(feed(4.6))).toEqual(['RAISED WARNING']);
  });

  test('clears only below threshold minus hysteresis, after persistence samples', () => {
    const engine = new AlarmEngine([RULE]);
    const feed = feeder(engine);
    for (let i = 0; i < 5; i += 1) feed(5.0);
    [4.3, 4.25, 4.21, 4.4].forEach((v) => expect(feed(v)).toEqual([])); // 4.2 <= v: in the band
    [4.1, 4.1, 4.1, 4.1].forEach((v) => expect(feed(v)).toEqual([]));
    const [cleared] = feed(4.1);
    expect(cleared.state).toBe('CLEARED');
    expect(engine.active()).toEqual([]);
  });

  test('a value back in the band resets the clear counter', () => {
    const feed = feeder(new AlarmEngine([RULE]));
    for (let i = 0; i < 5; i += 1) feed(5.0);
    [4.0, 4.0, 4.0, 4.0, 4.3].forEach((v) => expect(feed(v)).toEqual([]));
    [4.0, 4.0, 4.0, 4.0].forEach((v) => expect(feed(v)).toEqual([]));
    expect(states(feed(4.0))).toEqual(['CLEARED WARNING']);
  });

  test('HI and HIHI are independent alarms with their own ids and clear thresholds', () => {
    const engine = new AlarmEngine([RULE]);
    const feed = feeder(engine);
    const raised = [];
    for (let i = 0; i < 5; i += 1) raised.push(...feed(7.5));
    expect(states(raised)).toEqual(['RAISED WARNING', 'RAISED CRITICAL']);
    expect(new Set(raised.map((e) => e.alarm_id)).size).toBe(2);
    const down = [];
    for (let i = 0; i < 5; i += 1) down.push(...feed(6.0)); // below 6.8, above 4.2
    expect(states(down)).toEqual(['CLEARED CRITICAL']);
    expect(engine.active().map((a) => a.severity)).toEqual(['WARNING']);
  });

  test.each([
    ['a seq gap', (feed) => feed(4.6, { gap: true })],
    ['a seq rollback', (feed) => feed(4.6, { seqOverride: 0 })],
  ])('%s breaks consecutiveness without changing state', (_, breakIt) => {
    const engine = new AlarmEngine([RULE]);
    const feed = feeder(engine);
    [4.6, 4.6, 4.6, 4.6].forEach((v) => feed(v));
    expect(breakIt(feed)).toEqual([]); // counter restarts at 1 with this sample
    [4.6, 4.6, 4.6].forEach((v) => expect(feed(v)).toEqual([]));
    expect(states(feed(4.6))).toEqual(['RAISED WARNING']);
  });

  test.each([[null], [undefined], [Number.NaN], [Number.POSITIVE_INFINITY]])('a %p value resets the counters', (bad) => {
    const feed = feeder(new AlarmEngine([RULE]));
    [4.6, 4.6, 4.6, 4.6].forEach((v) => feed(v));
    expect(feed(bad)).toEqual([]);
    [4.6, 4.6, 4.6, 4.6].forEach((v) => expect(feed(v)).toEqual([]));
    expect(states(feed(4.6))).toEqual(['RAISED WARNING']);
  });

  test('a break never clears an active alarm', () => {
    const engine = new AlarmEngine([RULE]);
    const feed = feeder(engine);
    for (let i = 0; i < 5; i += 1) feed(5.0);
    feed(null);
    feed(5.0, { seqOverride: 0 });
    expect(engine.active()).toHaveLength(1);
  });

  test('restoreActive marks the alarm active with the given id', () => {
    const alarm = new LevelAlarm(RULE, 'CRITICAL');
    alarm.restoreActive('x/1');
    expect([alarm.state, alarm.alarmId, alarm.threshold]).toEqual(['ACTIVE', 'x/1', 7.1]);
  });
});

describe('AlarmEngine', () => {
  test('ignores assets without rules', () => {
    expect(new AlarmEngine([RULE]).process({ asset_id: 'pump-09', seq: 0, ts: 1, vibration_rms_mm_s: 99 })).toEqual([]);
  });

  test('restore returns events that match no rule', () => {
    const engine = new AlarmEngine([RULE]);
    const unmatched = engine.restore([
      { alarm_id: 'a', asset_id: 'pump-01', channel: 'vibration_rms_mm_s', severity: 'WARNING' },
      { alarm_id: 'b', asset_id: 'pump-09', channel: 'vibration_rms_mm_s', severity: 'WARNING' },
      { alarm_id: 'c', asset_id: 'pump-01', channel: 'oil', severity: 'WARNING' },
    ]);
    expect(unmatched.map((e) => e.alarm_id)).toEqual(['b', 'c']);
    expect(engine.active()).toEqual([{ alarm_id: 'a', asset_id: 'pump-01', channel: 'vibration_rms_mm_s', severity: 'WARNING' }]);
  });

  test('the real configuration yields 30 level alarms', () => {
    const engine = new AlarmEngine(loadConfig({ env: {} }).rules);
    const count = [...engine.byAsset.values()].reduce((n, a) => n + a.alarms.length, 0);
    expect(count).toBe(30);
  });
});

// The Task 2.2 verification that approved the state machine (2026-10-01),
// replayed in memory with the real rules: F1 raises, a restart restores from
// the emitted events, F2 keeps values high and must emit nothing, F3 returns
// to normal and must clear with the same alarm_ids.
describe('Task 2.2 F1-F3 replay', () => {
  const NORMAL = {
    'motor-01': 1.8, 'motor-02': 1.4, 'pump-01': 2.6, 'pump-02': 2.6, 'compressor-01': 3.2,
  };
  const FAULTED = { ...NORMAL, 'pump-01': 6.5, 'compressor-01': 7.25 }; // imbalance, measured levels

  function run(engine, values, cycles) {
    const events = [];
    for (let seq = 0; seq < cycles; seq += 1) {
      for (const [asset, vibration] of Object.entries(values)) {
        events.push(...engine.process({
          asset_id: asset, seq, ts: 1790889302000 + seq * 1000, synthetic: true,
          bearing_temperature_c: 55, motor_current_a: 20, speed_rpm: 1480, vibration_rms_mm_s: vibration,
        }));
      }
    }
    return events;
  }

  // What the compacted telemetry.alarms holds: the latest event per alarm_id.
  function latestActive(events) {
    const latest = new Map();
    for (const e of events) latest.set(e.alarm_id, e);
    return [...latest.values()].filter((e) => e.state === 'RAISED');
  }

  test('raise, restore, no re-emission, clear with the same ids', () => {
    const { rules } = loadConfig({ env: {} });

    const first = new AlarmEngine(rules);
    const f1 = run(first, FAULTED, 30);
    expect(f1.map((e) => [e.state, e.asset_id, e.severity, e.source_seq])).toEqual([
      ['RAISED', 'pump-01', 'WARNING', 4],
      ['RAISED', 'compressor-01', 'WARNING', 4],
      ['RAISED', 'compressor-01', 'CRITICAL', 4],
    ]);

    const restarted = new AlarmEngine(rules);
    expect(restarted.restore(latestActive(f1))).toEqual([]);
    expect(restarted.active().map((a) => a.alarm_id).sort()).toEqual(f1.map((e) => e.alarm_id).sort());

    expect(run(restarted, FAULTED, 30)).toEqual([]); // F2: seq starts again at 0, values stay high

    const f3 = run(restarted, NORMAL, 30);
    expect(f3.every((e) => e.state === 'CLEARED' && e.source_seq === 4)).toBe(true);
    expect(f3.map((e) => e.alarm_id).sort()).toEqual(f1.map((e) => e.alarm_id).sort());
    expect(restarted.active()).toEqual([]);
  });
});
