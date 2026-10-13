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

  test('restoreFrom a RAISED marks the alarm active with its id; a CLEARED leaves it normal', () => {
    const alarm = new LevelAlarm(RULE, 'CRITICAL');
    alarm.restoreFrom({ alarm_id: 'x/1', state: 'RAISED', ts: 5000 });
    expect([alarm.state, alarm.alarmId, alarm.threshold, alarm.latestEventTs]).toEqual(['ACTIVE', 'x/1', 7.1, 5000]);
    alarm.restoreFrom({ alarm_id: 'x/1', state: 'CLEARED', ts: 9000 });
    expect([alarm.state, alarm.alarmId, alarm.latestEventTs]).toEqual(['NORMAL', null, 9000]);
  });

  test('restoreFrom ignores an event older than one already applied', () => {
    const alarm = new LevelAlarm(RULE, 'WARNING');
    alarm.restoreFrom({ alarm_id: 'x/9', state: 'CLEARED', ts: 9000 });
    alarm.restoreFrom({ alarm_id: 'x/5', state: 'RAISED', ts: 5000 });
    expect([alarm.state, alarm.alarmId, alarm.latestEventTs]).toEqual(['NORMAL', null, 9000]);
  });
});

describe('AlarmEngine', () => {
  test('ignores assets without rules', () => {
    expect(new AlarmEngine([RULE]).process({ asset_id: 'pump-09', seq: 0, ts: 1, vibration_rms_mm_s: 99 })).toEqual([]);
  });

  test('restore returns the RAISED events that match no rule', () => {
    const engine = new AlarmEngine([RULE]);
    const ev = (id, asset, channel, state = 'RAISED') => ({ alarm_id: id, asset_id: asset, channel, severity: 'WARNING', state, ts: 1 });
    const unmatched = engine.restore([
      ev('a', 'pump-01', 'vibration_rms_mm_s'),
      ev('b', 'pump-09', 'vibration_rms_mm_s'),
      ev('c', 'pump-01', 'oil'),
      ev('d', 'pump-09', 'oil', 'CLEARED'), // no rule, but nothing to restore: no warning
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

  // Each phase starts its seq at 0 (the simulator restarted) at a later time:
  // the clock moves on across a restart, and the replay guard relies on it.
  function run(engine, values, cycles, startMs = 1790889302000) {
    const events = [];
    for (let seq = 0; seq < cycles; seq += 1) {
      for (const [asset, vibration] of Object.entries(values)) {
        events.push(...engine.process({
          asset_id: asset, seq, ts: startMs + seq * 1000, synthetic: true,
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

    expect(run(restarted, FAULTED, 30, 1790889402000)).toEqual([]); // F2: seq starts again at 0, values stay high

    const f3 = run(restarted, NORMAL, 30, 1790889502000);
    expect(f3.every((e) => e.state === 'CLEARED' && e.source_seq === 4)).toBe(true);
    expect(f3.map((e) => e.alarm_id).sort()).toEqual(f1.map((e) => e.alarm_id).sort());
    expect(restarted.active()).toEqual([]);
  });
});

// The replay guard (H3 fix): a sample stamped at or before an alarm's latest
// event, RAISED or CLEARED, does not count for that alarm.
describe('replay guard', () => {
  const T = 1790889302000;
  const at = (seq, value, ts = T + seq * 1000) => ({ asset_id: 'pump-01', seq, ts, synthetic: true, vibration_rms_mm_s: value });
  const feedAll = (engine, samples) => samples.flatMap((s) => engine.process(s));
  const id = (severity, seq) => alarmId('pump-01', 'vibration_rms_mm_s', severity, T + seq * 1000);
  const restored = (state, severity, raisedSeq, eventSeq = raisedSeq) => ({
    alarm_id: id(severity, raisedSeq), asset_id: 'pump-01', channel: 'vibration_rms_mm_s', severity, state, ts: T + eventSeq * 1000,
  });
  const range = (from, to, value) => Array.from({ length: to - from }, (_, i) => at(from + i, value));

  test('a restored ACTIVE alarm ignores the samples before, and at, its raise (H3)', () => {
    const engine = new AlarmEngine([RULE]);
    engine.restore([restored('RAISED', 'WARNING', 9)]);
    // The replay: normal samples that preceded the raise, then the raising ones.
    expect(feedAll(engine, [...range(0, 5, 2.0), ...range(5, 10, 5.0)])).toEqual([]);
    expect(engine.active().map((a) => a.alarm_id)).toEqual([id('WARNING', 9)]);
    expect(engine.skippedByReplayGuard()).toEqual({ 'pump-01/vibration_rms_mm_s': 10 });
  });

  test('samples after the raise count: the real clear has the restored id and a later ts', () => {
    const engine = new AlarmEngine([RULE]);
    engine.restore([restored('RAISED', 'WARNING', 9)]);
    const events = feedAll(engine, [...range(0, 10, 2.0), ...range(10, 15, 2.0)]);
    expect(events.map((e) => [e.state, e.alarm_id, e.source_seq])).toEqual([['CLEARED', id('WARNING', 9), 14]]);
    expect(engine.skippedByReplayGuard()).toEqual({ 'pump-01/vibration_rms_mm_s': 10 });
  });

  test('a restored CLEARED alarm is not raised again by the replay of its high samples (phantom alarm)', () => {
    const engine = new AlarmEngine([RULE]);
    engine.restore([restored('CLEARED', 'WARNING', 4, 19)]);
    expect(feedAll(engine, [...range(10, 15, 5.0), ...range(15, 20, 2.0)])).toEqual([]);
    expect(engine.active()).toEqual([]);
    // After the CLEARED, samples count again: a new alarm, with a new id.
    expect(feedAll(engine, range(20, 25, 5.0)).map((e) => [e.state, e.alarm_id])).toEqual([['RAISED', id('WARNING', 24)]]);
  });

  test('HI and HIHI each have their own guard', () => {
    const engine = new AlarmEngine([RULE]);
    // WARNING active since seq 4; CRITICAL raised at seq 4 and cleared at seq 12.
    engine.restore([restored('RAISED', 'WARNING', 4), restored('CLEARED', 'CRITICAL', 4, 12)]);
    // Seq 0 to 12 are guarded for CRITICAL; for WARNING, seq 5 on count, and every value keeps it active.
    expect(feedAll(engine, [...range(0, 8, 7.5), ...range(8, 13, 6.0)])).toEqual([]);
    expect(feedAll(engine, range(13, 18, 7.5)).map((e) => [e.state, e.severity, e.source_seq])).toEqual([['RAISED', 'CRITICAL', 17]]);
    expect(engine.active().map((a) => a.severity)).toEqual(['WARNING', 'CRITICAL']);
  });

  test('a live transition sets the guard: an in-process replay emits nothing', () => {
    const engine = new AlarmEngine([RULE]);
    const samples = [...range(0, 5, 2.0), ...range(5, 10, 5.0)];
    expect(feedAll(engine, samples).map((e) => e.state)).toEqual(['RAISED']);
    feedAll(engine, range(10, 12, 2.0)); // two of the five clear samples, then the offsets go back
    expect(feedAll(engine, samples)).toEqual([]);
    // The seq going back reset the counters: the run before the replay is not resumed.
    expect(feedAll(engine, range(10, 14, 2.0))).toEqual([]);
    expect(feedAll(engine, [at(14, 2.0)]).map((e) => e.state)).toEqual(['CLEARED']);
  });

  test('a sample with the same ts as the latest event is skipped', () => {
    const engine = new AlarmEngine([RULE]);
    engine.restore([restored('RAISED', 'WARNING', 9)]);
    expect(feedAll(engine, [at(10, 2.0, T + 9000)])).toEqual([]);
    expect(engine.skippedByReplayGuard()).toEqual({ 'pump-01/vibration_rms_mm_s': 1 });
  });

  test('a ts going back before the latest event is ignored and counted, never cleared or raised (clock risk)', () => {
    const engine = new AlarmEngine([RULE]);
    expect(feedAll(engine, range(0, 5, 5.0)).map((e) => e.state)).toEqual(['RAISED']); // latest event at seq 4
    // The producer's clock goes back 60 s; seq carries on. These normal values would otherwise clear the alarm.
    const back = Array.from({ length: 10 }, (_, i) => at(5 + i, 2.0, T - 60000 + i * 1000));
    expect(feedAll(engine, back)).toEqual([]);
    expect(engine.active()).toHaveLength(1);
    expect(engine.skippedByReplayGuard()).toEqual({ 'pump-01/vibration_rms_mm_s': 10 });
    // Once the time passes the latest event, samples count again.
    const later = Array.from({ length: 5 }, (_, i) => at(15 + i, 2.0, T + 5000 + i * 1000));
    expect(feedAll(engine, later).map((e) => e.state)).toEqual(['CLEARED']);
  });

  test('a ts going back never raises a normal alarm either', () => {
    const engine = new AlarmEngine([RULE]);
    engine.restore([restored('CLEARED', 'WARNING', 4, 9)]);
    const back = Array.from({ length: 10 }, (_, i) => at(10 + i, 5.0, T - 60000 + i * 1000));
    expect(feedAll(engine, back)).toEqual([]);
    expect(engine.active()).toEqual([]);
  });

  test('a channel skipped by both severities counts once per sample', () => {
    const engine = new AlarmEngine([RULE]);
    engine.restore([restored('RAISED', 'WARNING', 9), restored('RAISED', 'CRITICAL', 9)]);
    feedAll(engine, range(0, 3, 8.0));
    expect(engine.skippedByReplayGuard()).toEqual({ 'pump-01/vibration_rms_mm_s': 3 });
  });
});
