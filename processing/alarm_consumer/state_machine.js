'use strict';

/**
 * Alarm state machines: pure logic, no Kafka.
 *
 * Pattern: ANSI/ISA-18.2 style absolute high alarms. Each rule in
 * config/alarms.yaml, one (asset_id, channel), has two independent level
 * alarms, HI at the warning threshold and HIHI at the critical threshold, as
 * separate alarm points rather than one alarm changing severity. Each has its
 * own state and alarm_id, so a value of 100 degC raises WARNING and then
 * CRITICAL, and each clears at its own threshold.
 *
 * Per (asset_id, channel, severity), two states:
 *
 *   NORMAL  value >= threshold                -> raise counter + 1;
 *                                                at persistence_samples: ACTIVE, emit RAISED
 *           value <  threshold                -> raise counter = 0
 *   ACTIVE  value <  threshold - hysteresis   -> clear counter + 1;
 *                                                at persistence_samples: NORMAL, emit CLEARED
 *           value >= threshold - hysteresis   -> clear counter = 0
 *
 * Persistence (ISA-18.2 "on-delay"/"off-delay") and hysteresis (deadband)
 * are counted in samples and channel units, exactly as config/alarms.yaml
 * defines them. There is no time-based debounce and no periodic clean-up: an
 * alarm stays ACTIVE until its clear condition holds.
 *
 * "Consecutive" means consecutive samples of the asset. The run of samples
 * is broken by a gap in the asset's seq (a lost sample), by seq going back
 * (the simulator restarted) or by a null or non-finite value in the
 * channel. A break sets the pending raise and clear counters to zero; it
 * never changes an alarm's state, so an ACTIVE alarm stays ACTIVE.
 *
 * alarm_id is deterministic: asset_id/channel/severity/<ts of the RAISED
 * sample, epoch ms>. Processing the same samples again yields the same id,
 * which is what makes at-least-once delivery deduplicable downstream.
 *
 * Replay guard: each level alarm remembers the ts of its latest event, RAISED
 * or CLEARED, emitted here or restored from telemetry.alarms. A sample stamped
 * at or before that ts is already accounted for by the event, so it does not
 * count for that alarm: it changes neither the state nor the counters, and
 * the engine counts it as skipped. Without the guard, a replay of samples
 * that preceded the latest event (the consumer stopped after an alarm send
 * and before its offset commit, so the samples come again after the restore)
 * re-evaluates history against the restored state: a restored ACTIVE alarm
 * met earlier normal samples and emitted a CLEARED stamped before its own
 * raise, then the same RAISED again (Task 3.4, H3); a restored CLEARED alarm
 * met the high samples before its clear and was raised and cleared again
 * under a new alarm_id. The guard compares ts, not seq, because seq starts
 * again at 0 when the simulator restarts. Its cost: if a producer's clock
 * goes back, that alarm ignores the samples until the time passes its
 * latest event.
 */

const SEVERITIES = Object.freeze(['WARNING', 'CRITICAL']);

function alarmId(assetId, channel, severity, raisedTs) {
  return `${assetId}/${channel}/${severity}/${raisedTs}`;
}

class LevelAlarm {
  constructor(rule, severity) {
    this.assetId = rule.asset_id;
    this.channel = rule.channel;
    this.severity = severity;
    this.threshold = severity === 'CRITICAL' ? rule.critical : rule.warning;
    this.hysteresis = rule.hysteresis;
    this.persistence = rule.persistence_samples;
    this.state = 'NORMAL';
    this.alarmId = null;
    this.raiseCount = 0;
    this.clearCount = 0;
    this.latestEventTs = undefined; // ts of the latest RAISED or CLEARED (replay guard)
  }

  /** True when the sample is at or before this alarm's latest event: already accounted for. */
  accountedFor(sample) {
    return this.latestEventTs !== undefined && sample.ts <= this.latestEventTs;
  }

  resetCounters() {
    this.raiseCount = 0;
    this.clearCount = 0;
  }

  /** Feed one sample value; returns an alarm_v1 event on a transition, else null. */
  evaluate(value, sample) {
    if (this.state === 'NORMAL') {
      if (value >= this.threshold) {
        this.raiseCount += 1;
        if (this.raiseCount >= this.persistence) {
          this.state = 'ACTIVE';
          this.alarmId = alarmId(this.assetId, this.channel, this.severity, sample.ts);
          this.resetCounters();
          return this.event('RAISED', value, sample);
        }
      } else {
        this.raiseCount = 0;
      }
      return null;
    }
    if (value < this.threshold - this.hysteresis) {
      this.clearCount += 1;
      if (this.clearCount >= this.persistence) {
        const event = this.event('CLEARED', value, sample);
        this.state = 'NORMAL';
        this.alarmId = null;
        this.resetCounters();
        return event;
      }
    } else {
      this.clearCount = 0;
    }
    return null;
  }

  /**
   * Rebuild the state from this alarm's latest event in telemetry.alarms:
   * ACTIVE with its alarm_id after a RAISED, NORMAL after a CLEARED. Either
   * way the event's ts becomes the replay guard. An event older than one
   * already applied is ignored.
   */
  restoreFrom(ev) {
    if (this.latestEventTs !== undefined && ev.ts <= this.latestEventTs) return;
    this.state = ev.state === 'RAISED' ? 'ACTIVE' : 'NORMAL';
    this.alarmId = ev.state === 'RAISED' ? ev.alarm_id : null;
    this.latestEventTs = ev.ts;
    this.resetCounters();
  }

  event(state, value, sample) {
    this.latestEventTs = sample.ts;
    return {
      alarm_id: this.alarmId,
      asset_id: this.assetId,
      channel: this.channel,
      severity: this.severity,
      state,
      ts: sample.ts,
      value,
      threshold: this.threshold,
      hysteresis: this.hysteresis,
      persistence_samples: this.persistence,
      source_seq: sample.seq,
      synthetic: sample.synthetic,
    };
  }
}

/** All level alarms, grouped by asset, with per-asset seq tracking. */
class AlarmEngine {
  constructor(rules) {
    this.byAsset = new Map(); // asset_id -> { lastSeq, alarms: LevelAlarm[] }
    this.skipped = new Map(); // "asset_id/channel" -> samples skipped by the replay guard
    for (const rule of rules) {
      if (!this.byAsset.has(rule.asset_id)) this.byAsset.set(rule.asset_id, { lastSeq: undefined, alarms: [] });
      for (const severity of SEVERITIES) this.byAsset.get(rule.asset_id).alarms.push(new LevelAlarm(rule, severity));
    }
  }

  find(assetId, channel, severity) {
    const asset = this.byAsset.get(assetId);
    return asset ? asset.alarms.find((a) => a.channel === channel && a.severity === severity) : undefined;
  }

  /**
   * Restore every level alarm from its latest event, RAISED or CLEARED (see
   * LevelAlarm.restoreFrom). Returns the RAISED events that match no
   * configured rule (the configuration changed since); a CLEARED event
   * without a rule restores nothing and needs no warning.
   */
  restore(latestEvents) {
    const unmatched = [];
    for (const ev of latestEvents) {
      const alarm = this.find(ev.asset_id, ev.channel, ev.severity);
      if (alarm) alarm.restoreFrom(ev);
      else if (ev.state === 'RAISED') unmatched.push(ev);
    }
    return unmatched;
  }

  /**
   * Process one telemetry sample; returns the transition events, in rule
   * order. A sample for an asset with no rules returns [].
   */
  process(sample) {
    const asset = this.byAsset.get(sample.asset_id);
    if (!asset) return [];
    const continuous = asset.lastSeq === undefined || sample.seq === asset.lastSeq + 1;
    asset.lastSeq = sample.seq;
    const events = [];
    const skippedChannels = new Set();
    for (const alarm of asset.alarms) {
      // A break resets the counters even when the guard skips the sample: an
      // in-process replay (seq going back) must not resume a run counted
      // before it.
      if (!continuous) alarm.resetCounters();
      if (alarm.accountedFor(sample)) {
        skippedChannels.add(alarm.channel);
        continue;
      }
      const value = sample[alarm.channel];
      if (typeof value !== 'number' || !Number.isFinite(value)) {
        alarm.resetCounters();
        continue;
      }
      const event = alarm.evaluate(value, sample);
      if (event) events.push(event);
    }
    for (const channel of skippedChannels) {
      const key = `${sample.asset_id}/${channel}`;
      this.skipped.set(key, (this.skipped.get(key) || 0) + 1);
    }
    return events;
  }

  /** Samples skipped by the replay guard so far, per "asset_id/channel". */
  skippedByReplayGuard() {
    return Object.fromEntries(this.skipped);
  }

  active() {
    const out = [];
    for (const { alarms } of this.byAsset.values()) {
      for (const a of alarms) if (a.state === 'ACTIVE') out.push({ alarm_id: a.alarmId, asset_id: a.assetId, channel: a.channel, severity: a.severity });
    }
    return out;
  }
}

module.exports = { AlarmEngine, LevelAlarm, SEVERITIES, alarmId };
