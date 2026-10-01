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

  /** Mark ACTIVE with a known alarm_id (state rebuilt from telemetry.alarms). */
  restoreActive(id) {
    this.state = 'ACTIVE';
    this.alarmId = id;
    this.resetCounters();
  }

  event(state, value, sample) {
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
   * Restore ACTIVE alarms from their latest RAISED events. Returns the events
   * that match no configured rule (the configuration changed since).
   */
  restore(activeEvents) {
    const unmatched = [];
    for (const ev of activeEvents) {
      const alarm = this.find(ev.asset_id, ev.channel, ev.severity);
      if (alarm) alarm.restoreActive(ev.alarm_id);
      else unmatched.push(ev);
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
    for (const alarm of asset.alarms) {
      if (!continuous) alarm.resetCounters();
      const value = sample[alarm.channel];
      if (typeof value !== 'number' || !Number.isFinite(value)) {
        alarm.resetCounters();
        continue;
      }
      const event = alarm.evaluate(value, sample);
      if (event) events.push(event);
    }
    return events;
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
