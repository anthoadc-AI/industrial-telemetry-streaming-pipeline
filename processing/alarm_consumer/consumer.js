'use strict';

/**
 * Kafka side of the alarm consumer: start-up checks, state restore from
 * telemetry.alarms, and the telemetry.raw consume loop.
 *
 * Delivery semantics: at-least-once, in a strict order per message:
 *
 *   decode -> update the state machines -> await producer.send (acks -1,
 *   idempotent producer) for every transition -> handler returns
 *
 * kafkajs commits a message's offset only after its handler has returned, so
 * an offset is never committed before the alarms it caused are acknowledged
 * by Kafka. If a send fails after the producer's own retries, the handler
 * throws; consumer-level retries are disabled (re-running the handler would
 * meet state that has already moved on and lose the event), so the consumer
 * stops without committing that offset and the process exits 1. On restart
 * the state is rebuilt from telemetry.alarms and the sample is processed
 * again.
 *
 * Why not exactly-once (Kafka transactions): a transaction would make the
 * alarm events and the input offset atomic, but it cannot cover the
 * consumer's in-memory state, which is rebuilt from telemetry.alarms anyway.
 * Duplicates that at-least-once can produce after a crash carry the same
 * deterministic alarm_id and, on the compacted topic, the same key: any
 * reader deduplicates them by alarm_id, and compaction keeps one event per
 * key. Transactions would add a transactional producer, read_committed
 * readers and coordinator round-trips for no observable difference here.
 *
 * Dead letters: a message that can never be processed goes to telemetry.dlq
 * (dlq_v1, payload intact) before its offset is committed. Two kinds qualify:
 * a permanent decode failure (no Confluent wire-format header, a schema id
 * the registry says does not exist, bytes that do not decode) and a broken
 * contract after decoding (a required field missing or of the wrong type).
 * A transient Schema Registry failure (unreachable, timeout, any answer other
 * than "schema not found") is never dead-lettered: valid telemetry would land
 * in the DLQ. It stops the handler without committing, like a failed alarm
 * send, and the message is processed again after a restart.
 *
 * The dead-letter send itself is awaited and, if it fails, stops the consumer
 * without committing, exactly as an alarm send does. The bridge does the
 * opposite (logs the payload and carries on). The asymmetry follows the
 * source: this consumer reads Kafka and can re-read an uncommitted offset, so
 * stopping loses nothing; the bridge reads MQTT, which cannot redeliver.
 *
 * Latency fields (Phase 3, Task 3.1-A), on every alarm_raised and
 * alarm_cleared line, in milliseconds; every clock is on this host:
 *   consume_to_alarm_ack_ms  the handler received the telemetry message ->
 *                            Kafka acknowledged the alarm (await send
 *                            resolves on the broker's response); monotonic,
 *                            process.hrtime
 *   event_to_alarm_ack_ms    the sample's ts -> that acknowledgement, on the
 *                            high-resolution wall clock below
 *   alarm_ack_ts_ms          that acknowledgement as an absolute time, so a
 *                            benchmark can join it with its own send times;
 *                            milliseconds with a fraction, not an integer of
 *                            nanoseconds, which a double cannot hold exactly
 * Date.now() resolves 1 ms, too coarse for these. The wall clock used is
 * performance.timeOrigin + performance.now(): sub-millisecond, but fixed at
 * process start, so it does not follow later system clock adjustments;
 * clockReading() reports it beside Date.now() at start and at exit so the
 * divergence over a run is visible. The sample ts has millisecond resolution
 * and is truncated by the simulator: event_to_alarm_ack_ms overstates by 0 to
 * 1 ms.
 */

/** Wall-clock time in milliseconds with sub-millisecond resolution. */
function hrWallMs() {
  return performance.timeOrigin + performance.now();
}

/** Both wall clocks, for the run_started and run_finished lines. */
function clockReading() {
  const hr = hrWallMs();
  const wall = Date.now();
  return { date_now_ms: wall, hr_wall_ms: Math.round(hr * 1000) / 1000, hr_minus_date_now_ms: Math.round((hr - wall) * 1000) / 1000 };
}

function roundMs(value) {
  return Math.round(value * 1000) / 1000;
}

const { Kafka, logLevel, Partitioners, ConfigResourceTypes } = require('kafkajs');
const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry');
const jsonlog = require('../../common/jsonlog');

const KAFKAJS_LEVELS = { [logLevel.ERROR]: 'ERROR', [logLevel.WARN]: 'WARNING', [logLevel.INFO]: 'INFO', [logLevel.DEBUG]: 'DEBUG' };
const RESTORE_TIMEOUT_MS = 60000;
const DETAIL_MAX = 1000;

/** A Schema Registry failure that is not a verdict on the message: never dead-lettered. */
class TransientRegistryError extends Error {
  constructor(message) {
    super(message);
    this.name = 'TransientRegistryError';
  }
}

// Fields the state machines and alarm_v1 need, checked after decoding: the
// writer schema guarantees its own fields, not that it is telemetry_v1.
const REQUIRED = [
  ['asset_id', (v) => typeof v === 'string' && v !== ''],
  ['seq', (v) => Number.isInteger(v)],
  ['ts', (v) => typeof v === 'number' && Number.isFinite(v)],
  ['synthetic', (v) => typeof v === 'boolean'],
];

/** kafkajs logCreator writing through common/jsonlog.js, so stderr stays JSON lines. */
function kafkajsLogCreator() {
  const log = jsonlog.getLogger('kafkajs');
  return () => ({ namespace, level, log: entry }) => {
    const { message, timestamp, logger, ...fields } = entry; // timestamp and logger are ours to write
    log.log(KAFKAJS_LEVELS[level] || 'INFO', message, { namespace, ...fields });
  };
}

function createKafka({ brokers, clientId, debug }) {
  return new Kafka({
    clientId,
    brokers,
    logLevel: debug ? logLevel.INFO : logLevel.WARN,
    logCreator: kafkajsLogCreator(),
    retry: { retries: 5 },
  });
}

/**
 * Fail-fast checks: the three topics exist and telemetry.alarms is compacted
 * (the state restore depends on it). Throws with a reason on any failure.
 */
async function checkTopics(admin, { rawTopic, alarmsTopic, dlqTopic, alarmsConfigs }) {
  await admin.fetchTopicMetadata({ topics: [rawTopic, alarmsTopic, dlqTopic] });
  const { resources } = await admin.describeConfigs({
    resources: [{ type: ConfigResourceTypes.TOPIC, name: alarmsTopic }],
    includeSynonyms: false,
  });
  const actual = Object.fromEntries(resources[0].configEntries.map((c) => [c.configName, c.configValue]));
  const differing = Object.entries(alarmsConfigs).filter(([k, v]) => actual[k] !== v);
  if (differing.length) {
    throw new Error(`topic ${alarmsTopic} settings differ from config/topics.yaml: `
      + differing.map(([k, v]) => `${k}=${actual[k]} (expected ${v})`).join(', '));
  }
}

async function registerSchema(registry, { topic, schema }) {
  // updateConfig false: the global compatibility set by broker/kafka/provision.py
  // governs; no subject-level override is created.
  const { id } = await registry.register({ type: SchemaType.AVRO, schema },
    { subject: `${topic}-value`, updateConfig: false });
  return id;
}

/**
 * Read telemetry.alarms from the beginning up to the end offsets taken at
 * start, keep the latest event per alarm_id (the message key), and return
 * the events whose latest state is RAISED: the alarms that are active.
 */
async function readActiveAlarms({ kafka, admin, registry, alarmsTopic, log }) {
  const offsets = await admin.fetchTopicOffsets(alarmsTopic);
  const targets = new Map(offsets.filter((o) => Number(o.high) > Number(o.low)).map((o) => [o.partition, Number(o.high)]));
  const stats = { partitions: offsets.length, records_read: 0, alarms_seen: 0 };
  if (targets.size === 0) return { active: [], stats };

  const latest = new Map();
  const consumer = kafka.consumer({ groupId: `alarm-consumer-restore-${process.pid}-${Date.now()}` });
  await consumer.connect();
  try {
    await consumer.subscribe({ topic: alarmsTopic, fromBeginning: true });
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`state restore did not reach the end of ${alarmsTopic} within ${RESTORE_TIMEOUT_MS} ms`)), RESTORE_TIMEOUT_MS);
      const remaining = new Set(targets.keys());
      consumer.run({
        autoCommit: false,
        eachMessage: async ({ partition, message }) => {
          if (!remaining.has(partition)) return;
          stats.records_read += 1;
          if (message.value) { // a tombstone deletes the key
            const event = await registry.decode(message.value);
            latest.set(message.key.toString(), event);
          } else if (message.key) {
            latest.delete(message.key.toString());
          }
          if (Number(message.offset) + 1 >= targets.get(partition)) {
            remaining.delete(partition);
            if (remaining.size === 0) {
              clearTimeout(timer);
              resolve();
            }
          }
        },
      }).catch(reject);
    });
  } finally {
    await consumer.disconnect();
  }
  stats.alarms_seen = latest.size;
  const active = [...latest.values()].filter((e) => e.state === 'RAISED');
  log.debug('restore_read', stats);
  return { active, stats };
}

/**
 * Decode one telemetry.raw value. Returns { sample } for valid telemetry or
 * { deadLetter: { reason, detail, schemaId } } for a message that can never
 * be processed; throws TransientRegistryError when the registry cannot give a
 * verdict. Only an explicit HTTP 404 (schema id not found) from the registry
 * counts as permanent: the HTTP client reports network failures as responses
 * too, so no other status proves the message is at fault.
 *
 * Evidence (Task 2.3 verification, 2026-10-01): with the Schema Registry
 * container stopped, resolving a schema id failed with "Error, status 400".
 * Had any 4xx been treated as permanent, that valid telemetry record would
 * have been dead-lettered and its offset committed, losing it from the
 * pipeline. Classified as transient, the consumer stopped with the offset
 * uncommitted (lag 1), and a restart with the registry back processed it.
 */
async function decodeTelemetry(registry, value) {
  const deadLetter = (reason, detail, schemaId) => ({ deadLetter: { reason, detail, schemaId } });
  if (!value || value.length === 0) return deadLetter('undecodable', 'empty value', null);
  if (value.length < 5 || value[0] !== 0) {
    return deadLetter('undecodable', `no Confluent wire-format header (magic byte ${value[0]}, ${value.length} bytes)`, null);
  }
  const schemaId = value.readInt32BE(1);
  try {
    await registry.getSchema(schemaId);
  } catch (e) {
    if (e && e.status === 404) return deadLetter('undecodable', `schema id ${schemaId} not found in Schema Registry`, schemaId);
    // e may not be an Error: the client can reject with any value.
    const reason = e instanceof Error ? e.message : String(e);
    throw new TransientRegistryError(`Schema Registry unavailable resolving schema id ${schemaId}: ${reason}`);
  }
  let sample;
  try {
    sample = await registry.decode(value);
  } catch (e) {
    return deadLetter('undecodable', `Avro decode with schema id ${schemaId} failed: ${e.message}`, schemaId);
  }
  const missing = REQUIRED.filter(([field, ok]) => !ok(sample[field])).map(([field]) => field);
  if (missing.length) {
    return deadLetter('missing_field',
      `required field(s) missing or of the wrong type after decoding with schema id ${schemaId}: ${missing.join(', ')}`, schemaId);
  }
  return { sample };
}

/**
 * Consume telemetry.raw and produce alarm transitions. Returns the running
 * kafkajs consumer; onProcessed(count) is called after each processed message
 * so the caller can stop at --count.
 */
async function runConsumer({ kafka, registry, engine, groupId, rawTopic, alarmsTopic, alarmSchemaId, dlqTopic, dlqSchemaId,
  producer, counters, log, onProcessed }) {
  const consumer = kafka.consumer({
    groupId,
    // A failed handler is not retried by kafkajs (see the module comment);
    // the consumer stops instead.
    retry: { retries: 0, restartOnFailure: async () => false },
  });
  await consumer.connect();
  await consumer.subscribe({ topic: rawTopic, fromBeginning: true });
  await consumer.run({
    autoCommit: true,
    eachMessage: async ({ topic, partition, message }) => {
      const receivedHr = process.hrtime.bigint();
      let decoded;
      try {
        decoded = await decodeTelemetry(registry, message.value);
      } catch (e) {
        if (e instanceof TransientRegistryError) {
          // Stop without committing; the message is processed after a restart.
          log.error('schema_registry_unavailable', { topic, partition, offset: message.offset, error: e.message });
        }
        throw e;
      }
      if (decoded.deadLetter) {
        const { reason, detail, schemaId } = decoded.deadLetter;
        const record = {
          payload: message.value || Buffer.alloc(0),
          reason,
          detail: detail.slice(0, DETAIL_MAX),
          source: 'alarm_consumer',
          source_topic: topic,
          source_partition: partition,
          source_offset: Number(message.offset),
          source_key: message.key || null,
          schema_id: schemaId,
          ts: Date.now(),
        };
        // Key: the original Kafka key (the bridge's asset_id), which does not
        // depend on the payload; null when the message had none. The send is
        // awaited: a failure stops the consumer without committing (see above).
        const value = await registry.encode(dlqSchemaId, record);
        await producer.send({ topic: dlqTopic, acks: -1, messages: [{ key: message.key || null, value }] });
        counters.messages_dead_lettered += 1;
        log.warning('message_dead_lettered', {
          reason, detail: record.detail, source: 'alarm_consumer', key: message.key ? message.key.toString() : null,
          source_topic: topic, source_partition: partition, source_offset: message.offset, schema_id: schemaId,
          payload_bytes: record.payload.length,
        });
        counters.messages_processed += 1;
        onProcessed(counters.messages_processed);
        return;
      }
      const { sample } = decoded;
      if (!engine.byAsset.has(sample.asset_id)) counters.messages_without_rules += 1;
      for (const event of engine.process(sample)) {
        const value = await registry.encode(alarmSchemaId, event);
        await producer.send({ topic: alarmsTopic, acks: -1, messages: [{ key: event.alarm_id, value }] });
        const ackedHr = process.hrtime.bigint();
        const ackedWall = hrWallMs();
        counters[event.state === 'RAISED' ? 'alarms_raised' : 'alarms_cleared'] += 1;
        log.info(event.state === 'RAISED' ? 'alarm_raised' : 'alarm_cleared', {
          alarm_id: event.alarm_id, asset_id: event.asset_id, channel: event.channel, severity: event.severity,
          value: event.value, threshold: event.threshold, source_seq: event.source_seq, sample_ts: event.ts,
          source_partition: partition, source_offset: message.offset,
          consume_to_alarm_ack_ms: roundMs(Number(ackedHr - receivedHr) / 1e6),
          event_to_alarm_ack_ms: roundMs(ackedWall - Number(event.ts)),
          alarm_ack_ts_ms: roundMs(ackedWall),
        });
      }
      counters.messages_processed += 1;
      onProcessed(counters.messages_processed);
    },
  });
  return consumer;
}

/**
 * The asset a record belongs to: the first '/'-separated segment of its key.
 * An alarm_id is asset_id/channel/severity/<ts>; a dead letter's key is the
 * asset_id itself.
 */
function assetOfKey(key) {
  return String(key).split('/')[0];
}

/**
 * Partitioner for everything this consumer produces: the record keeps its key,
 * but the partition is chosen from the asset alone (Task 3.2, owner decision).
 *
 * Why key and partition use different fields:
 * - The key must be the alarm_id. telemetry.alarms is compacted on it, and
 *   the state restore relies on the latest event per alarm_id.
 * - The partition must be the asset. Kafka orders records only within a
 *   partition, and the ThingsBoard sink must see an asset's alarm events in
 *   the order they were produced: its alarm mapping applies an event only if
 *   it is newer than the last one applied for that asset and alarm type. With
 *   the partition chosen from the alarm_id (the default), consecutive alarms
 *   of one asset land on different partitions and a lagging or replaying
 *   reader can see a newer alarm before an older one, which it then drops
 *   (measured in Task 3.2: 19 of 48 alarms kept after a replay).
 * Any other producer writing to telemetry.alarms with the default partitioner
 * would break that order for the assets it writes; this function is the
 * contract, and docs/ARCHITECTURE.md (Task 3.4) records it.
 *
 * The partition is kafkajs's default (Java-compatible murmur2) of the asset,
 * so it matches what a Java or librdkafka murmur2 producer keyed by asset_id
 * would choose. A record without a key keeps the default behaviour.
 */
function AssetPartitioner() {
  const byDefault = Partitioners.DefaultPartitioner();
  return ({ topic, partitionMetadata, message }) => {
    if (message.key === null || message.key === undefined) {
      return byDefault({ topic, partitionMetadata, message });
    }
    return byDefault({ topic, partitionMetadata, message: { ...message, key: assetOfKey(message.key) } });
  };
}

function createProducer(kafka) {
  // kafkajs logs "Limiting retries for the idempotent producer may invalidate
  // EoS guarantees" because the client's retries are bounded (5). That is
  // intended: nothing here relies on producer exactly-once, and a send that
  // exhausts its retries must fail the handler so the consumer stops without
  // committing, instead of blocking it indefinitely.
  return kafka.producer({
    idempotent: true,
    maxInFlightRequests: 1,
    // Partition by asset, key by alarm_id: see AssetPartitioner.
    createPartitioner: AssetPartitioner,
  });
}

function createRegistry(host) {
  return new SchemaRegistry({ host });
}

module.exports = {
  createKafka, createProducer, createRegistry, checkTopics, registerSchema, readActiveAlarms, runConsumer,
  decodeTelemetry, TransientRegistryError, hrWallMs, clockReading, AssetPartitioner, assetOfKey,
};
