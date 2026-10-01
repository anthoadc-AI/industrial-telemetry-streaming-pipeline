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
 */

const { Kafka, logLevel, Partitioners, ConfigResourceTypes } = require('kafkajs');
const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry');
const jsonlog = require('../../common/jsonlog');

const KAFKAJS_LEVELS = { [logLevel.ERROR]: 'ERROR', [logLevel.WARN]: 'WARNING', [logLevel.INFO]: 'INFO', [logLevel.DEBUG]: 'DEBUG' };
const RESTORE_TIMEOUT_MS = 60000;

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
 * Fail-fast checks: both topics exist and telemetry.alarms is compacted
 * (the state restore depends on it). Throws with a reason on any failure.
 */
async function checkTopics(admin, { rawTopic, alarmsTopic, alarmsConfigs }) {
  await admin.fetchTopicMetadata({ topics: [rawTopic, alarmsTopic] });
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

async function registerAlarmSchema(registry, { alarmsTopic, alarmSchema }) {
  // updateConfig false: the global compatibility set by broker/kafka/provision.py
  // governs; no subject-level override is created.
  const { id } = await registry.register({ type: SchemaType.AVRO, schema: alarmSchema },
    { subject: `${alarmsTopic}-value`, updateConfig: false });
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
 * Consume telemetry.raw and produce alarm transitions. Returns the running
 * kafkajs consumer; onProcessed(count) is called after each processed message
 * so the caller can stop at --count.
 */
async function runConsumer({ kafka, registry, engine, groupId, rawTopic, alarmsTopic, alarmSchemaId, producer, counters, log, onProcessed }) {
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
      let sample;
      try {
        sample = await registry.decode(message.value);
      } catch (e) {
        counters.messages_undecodable += 1;
        log.warning('message_undecodable', { topic, partition, offset: message.offset, error: e.message });
        // TODO(task-2.3): produce the raw bytes, base64, and the reason to telemetry.dlq.
        counters.messages_processed += 1;
        onProcessed(counters.messages_processed);
        return;
      }
      if (!engine.byAsset.has(sample.asset_id)) counters.messages_without_rules += 1;
      for (const event of engine.process(sample)) {
        const value = await registry.encode(alarmSchemaId, event);
        await producer.send({ topic: alarmsTopic, acks: -1, messages: [{ key: event.alarm_id, value }] });
        counters[event.state === 'RAISED' ? 'alarms_raised' : 'alarms_cleared'] += 1;
        log.info(event.state === 'RAISED' ? 'alarm_raised' : 'alarm_cleared', {
          alarm_id: event.alarm_id, asset_id: event.asset_id, channel: event.channel, severity: event.severity,
          value: event.value, threshold: event.threshold, source_seq: event.source_seq, sample_ts: event.ts,
          source_partition: partition, source_offset: message.offset,
        });
      }
      counters.messages_processed += 1;
      onProcessed(counters.messages_processed);
    },
  });
  return consumer;
}

function createProducer(kafka) {
  return kafka.producer({
    idempotent: true,
    maxInFlightRequests: 1,
    // kafkajs 2.x warns when the default partitioner is implicit; it is the
    // Java-compatible murmur2 partitioner, chosen explicitly here.
    createPartitioner: Partitioners.DefaultPartitioner,
  });
}

function createRegistry(host) {
  return new SchemaRegistry({ host });
}

module.exports = { createKafka, createProducer, createRegistry, checkTopics, registerAlarmSchema, readActiveAlarms, runConsumer };
