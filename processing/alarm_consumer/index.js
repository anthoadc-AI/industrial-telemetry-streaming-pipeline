'use strict';

/**
 * Alarm consumer: telemetry.raw (Avro) -> alarm state machines -> telemetry.alarms (Avro).
 *
 *   node processing/alarm_consumer/index.js --count 500
 *
 * Start-up, failing fast with exit 1 at the first problem:
 *   1. load config/ (rules, topics, alarm and dead-letter schemas);
 *   2. connect to Kafka; telemetry.raw, telemetry.alarms and telemetry.dlq
 *      must exist, and telemetry.alarms must carry the settings in
 *      config/topics.yaml (cleanup.policy=compact);
 *   3. register alarm_v1 and dlq_v1 in Schema Registry;
 *   4. rebuild the active alarms from telemetry.alarms (latest event per
 *      alarm_id), so a restart neither re-raises an active alarm nor
 *      orphans it;
 *   5. consume telemetry.raw in consumer group --group-id.
 *
 * Stopping: after --count processed telemetry messages, after --duration
 * seconds, or on SIGINT, SIGTERM or (Windows) SIGBREAK. The consumer then
 * stops, commits the offsets of the messages it has finished, and exits 0.
 * A failed alarm or dead-letter send, or a Schema Registry that cannot be
 * reached while decoding, exits 1 without committing that message (see
 * consumer.js). Invalid arguments exit 2.
 *
 * Logging: JSON lines on stderr through common/jsonlog.js, including
 * kafkajs's own logs and Node.js process warnings.
 */

const { parseArgs } = require('node:util');
const jsonlog = require('../../common/jsonlog');
const { loadConfig, ConfigError } = require('./config_loader');
const { AlarmEngine } = require('./state_machine');
const kafkaSide = require('./consumer');

const log = jsonlog.getLogger('processing.alarm_consumer');

const USAGE = 'node processing/alarm_consumer/index.js [--count N] [--duration S] [--bootstrap-servers HOST:PORT] '
  + '[--schema-registry-url URL] [--group-id ID] [--log-level DEBUG|INFO|WARNING|ERROR]';

function parseCli(argv) {
  const { values } = parseArgs({
    args: argv,
    options: {
      count: { type: 'string' },
      duration: { type: 'string' },
      'bootstrap-servers': { type: 'string', default: process.env.KAFKA_BOOTSTRAP || 'localhost:9092' },
      'schema-registry-url': { type: 'string', default: process.env.SCHEMA_REGISTRY_URL || 'http://localhost:8081' },
      'group-id': { type: 'string', default: 'alarm-consumer' },
      'log-level': { type: 'string', default: 'INFO' },
    },
    strict: true,
  });
  const positive = (name, text, integer) => {
    if (text === undefined) return undefined;
    const n = Number(text);
    if (!Number.isFinite(n) || n <= 0 || (integer && !Number.isInteger(n))) {
      throw new Error(`--${name} must be a positive ${integer ? 'integer' : 'number'}`);
    }
    return n;
  };
  if (!['DEBUG', 'INFO', 'WARNING', 'ERROR'].includes(values['log-level'].toUpperCase())) {
    throw new Error('--log-level must be one of DEBUG, INFO, WARNING, ERROR');
  }
  return {
    count: positive('count', values.count, true),
    duration: positive('duration', values.duration, false),
    brokers: values['bootstrap-servers'].split(','),
    registryUrl: values['schema-registry-url'],
    groupId: values['group-id'],
    logLevel: values['log-level'].toUpperCase(),
  };
}

// Orchestration entry point: wires real Kafka and Schema Registry clients and
// runs until stopped. Exercised by the integration tests (Task 2.5), not by
// unit tests; excluded from unit coverage by owner decision (Task 2.4), the
// only exclusion in this file. parseCli above is unit-tested.
/* c8 ignore start */
async function main() {
  jsonlog.configure({ level: 'INFO' });
  // Node.js prints process warnings to stderr in its own format; route them
  // through the JSON logger instead.
  process.removeAllListeners('warning');
  process.on('warning', (w) => log.warning('node_warning', { name: w.name, message: w.message, code: w.code }));

  let args;
  try {
    args = parseCli(process.argv.slice(2));
  } catch (e) {
    log.error('argument_invalid', { error: e.message, usage: USAGE });
    return 2;
  }
  jsonlog.configure({ level: args.logLevel });

  let config;
  try {
    config = loadConfig();
  } catch (e) {
    if (!(e instanceof ConfigError)) throw e;
    log.error('config_invalid', { error: e.message });
    return 1;
  }
  const rawTopic = config.topics.telemetry_raw.name;
  const alarmsTopic = config.topics.telemetry_alarms.name;
  const dlqTopic = config.topics.telemetry_dlq.name;
  const engine = new AlarmEngine(config.rules);

  const kafka = kafkaSide.createKafka({ brokers: args.brokers, clientId: `alarm-consumer-${process.pid}`, debug: args.logLevel === 'DEBUG' });
  const admin = kafka.admin();
  const producer = kafkaSide.createProducer(kafka);
  const registry = kafkaSide.createRegistry(args.registryUrl);
  const counters = {
    messages_processed: 0, messages_dead_lettered: 0, messages_without_rules: 0,
    alarms_restored: 0, alarms_raised: 0, alarms_cleared: 0,
  };

  let consumer = null;
  let stopping = null;
  let durationTimer = null;
  const stop = (reason, code) => {
    if (!stopping) {
      stopping = (async () => {
        clearTimeout(durationTimer);
        for (const client of [consumer, producer, admin]) {
          if (client) await client.disconnect().catch((e) => log.warning('disconnect_failed', { error: e.message }));
        }
        const summary = { reason, ...counters, active_at_exit: engine.active().length, clocks: kafkaSide.clockReading() };
        if (code === 0) log.info('run_finished', summary);
        else log.error('run_failed', summary);
        return code;
      })();
    }
    return stopping;
  };
  for (const name of ['SIGINT', 'SIGTERM', 'SIGBREAK']) {
    process.on(name, () => {
      log.info('signal_received', { signal: name });
      stop('signal', 0).then((code) => process.exit(code));
    });
  }

  // Start-up, fail fast.
  try {
    await admin.connect();
    await kafkaSide.checkTopics(admin, { rawTopic, alarmsTopic, dlqTopic, alarmsConfigs: config.topics.telemetry_alarms.configs });
    log.info('kafka_ready', { brokers: args.brokers, raw_topic: rawTopic, alarms_topic: alarmsTopic, dlq_topic: dlqTopic });
  } catch (e) {
    log.error('kafka_not_ready', { brokers: args.brokers, error: e.message });
    return stop('startup_failed', 1);
  }
  let alarmSchemaId;
  let dlqSchemaId;
  try {
    alarmSchemaId = await kafkaSide.registerSchema(registry, { topic: alarmsTopic, schema: config.alarmSchema });
    log.info('schema_registered', { subject: `${alarmsTopic}-value`, schema_id: alarmSchemaId });
    dlqSchemaId = await kafkaSide.registerSchema(registry, { topic: dlqTopic, schema: config.dlqSchema });
    log.info('schema_registered', { subject: `${dlqTopic}-value`, schema_id: dlqSchemaId });
  } catch (e) {
    log.error('schema_registration_failed', { url: args.registryUrl, error: e.message });
    return stop('startup_failed', 1);
  }
  try {
    await producer.connect();
    const { active, stats } = await kafkaSide.readActiveAlarms({ kafka, admin, registry, alarmsTopic, log });
    const unmatched = engine.restore(active);
    counters.alarms_restored = active.length - unmatched.length;
    log.info('alarms_restored', { ...stats, active: counters.alarms_restored, alarm_ids: engine.active().map((a) => a.alarm_id) });
    for (const ev of unmatched) log.warning('restored_alarm_without_rule', { alarm_id: ev.alarm_id });
  } catch (e) {
    log.error('state_restore_failed', { error: e.message });
    return stop('startup_failed', 1);
  }
  if (stopping) return stopping; // a signal arrived during start-up

  try {
    consumer = await kafkaSide.runConsumer({
      kafka, registry, engine, groupId: args.groupId, rawTopic, alarmsTopic, alarmSchemaId, dlqTopic, dlqSchemaId,
      producer, counters, log,
      onProcessed: (n) => {
        if (args.count !== undefined && n >= args.count && !stopping) {
          consumer.pause([{ topic: rawTopic }]);
          setImmediate(() => stop('count', 0).then((code) => process.exit(code)));
        }
      },
    });
  } catch (e) {
    log.error('consumer_start_failed', { error: e.message });
    return stop('startup_failed', 1);
  }
  consumer.on(consumer.events.CRASH, ({ payload }) => {
    log.error('consumer_crashed', { error: payload.error && payload.error.message, restart: payload.restart });
    if (!payload.restart) stop('crash', 1).then((code) => process.exit(code));
  });
  log.info('run_started', {
    group_id: args.groupId, raw_topic: rawTopic, alarms_topic: alarmsTopic, dlq_topic: dlqTopic, rules: config.rules.length,
    level_alarms: config.rules.length * 2, count: args.count ?? null, duration_s: args.duration ?? null,
    clocks: kafkaSide.clockReading(),
  });
  if (args.duration !== undefined) {
    durationTimer = setTimeout(() => stop('duration', 0).then((code) => process.exit(code)), args.duration * 1000);
  }
  return null; // keep running; a stop path exits the process
}

if (require.main === module) {
  main().then((code) => {
    if (code !== null && code !== undefined) process.exit(code);
  }).catch((e) => {
    log.exception('unexpected_error', e);
    process.exit(1);
  });
}
/* c8 ignore stop */

module.exports = { parseCli, USAGE };
