'use strict';

// processing/alarm_consumer/consumer.js with Kafka, the producer and Schema
// Registry faked. The kafkajs eachMessage handler is captured and called
// directly, so the per-message order (decode -> state -> await send) and every
// failure path can be checked without a broker. That kafkajs then leaves a
// throwing handler's offset uncommitted is the library's behaviour, covered by
// the integration tests.

const consumerModule = require('../../processing/alarm_consumer/consumer');
const { AlarmEngine } = require('../../processing/alarm_consumer/state_machine');
const jsonlog = require('../../common/jsonlog');

const {
  decodeTelemetry, runConsumer, readActiveAlarms, checkTopics, registerSchema,
  createKafka, createProducer, createRegistry, TransientRegistryError,
} = consumerModule;

const RULE = {
  asset_id: 'pump-01', channel: 'vibration_rms_mm_s', direction: 'high',
  warning: 4.5, critical: 7.1, hysteresis: 0.3, persistence_samples: 5,
};

// Confluent framing (magic byte, 4-byte schema id) around a JSON body: enough
// for the classification logic, which only reads the header itself.
function framed(schemaId, body) {
  const header = Buffer.alloc(5);
  header.writeInt32BE(schemaId, 1);
  return Buffer.concat([header, Buffer.from(JSON.stringify(body))]);
}

function fakeRegistry({ known = [3], getSchemaError = null, decodeError = null } = {}) {
  return {
    getSchema: jest.fn(async (id) => {
      if (getSchemaError) throw getSchemaError;
      if (!known.includes(id)) throw Object.assign(new Error('Schema not found'), { status: 404 });
      return {};
    }),
    decode: jest.fn(async (buffer) => {
      if (decodeError) throw decodeError;
      return JSON.parse(buffer.subarray(5).toString());
    }),
    encode: jest.fn(async (id, payload) => Buffer.from(JSON.stringify({ id, payload }))),
    register: jest.fn(async () => ({ id: 9 })),
  };
}

function sample(seq, vibration, extra = {}) {
  return { asset_id: 'pump-01', seq, ts: 1790889302000 + seq * 1000, synthetic: true, vibration_rms_mm_s: vibration, ...extra };
}

function captureLogs() {
  const lines = [];
  jsonlog.configure({ level: 'DEBUG', stream: { write: (s) => lines.push(JSON.parse(s)) } });
  return lines;
}

// Logs are discarded unless a test captures them.
beforeEach(() => jsonlog.configure({ level: 'DEBUG', stream: { write: () => {} } }));
afterEach(() => jsonlog.configure({ level: 'INFO', stream: process.stderr }));

describe('decodeTelemetry', () => {
  test('valid telemetry', async () => {
    expect(await decodeTelemetry(fakeRegistry(), framed(3, sample(0, 2.6)))).toEqual({ sample: sample(0, 2.6) });
  });

  test.each([
    ['an empty value', null, 'empty value', null],
    ['no wire-format header', Buffer.from('\u0001not avro'), 'no Confluent wire-format header (magic byte 1, 9 bytes)', null],
    ['a header shorter than 5 bytes', Buffer.from([0, 0]), 'no Confluent wire-format header (magic byte 0, 2 bytes)', null],
    ['a schema id the registry does not know', framed(77, {}), 'schema id 77 not found in Schema Registry', 77],
  ])('%s is a permanent failure', async (_, value, detail, schemaId) => {
    expect(await decodeTelemetry(fakeRegistry(), value)).toEqual({ deadLetter: { reason: 'undecodable', detail, schemaId } });
  });

  test('bytes that do not decode are a permanent failure', async () => {
    const result = await decodeTelemetry(fakeRegistry({ decodeError: new Error('truncated buffer') }), framed(3, {}));
    expect(result.deadLetter).toEqual({ reason: 'undecodable', detail: 'Avro decode with schema id 3 failed: truncated buffer', schemaId: 3 });
  });

  test('a missing or mistyped required field is missing_field', async () => {
    const result = await decodeTelemetry(fakeRegistry(), framed(3, { asset_id: '', seq: '1', ts: 1, synthetic: 'yes' }));
    expect(result.deadLetter.reason).toBe('missing_field');
    expect(result.deadLetter.detail).toMatch(/: asset_id, seq, synthetic$/);
  });

  test.each([400, 500, undefined])('a registry failure with status %p is transient, never a dead letter', async (status) => {
    const registry = fakeRegistry({ getSchemaError: Object.assign(new Error('Error, status 400'), { status }) });
    await expect(decodeTelemetry(registry, framed(3, sample(0, 1)))).rejects.toBeInstanceOf(TransientRegistryError);
  });
});

function fakeKafka() {
  const captured = {};
  const consumer = {
    connect: jest.fn(async () => {}),
    subscribe: jest.fn(async () => {}),
    run: jest.fn(async ({ eachMessage }) => { captured.eachMessage = eachMessage; }),
    disconnect: jest.fn(async () => {}),
  };
  return { kafka: { consumer: jest.fn(() => consumer) }, consumer, captured };
}

async function startConsumer({ registry = fakeRegistry(), producer, engine = new AlarmEngine([RULE]) } = {}) {
  const { kafka, captured } = fakeKafka();
  const counters = { messages_processed: 0, messages_dead_lettered: 0, messages_without_rules: 0, alarms_raised: 0, alarms_cleared: 0 };
  const processed = [];
  const sends = producer || { send: jest.fn(async () => {}) };
  await runConsumer({
    kafka, registry, engine, groupId: 'g', rawTopic: 'telemetry.raw', alarmsTopic: 'telemetry.alarms', alarmSchemaId: 1,
    dlqTopic: 'telemetry.dlq', dlqSchemaId: 2, producer: sends, counters, log: jsonlog.getLogger('test'),
    onProcessed: (n) => processed.push(n),
  });
  const deliver = (offset, value, key = Buffer.from('pump-01')) => captured.eachMessage({
    topic: 'telemetry.raw', partition: 0, message: { offset: String(offset), key, value },
  });
  return { deliver, counters, processed, producer: sends, kafka };
}

describe('runConsumer handler', () => {
  test('consumer is created without kafkajs retries and subscribes from the beginning', async () => {
    const { kafka } = fakeKafka();
    await runConsumer({ kafka, registry: fakeRegistry(), engine: new AlarmEngine([RULE]), groupId: 'g', rawTopic: 't', counters: {}, log: jsonlog.getLogger('t'), onProcessed: () => {} });
    const options = kafka.consumer.mock.calls[0][0];
    expect(options.retry.retries).toBe(0);
    await expect(options.retry.restartOnFailure()).resolves.toBe(false);
  });

  test('an alarm is sent, keyed by alarm_id, before the message counts as processed', async () => {
    const lines = captureLogs();
    const order = [];
    const producer = { send: jest.fn(async () => { order.push('send'); }) };
    const c = await startConsumer({ producer });
    for (let seq = 0; seq < 5; seq += 1) await c.deliver(seq, framed(3, sample(seq, 5.0)));
    expect(producer.send).toHaveBeenCalledTimes(1);
    const [{ topic, acks, messages }] = producer.send.mock.calls[0];
    expect([topic, acks, messages[0].key]).toEqual(['telemetry.alarms', -1, 'pump-01/vibration_rms_mm_s/WARNING/1790889306000']);
    expect(JSON.parse(messages[0].value).payload.state).toBe('RAISED');
    expect(c.processed).toEqual([1, 2, 3, 4, 5]);
    expect(c.counters.alarms_raised).toBe(1);
    expect(lines.find((l) => l.event === 'alarm_raised')).toMatchObject({ source_offset: '4', severity: 'WARNING' });
    expect(order).toEqual(['send']);
  });

  test('a cleared alarm is counted', async () => {
    const c = await startConsumer();
    for (let seq = 0; seq < 5; seq += 1) await c.deliver(seq, framed(3, sample(seq, 5.0)));
    for (let seq = 5; seq < 10; seq += 1) await c.deliver(seq, framed(3, sample(seq, 2.0)));
    expect([c.counters.alarms_raised, c.counters.alarms_cleared]).toEqual([1, 1]);
  });

  test('a message for an asset without rules is counted and processed', async () => {
    const c = await startConsumer();
    await c.deliver(0, framed(3, sample(0, 9, { asset_id: 'pump-09' })));
    expect([c.counters.messages_without_rules, c.processed]).toEqual([1, [1]]);
  });

  test('an undecodable message is dead-lettered with its coordinates and original bytes', async () => {
    const lines = captureLogs();
    const c = await startConsumer();
    const value = Buffer.from('\u0001not avro');
    await c.deliver(31, value);
    const [{ topic, acks, messages }] = c.producer.send.mock.calls[0];
    expect([topic, acks, messages[0].key.toString()]).toEqual(['telemetry.dlq', -1, 'pump-01']);
    const record = JSON.parse(messages[0].value).payload;
    expect(Buffer.from(record.payload.data)).toEqual(value);
    expect(record).toMatchObject({ reason: 'undecodable', source: 'alarm_consumer', source_topic: 'telemetry.raw', source_partition: 0, source_offset: 31, schema_id: null });
    expect([c.counters.messages_dead_lettered, c.processed]).toEqual([1, [1]]);
    expect(lines.find((l) => l.event === 'message_dead_lettered')).toMatchObject({ level: 'WARNING', key: 'pump-01', reason: 'undecodable' });
  });

  test('a dead letter without a Kafka key and with an empty value', async () => {
    const c = await startConsumer();
    await c.deliver(2, null, null);
    const [{ messages }] = c.producer.send.mock.calls[0];
    expect(messages[0].key).toBeNull();
    expect(JSON.parse(messages[0].value).payload).toMatchObject({ source_key: null, detail: 'empty value' });
  });

  test('a long detail is truncated to 1000 characters', async () => {
    const c = await startConsumer({ registry: fakeRegistry({ decodeError: new Error('x'.repeat(5000)) }) });
    await c.deliver(0, framed(3, {}));
    expect(JSON.parse(c.producer.send.mock.calls[0][0].messages[0].value).payload.detail).toHaveLength(1000);
  });

  test('a failed dead-letter send fails the handler: nothing counted, offset not resolved', async () => {
    // Task 2.3 pending path: the dead-letter send fails in the consumer.
    const producer = { send: jest.fn(async ({ topic }) => { if (topic === 'telemetry.dlq') throw new Error('broker down'); }) };
    const c = await startConsumer({ producer });
    await expect(c.deliver(0, Buffer.from('\u0001x'))).rejects.toThrow('broker down');
    expect([c.counters.messages_dead_lettered, c.counters.messages_processed, c.processed]).toEqual([0, 0, []]);
  });

  test('a failed alarm send fails the handler: nothing counted, offset not resolved', async () => {
    const producer = { send: jest.fn(async () => { throw new Error('broker down'); }) };
    const c = await startConsumer({ producer });
    for (let seq = 0; seq < 4; seq += 1) await c.deliver(seq, framed(3, sample(seq, 5.0)));
    await expect(c.deliver(4, framed(3, sample(4, 5.0)))).rejects.toThrow('broker down');
    expect([c.counters.alarms_raised, c.processed]).toEqual([0, [1, 2, 3, 4]]);
  });

  test('a transient registry failure fails the handler and sends nothing', async () => {
    const lines = captureLogs();
    const registry = fakeRegistry({ getSchemaError: Object.assign(new Error('Error, status 400'), { status: 400 }) });
    const c = await startConsumer({ registry });
    await expect(c.deliver(32, framed(3, sample(0, 1)))).rejects.toBeInstanceOf(TransientRegistryError);
    expect(c.producer.send).not.toHaveBeenCalled();
    expect(lines.find((l) => l.event === 'schema_registry_unavailable')).toMatchObject({ level: 'ERROR', offset: '32' });
  });

  test('a registry rejection that is not an Error is still transient', async () => {
    // Found by this test: the message was read from the rejection value, so
    // a null rejection raised a TypeError instead (fixed in consumer.js).
    const registry = fakeRegistry();
    registry.getSchema = jest.fn(async () => { throw null; }); // eslint-disable-line no-throw-literal
    const c = await startConsumer({ registry });
    await expect(c.deliver(0, framed(3, {}))).rejects.toThrow('resolving schema id 3: null');
  });
});

describe('readActiveAlarms', () => {
  function restoreKafka(records) {
    return {
      consumer: jest.fn(() => ({
        connect: jest.fn(async () => {}),
        subscribe: jest.fn(async () => {}),
        disconnect: jest.fn(async () => {}),
        run: jest.fn(async ({ eachMessage }) => {
          for (const r of records) await eachMessage(r);
        }),
      })),
    };
  }
  const registry = { decode: jest.fn(async (buffer) => JSON.parse(buffer.toString())) };
  const event = (id, state) => ({ alarm_id: id, asset_id: 'pump-01', channel: 'vibration_rms_mm_s', severity: 'WARNING', state });
  const rec = (partition, offset, ev, key = ev && ev.alarm_id) => ({
    partition, message: { offset: String(offset), key: key ? Buffer.from(key) : null, value: ev ? Buffer.from(JSON.stringify(ev)) : null },
  });

  test('keeps the latest event per alarm_id and returns those still RAISED', async () => {
    const records = [
      rec(0, 0, event('a', 'RAISED')), rec(0, 1, event('a', 'CLEARED')),
      rec(1, 0, event('b', 'RAISED')),
      rec(2, 0, event('c', 'RAISED')), rec(2, 1, null, 'c'), // tombstone deletes c
      rec(5, 0, event('z', 'RAISED')), // a partition not in the target set is ignored
    ];
    const admin = { fetchTopicOffsets: jest.fn(async () => [
      { partition: 0, low: '0', high: '2' }, { partition: 1, low: '0', high: '1' }, { partition: 2, low: '0', high: '2' },
    ]) };
    const { active, stats } = await readActiveAlarms({ kafka: restoreKafka(records), admin, registry, alarmsTopic: 'telemetry.alarms', log: jsonlog.getLogger('t') });
    expect(active.map((e) => e.alarm_id)).toEqual(['b']);
    expect(stats).toEqual({ partitions: 3, records_read: 5, alarms_seen: 2 });
  });

  test('an empty topic needs no consumer', async () => {
    const kafka = restoreKafka([]);
    const admin = { fetchTopicOffsets: jest.fn(async () => [{ partition: 0, low: '4', high: '4' }]) };
    const { active } = await readActiveAlarms({ kafka, admin, registry, alarmsTopic: 't', log: jsonlog.getLogger('t') });
    expect(active).toEqual([]);
    expect(kafka.consumer).not.toHaveBeenCalled();
  });

  test('a restore that cannot reach the end fails', async () => {
    jest.useFakeTimers();
    try {
      const admin = { fetchTopicOffsets: jest.fn(async () => [{ partition: 0, low: '0', high: '9' }]) };
      const pending = readActiveAlarms({ kafka: restoreKafka([rec(0, 0, event('a', 'RAISED'))]), admin, registry, alarmsTopic: 't', log: jsonlog.getLogger('t') });
      const assertion = expect(pending).rejects.toThrow('did not reach the end of t');
      await jest.advanceTimersByTimeAsync(60001);
      await assertion;
    } finally {
      jest.useRealTimers();
    }
  });
});

describe('start-up helpers', () => {
  test('checkTopics requires the three topics and compaction', async () => {
    const admin = {
      fetchTopicMetadata: jest.fn(async () => ({})),
      describeConfigs: jest.fn(async () => ({ resources: [{ configEntries: [{ configName: 'cleanup.policy', configValue: 'compact' }] }] })),
    };
    await checkTopics(admin, { rawTopic: 'r', alarmsTopic: 'a', dlqTopic: 'd', alarmsConfigs: { 'cleanup.policy': 'compact' } });
    expect(admin.fetchTopicMetadata).toHaveBeenCalledWith({ topics: ['r', 'a', 'd'] });
    admin.describeConfigs = jest.fn(async () => ({ resources: [{ configEntries: [{ configName: 'cleanup.policy', configValue: 'delete' }] }] }));
    await expect(checkTopics(admin, { rawTopic: 'r', alarmsTopic: 'a', dlqTopic: 'd', alarmsConfigs: { 'cleanup.policy': 'compact' } }))
      .rejects.toThrow('topic a settings differ from config/topics.yaml: cleanup.policy=delete (expected compact)');
  });

  test('registerSchema registers under <topic>-value without a subject-level override', async () => {
    const registry = fakeRegistry();
    expect(await registerSchema(registry, { topic: 'telemetry.dlq', schema: '{}' })).toBe(9);
    expect(registry.register.mock.calls[0][1]).toEqual({ subject: 'telemetry.dlq-value', updateConfig: false });
  });

  test('kafkajs logs go through the JSON logger with Python level names', () => {
    const lines = captureLogs();
    const kafka = createKafka({ brokers: ['localhost:9092'], clientId: 'test', debug: false });
    kafka.logger().warn('something odd', { broker: 'b1' });
    kafka.logger().error('broken');
    expect(lines.map((l) => [l.logger, l.level, l.event, l.broker])).toEqual([
      ['kafkajs', 'WARNING', 'something odd', 'b1'], ['kafkajs', 'ERROR', 'broken', undefined],
    ]);
    expect(lines[0].ts).toMatch(/Z$/); // kafkajs's own timestamp is replaced by ours
    createKafka({ brokers: ['localhost:9092'], clientId: 'test', debug: true }).logger().info('visible at debug');
    expect(lines.at(-1)).toMatchObject({ level: 'INFO', event: 'visible at debug' });
  });

  test('the producer and registry clients are built offline', () => {
    const kafka = createKafka({ brokers: ['localhost:9092'], clientId: 'test' });
    expect(typeof createProducer(kafka).send).toBe('function');
    expect(typeof createRegistry('http://localhost:8081').decode).toBe('function');
  });
});
