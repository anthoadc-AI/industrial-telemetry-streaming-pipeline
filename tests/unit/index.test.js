'use strict';

// processing/alarm_consumer/index.js: argument parsing. Requiring the module
// does not start the consumer (require.main guard); main() is exercised by
// the integration tests.

const { parseCli, USAGE } = require('../../processing/alarm_consumer/index');

const ENV_KEYS = ['KAFKA_BOOTSTRAP', 'SCHEMA_REGISTRY_URL'];
let saved;
beforeEach(() => { saved = Object.fromEntries(ENV_KEYS.map((k) => [k, process.env[k]])); ENV_KEYS.forEach((k) => delete process.env[k]); });
afterEach(() => ENV_KEYS.forEach((k) => { if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k]; }));

test('defaults', () => {
  expect(parseCli([])).toEqual({
    count: undefined, duration: undefined, brokers: ['localhost:9092'], registryUrl: 'http://localhost:8081',
    groupId: 'alarm-consumer', logLevel: 'INFO',
  });
});

test('values and environment defaults', () => {
  process.env.KAFKA_BOOTSTRAP = 'k1:9092,k2:9092';
  process.env.SCHEMA_REGISTRY_URL = 'http://sr:8081';
  expect(parseCli(['--count', '5', '--duration', '1.5', '--group-id', 'g', '--log-level', 'debug'])).toEqual({
    count: 5, duration: 1.5, brokers: ['k1:9092', 'k2:9092'], registryUrl: 'http://sr:8081', groupId: 'g', logLevel: 'DEBUG',
  });
});

test.each([
  [['--count', '0'], '--count must be a positive integer'],
  [['--count', '2.5'], '--count must be a positive integer'],
  [['--duration=-1'], '--duration must be a positive number'],
  [['--duration', 'soon'], '--duration must be a positive number'],
  [['--log-level', 'TRACE'], '--log-level must be one of DEBUG, INFO, WARNING, ERROR'],
  [['--bogus'], "Unknown option '--bogus'"],
])('%p is rejected', (args, message) => {
  expect(() => parseCli(args)).toThrow(message);
});

test('usage names every option', () => {
  for (const option of ['--count', '--duration', '--bootstrap-servers', '--schema-registry-url', '--group-id', '--log-level']) {
    expect(USAGE).toContain(option);
  }
});
