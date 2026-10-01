'use strict';

// common/jsonlog.js: line format, field order, levels and threshold, extras,
// exceptions, and the logger cache. Byte equality with the Python logger is
// asserted by tests/unit/test_jsonlog_equivalence.py.

const jsonlog = require('../../common/jsonlog');

function capture(level = 'DEBUG') {
  const raw = [];
  jsonlog.configure({ level, stream: { write: (s) => raw.push(s) } });
  return raw;
}

afterEach(() => jsonlog.configure({ level: 'INFO', stream: process.stderr }));

test('one compact JSON line per record, fields in order', () => {
  const raw = capture();
  jsonlog.getLogger('t.format').info('run_started', { count: 3, text: 'señal ºC' });
  expect(raw).toHaveLength(1);
  expect(raw[0].endsWith('\n')).toBe(true);
  const line = raw[0].trimEnd();
  expect(line).toMatch(/^\{"ts":"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z","level":"INFO","logger":"t\.format","event":"run_started","count":3,"text":"señal ºC"\}$/);
});

test.each(['debug', 'info', 'warning', 'error', 'critical'])('%s writes the Python level name', (method) => {
  const raw = capture();
  jsonlog.getLogger('t')[method]('e');
  expect(JSON.parse(raw[0]).level).toBe(method.toUpperCase());
});

test('records below the threshold are dropped', () => {
  const raw = capture('warning');
  const log = jsonlog.getLogger('t');
  log.info('hidden');
  log.warning('shown');
  expect(raw.map((l) => JSON.parse(l).event)).toEqual(['shown']);
});

test('exception() carries the stack, or the value when it is not an Error', () => {
  const raw = capture();
  const log = jsonlog.getLogger('t');
  log.exception('failed', new Error('boom'), { step: 2 });
  log.exception('failed', 'plain string');
  const [withStack, plain] = raw.map((l) => JSON.parse(l));
  expect(withStack).toMatchObject({ level: 'ERROR', step: 2 });
  expect(withStack.exception).toMatch(/^Error: boom\n\s+at /);
  expect(plain.exception).toBe('plain string');
});

test('BigInt values are written as strings; undefined extras are dropped', () => {
  const raw = capture();
  jsonlog.getLogger('t').info('e', { big: 2n ** 64n, gone: undefined });
  expect(JSON.parse(raw[0])).toMatchObject({ big: '18446744073709551616' });
  expect('gone' in JSON.parse(raw[0])).toBe(false);
});

test('getLogger caches one logger per name', () => {
  expect(jsonlog.getLogger('same')).toBe(jsonlog.getLogger('same'));
  expect(jsonlog.getLogger('same')).not.toBe(jsonlog.getLogger('other'));
});

test('unknown levels are rejected', () => {
  expect(() => jsonlog.configure({ level: 'VERBOSE' })).toThrow('unknown level VERBOSE');
  expect(() => jsonlog.getLogger('t').log('VERBOSE', 'e')).toThrow('unknown level VERBOSE');
});

test('configure() defaults to INFO on stderr', () => {
  const write = jest.spyOn(process.stderr, 'write').mockImplementation(() => true);
  try {
    jsonlog.configure();
    jsonlog.getLogger('t').debug('hidden');
    jsonlog.getLogger('t').info('shown');
    expect(write).toHaveBeenCalledTimes(1);
    expect(JSON.parse(write.mock.calls[0][0]).event).toBe('shown');
  } finally {
    write.mockRestore();
  }
});
