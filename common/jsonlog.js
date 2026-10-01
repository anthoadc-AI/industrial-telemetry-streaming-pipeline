'use strict';

/**
 * JSON-lines logging for the Node.js components, the counterpart of
 * common/jsonlog.py: the same record produces the same line in both.
 *
 * Each record is one line of compact JSON (non-ASCII written as UTF-8, not
 * escaped) with the fields, in this order:
 *
 *   ts        ISO 8601 UTC with milliseconds and a Z suffix
 *   level     Python level name: DEBUG, INFO, WARNING, ERROR, CRITICAL
 *   logger    logger name
 *   event     the log message, used as an event name
 *   ...       every field passed in `extra`, flattened
 *   exception the error's stack, when one is passed to exception()
 *
 * Known differences from the Python logger, outside the format contract: a
 * float with an integral value is written `1` here and `1.0` by Python; NaN
 * and infinity are written as null here and as bare NaN/Infinity by Python;
 * values JSON cannot represent natively (a Date, a BigInt) are written by
 * each runtime's own conversion; exception text is each runtime's own trace.
 *
 * Usage:
 *
 *   const jsonlog = require('../../common/jsonlog');
 *   jsonlog.configure({ level: 'INFO' });
 *   const log = jsonlog.getLogger('processing.alarm_consumer');
 *   log.info('run_started', { topic: 'telemetry.raw' });
 */

const LEVELS = Object.freeze({ DEBUG: 10, INFO: 20, WARNING: 30, ERROR: 40, CRITICAL: 50 });

const settings = { threshold: LEVELS.INFO, stream: process.stderr };
const loggers = new Map();

function replacer(key, value) {
  return typeof value === 'bigint' ? value.toString() : value;
}

class JsonLogger {
  constructor(name) {
    this.name = name;
  }

  log(level, event, extra = {}, error = undefined) {
    if (LEVELS[level] === undefined) {
      throw new Error(`unknown level ${level}; expected one of ${Object.keys(LEVELS).join(', ')}`);
    }
    if (LEVELS[level] < settings.threshold) {
      return;
    }
    const entry = { ts: new Date().toISOString(), level, logger: this.name, event: String(event) };
    Object.assign(entry, extra);
    if (error !== undefined) {
      entry.exception = error instanceof Error && error.stack ? error.stack : String(error);
    }
    settings.stream.write(`${JSON.stringify(entry, replacer)}\n`);
  }

  debug(event, extra) { this.log('DEBUG', event, extra); }

  info(event, extra) { this.log('INFO', event, extra); }

  warning(event, extra) { this.log('WARNING', event, extra); }

  error(event, extra) { this.log('ERROR', event, extra); }

  critical(event, extra) { this.log('CRITICAL', event, extra); }

  /** ERROR record carrying the error's stack in the exception field. */
  exception(event, error, extra) { this.log('ERROR', event, extra, error); }
}

/**
 * Send every record at `level` and above to `stream` (default stderr) as JSON lines.
 */
function configure({ level = 'INFO', stream = process.stderr } = {}) {
  const threshold = LEVELS[String(level).toUpperCase()];
  if (threshold === undefined) {
    throw new Error(`unknown level ${level}; expected one of ${Object.keys(LEVELS).join(', ')}`);
  }
  settings.threshold = threshold;
  settings.stream = stream;
}

function getLogger(name) {
  if (!loggers.has(name)) {
    loggers.set(name, new JsonLogger(name));
  }
  return loggers.get(name);
}

module.exports = { configure, getLogger, JsonLogger, LEVELS };
