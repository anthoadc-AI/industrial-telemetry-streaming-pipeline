/**
 * Diagnostic (Task 3.3 STEP 1, item 1.1.2, disposable): ThingsBoard WebSocket observer, proof of concept.
 *
 * Subscribes to one device's latest telemetry through ThingsBoard's WebSocket
 * API (/api/ws, one authCmd then a TIMESERIES command with scope
 * LATEST_TELEMETRY) and writes one JSON line per received value to stdout:
 * {seq, ts, arrival_ms}, arrival_ms on performance.timeOrigin + now(). Exits
 * after --expect values or after --idle-s seconds without one. Started by
 * ws_poc.py, which passes the JWT in TB_JWT (never printed) and the device id.
 *
 *   TB_JWT=... node scripts/bench/diagnostics/ws_observer_poc.js --url ws://localhost:8080/api/ws \
 *     --device <id> --key seq --expect 600 --since <epoch ms> --idle-s 20
 */
'use strict';

const { parseArgs } = require('node:util');

const { values } = parseArgs({
  options: {
    url: { type: 'string' }, device: { type: 'string' }, key: { type: 'string', default: 'seq' },
    expect: { type: 'string' }, 'idle-s': { type: 'string', default: '20' }, since: { type: 'string', default: '0' },
  },
});
const expect = Number(values.expect);
const since = Number(values.since); // epoch ms: points stamped earlier are reported but not counted
const idleMs = Number(values['idle-s']) * 1000;
const nowMs = () => performance.timeOrigin + performance.now();
const emit = (obj) => process.stdout.write(`${JSON.stringify(obj)}\n`);

let received = 0;
let idleTimer = null;
const ws = new WebSocket(values.url);

function finish(reason) {
  emit({ event: 'observer_finished', reason, received });
  try { ws.close(); } catch { /* already closed */ }
  process.exit(0);
}

function armIdle() {
  clearTimeout(idleTimer);
  idleTimer = setTimeout(() => finish('idle'), idleMs);
}

ws.addEventListener('open', () => {
  ws.send(JSON.stringify({
    authCmd: { cmdId: 0, token: process.env.TB_JWT },
    cmds: [{ type: 'TIMESERIES', entityType: 'DEVICE', entityId: values.device, keys: values.key,
      scope: 'LATEST_TELEMETRY', cmdId: 1 }],
  }));
  armIdle();
});

let first = true;
ws.addEventListener('message', (msg) => {
  const arrival = nowMs();
  const body = JSON.parse(msg.data);
  if (body.errorCode) {
    emit({ event: 'subscription_error', errorCode: body.errorCode, errorMsg: body.errorMsg });
    return;
  }
  const points = (body.data && body.data[values.key]) || [];
  if (first) {
    // The first message answers the subscription; it may carry the latest
    // value already stored. Its points are kept: ws_poc.py drops anything
    // stamped before the run.
    first = false;
    emit({ event: 'subscribed', initial_points: points.length });
  }
  for (const [ts, value] of points) {
    if (Number(ts) >= since) received += 1;
    emit({ seq: Number(value), ts: Number(ts), arrival_ms: Math.round(arrival * 1000) / 1000, batch: points.length });
  }
  armIdle();
  if (received >= expect) finish('expected_count');
});

ws.addEventListener('error', (e) => { emit({ event: 'ws_error', error: String(e.message || e) }); process.exit(1); });
ws.addEventListener('close', (e) => { emit({ event: 'ws_closed', code: e.code }); process.exit(received >= expect ? 0 : 1); });
