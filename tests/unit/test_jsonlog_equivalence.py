"""common/jsonlog.py and common/jsonlog.js must write the same line for the same record.

Each case goes through both loggers at threshold INFO; the lines are compared
as strings after removing the leading "ts" field, the only value that
legitimately differs. Cases cover every level, a record below the threshold,
and extras of every JSON type the components log, including non-ASCII text,
characters JSON must escape, and nested values.

Floats with integral values, NaN, infinity and non-JSON types are excluded:
the two runtimes are documented to write them differently.

Runs under pytest, or directly: python tests/unit/test_jsonlog_equivalence.py
Requires node on PATH.
"""

from __future__ import annotations

import io
import json
import logging
import re
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
from common import jsonlog  # noqa: E402

LOGGER_NAME = "tests.jsonlog_equivalence"

CASES = [
    {"level": "INFO", "event": "run_started", "extra": {}},
    {"level": "DEBUG", "event": "below_threshold", "extra": {"seq": 1}},
    {"level": "WARNING", "event": "message_invalid", "extra": {
        "mqtt_topic": "plant/pump-01/telemetry", "reason": "schema_mismatch", "payload_bytes": 212}},
    {"level": "ERROR", "event": "message_delivery_failed", "extra": {
        "asset_id": "compressor-01", "seq": 9007199254740991, "error": "KafkaError{code=_MSG_TIMED_OUT}"}},
    {"level": "CRITICAL", "event": "types", "extra": {
        "int": -42, "float": 2.5, "small": -0.125, "true": True, "false": False, "none": None,
        "list": [1, "a", None, [2.75]], "dict": {"nested": {"k": "v"}, "n": 3}}},
    {"level": "INFO", "event": "text", "extra": {
        "non_ascii": "señal ºC — ñandú", "emoji": "\U0001F527", "quote": 'say "hi"', "backslash": "C\\path",
        "controls": "tab\tnewline\ncr\rnul-free\u0001end", "slash": "a/b"}},
    {"level": "INFO", "event": "latency", "extra": {
        "event_to_receive_ms": 3.018, "receive_to_produce_ms": 0.243, "produce_to_ack_ms": 44.5,
        "event_to_ack_ms": 47.761, "receive_ts_ms": 1790889302300.123}},
]

NODE_SCRIPT = r"""
const jsonlog = require(process.argv[1]);
let input = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', (chunk) => { input += chunk; });
process.stdin.on('end', () => {
  const { name, cases } = JSON.parse(input);
  jsonlog.configure({ level: 'INFO', stream: process.stdout });
  const log = jsonlog.getLogger(name);
  for (const c of cases) {
    log.log(c.level, c.event, c.extra);
  }
});
"""

TS_PREFIX = re.compile(r'^\{"ts":"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z",')


def strip_ts(line: str) -> str:
    stripped, n = TS_PREFIX.subn("{", line)
    assert n == 1, f"line does not start with a well-formed ts field: {line!r}"
    return stripped


def python_lines() -> list[str]:
    buffer = io.StringIO()
    previous = list(logging.getLogger().handlers), logging.getLogger().level
    try:
        jsonlog.configure("INFO", stream=buffer)
        log = logging.getLogger(LOGGER_NAME)
        for c in CASES:
            log.log(logging.getLevelName(c["level"]), c["event"], extra=c["extra"])
    finally:
        logging.getLogger().handlers[:], level = previous
        logging.getLogger().setLevel(level)
    return buffer.getvalue().splitlines()


def node_lines() -> list[str]:
    node = shutil.which("node")
    assert node, "node is not on PATH"
    result = subprocess.run(
        [node, "-e", NODE_SCRIPT, str(REPO_ROOT / "common" / "jsonlog.js")],
        input=json.dumps({"name": LOGGER_NAME, "cases": CASES}).encode("utf-8"),
        capture_output=True, check=True,
    )
    return result.stdout.decode("utf-8").splitlines()


def test_same_lines_excluding_ts():
    py, js = python_lines(), node_lines()
    expected = sum(1 for c in CASES if c["level"] != "DEBUG")
    assert len(py) == expected, f"python wrote {len(py)} lines, expected {expected}"
    assert len(js) == expected, f"node wrote {len(js)} lines, expected {expected}"
    for i, (a, b) in enumerate(zip(py, js)):
        assert strip_ts(a) == strip_ts(b), f"line {i} differs:\n  python: {a}\n  node:   {b}"


def test_lines_are_valid_compact_json():
    for line in python_lines() + node_lines():
        entry = json.loads(line)
        assert list(entry)[:4] == ["ts", "level", "logger", "event"]
        # Re-serialising compactly, without escaping, must give the line back
        # exactly: no spaces after separators, non-ASCII not escaped.
        assert json.dumps(entry, separators=(",", ":"), ensure_ascii=False) == line, f"not compact: {line!r}"


# Absolute times in milliseconds with a fraction: the bridge's receive_ts_ms and the
# consumer's alarm_ack_ts_ms. Python writes them through jsonlog; Node must read the
# same value. (A value whose fraction is .000 is written 1790889302300.0 by Python and
# 1790889302300 by Node, the integral-float difference above; the value is the same.)
TIMESTAMP_VALUES = [
    round(1_790_889_302_300_123_456 / 1e6, 3),   # from time.time_ns(), as the bridge computes it
    1790889302300.0, 1790889302300.001, 1790889302300.999, 1790889302300.5,
    1999999999999.999, 1700000000000.001,
]

NODE_READ_SCRIPT = r"""
let input = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', (chunk) => { input += chunk; });
process.stdin.on('end', () => {
  const values = input.split('\n').filter(Boolean).map((line) => JSON.parse(line).receive_ts_ms);
  process.stdout.write(JSON.stringify(values));
});
"""


def test_millisecond_timestamps_read_the_same_in_python_and_node():
    buffer = io.StringIO()
    previous = list(logging.getLogger().handlers), logging.getLogger().level
    try:
        jsonlog.configure("INFO", stream=buffer)
        for value in TIMESTAMP_VALUES:
            logging.getLogger("bridge.mqtt_to_kafka").info("message_delivered", extra={"receive_ts_ms": value})
    finally:
        logging.getLogger().handlers[:], level = previous
        logging.getLogger().setLevel(level)
    lines = buffer.getvalue()
    python_read = [json.loads(line)["receive_ts_ms"] for line in lines.splitlines()]
    node = shutil.which("node")
    assert node, "node is not on PATH"
    result = subprocess.run([node, "-e", NODE_READ_SCRIPT], input=lines.encode("utf-8"), capture_output=True, check=True)
    node_read = json.loads(result.stdout)
    assert python_read == TIMESTAMP_VALUES
    assert node_read == python_read  # the same doubles, value for value
    assert python_read[0] == 1790889302300.123


if __name__ == "__main__":
    py, js = python_lines(), node_lines()
    for a, b in zip(py, js):
        print("python:", a)
        print("node:  ", b)
    test_same_lines_excluding_ts()
    test_lines_are_valid_compact_json()
    test_millisecond_timestamps_read_the_same_in_python_and_node()
    print(f"PASS: {len(py)} records, identical excluding ts")
