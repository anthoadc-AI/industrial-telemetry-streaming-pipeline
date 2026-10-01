"""common/jsonlog.py: line format, field order, extras, exceptions, threshold and warning capture."""

from __future__ import annotations

import io
import json
import logging
import re
import warnings

from common import jsonlog


def test_line_format_and_field_order(json_log):
    logging.getLogger("t.format").info("run_started", extra={"count": 3, "text": "señal ºC"})
    [entry] = json_log()
    assert list(entry) == ["ts", "level", "logger", "event", "count", "text"]
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", entry["ts"])
    assert (entry["level"], entry["logger"], entry["event"], entry["text"]) == ("INFO", "t.format", "run_started", "señal ºC")


def test_compact_and_not_ascii_escaped():
    buffer = io.StringIO()
    root, saved = logging.getLogger(), (list(logging.getLogger().handlers), logging.getLogger().level)
    try:
        jsonlog.configure("INFO", stream=buffer)
        logging.getLogger("t").warning("e", extra={"a": [1, 2], "b": "ñ"})
    finally:
        root.handlers[:], level = saved
        root.setLevel(level)
    line = buffer.getvalue().strip()
    assert ", " not in line and '": ' not in line and "ñ" in line and "\\u00f1" not in line


def test_exception_and_non_json_values(json_log):
    try:
        raise ValueError("boom")
    except ValueError:
        logging.getLogger("t").exception("failed", extra={"obj": object(), "path": __import__("pathlib").Path("a")})
    [entry] = json_log()
    assert entry["level"] == "ERROR"
    assert "ValueError: boom" in entry["exception"]
    assert entry["obj"].startswith("<object object") and entry["path"] == "a"


def test_threshold_filters_records():
    buffer = io.StringIO()
    root, saved = logging.getLogger(), (list(logging.getLogger().handlers), logging.getLogger().level)
    try:
        jsonlog.configure("warning", stream=buffer)
        logging.getLogger("t").info("hidden")
        logging.getLogger("t").warning("shown")
    finally:
        root.handlers[:], level = saved
        root.setLevel(level)
    assert [json.loads(l)["event"] for l in buffer.getvalue().splitlines()] == ["shown"]


def test_library_warnings_become_json_lines(json_log):
    # pytest installs its own warning recorder for each test, replacing the
    # hook configure() set up in the fixture; set it up again inside the test.
    with warnings.catch_warnings():
        warnings.simplefilter("always")
        logging.captureWarnings(False)
        logging.captureWarnings(True)
        warnings.warn("library notice", UserWarning)
    [entry] = json_log()
    assert entry["logger"] == "py.warnings" and "library notice" in entry["event"]


def test_default_stream_is_utf8_stderr(monkeypatch):
    class Stream(io.StringIO):
        reconfigured = None

        def reconfigure(self, **kwargs):
            Stream.reconfigured = kwargs

    fake = Stream()
    monkeypatch.setattr("sys.stderr", fake)
    root, saved = logging.getLogger(), (list(logging.getLogger().handlers), logging.getLogger().level)
    try:
        jsonlog.configure("INFO")
        logging.getLogger("t").info("x")
    finally:
        root.handlers[:], level = saved
        root.setLevel(level)
        logging.captureWarnings(False)
    assert Stream.reconfigured == {"encoding": "utf-8", "errors": "backslashreplace"}
    assert json.loads(fake.getvalue())["event"] == "x"
