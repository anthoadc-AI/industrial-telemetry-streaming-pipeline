"""JSON-lines logging on the standard logging module, shared by every Python component.

Each record becomes one line of compact JSON (no spaces after separators,
non-ASCII characters written as UTF-8, not escaped) with the fields, in this
order:

    ts        ISO 8601 UTC with milliseconds and a Z suffix
    level     Python level name: DEBUG, INFO, WARNING, ERROR, CRITICAL
    logger    logger name
    event     the log message, used as an event name
    ...       every field passed through ``extra``, flattened
    exception the formatted traceback, when the record carries one

common/jsonlog.js writes the same line for the same record, so Python and
Node.js components share one log format. Known differences, outside the
format contract: a float with an integral value is written ``1.0`` here and
``1`` by Node.js; NaN and infinity are written as bare ``NaN`` and
``Infinity`` here (not valid JSON) and ``null`` by Node.js; exception text
is each runtime's own traceback.

Usage:

    from common import jsonlog
    jsonlog.configure("INFO")
    logging.getLogger("edge.simulator").info("run_started", extra={"count": 200})
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import TextIO

_STANDARD = set(vars(logging.makeLogRecord({}))) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        entry.update({k: v for k, v in vars(record).items() if k not in _STANDARD})
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str, separators=(",", ":"), ensure_ascii=False)


def configure(level: str, stream: TextIO | None = None) -> None:
    """Send every log record, at ``level`` and above, to ``stream`` (default stderr) as JSON lines."""
    if stream is None:
        stream = sys.stderr
        # UTF-8 regardless of the console code page, so non-ASCII text is
        # written exactly as Node.js writes it.
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    # Library warnings (for example deprecation notices at import time) go
    # through the same handler, so stderr stays pure JSON lines.
    logging.captureWarnings(True)
