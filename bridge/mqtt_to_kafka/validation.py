"""Validate MQTT telemetry payloads against the telemetry Avro schema.

A payload is accepted only if all of the following hold:

1. it is UTF-8 JSON encoding an object;
2. ``ts`` is an ISO 8601 timestamp with a UTC offset (``Z`` or ``+00:00``
   style), converted to an aware datetime for the Avro timestamp-millis
   field;
3. it has no field that the schema does not define (fastavro does not check
   this, so it is checked here);
4. it validates against the schema with fastavro: every required field
   present, every value of the declared type;
5. no numeric value is NaN or infinite: Python's json module accepts the
   literals NaN, Infinity and -Infinity, and an Avro double can carry
   them, so the schema check alone lets them through;
6. its ``asset_id`` equals the asset named by the MQTT topic.

There is no range check: config/telemetry.yaml defines the units of each
channel but no physical limits, so out-of-range values have no source of
truth to be checked against.

Any failure raises ``InvalidMessage`` with a short machine-readable reason and
a human-readable detail.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fastavro
from fastavro.validation import ValidationError, validate


class InvalidMessage(Exception):
    def __init__(self, reason: str, detail: str):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


class TopicPattern:
    """Extract the asset id from a topic built from a template such as plant/{asset_id}/telemetry."""

    def __init__(self, template: str):
        prefix, _, suffix = template.partition("{asset_id}")
        self.prefix, self.suffix = prefix, suffix

    def asset_id(self, topic: str) -> str | None:
        if not (topic.startswith(self.prefix) and topic.endswith(self.suffix)):
            return None
        middle = topic[len(self.prefix):len(topic) - len(self.suffix)]
        return middle if middle and "/" not in middle else None


class TelemetryValidator:
    def __init__(self, schema_path: Path, topic_template: str):
        self.schema_str = schema_path.read_text(encoding="utf-8")
        schema = json.loads(self.schema_str)
        self.parsed = fastavro.parse_schema(schema)
        self.fields = {f["name"] for f in schema["fields"]}
        self.topics = TopicPattern(topic_template)

    def validate(self, mqtt_topic: str, payload: bytes) -> dict[str, Any]:
        """Return the Avro-ready record, or raise InvalidMessage."""
        try:
            record = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise InvalidMessage("invalid_json", str(e)) from None
        if not isinstance(record, dict):
            raise InvalidMessage("invalid_json", f"expected an object, got {type(record).__name__}")

        ts = record.get("ts")
        if not isinstance(ts, str):
            raise InvalidMessage("invalid_ts", "ts must be an ISO 8601 string")
        try:
            parsed_ts = datetime.fromisoformat(ts)
        except ValueError as e:
            raise InvalidMessage("invalid_ts", str(e)) from None
        if parsed_ts.utcoffset() is None:
            raise InvalidMessage("invalid_ts", "ts has no UTC offset")
        record["ts"] = parsed_ts.astimezone(timezone.utc)

        unknown = record.keys() - self.fields
        if unknown:
            raise InvalidMessage("schema_mismatch", f"unknown field(s) {sorted(unknown)}")
        try:
            validate(record, self.parsed, raise_errors=True)
        except ValidationError as e:
            raise InvalidMessage("schema_mismatch", "; ".join(str(err) for err in e.errors) or str(e)) from None

        non_finite = sorted(k for k, v in record.items() if isinstance(v, float) and not math.isfinite(v))
        if non_finite:
            raise InvalidMessage("non_finite_value", f"NaN or infinite value in field(s) {non_finite}")

        topic_asset = self.topics.asset_id(mqtt_topic)
        if topic_asset != record["asset_id"]:
            raise InvalidMessage("topic_mismatch", f"topic {mqtt_topic!r} does not match asset_id {record['asset_id']!r}")
        return record
