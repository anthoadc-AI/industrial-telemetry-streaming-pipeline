# Schemas

Avro schemas for the Kafka topics defined in `config/topics.yaml`. The
schemas are registered in Schema Registry by the producers that use them,
under the default subject naming, `<topic>-value`.

| File | Record | Topic |
|---|---|---|
| `telemetry_v1.avsc` | `industrial_telemetry.Telemetry` | `telemetry.raw` |
| `alarm_v1.avsc` | `industrial_telemetry.Alarm` | `telemetry.alarms` |
| `dlq_v1.avsc` | `industrial_telemetry.DeadLetter` | `telemetry.dlq` |

## Versioning policy

- **Backward-compatible changes evolve the existing file in place.** Adding
  a field with a default, or removing a field that has one, keeps the file
  name. Schema Registry, set to `BACKWARD` compatibility, rejects any
  registration that breaks this rule.
- **Breaking changes get a new file with the next suffix**, for example
  `telemetry_v2.avsc`, and a new topic. Examples are renaming or retyping
  a field, or adding a field without a default. The two versions run side
  by side until every consumer has moved.
- **The record name and namespace never change**, so that Avro schema
  resolution keeps matching old and new data. The version lives in the file
  name, not in the namespace.
- Enums carry a `default` symbol, so a reader with an older schema can
  still decode a symbol added later.

## Conventions

- Timestamps are `long` with `logicalType: timestamp-millis`, in UTC.
- Channel fields carry their unit in the name (`_c`, `_mm_s`, `_a`, `_rpm`)
  and are nullable with a `null` default, so an asset that lacks a sensor
  sends `null` rather than a made-up value.
- Every telemetry and alarm record has a `synthetic` flag. All data in
  this project is synthetic.
- `dlq_v1` carries the rejected message as `bytes`, untouched; it is shown
  as base64 only in logs and in `scripts/smoke/dlq_dump.py`.
