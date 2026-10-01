"""Create the Kafka topics and set the Schema Registry compatibility level.

    python -m broker.kafka.provision

Topics come from config/topics.yaml, with their partition count,
replication factor and topic-level `configs` (for example
cleanup.policy=compact on telemetry.alarms). The broker has automatic topic
creation disabled, so this is the only way topics come into existence.
Existing topics are left alone; a topic whose partition count or configured
settings differ from config/ is reported as an error rather than altered.

Schema Registry's global compatibility level is set to BACKWARD, the policy
schemas/README.md describes, and read back to confirm.

Idempotent: re-running changes nothing once the state matches. Exits 0 when
every topic exists as configured and the compatibility level is confirmed,
1 otherwise.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import urllib.error
import urllib.request

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

from common import jsonlog
from config.loader import ConfigError, load_config

log = logging.getLogger("broker.kafka.provision")

COMPATIBILITY = "BACKWARD"
TIMEOUT_S = 15.0


def provision_topics(bootstrap: str) -> bool:
    topics = load_config().topics.kafka.values()
    admin = AdminClient({"bootstrap.servers": bootstrap})
    try:
        existing = admin.list_topics(timeout=TIMEOUT_S).topics
    except KafkaException as e:
        log.error("kafka_unreachable", extra={"bootstrap": bootstrap, "error": str(e)})
        return False

    ok = True
    missing = [t for t in topics if t.name not in existing]
    for t in topics:
        if t.name in existing:
            partitions = len(existing[t.name].partitions)
            if partitions != t.partitions:
                log.error("topic_partition_mismatch", extra={"topic": t.name, "partitions": partitions,
                                                             "configured_partitions": t.partitions, "altered": False})
                ok = False
            elif not configs_match(admin, t):
                ok = False
            else:
                log.info("topic_exists", extra={"topic": t.name, "partitions": partitions, "configs": dict(t.configs)})
    if missing:
        futures = admin.create_topics(
            [NewTopic(t.name, num_partitions=t.partitions, replication_factor=t.replication_factor,
                      config=dict(t.configs)) for t in missing],
            operation_timeout=TIMEOUT_S,
        )
        configured = {t.name: dict(t.configs) for t in missing}
        for name, future in futures.items():
            try:
                future.result()
                log.info("topic_created", extra={"topic": name, "configs": configured[name]})
            except KafkaException as e:
                log.error("topic_create_failed", extra={"topic": name, "error": str(e)})
                ok = False
    return ok


def configs_match(admin: AdminClient, topic) -> bool:
    """True when every setting in config/ has the same value on the existing topic."""
    if not topic.configs:
        return True
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic.name)
    try:
        actual = admin.describe_configs([resource])[resource].result(timeout=TIMEOUT_S)
    except KafkaException as e:
        log.error("topic_config_unreadable", extra={"topic": topic.name, "error": str(e)})
        return False
    differing = {k: (actual[k].value if k in actual else None) for k, v in topic.configs.items()
                 if k not in actual or actual[k].value != v}
    if differing:
        log.error("topic_config_mismatch", extra={"topic": topic.name, "actual": differing,
                                                  "configured": dict(topic.configs), "altered": False})
        return False
    return True


def set_compatibility(registry_url: str) -> bool:
    url = registry_url.rstrip("/") + "/config"
    body = json.dumps({"compatibility": COMPATIBILITY}).encode()
    request = urllib.request.Request(url, data=body, method="PUT",
                                     headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_S):
            pass
        with urllib.request.urlopen(url, timeout=TIMEOUT_S) as response:
            level = json.load(response).get("compatibilityLevel")
    except (urllib.error.URLError, OSError, ValueError) as e:
        log.error("schema_registry_unreachable", extra={"url": registry_url, "error": str(e)})
        return False
    if level != COMPATIBILITY:
        log.error("schema_registry_compatibility_mismatch", extra={"compatibility": level, "expected": COMPATIBILITY})
        return False
    log.info("schema_registry_compatibility", extra={"compatibility": level})
    return True


def main() -> int:
    jsonlog.configure("INFO")
    try:
        load_config()
    except ConfigError as e:
        log.error("config_invalid", extra={"error": str(e)})
        return 1
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    registry = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    topics_ok = provision_topics(bootstrap)
    compat_ok = set_compatibility(registry)
    return 0 if topics_ok and compat_ok else 1


if __name__ == "__main__":
    sys.exit(main())
