"""Create the Kafka topics and set the Schema Registry compatibility level.

    python -m broker.kafka.provision

Topics come from config/topics.yaml, with their partition count and
replication factor. The broker has automatic topic creation disabled, so
this is the only way topics come into existence. Existing topics are left
alone; a topic whose partition count differs from the configuration is
reported as an error rather than altered.

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
from confluent_kafka.admin import AdminClient, NewTopic

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
        log.error("kafka unreachable at %s: %s", bootstrap, e)
        return False

    ok = True
    missing = [t for t in topics if t.name not in existing]
    for t in topics:
        if t.name in existing:
            partitions = len(existing[t.name].partitions)
            if partitions != t.partitions:
                log.error("topic %s exists with %d partitions, config says %d; not altered", t.name, partitions, t.partitions)
                ok = False
            else:
                log.info("topic %s exists (%d partitions)", t.name, partitions)
    if missing:
        futures = admin.create_topics(
            [NewTopic(t.name, num_partitions=t.partitions, replication_factor=t.replication_factor) for t in missing],
            operation_timeout=TIMEOUT_S,
        )
        for name, future in futures.items():
            try:
                future.result()
                log.info("topic %s created", name)
            except KafkaException as e:
                log.error("topic %s not created: %s", name, e)
                ok = False
    return ok


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
        log.error("schema registry at %s: %s", registry_url, e)
        return False
    if level != COMPATIBILITY:
        log.error("schema registry compatibility reads back as %s, expected %s", level, COMPATIBILITY)
        return False
    log.info("schema registry global compatibility is %s", level)
    return True


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="provision: %(levelname)s %(message)s")
    try:
        load_config()
    except ConfigError as e:
        log.error("invalid configuration: %s", e)
        return 1
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    registry = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    topics_ok = provision_topics(bootstrap)
    compat_ok = set_compatibility(registry)
    return 0 if topics_ok and compat_ok else 1


if __name__ == "__main__":
    sys.exit(main())
