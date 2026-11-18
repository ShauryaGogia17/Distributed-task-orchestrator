#!/usr/bin/env bash
# Idempotent bootstrap; any CLI error aborts the one-shot service.
set -euo pipefail

while read -r topic partitions; do
    /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka:19092 \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor 1
done < /opt/bootstrap/kafka-topics.txt

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe
