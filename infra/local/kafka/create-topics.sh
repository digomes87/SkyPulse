#!/usr/bin/env bash
# Creates the topics listed in a topics file. Safe to run repeatedly.
set -euo pipefail

bootstrap="$1"
topics_file="$2"

grep -Ev '^\s*(#|$)' "$topics_file" | while read -r topic partitions retention_ms; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$bootstrap" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config "retention.ms=$retention_ms"
done

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$bootstrap" --list
