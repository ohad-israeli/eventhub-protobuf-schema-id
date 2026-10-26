#!/usr/bin/env bash
# Registers the three event-type schemas under the subjects the connector will
# look up (TopicNameStrategy -> "<topic>-value"), using your logged-in
# `confluent login` session (no API key needed). Each schema's registered ID
# is immutable once assigned -- note the three IDs this prints; they go into
# connector/connector-config.json's subject.id.map.
set -euo pipefail
cd "$(dirname "$0")/.."

: "${CC_ENVIRONMENT_ID:?set CC_ENVIRONMENT_ID (see .env after terraform apply)}"
confluent environment use "$CC_ENVIRONMENT_ID"

confluent schema-registry schema create \
  --subject iot.temperature-readings-value \
  --schema proto/temperature_reading.proto \
  --type protobuf

confluent schema-registry schema create \
  --subject iot.vibration-readings-value \
  --schema proto/vibration_reading.proto \
  --type protobuf

confluent schema-registry schema create \
  --subject iot.pressure-readings-value \
  --schema proto/pressure_reading.proto \
  --type protobuf
