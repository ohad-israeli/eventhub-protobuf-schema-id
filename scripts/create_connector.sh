#!/usr/bin/env bash
# Renders connector/connector-config.json against ../.env (never printed, never
# committed - see .gitignore) into connector/connector-config.local.json, then
# creates the fully managed Azure Event Hubs Source connector from it.
set -euo pipefail
cd "$(dirname "$0")/.."

set -a
source .env
set +a

: "${CC_CLUSTER_ID:?missing in .env}"

# Only substitute our own placeholders -- connector/connector-config.json also
# contains a literal "$Default" consumer group value that must NOT be touched.
envsubst '${CC_API_KEY} ${CC_API_SECRET} ${EVENTHUBS_SAS_KEYNAME} ${EVENTHUBS_SAS_KEY} ${EVENTHUB_NAMESPACE} ${CC_SMT_ARTIFACT_ID} ${TEMPERATURE_SCHEMA_ID} ${VIBRATION_SCHEMA_ID} ${PRESSURE_SCHEMA_ID}' \
  < connector/connector-config.json > connector/connector-config.local.json

confluent connect cluster create \
  --config-file connector/connector-config.local.json \
  --cluster "$CC_CLUSTER_ID"
