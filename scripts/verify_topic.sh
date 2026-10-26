#!/usr/bin/env bash
# Consumes the output topic, decoding the value via Schema Registry -- each
# message's schema ID is embedded in the payload itself (magic byte + ID),
# so the CLI resolves and decodes it the standard way.
set -euo pipefail
cd "$(dirname "$0")/.."

set -a
source .env
set +a

: "${CC_CLUSTER_ID:?missing in .env}"
: "${KAFKA_TOPIC:?missing in .env}"

confluent kafka topic consume "$KAFKA_TOPIC" \
  --cluster "$CC_CLUSTER_ID" \
  --from-beginning \
  --value-format protobuf
