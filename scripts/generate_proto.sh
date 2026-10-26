#!/usr/bin/env bash
# Compiles proto/*.proto (one schema per event type) to producer/*_pb2.py.
# Requires: pip install grpcio-tools (or a standalone `protoc` on PATH).
set -euo pipefail
cd "$(dirname "$0")/.."

python3 -m grpc_tools.protoc \
  -I proto \
  --python_out=producer \
  proto/temperature_reading.proto \
  proto/vibration_reading.proto \
  proto/pressure_reading.proto

echo "Generated producer/{temperature,vibration,pressure}_reading_pb2.py"
