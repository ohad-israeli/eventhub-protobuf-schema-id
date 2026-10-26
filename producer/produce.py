#!/usr/bin/env python3
"""
Publishes simulated industrial-equipment sensor readings to Azure Event Hubs
as raw, native Protobuf bytes — the way a real IoT/edge client would, with
no Confluent wire format (no magic byte, no embedded schema ID) involved.

Each event type (temperature/vibration/pressure) has its own distinct
Protobuf schema, registered under its own Schema Registry subject.

Run `scripts/generate_proto.sh` first to produce the *_pb2.py modules.

Required env vars:
  EVENTHUB_CONNECTION_STR   Namespace-level or Event Hub-level connection string
  EVENTHUB_NAME             Name of the event hub (ignored if the connection
                             string already targets a specific hub)
"""
import argparse
import os
import random
import time

from azure.eventhub import EventData, EventHubProducerClient

import temperature_reading_pb2 as temp_pb
import vibration_reading_pb2 as vib_pb
import pressure_reading_pb2 as pres_pb

EQUIPMENT = [
    ("pump-07", "pump", "plant-a-line-3"),
    ("pump-11", "pump", "plant-a-line-3"),
    ("compressor-02", "compressor", "plant-a-line-1"),
    ("compressor-05", "compressor", "plant-b-line-2"),
    ("motor-19", "motor", "plant-b-line-2"),
]

SCHEMA_VERSION = 1


def build_temperature() -> temp_pb.TemperatureReading:
    equipment_id, equipment_type, site = random.choice(EQUIPMENT)
    return temp_pb.TemperatureReading(
        equipment_id=equipment_id,
        equipment_type=equipment_type,
        value=round(random.gauss(68.0, 6.0), 2),
        unit="C",
        timestamp_ms=int(time.time() * 1000),
        site=site,
    )


def build_vibration() -> vib_pb.VibrationReading:
    equipment_id, equipment_type, site = random.choice(EQUIPMENT)
    return vib_pb.VibrationReading(
        equipment_id=equipment_id,
        equipment_type=equipment_type,
        value=round(random.gauss(2.3, 0.8), 2),
        unit="mm/s",
        timestamp_ms=int(time.time() * 1000),
        site=site,
        axis=random.choice(["X", "Y", "Z"]),
    )


def build_pressure() -> pres_pb.PressureReading:
    equipment_id, equipment_type, site = random.choice(EQUIPMENT)
    return pres_pb.PressureReading(
        equipment_id=equipment_id,
        equipment_type=equipment_type,
        value=round(random.gauss(112.0, 10.0), 2),
        unit="psi",
        timestamp_ms=int(time.time() * 1000),
        site=site,
        max_rated_psi=150.0,
    )


EVENT_TYPES = {
    "TEMPERATURE": {
        "build": build_temperature,
        "topic": "iot.temperature-readings",
        "schema_subject": "iot.temperature-readings-value",
    },
    "VIBRATION": {
        "build": build_vibration,
        "topic": "iot.vibration-readings",
        "schema_subject": "iot.vibration-readings-value",
    },
    "PRESSURE": {
        "build": build_pressure,
        "topic": "iot.pressure-readings",
        "schema_subject": "iot.pressure-readings-value",
    },
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=20, help="number of readings to send")
    parser.add_argument("--interval", type=float, default=0.5, help="seconds between readings")
    args = parser.parse_args()

    conn_str = os.environ["EVENTHUB_CONNECTION_STR"]
    eventhub_name = os.environ.get("EVENTHUB_NAME")

    producer = EventHubProducerClient.from_connection_string(
        conn_str=conn_str, eventhub_name=eventhub_name
    )

    with producer:
        for i in range(args.count):
            sensor_type = random.choice(list(EVENT_TYPES))
            profile = EVENT_TYPES[sensor_type]
            reading = profile["build"]()

            # SerializeToString() is the plain, native Protobuf wire format --
            # no Confluent magic byte / schema ID prefix.
            payload = reading.SerializeToString()

            event = EventData(payload)
            # Plain, human-readable properties -- carried through for the eventual
            # schema-ID-header step; not used for routing (see partition_key below).
            event.properties = {
                "schema_subject": profile["schema_subject"],
                "schema_version": SCHEMA_VERSION,
            }

            # Event Hubs' partition_key survives as its own standalone Kafka header
            # (x-opt-partition-key), and also avoids a Confluent Cloud bug where
            # AzureEventHubsSource crashes any attached custom SMT on records with
            # no partition_key set.
            batch = producer.create_batch(partition_key=profile["topic"])
            batch.add(event)
            producer.send_batch(batch)

            print(
                f"[{i + 1}/{args.count}] sent {reading.equipment_id} "
                f"{sensor_type}={reading.value}{reading.unit} "
                f"({len(payload)} bytes, raw protobuf) -> {profile['topic']}"
            )
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
