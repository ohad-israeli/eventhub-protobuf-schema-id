# Schema-tagging raw Protobuf from Azure Event Hubs into Confluent Cloud

> Article: pending — link will be added once published.

Industrial equipment publishes sensor readings (temperature, vibration, pressure) as
native Protobuf bytes straight to Azure Event Hubs — no Confluent client, no Confluent
wire format, just `SerializeToString()`. The fully managed **Azure Event Hubs Source**
connector in Confluent Cloud ingests those events, and a custom Single Message
Transform stamps each record with its Protobuf schema ID and fans the single Event Hub
out into three topics — one per event type, each with its own distinct, independently
registered Protobuf schema — based entirely on what the producer declares per message.

The original plan was to use Confluent's newer [schema IDs in Kafka
headers](https://www.confluent.io/blog/schema-id-kafka-headers-data-governance/)
convention, attaching the ID to a `__value_schema_id` header so the payload bytes stay
byte-for-byte untouched. That turned out not to be viable through this connector's
custom-SMT execution path: a binary header value doesn't survive the round trip intact
(the proxy's internal transport consistently altered the raw bytes, however the header
was typed). We reported this to Confluent and pivoted to the mechanism below, which
*is* reliable here: the SMT wraps the payload in Confluent's standard wire format
(magic byte + schema ID + payload) — the classic, well-established way a schema ID
travels with a message — instead of a header.

## The gotcha that cost the most time

**`AzureEventHubsSource` crashes any attached custom SMT — but only on records whose
Event Hub message has no `partition_key` set.** The connector's custom-SMT execution
path serializes each record through an internal schema-aware converter before handing
it to the transform; that converter has a required field with no default that's null
whenever `partition_key` is unset, and the whole task dies with a generic "contact
Support" error that gives no indication this is the cause.

This was reproduced and isolated cleanly: the identical SMT jar works without
modification on `DatagenSource` (structured output) and on `AzureBlobSource` with
`input.data.format=BYTES` processing the exact same Protobuf bytes, and on a
self-managed (non-Confluent-Cloud) Kafka Connect worker processing genuine
`byte[]`-valued records against the same cluster. It only fails on
`AzureEventHubsSource`, and only for unkeyed messages — a single isolated test message
with `partition_key` unset reliably crashes the task; the identical message with any
non-null `partition_key` processes correctly every time.

**The fix: always set a `partition_key` on every Event Hub message**, even if you don't
need it for anything else. `produce.py` does this unconditionally.

## What this repo demonstrates

- A producer publishing realistic IoT sensor readings to Azure Event Hubs as raw,
  un-prefixed Protobuf — exactly as a native (non-Confluent) device/edge client would.
- A custom SMT (`smt/`) that, per record:
  1. reads a `schema_subject` declaration the producer attaches as a plain Event Hub
     property (bundled by the connector into one `properties` header — see below),
  2. resolves that to a schema ID via a static map (populated once, offline — a
     specific registered version's ID never changes, so there's nothing to cache or
     invalidate),
  3. rewrites the record's value into Confluent's standard wire format (magic byte +
     4-byte schema ID + Protobuf message-index byte + the original payload bytes,
     untouched), and
  4. reroutes the record to the topic matching its declared subject
     (`<topic>-value` → `<topic>`),

  letting **one Event Hub carry multiple event types/schemas**, fanning out to multiple
  Kafka topics, driven entirely by what the producer declares per message — no
  per-event-type connector instance needed.
- Verification that messages land, correctly routed, schema-tagged, and decodable, in
  Confluent Cloud.

**Out of scope:** any downstream consumption (e.g. a Snowflake sink) — this repo stops
at proving the data is correctly ingested, schematized, and routed.

## Why the `properties` header needs custom code at all

Azure Event Hubs speaks AMQP, not Kafka. Custom producer metadata (`EventData.properties`)
is a single AMQP `application-properties` map — not a list of discrete Kafka headers.
The connector (reading via AMQP) has to flatten that whole map into **one** Kafka header,
literally named `properties`, as a JSON-ish blob:

```
properties: {"schema_subject":"iot.pressure-readings-value"}
```

No built-in SMT can parse this apart:
- `ExtractTopic$Header` only supports literal/whole-header values for the `$Header`
  variant — no JSON-path extraction (confirmed in Confluent's own docs; `$Key`/`$Value`
  support it, `$Header` doesn't).
- `HeaderToValue` can extract structured data, but requires the record's *value* to
  already be a `Struct` — ours is raw bytes, and it fails with a precise
  `DataException: Only Struct objects supported for [Header field insertion], found: [B]`.

Note: the "properties" header's actual in-memory type observed at runtime is a
`java.util.Map`, not a JSON string — the CLI's display of it as quoted JSON text is
just a rendering choice. The SMT handles both forms.

Routing alone (without schema-tagging the value) *can* be done with zero custom code:
setting `partition_key` on the Event Hub message survives as its own standalone header
(`x-opt-partition-key`, not bundled like `properties`), which the built-in
`io.confluent.connect.transforms.ExtractTopic$Header` can route on directly. This repo
uses the custom SMT because it also needs to schema-tag the value, which no built-in
transform can do.

## Layout

```
proto/
  temperature_reading.proto   # three distinct schemas, one per event type
  vibration_reading.proto
  pressure_reading.proto
producer/
  produce.py                  # sends raw protobuf + partition_key + properties
smt/                           # the custom SMT (Java/Maven)
  src/main/java/com/ohadisraeli/smt/SchemaIdValuePrefixRouter.java
connector/
  connector-config.json        # fully managed Azure Event Hubs Source connector config
scripts/
  generate_proto.sh            # compiles the .proto files to *_pb2.py
  register_schema.sh           # registers the three schemas in Schema Registry
  create_connector.sh          # builds + creates the connector from connector-config.json
  verify_topic.sh              # consumes a topic and shows the decoded value
terraform/                     # Confluent Cloud environment, cluster, topics
```

## Prerequisites

- An Azure Event Hubs namespace + event hub with a SAS policy for Send + Listen.
- A Confluent Cloud environment with a Kafka cluster and Schema Registry, a Kafka API
  key/secret, and the Confluent CLI logged in.
- Python 3.10+, Java 11+, Maven.

## Run it

```bash
# 1. Confluent Cloud infra (environment, cluster, topics) via terraform/
# 2. Compile the schemas and producer deps
pip install -r producer/requirements.txt
scripts/generate_proto.sh

# 3. Register the three schemas -- note the printed IDs
scripts/register_schema.sh

# 4. Build and upload the custom SMT
cd smt && mvn package && cd ..
confluent connect artifact create schema-id-value-prefix-smt \
  --artifact-file smt/target/schema-id-value-prefix-smt-1.0.0.jar \
  --cloud azure --environment <env-id> --description "schema ID value-prefix + routing SMT"

# 5. Fill in connector/connector-config.json's placeholders (SMT artifact ID, the
#    three schema IDs from step 3), then:
scripts/create_connector.sh

# 6. Produce
export EVENTHUB_CONNECTION_STR="Endpoint=sb://<namespace>.servicebus.windows.net/;..."
export EVENTHUB_NAME=sensor-readings
python3 producer/produce.py --count 50

# 7. Verify
KAFKA_TOPIC=iot.temperature-readings scripts/verify_topic.sh
```

Each topic's messages should show "Schema associated" with that topic's own schema in
Confluent Cloud's Topic viewer, decoding correctly straight from the payload.

## Status

Proven live end-to-end: real Protobuf traffic, through the real connector, correctly
schema-tagged and routed across three topics with three distinct schemas, connector
staying healthy throughout. See the article for the full story, including the
header-based approach that didn't pan out.
