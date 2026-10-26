# ------------------------------------------------------------------------------
# Confluent Cloud side of the demo: a dedicated environment + a Basic Kafka
# cluster on AZURE (same cloud/region as the Event Hubs namespace, so the fully
# managed Azure Event Hubs Source connector has the simplest possible path to
# it), Schema Registry (auto-provisioned once the first cluster exists), and
# the service account + API keys the connector and verification scripts use.
#
# Deliberately a fresh environment rather than reusing a shared SE-team one —
# every prior project in this workspace does the same for isolation + clean
# teardown.
# ------------------------------------------------------------------------------

resource "confluent_environment" "this" {
  display_name = var.environment_name

  stream_governance {
    package = var.stream_governance_package
  }
}

resource "confluent_kafka_cluster" "basic" {
  display_name = var.cluster_name
  availability = var.availability
  cloud        = var.cloud
  region       = var.region

  basic {}

  environment {
    id = confluent_environment.this.id
  }
}

resource "confluent_service_account" "app" {
  display_name = "${var.cluster_name}-sa"
  description  = "eventhub-protobuf-schema-headers demo service account"
}

resource "confluent_role_binding" "app_cluster_admin" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.basic.rbac_crn
}

resource "confluent_api_key" "cluster" {
  display_name = "${var.cluster_name}-cluster-key"
  description  = "Kafka API key for the Basic cluster"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.basic.id
    api_version = confluent_kafka_cluster.basic.api_version
    kind        = confluent_kafka_cluster.basic.kind
    environment {
      id = confluent_environment.this.id
    }
  }

  depends_on = [confluent_role_binding.app_cluster_admin]
}

# Schema Registry is auto-provisioned for the environment once the first Kafka
# cluster exists.
data "confluent_schema_registry_cluster" "sr" {
  environment {
    id = confluent_environment.this.id
  }
  depends_on = [confluent_kafka_cluster.basic]
}

resource "confluent_role_binding" "app_sr" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "DeveloperManage"
  crn_pattern = "${data.confluent_schema_registry_cluster.sr.resource_name}/subject=*"
}

resource "confluent_api_key" "schema_registry" {
  display_name = "${var.cluster_name}-sr-key"
  description  = "Schema Registry API key"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr.id
    api_version = data.confluent_schema_registry_cluster.sr.api_version
    kind        = data.confluent_schema_registry_cluster.sr.kind
    environment {
      id = confluent_environment.this.id
    }
  }

  depends_on = [confluent_role_binding.app_sr]
}

# Explicit topic (rather than relying on connector auto-create, which Confluent
# Cloud brokers disable by default). Kept as the connector's required static
# "kafka.topic" landing value, but the custom SMT reroutes every record away
# from it to one of the per-event-type topics below.
resource "confluent_kafka_topic" "sensor_readings" {
  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }
  topic_name       = var.kafka_topic
  partitions_count = 6
  rest_endpoint    = confluent_kafka_cluster.basic.rest_endpoint

  credentials {
    key    = confluent_api_key.cluster.id
    secret = confluent_api_key.cluster.secret
  }
}

# Per-event-type destination topics. The custom SMT routes each record here
# based on the "schema_subject" the producer declares per message (one Event
# Hub, multiple event types/schemas, fanned out by the SMT -- not the
# connector, which only supports a single static output topic).
resource "confluent_kafka_topic" "event_type_topics" {
  for_each = toset(var.event_type_topics)

  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }
  topic_name       = each.value
  partitions_count = 6
  rest_endpoint    = confluent_kafka_cluster.basic.rest_endpoint

  credentials {
    key    = confluent_api_key.cluster.id
    secret = confluent_api_key.cluster.secret
  }
}
