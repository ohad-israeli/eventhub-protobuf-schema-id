output "environment_id" {
  value       = confluent_environment.this.id
  description = "Confluent Cloud environment ID."
}

output "cluster_id" {
  value       = confluent_kafka_cluster.basic.id
  description = "Kafka cluster ID (lkc-...)."
}

output "cluster_bootstrap" {
  value       = confluent_kafka_cluster.basic.bootstrap_endpoint
  description = "Kafka bootstrap endpoint."
}

output "cluster_rest_endpoint" {
  value       = confluent_kafka_cluster.basic.rest_endpoint
  description = "Kafka REST endpoint."
}

output "schema_registry_url" {
  value       = data.confluent_schema_registry_cluster.sr.rest_endpoint
  description = "Schema Registry endpoint."
}

output "kafka_topic" {
  value       = confluent_kafka_topic.sensor_readings.topic_name
  description = "Output topic the connector writes into."
}

# Sensitive credential outputs (view with: terraform output -raw <name>)
output "cluster_api_key" {
  value     = confluent_api_key.cluster.id
  sensitive = true
}
output "cluster_api_secret" {
  value     = confluent_api_key.cluster.secret
  sensitive = true
}
output "schema_registry_api_key" {
  value     = confluent_api_key.schema_registry.id
  sensitive = true
}
output "schema_registry_api_secret" {
  value     = confluent_api_key.schema_registry.secret
  sensitive = true
}

# Convenience: append the demo's .env in one shot:
#   terraform output -raw dotenv >> ../.env
output "dotenv" {
  sensitive = true
  value     = <<-EOT
    CC_ENVIRONMENT_ID=${confluent_environment.this.id}
    CC_CLUSTER_ID=${confluent_kafka_cluster.basic.id}
    CC_BOOTSTRAP=${replace(confluent_kafka_cluster.basic.bootstrap_endpoint, "SASL_SSL://", "")}
    CC_REST_ENDPOINT=${confluent_kafka_cluster.basic.rest_endpoint}
    CC_API_KEY=${confluent_api_key.cluster.id}
    CC_API_SECRET=${confluent_api_key.cluster.secret}
    CC_SR_URL=${data.confluent_schema_registry_cluster.sr.rest_endpoint}
    CC_SR_API_KEY=${confluent_api_key.schema_registry.id}
    CC_SR_API_SECRET=${confluent_api_key.schema_registry.secret}
    KAFKA_TOPIC=${confluent_kafka_topic.sensor_readings.topic_name}
  EOT
}
