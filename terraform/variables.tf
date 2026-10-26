variable "confluent_cloud_api_key" {
  type        = string
  sensitive   = true
  description = "Confluent Cloud API key (Cloud/resource-management key)."
}

variable "confluent_cloud_api_secret" {
  type        = string
  sensitive   = true
  description = "Confluent Cloud API secret."
}

variable "environment_name" {
  type        = string
  default     = "eventhub-protobuf-schema-headers"
  description = "Display name for the dedicated Confluent Cloud environment."
}

variable "cluster_name" {
  type        = string
  default     = "eventhub-schema-headers"
  description = "Display name for the Basic Kafka cluster."
}

variable "cloud" {
  type        = string
  default     = "AZURE"
  description = "Cloud provider for the cluster. AZURE, to sit alongside the Event Hubs namespace."
}

variable "region" {
  type        = string
  default     = "eastus"
  description = "Azure region (Confluent naming). Matches the Event Hubs namespace region."
}

variable "availability" {
  type        = string
  default     = "SINGLE_ZONE"
  description = "SINGLE_ZONE is the only option for a Basic cluster."
}

variable "stream_governance_package" {
  type        = string
  default     = "ESSENTIALS"
  description = "Stream Governance / Schema Registry package: ESSENTIALS or ADVANCED."
}

variable "kafka_topic" {
  type        = string
  default     = "iot.sensor-readings"
  description = "Static landing topic required by the Azure Event Hubs Source connector config. Unused in practice once the custom SMT reroutes every record to one of var.event_type_topics."
}

variable "event_type_topics" {
  type        = list(string)
  default     = ["iot.temperature-readings", "iot.vibration-readings", "iot.pressure-readings"]
  description = "Per-event-type destination topics the custom SMT routes records into, derived from each message's declared schema subject (\"<topic>-value\")."
}
