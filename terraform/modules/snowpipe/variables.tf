# =============================================================================
# Module Snowpipe — Variables
# =============================================================================

variable "database_name" {
  description = "Snowflake database name"
  type        = string
}

variable "schema_name" {
  description = "Snowflake schema name for Snowpipe resources"
  type        = string
}

variable "gold_bucket_name" {
  description = "GCS Gold bucket name"
  type        = string
}

variable "table_predictions_fqn" {
  description = "Fully qualified name of the target predictions table"
  type        = string
}

variable "table_actuals_fqn" {
  description = "Fully qualified name of the target actuals table"
  type        = string
}

variable "project_id" {
  description = "GCP Project ID (for Pub/Sub)"
  type        = string
}
