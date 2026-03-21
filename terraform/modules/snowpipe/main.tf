# =============================================================================
# Module Snowpipe — Auto-ingestion from GCS Gold -> Snowflake
# =============================================================================

# ---- Pub/Sub Topic & Subscription ----
resource "google_pubsub_topic" "snowpipe" {
  name    = "snowpipe-gold-notifications"
  project = var.project_id
}

# Grant GCS service account permission to publish to the topic
data "google_storage_project_service_account" "gcs_sa" {
  project = var.project_id
}

resource "google_pubsub_topic_iam_member" "gcs_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.snowpipe.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${data.google_storage_project_service_account.gcs_sa.email_address}"
}

resource "google_pubsub_subscription" "snowpipe" {
  name    = "snowpipe-gold-sub"
  project = var.project_id
  topic   = google_pubsub_topic.snowpipe.name

  message_retention_duration = "604800s" # 7 days
  ack_deadline_seconds       = 20

  expiration_policy {
    ttl = "" # Never expires
  }
}

# ---- Storage Integration (Snowflake <-> GCS) ----
resource "snowflake_storage_integration_gcs" "this" {
  name    = "GCS_GOLD_INTEGRATION"
  enabled = true

  storage_allowed_locations = [
    "gcs://${var.gold_bucket_name}/"
  ]
}

# ---- File Format ----
# USE_LOGICAL_TYPE = TRUE est CRITIQUE pour que Snowflake interprete
# correctement les timestamps Parquet (microseconds/nanoseconds).
# Sans ca, Snowflake traite les int64 comme des jours -> dates corrompues (SNOW-884196)
resource "snowflake_file_format" "parquet" {
  name     = "PARQUET_FORMAT"
  database = var.database_name
  schema   = var.schema_name

  format_type = "PARQUET"

  comment = "Parquet format with logical type support for timestamps"
}

# ---- External Stage ----
resource "snowflake_stage_external_gcs" "gold" {
  name     = "GCS_GOLD_STAGE"
  database = var.database_name
  schema   = var.schema_name

  url                 = "gcs://${var.gold_bucket_name}/"
  storage_integration = snowflake_storage_integration_gcs.this.name
}

# ---- Notification Integration (Pub/Sub -> Snowflake) ----
resource "snowflake_notification_integration" "gcs_pubsub" {
  name    = "GCS_GOLD_NOTIFICATION"
  enabled = true

  notification_provider       = "GCP_PUBSUB"
  gcp_pubsub_subscription_name = google_pubsub_subscription.snowpipe.id
}

# ---- Snowpipe : Predictions ----
# Ingere les Parquet depuis gs://gold/predictions/ -> ENERGY_PREDICTIONS
# USE_LOGICAL_TYPE = TRUE : interprete correctement les timestamps Parquet
resource "snowflake_pipe" "predictions" {
  name     = "PREDICTIONS_PIPE"
  database = var.database_name
  schema   = var.schema_name

  copy_statement = "COPY INTO ${var.table_predictions_fqn} FROM @${snowflake_stage_external_gcs.gold.fully_qualified_name}/predictions/ FILE_FORMAT = (TYPE = 'PARQUET' USE_LOGICAL_TYPE = TRUE) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"

  auto_ingest = true
  integration = snowflake_notification_integration.gcs_pubsub.name
}

# ---- Snowpipe : Actuals ----
# Ingere les Parquet depuis gs://gold/actuals/ -> ENERGY_ACTUALS
resource "snowflake_pipe" "actuals" {
  name     = "ACTUALS_PIPE"
  database = var.database_name
  schema   = var.schema_name

  copy_statement = "COPY INTO ${var.table_actuals_fqn} FROM @${snowflake_stage_external_gcs.gold.fully_qualified_name}/actuals/ FILE_FORMAT = (TYPE = 'PARQUET' USE_LOGICAL_TYPE = TRUE) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"

  auto_ingest = true
  integration = snowflake_notification_integration.gcs_pubsub.name
}
