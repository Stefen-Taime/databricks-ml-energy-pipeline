# =============================================================================
# Outputs — Useful values after terraform apply
# =============================================================================

# ---- GCS ----
output "gcs_bronze_bucket" {
  description = "GCS Bronze bucket name (raw data)"
  value       = module.gcs.bronze_bucket_name
}

output "gcs_silver_bucket" {
  description = "GCS Silver bucket name (cleaned Delta Lake)"
  value       = module.gcs.silver_bucket_name
}

output "gcs_gold_bucket" {
  description = "GCS Gold bucket name (predictions Delta Lake)"
  value       = module.gcs.gold_bucket_name
}

# ---- Pub/Sub ----
output "pubsub_topic" {
  description = "Pub/Sub topic for GCS Gold notifications"
  value       = module.snowpipe.pubsub_topic_name
}

output "pubsub_subscription" {
  description = "Pub/Sub subscription for Snowpipe"
  value       = module.snowpipe.pubsub_subscription_name
}

# ---- Snowflake ----
output "snowflake_database" {
  description = "Snowflake database name"
  value       = module.snowflake.database_name
}

output "snowflake_warehouse" {
  description = "Snowflake warehouse name"
  value       = module.snowflake.warehouse_name
}

output "snowpipe_name" {
  description = "Snowpipe name for auto-ingestion"
  value       = module.snowpipe.pipe_name
}

output "snowflake_storage_integration_name" {
  description = "Storage integration name"
  value       = module.snowpipe.integration_name
}

output "snowflake_notification_integration_name" {
  description = "Notification integration name (Pub/Sub)"
  value       = module.snowpipe.notification_integration_name
}

output "snowflake_notification_sa" {
  description = "GCP SA for notification integration — grant Pub/Sub Subscriber"
  value       = module.snowpipe.gcs_service_account_notification
}

output "snowflake_storage_integration_details" {
  description = "Storage integration describe output — look for STORAGE_GCP_SERVICE_ACCOUNT"
  value       = module.snowpipe.gcs_service_account_storage
}

# ---- Post-apply instructions ----
output "post_apply_instructions" {
  description = "Steps to complete after terraform apply"
  value       = <<-EOT

    ============================================================
    POST-APPLY STEPS (manual — one-time setup)
    ============================================================

    1. Grant Pub/Sub subscriber access to the notification SA:
         gcloud pubsub subscriptions add-iam-policy-binding snowpipe-gold-sub \
           --member="serviceAccount:${module.snowpipe.gcs_service_account_notification}" \
           --role="roles/pubsub.subscriber" \
           --project="${var.gcp_project_id}"

    2. Grant GCS read access to the storage SA:
       (Get the SA from: terraform output snowflake_storage_integration_details)
         gsutil iam ch serviceAccount:<STORAGE_GCP_SERVICE_ACCOUNT>:objectViewer \
           gs://${module.gcs.gold_bucket_name}

    3. Verify Snowpipe:
       In Snowflake, run:
         SELECT SYSTEM$PIPE_STATUS('${module.snowflake.database_name}.${module.snowflake.schema_predictions_name}.PREDICTIONS_PIPE');

    ============================================================
  EOT
}
