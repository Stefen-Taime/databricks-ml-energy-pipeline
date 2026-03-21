# =============================================================================
# Module Snowpipe — Outputs
# =============================================================================

output "pubsub_topic_name" {
  description = "Pub/Sub topic name"
  value       = google_pubsub_topic.snowpipe.name
}

output "pubsub_topic_id" {
  description = "Pub/Sub topic ID (for GCS notification)"
  value       = google_pubsub_topic.snowpipe.id
}

output "pubsub_subscription_name" {
  description = "Pub/Sub subscription name"
  value       = google_pubsub_subscription.snowpipe.name
}

output "pipe_name" {
  description = "Snowpipe name"
  value       = snowflake_pipe.predictions.name
}

output "integration_name" {
  description = "Storage integration name"
  value       = snowflake_storage_integration_gcs.this.name
}

output "notification_integration_name" {
  description = "Notification integration name (Pub/Sub)"
  value       = snowflake_notification_integration.gcs_pubsub.name
}

output "gcs_service_account_storage" {
  description = "GCP SA for storage integration (needs objectViewer on Gold bucket)"
  value       = snowflake_storage_integration_gcs.this.describe_output
}

output "gcs_service_account_notification" {
  description = "GCP SA for notification integration (needs Pub/Sub Subscriber)"
  value       = snowflake_notification_integration.gcs_pubsub.gcp_pubsub_service_account
}

output "pubsub_iam_ready" {
  description = "Dependency marker : IAM is configured"
  value       = true
  depends_on  = [google_pubsub_topic_iam_member.gcs_publisher]
}
