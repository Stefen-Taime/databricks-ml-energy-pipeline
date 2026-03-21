# =============================================================================
# Module GCS — Outputs
# =============================================================================

output "bronze_bucket_name" {
  description = "GCS Bronze bucket name"
  value       = google_storage_bucket.bronze.name
}

output "silver_bucket_name" {
  description = "GCS Silver bucket name"
  value       = google_storage_bucket.silver.name
}

output "gold_bucket_name" {
  description = "GCS Gold bucket name"
  value       = google_storage_bucket.gold.name
}
