# =============================================================================
# Module GCS — Variables
# =============================================================================

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
}

variable "bucket_base_name" {
  description = "Base name for Medallion buckets (suffixed with -bronze, -silver, -gold)"
  type        = string
}

variable "local_data_path" {
  description = "Chemin local vers le dossier data/ (dataset Kaggle)"
  type        = string
}

variable "pubsub_topic_id" {
  description = "Pub/Sub topic ID for GCS Gold notifications (Snowpipe)"
  type        = string
}

variable "pubsub_iam_ready" {
  description = "Dependency marker : Pub/Sub IAM must be set before notification"
  type        = bool
  default     = true
}
