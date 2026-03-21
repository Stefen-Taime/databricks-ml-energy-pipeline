# =============================================================================
# Module GCS — Buckets Medallion + Upload donnees Kaggle
# =============================================================================

# ---- Bronze (Raw Data) ----
resource "google_storage_bucket" "bronze" {
  name     = "${var.bucket_base_name}-bronze"
  location = var.region
  project  = var.project_id

  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}

# ---- Silver (Cleaned — Delta Lake) ----
resource "google_storage_bucket" "silver" {
  name     = "${var.bucket_base_name}-silver"
  location = var.region
  project  = var.project_id

  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }
}

# ---- Gold (Predictions — Delta Lake) ----
resource "google_storage_bucket" "gold" {
  name     = "${var.bucket_base_name}-gold"
  location = var.region
  project  = var.project_id

  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }
}

# =============================================================================
# Upload des donnees statiques Kaggle vers Bronze (gsutil)
# =============================================================================

resource "null_resource" "upload_smart_meters" {
  depends_on = [google_storage_bucket.bronze]

  provisioner "local-exec" {
    command = "gsutil -m cp ${var.local_data_path}/halfhourly_dataset/halfhourly_dataset/block_*.csv gs://${google_storage_bucket.bronze.name}/smart_meters/"
  }

  triggers = {
    bucket = google_storage_bucket.bronze.name
  }
}

resource "null_resource" "upload_households" {
  depends_on = [google_storage_bucket.bronze]

  provisioner "local-exec" {
    command = <<-EOT
      gsutil cp ${var.local_data_path}/informations_households.csv gs://${google_storage_bucket.bronze.name}/households/
      gsutil cp ${var.local_data_path}/acorn_details.csv gs://${google_storage_bucket.bronze.name}/households/
    EOT
  }

  triggers = {
    bucket = google_storage_bucket.bronze.name
  }
}

resource "null_resource" "upload_weather" {
  depends_on = [google_storage_bucket.bronze]

  provisioner "local-exec" {
    command = "gsutil cp ${var.local_data_path}/weather_hourly_darksky.csv gs://${google_storage_bucket.bronze.name}/weather/"
  }

  triggers = {
    bucket = google_storage_bucket.bronze.name
  }
}

resource "null_resource" "upload_holidays" {
  depends_on = [google_storage_bucket.bronze]

  provisioner "local-exec" {
    command = "gsutil cp ${var.local_data_path}/uk_bank_holidays.csv gs://${google_storage_bucket.bronze.name}/holidays/"
  }

  triggers = {
    bucket = google_storage_bucket.bronze.name
  }
}

# ---- Placeholders pour les dossiers API (remplis par Notebook 1) ----
resource "google_storage_bucket_object" "bronze_neso" {
  name    = "neso/"
  bucket  = google_storage_bucket.bronze.name
  content = " "
}

resource "google_storage_bucket_object" "bronze_elexon" {
  name    = "elexon/"
  bucket  = google_storage_bucket.bronze.name
  content = " "
}

resource "google_storage_bucket_object" "bronze_carbon" {
  name    = "carbon/"
  bucket  = google_storage_bucket.bronze.name
  content = " "
}

# ---- GCS Notification (Pub/Sub) for Snowpipe on Gold bucket ----
resource "google_storage_notification" "gold_snowpipe_notification" {
  bucket         = google_storage_bucket.gold.name
  payload_format = "JSON_API_V1"
  topic          = var.pubsub_topic_id
  event_types    = ["OBJECT_FINALIZE"]
}
