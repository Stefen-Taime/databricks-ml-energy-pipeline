# =============================================================================
# Providers — GCP + Snowflake
# =============================================================================

terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  password          = var.snowflake_password
  role              = var.snowflake_role

  preview_features_enabled = [
    "snowflake_table_resource",
    "snowflake_storage_integration_gcs_resource",
    "snowflake_file_format_resource",
    "snowflake_stage_external_gcs_resource",
    "snowflake_pipe_resource",
    "snowflake_notification_integration_resource",
  ]
}
