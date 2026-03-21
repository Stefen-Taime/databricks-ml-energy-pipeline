# =============================================================================
# Variables — ML Predict User Consumption
# =============================================================================

# ---- GCP ----
variable "gcp_project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "gcp_region" {
  description = "GCP region for the buckets"
  type        = string
  default     = "europe-west1"
}

variable "gcs_bucket_name" {
  description = "Base name for the GCS Medallion buckets (will be suffixed with -bronze, -silver, -gold)"
  type        = string
  default     = "ml-energy-consumption"
}

# ---- Local Data ----
variable "local_data_path" {
  description = "Chemin local vers le dossier data/ (dataset Kaggle dezipped)"
  type        = string
  default     = "../data"
}

# ---- Snowflake ----
variable "snowflake_organization_name" {
  description = "Snowflake organization name (e.g. KYEUCFS)"
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account name (e.g. BQ14035)"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake username"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
  sensitive   = true
}

variable "snowflake_role" {
  description = "Snowflake role to use"
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "snowflake_warehouse" {
  description = "Snowflake warehouse name to create"
  type        = string
  default     = "ML_ENERGY_WH"
}

variable "snowflake_database" {
  description = "Snowflake database name to create"
  type        = string
  default     = "ML_ENERGY_DB"
}
