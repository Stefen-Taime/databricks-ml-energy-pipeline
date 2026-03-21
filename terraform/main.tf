# =============================================================================
# Main — Appel des modules
# =============================================================================

# ---- Module Snowpipe (Pub/Sub + Snowflake integration) ----
# Doit etre cree AVANT le module GCS (la notification Gold a besoin du topic)
module "snowpipe" {
  source = "./modules/snowpipe"

  project_id            = var.gcp_project_id
  database_name         = module.snowflake.database_name
  schema_name           = module.snowflake.schema_predictions_name
  gold_bucket_name      = module.gcs.gold_bucket_name
  table_predictions_fqn = module.snowflake.table_predictions_fqn
  table_actuals_fqn     = module.snowflake.table_actuals_fqn
}

# ---- Module GCS (Buckets Medallion + Upload Kaggle) ----
module "gcs" {
  source = "./modules/gcs"

  project_id       = var.gcp_project_id
  region           = var.gcp_region
  bucket_base_name = var.gcs_bucket_name
  local_data_path  = var.local_data_path
  pubsub_topic_id  = module.snowpipe.pubsub_topic_id
  pubsub_iam_ready = module.snowpipe.pubsub_iam_ready
}

# ---- Module Snowflake (Database, Schemas, Tables, Warehouse) ----
module "snowflake" {
  source = "./modules/snowflake"

  warehouse_name = var.snowflake_warehouse
  database_name  = var.snowflake_database
}
