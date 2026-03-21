# =============================================================================
# Module Snowflake — Outputs
# =============================================================================

output "database_name" {
  description = "Snowflake database name"
  value       = snowflake_database.this.name
}

output "warehouse_name" {
  description = "Snowflake warehouse name"
  value       = snowflake_warehouse.this.name
}

output "schema_predictions_name" {
  description = "Snowflake PREDICTIONS schema name"
  value       = snowflake_schema.predictions.name
}

output "schema_analytics_name" {
  description = "Snowflake ANALYTICS schema name"
  value       = snowflake_schema.analytics.name
}

output "table_predictions_fqn" {
  description = "Fully qualified name of the predictions table"
  value       = snowflake_table.predictions.fully_qualified_name
}

output "table_actuals_fqn" {
  description = "Fully qualified name of the actuals table"
  value       = snowflake_table.actuals.fully_qualified_name
}
