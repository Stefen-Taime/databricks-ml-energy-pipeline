# =============================================================================
# Module Snowflake — Database, Schemas, Tables, Warehouse
# =============================================================================

# ---- Warehouse ----
resource "snowflake_warehouse" "this" {
  name           = var.warehouse_name
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = "true"
}

# ---- Database ----
resource "snowflake_database" "this" {
  name = var.database_name
}

# ---- Schemas ----
resource "snowflake_schema" "raw" {
  name     = "RAW"
  database = snowflake_database.this.name
}

resource "snowflake_schema" "predictions" {
  name     = "PREDICTIONS"
  database = snowflake_database.this.name
}

resource "snowflake_schema" "analytics" {
  name     = "ANALYTICS"
  database = snowflake_database.this.name
}

# =============================================================================
# Tables — Schema PREDICTIONS
# =============================================================================

# Main predictions table — fed by Snowpipe from GCS Gold
resource "snowflake_table" "predictions" {
  database = snowflake_database.this.name
  schema   = snowflake_schema.predictions.name
  name     = "ENERGY_PREDICTIONS"

  column {
    name = "USER_ID"
    type = "VARCHAR(50)"
  }

  column {
    name = "TIMESTAMP"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "KWH_PREDICTED"
    type = "FLOAT"
  }

  column {
    name = "MODEL_VERSION"
    type = "VARCHAR(50)"
  }

  column {
    name = "PREDICTION_DATE"
    type = "DATE"
  }

  column {
    name     = "LOADED_AT"
    type     = "TIMESTAMP_NTZ"
    nullable = true
  }
}

# Actual consumption table — for comparing predicted vs real
resource "snowflake_table" "actuals" {
  database = snowflake_database.this.name
  schema   = snowflake_schema.predictions.name
  name     = "ENERGY_ACTUALS"

  column {
    name = "USER_ID"
    type = "VARCHAR(50)"
  }

  column {
    name = "TIMESTAMP"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "KWH_ACTUAL"
    type = "FLOAT"
  }

  column {
    name     = "LOADED_AT"
    type     = "TIMESTAMP_NTZ"
    nullable = true
  }
}

# =============================================================================
# Tables — Schema ANALYTICS
# =============================================================================

# Households ACORN profiles for comparison features
resource "snowflake_table" "households" {
  database = snowflake_database.this.name
  schema   = snowflake_schema.analytics.name
  name     = "HOUSEHOLDS"

  column {
    name = "USER_ID"
    type = "VARCHAR(50)"
  }

  column {
    name = "ACORN_GROUP"
    type = "VARCHAR(20)"
  }

  column {
    name = "ACORN_CATEGORY"
    type = "VARCHAR(50)"
  }

  column {
    name = "TARIFF_TYPE"
    type = "VARCHAR(20)"
  }
}

# Model metrics — for monitoring model performance over time
resource "snowflake_table" "model_metrics" {
  database = snowflake_database.this.name
  schema   = snowflake_schema.analytics.name
  name     = "MODEL_METRICS"

  column {
    name = "MODEL_VERSION"
    type = "VARCHAR(50)"
  }

  column {
    name = "WALK_FORWARD_ROUND"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TRAIN_END_DATE"
    type = "DATE"
  }

  column {
    name = "TEST_MONTH"
    type = "VARCHAR(10)"
  }

  column {
    name = "MAPE"
    type = "FLOAT"
  }

  column {
    name = "RMSE"
    type = "FLOAT"
  }

  column {
    name = "MAE"
    type = "FLOAT"
  }

  column {
    name = "R2_SCORE"
    type = "FLOAT"
  }

  column {
    name     = "CREATED_AT"
    type     = "TIMESTAMP_NTZ"
    nullable = true
  }
}
