# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline DLT / Lakeflow Declarative -- Reference
# MAGIC
# MAGIC **IMPORTANT** : Ce notebook est une reference pour un futur refactoring vers
# MAGIC Delta Live Tables (renomme Lakeflow Spark Declarative Pipelines).
# MAGIC
# MAGIC **Pre-requis** : Databricks **Premium Edition** (DLT non disponible sur Free Edition).
# MAGIC
# MAGIC Ce notebook remplace les 4 notebooks actuels (01_ingestion, 02_nettoyage,
# MAGIC 03_training, 04_inference) par un pipeline declaratif unique avec :
# MAGIC - Gestion automatique de la qualite (`@dlt.expect`)
# MAGIC - Lineage natif
# MAGIC - Auto-recovery en cas d'echec
# MAGIC - Monitoring integre
# MAGIC
# MAGIC **Architecture cible :**
# MAGIC ```
# MAGIC GCS Bronze (raw) --> [DLT Bronze tables] --> [DLT Silver tables] --> [DLT Gold tables]
# MAGIC                          expectations           nettoyage             predictions
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration GCS (via Databricks Secrets)
BUCKET_BRONZE = "ml-energy-consumption-bronze"
BUCKET_SILVER = "ml-energy-consumption-silver"
BUCKET_GOLD = "ml-energy-consumption-gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer -- Ingestion brute
# MAGIC
# MAGIC Les tables Bronze sont des copies fideles des donnees sources,
# MAGIC avec des expectations minimales (non-null, format valide).

# COMMAND ----------

@dlt.table(
    name="bronze_smart_meters",
    comment="Smart meters raw data - half-hourly energy consumption per household",
    table_properties={"quality": "bronze"},
)
@dlt.expect_or_drop("valid_lclid", "LCLid IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "tstp IS NOT NULL")
def bronze_smart_meters():
    """Charge les 112 blocs CSV Smart Meters depuis GCS Bronze."""
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"gs://{BUCKET_BRONZE}/smart_meters/block_*.csv")
    )

# COMMAND ----------

@dlt.table(
    name="bronze_weather",
    comment="DarkSky hourly weather data for London",
    table_properties={"quality": "bronze"},
)
@dlt.expect("valid_time", "time IS NOT NULL")
def bronze_weather():
    """Charge les donnees meteo horaires depuis GCS Bronze."""
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"gs://{BUCKET_BRONZE}/weather/weather_hourly_darksky.csv")
    )

# COMMAND ----------

@dlt.table(
    name="bronze_households",
    comment="Household information with ACORN socio-demographic profiles",
    table_properties={"quality": "bronze"},
)
@dlt.expect_or_drop("valid_lclid", "LCLid IS NOT NULL")
def bronze_households():
    """Charge les informations foyers depuis GCS Bronze."""
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"gs://{BUCKET_BRONZE}/households/informations_households.csv")
    )

# COMMAND ----------

@dlt.table(
    name="bronze_holidays",
    comment="UK bank holidays",
    table_properties={"quality": "bronze"},
)
def bronze_holidays():
    """Charge les jours feries UK depuis GCS Bronze."""
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"gs://{BUCKET_BRONZE}/holidays/uk_bank_holidays.csv")
    )

# COMMAND ----------

@dlt.table(
    name="bronze_neso_demand",
    comment="NESO national demand data (API-sourced JSON)",
    table_properties={"quality": "bronze"},
)
def bronze_neso_demand():
    """Charge les donnees NESO depuis GCS Bronze (si disponible)."""
    return spark.read.json(f"gs://{BUCKET_BRONZE}/neso/demand_national_raw.json")

# COMMAND ----------

@dlt.table(
    name="bronze_elexon_prices",
    comment="Elexon BMRS system prices (API-sourced JSON)",
    table_properties={"quality": "bronze"},
)
def bronze_elexon_prices():
    """Charge les prix Elexon depuis GCS Bronze (si disponible)."""
    return spark.read.json(f"gs://{BUCKET_BRONZE}/elexon/system_prices_raw.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer -- Nettoyage et transformation
# MAGIC
# MAGIC Les tables Silver appliquent le nettoyage : deduplication, imputation,
# MAGIC filtrage, interpolation. Les expectations garantissent la qualite.

# COMMAND ----------

@dlt.table(
    name="silver_smart_meters",
    comment="Cleaned smart meters: deduplicated, LOCF imputed, positive kWh only",
    table_properties={"quality": "silver"},
)
@dlt.expect("positive_kwh", "kwh > 0")
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
@dlt.expect("kwh_not_extreme", "kwh < 10")  # Filtre valeurs aberrantes
def silver_smart_meters():
    """
    Nettoyage Smart Meters :
    - Renommage colonnes (LCLid -> user_id, tstp -> timestamp, energy(kWh/hh) -> kwh)
    - Conversion kwh en numerique
    - Suppression kWh <= 0
    - Deduplication par (user_id, timestamp)
    """
    return (
        dlt.read("bronze_smart_meters")
        .withColumnRenamed("LCLid", "user_id")
        .withColumnRenamed("tstp", "timestamp")
        .withColumn("kwh", col("`energy(kWh/hh)`").cast(DoubleType()))
        .withColumn("timestamp", to_timestamp("timestamp"))
        .filter(col("kwh") > 0)
        .filter(col("kwh").isNotNull())
        .filter(col("timestamp").isNotNull())
        .dropDuplicates(["user_id", "timestamp"])
        .select("user_id", "timestamp", "kwh")
    )

# COMMAND ----------

@dlt.table(
    name="silver_weather",
    comment="Weather data cleaned and interpolated to 30-min intervals",
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_temperature", "temperature IS NOT NULL")
def silver_weather():
    """
    Nettoyage Weather :
    - Selection colonnes utiles
    - Renommage time -> timestamp
    - Deduplication
    - Note: l'interpolation 30-min necessite un traitement plus complexe
      (voir implementation dans 02_nettoyage_silver.py)
    """
    weather_cols = [
        "time", "temperature", "humidity", "windSpeed", "windBearing",
        "pressure", "visibility", "dewPoint", "apparentTemperature"
    ]

    df = dlt.read("bronze_weather")

    # Garder les colonnes existantes
    existing = [c for c in weather_cols if c in df.columns]
    df = df.select(*existing)

    return (
        df
        .withColumnRenamed("time", "timestamp")
        .withColumn("timestamp", to_timestamp("timestamp"))
        .withColumn("temperature", col("temperature").cast(DoubleType()))
        .withColumn("humidity", col("humidity").cast(DoubleType()))
        .dropDuplicates(["timestamp"])
        .orderBy("timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="silver_households",
    comment="Household profiles with ACORN classification, cleaned",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
def silver_households():
    """
    Nettoyage Households :
    - Renommage colonnes
    - Remplacement ACORN manquants
    - Deduplication par user_id
    """
    return (
        dlt.read("bronze_households")
        .withColumnRenamed("LCLid", "user_id")
        .withColumnRenamed("Acorn", "acorn_group")
        .withColumnRenamed("Acorn_grouped", "acorn_category")
        .withColumnRenamed("stdorToU", "tariff_type")
        .fillna({"acorn_group": "ACORN-unknown", "acorn_category": "unknown"})
        .dropDuplicates(["user_id"])
        .select("user_id", "acorn_group", "acorn_category", "tariff_type")
    )

# COMMAND ----------

@dlt.table(
    name="silver_holidays",
    comment="Complete date series with binary is_holiday flag (2011-2014)",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
def silver_holidays():
    """
    Nettoyage Holidays :
    - Renommage colonnes
    - Generation serie complete de dates
    - Flag binaire is_holiday
    """
    df_hol = (
        dlt.read("bronze_holidays")
        .withColumnRenamed("Bank holidays", "date")
        .withColumnRenamed("Type", "holiday_name")
        .withColumn("date", to_date("date"))
        .filter(col("date").isNotNull())
    )

    # Generer grille de dates
    date_grid = spark.sql("""
        SELECT explode(sequence(
            date('2011-11-01'), date('2014-02-28'), interval 1 day
        )) AS date
    """)

    return (
        date_grid
        .join(df_hol, "date", "left")
        .withColumn("is_holiday", when(col("holiday_name").isNotNull(), 1).otherwise(0))
        .select("date", "is_holiday", "holiday_name")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer -- Feature Engineering et Predictions
# MAGIC
# MAGIC **Note** : L'etape de training (XGBoost + Walk-Forward + MLflow) reste
# MAGIC un notebook separe car c'est un processus iteratif avec des hyperparametres
# MAGIC a ajuster. DLT est concu pour les transformations de donnees declaratives,
# MAGIC pas pour l'entrainement de modeles ML.
# MAGIC
# MAGIC Le notebook 03_training.py reste inchange. Seules les etapes de
# MAGIC transformation de donnees (ingestion, nettoyage, inference) migrent vers DLT.

# COMMAND ----------

@dlt.table(
    name="gold_feature_dataset",
    comment="Complete feature dataset for ML training/inference",
    table_properties={"quality": "gold"},
)
@dlt.expect("valid_kwh", "kwh > 0")
@dlt.expect("has_lag_features", "kwh_lag_1 IS NOT NULL")
def gold_feature_dataset():
    """
    Feature Engineering :
    - Jointure Smart Meters + Weather + Households + Holidays
    - Features temporelles (heure, jour, mois, cycliques)
    - Features lag (t-1, t-2, t-4, t-48, t-336)
    - Features rolling (moyenne/ecart-type 24h)
    - Features interaction (temperature x heure)
    """
    from pyspark.sql.window import Window

    df = dlt.read("silver_smart_meters")

    # --- Features temporelles ---
    df = (df
        .withColumn("date", date_format("timestamp", "yyyy-MM-dd"))
        .withColumn("hour", hour("timestamp"))
        .withColumn("minute", minute("timestamp"))
        .withColumn("day_of_week", dayofweek("timestamp"))
        .withColumn("day_of_month", dayofmonth("timestamp"))
        .withColumn("month", month("timestamp"))
        .withColumn("is_weekend", when(dayofweek("timestamp").isin(1, 7), 1).otherwise(0))
        .withColumn("settlement_period", (col("hour") * 2 + floor(col("minute") / 30) + 1).cast(IntegerType()))
    )

    # --- Features cycliques ---
    df = (df
        .withColumn("hour_sin", sin(2 * 3.14159 * col("hour") / 24))
        .withColumn("hour_cos", cos(2 * 3.14159 * col("hour") / 24))
        .withColumn("dow_sin", sin(2 * 3.14159 * col("day_of_week") / 7))
        .withColumn("dow_cos", cos(2 * 3.14159 * col("day_of_week") / 7))
        .withColumn("month_sin", sin(2 * 3.14159 * col("month") / 12))
        .withColumn("month_cos", cos(2 * 3.14159 * col("month") / 12))
    )

    # --- Features lag ---
    user_window = Window.partitionBy("user_id").orderBy("timestamp")
    for lag_name, lag_n in [("kwh_lag_1", 1), ("kwh_lag_2", 2), ("kwh_lag_4", 4),
                             ("kwh_lag_48", 48), ("kwh_lag_336", 336)]:
        df = df.withColumn(lag_name, lag("kwh", lag_n).over(user_window))

    # --- Rolling features ---
    rolling_window = user_window.rowsBetween(-48, -1)
    df = (df
        .withColumn("kwh_rolling_mean_24h", avg("kwh").over(rolling_window))
        .withColumn("kwh_rolling_std_24h", stddev("kwh").over(rolling_window))
    )

    # --- Jointure Weather ---
    weather = dlt.read("silver_weather")
    df = df.join(weather, "timestamp", "left")

    # --- Features interaction ---
    df = df.withColumn("temp_x_hour", col("temperature") * col("hour"))
    df = df.withColumn("temp_x_hour_sin", col("temperature") * col("hour_sin"))
    if "apparentTemperature" in df.columns:
        df = df.withColumn("apparent_temp_x_hour", col("apparentTemperature") * col("hour"))

    # --- Jointure Households ---
    hh = dlt.read("silver_households")
    hh = (hh
        .withColumn("acorn_group_idx", dense_rank().over(Window.orderBy("acorn_group")) - 1)
        .withColumn("acorn_category_idx", dense_rank().over(Window.orderBy("acorn_category")) - 1)
        .withColumn("tariff_type_idx", dense_rank().over(Window.orderBy("tariff_type")) - 1)
    )
    df = df.join(hh.select("user_id", "acorn_group_idx", "acorn_category_idx", "tariff_type_idx"),
                 "user_id", "left")

    # --- Jointure Holidays ---
    holidays = dlt.read("silver_holidays").select("date", "is_holiday")
    df = df.join(holidays, "date", "left")
    df = df.withColumn("is_holiday", coalesce(col("is_holiday"), lit(0)))

    # --- Filtrer lignes sans lag complet ---
    df = df.filter(col("kwh_lag_336").isNotNull())

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline DLT -- Comment deployer
# MAGIC
# MAGIC ### 1. Creer le pipeline DLT
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "ML Energy Pipeline DLT",
# MAGIC   "target": "ml_energy",
# MAGIC   "libraries": [
# MAGIC     {"notebook": {"path": "/Users/<email>/ml-energy/dlt_pipeline_reference"}}
# MAGIC   ],
# MAGIC   "configuration": {
# MAGIC     "pipelines.enableTrackHistory": "true"
# MAGIC   },
# MAGIC   "continuous": false,
# MAGIC   "development": true
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### 2. Workflow hybride (DLT + Training)
# MAGIC
# MAGIC Puisque le training XGBoost ne peut pas etre en DLT, le workflow serait :
# MAGIC
# MAGIC ```
# MAGIC Task 1: DLT Pipeline (Bronze -> Silver -> Gold features)
# MAGIC Task 2: 03_training.py (depends on Task 1) -- lit gold_feature_dataset depuis UC
# MAGIC Task 3: 04_inference_gold.py (depends on Task 2) -- optionnel si gold_feature_dataset suffit
# MAGIC ```
# MAGIC
# MAGIC ### 3. Quality expectations vs Quality Gates actuelles
# MAGIC
# MAGIC | Quality Gate actuelle | Equivalent DLT |
# MAGIC |---|---|
# MAGIC | `df = df[df["kwh"] > 0]` | `@dlt.expect("positive_kwh", "kwh > 0")` |
# MAGIC | `df = df.dropna(subset=["timestamp"])` | `@dlt.expect_or_drop("valid_ts", "timestamp IS NOT NULL")` |
# MAGIC | `if wmape > 0.35: raise` | Reste dans 03_training.py (MLflow, pas DLT) |
# MAGIC | `df["kwh"] = df["kwh"].clip(lower=0)` | `@dlt.expect("non_negative_pred", "kwh_predicted >= 0")` |
