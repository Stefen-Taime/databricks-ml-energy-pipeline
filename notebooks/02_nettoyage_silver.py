# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2 --- Nettoyage Bronze -> Silver
# MAGIC
# MAGIC Telecharge les donnees brutes depuis GCS Bronze, applique le nettoyage Spark,
# MAGIC et ecrit les tables propres en **Parquet** dans GCS Silver.
# MAGIC
# MAGIC **Nettoyage par source :**
# MAGIC | Source | Traitement |
# MAGIC |---|---|
# MAGIC | Smart Meters | Dedoublonnage, imputation gaps (LOCF), suppression kWh negatifs, filtrage foyers actifs depuis mi-2012 |
# MAGIC | Weather | Interpolation horaire -> demi-horaire, suppression colonnes inutiles |
# MAGIC | NESO / Elexon | Aplatissement JSON, alignement timezone UTC -> UK local |
# MAGIC | Households | Verification jointure LCLid, labellisation ACORN-unknown |
# MAGIC | Holidays | Formatage en colonne binaire is_holiday |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Installation des dependances

# COMMAND ----------

%pip install google-cloud-storage pyarrow --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration et initialisation GCS

# COMMAND ----------

import json
import os
import io
import gc
from google.cloud import storage
from google.oauth2 import service_account

# ---------- Secrets ----------
GCP_SA_KEY_JSON = dbutils.secrets.get(scope="ml-energy", key="gcp-sa-key")

# ---------- Buckets ----------
BUCKET_BRONZE = "ml-energy-consumption-bronze"
BUCKET_SILVER = "ml-energy-consumption-silver"

# ---------- Unity Catalog ----------
# Recuperer le catalogue depuis le notebook precedent, sinon fallback
try:
    UC_CATALOG = dbutils.jobs.taskValues.get(
        taskKey="01_ingestion_bronze", key="uc_catalog", debugValue="ml_energy"
    )
except Exception:
    UC_CATALOG = "ml_energy"

print(f"Unity Catalog: {UC_CATALOG}.silver")

# ---------- GCS Client ----------
gcp_credentials = service_account.Credentials.from_service_account_info(
    json.loads(GCP_SA_KEY_JSON)
)
gcs_client = storage.Client(credentials=gcp_credentials, project=gcp_credentials.project_id)
bronze_bucket = gcs_client.bucket(BUCKET_BRONZE)
silver_bucket = gcs_client.bucket(BUCKET_SILVER)

print(f"Bronze: gs://{BUCKET_BRONZE}")
print(f"Silver: gs://{BUCKET_SILVER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fonctions utilitaires

# COMMAND ----------

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)

spark = SparkSession.builder.getOrCreate()


def read_csv_from_gcs_as_spark(bucket, prefix: str, schema=None, filter_fn=None, batch_size=10):
    """
    Lit les CSV d'un prefixe GCS par batch et retourne un Spark DataFrame.
    Charge chaque batch en Pandas puis convertit en Spark, pour eviter OOM.
    """
    blobs = [b for b in bucket.list_blobs(prefix=prefix)
             if b.size > 1 and b.name.endswith(".csv")
             and (filter_fn is None or filter_fn(b.name))]

    print(f"  {len(blobs)} fichiers CSV trouves dans {prefix}")

    result_df = None
    batch_dfs = []

    for i, blob in enumerate(blobs):
        content = blob.download_as_bytes()
        pdf = pd.read_csv(io.BytesIO(content))
        batch_dfs.append(pdf)

        # Toutes les batch_size fichiers, convertir en Spark et union
        if len(batch_dfs) >= batch_size or i == len(blobs) - 1:
            batch_pd = pd.concat(batch_dfs, ignore_index=True)
            # Forcer les colonnes numeriques (certains CSV ont "Null" en string)
            for col in batch_pd.columns:
                if batch_pd[col].dtype == object and col not in ["LCLid", "tstp"]:
                    batch_pd[col] = pd.to_numeric(batch_pd[col], errors="coerce")
            batch_spark = spark.createDataFrame(batch_pd)

            if result_df is None:
                result_df = batch_spark
            else:
                result_df = result_df.unionByName(batch_spark, allowMissingColumns=True)

            # Liberer la memoire Pandas
            del batch_dfs, batch_pd
            batch_dfs = []
            gc.collect()
            print(f"  Batch {i+1}/{len(blobs)} traite")

    return result_df


def read_csv_from_gcs(bucket, prefix: str, filter_fn=None):
    """Lit tous les CSV d'un prefixe GCS et retourne un pandas DataFrame (petits datasets)."""
    blobs = list(bucket.list_blobs(prefix=prefix))
    dfs = []
    for blob in blobs:
        if blob.size <= 1:
            continue
        if not blob.name.endswith(".csv"):
            continue
        if filter_fn and not filter_fn(blob.name):
            continue
        content = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(content))
        dfs.append(df)
        print(f"  Lu: {blob.name} ({len(df):,} lignes)")
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


def read_json_from_gcs(bucket, prefix: str):
    """Lit le premier fichier JSON d'un prefixe GCS et retourne les donnees."""
    blobs = list(bucket.list_blobs(prefix=prefix))
    for blob in blobs:
        if blob.size <= 1:
            continue
        if not blob.name.endswith(".json"):
            continue
        content = blob.download_as_bytes()
        return json.loads(content)
    return None


def upload_parquet_to_gcs(spark_df, gcs_prefix: str, partition_col=None):
    """
    Ecrit un Spark DataFrame en Parquet vers GCS Silver.
    Traite par batch pour eviter OOM sur le driver.
    """
    if partition_col and partition_col in spark_df.columns:
        # Recuperer la liste des partitions
        partitions = [row[0] for row in spark_df.select(partition_col).distinct().collect()]
        print(f"  {len(partitions)} partitions a ecrire...")

        for i, val in enumerate(partitions):
            pdf = spark_df.filter(F.col(partition_col) == val).toPandas()
            table = pa.Table.from_pandas(pdf)
            buf = io.BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)
            blob_name = f"{gcs_prefix}/{partition_col}={val}/data.parquet"
            blob = silver_bucket.blob(blob_name)
            blob.upload_from_file(buf, content_type="application/octet-stream")
            del pdf, table, buf
            if (i + 1) % 50 == 0:
                gc.collect()
                print(f"    {i+1}/{len(partitions)} partitions ecrites...")

        print(f"  Parquet ecrit (partitionne par {partition_col}): gs://{BUCKET_SILVER}/{gcs_prefix}/")
    else:
        pdf = spark_df.toPandas()
        table = pa.Table.from_pandas(pdf)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)
        blob_name = f"{gcs_prefix}/data.parquet"
        blob = silver_bucket.blob(blob_name)
        blob.upload_from_file(buf, content_type="application/octet-stream")
        print(f"  Parquet ecrit: gs://{BUCKET_SILVER}/{gcs_prefix}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Nettoyage Smart Meters
# MAGIC
# MAGIC - Union de tous les blocs CSV (par batch pour eviter OOM)
# MAGIC - Dedoublonnage par (LCLid, datetime)
# MAGIC - Suppression kWh negatifs ou nuls
# MAGIC - Filtrage des foyers actifs depuis mi-2012 minimum
# MAGIC - Imputation des gaps par LOCF (Last Observation Carried Forward)

# COMMAND ----------

# Traitement Smart Meters bloc par bloc pour eviter OOM sur serverless
# Strategie : chaque bloc CSV est lu en Pandas, nettoye, et ecrit en Parquet
# directement vers GCS Silver. Aucune accumulation en memoire.

print("Lecture et nettoyage Smart Meters bloc par bloc...")

sm_blobs = [b for b in bronze_bucket.list_blobs(prefix="smart_meters/")
            if b.size > 1 and b.name.endswith(".csv")]
print(f"  {len(sm_blobs)} blocs CSV a traiter")

total_rows_in = 0
total_rows_out = 0
all_users = set()

for i, blob in enumerate(sm_blobs):
    # 1. Lire le bloc en Pandas
    content = blob.download_as_bytes()
    pdf = pd.read_csv(io.BytesIO(content))
    total_rows_in += len(pdf)

    # 2. Renommer les colonnes
    rename_map = {"LCLid": "user_id", "tstp": "timestamp", "energy(kWh/hh)": "kwh"}
    pdf = pdf.rename(columns={k: v for k, v in rename_map.items() if k in pdf.columns})

    # 3. Convertir kwh en numerique (certains blocs ont "Null" en string)
    pdf["kwh"] = pd.to_numeric(pdf["kwh"], errors="coerce")

    # 4. Convertir timestamp
    pdf["timestamp"] = pd.to_datetime(pdf["timestamp"], errors="coerce")

    # 5. Supprimer kWh <= 0 ou null
    pdf = pdf[pdf["kwh"] > 0].dropna(subset=["kwh", "timestamp"])

    # 6. Dedoublonnage
    pdf = pdf.drop_duplicates(subset=["user_id", "timestamp"])

    # 7. Trier par user + timestamp (necessaire pour LOCF)
    pdf = pdf.sort_values(["user_id", "timestamp"])

    # 8. LOCF par user_id
    pdf["kwh"] = pdf.groupby("user_id")["kwh"].ffill()
    pdf = pdf.dropna(subset=["kwh"])

    # Collecter les users
    all_users.update(pdf["user_id"].unique())
    total_rows_out += len(pdf)

    # 9. Ecrire en Parquet vers GCS Silver (un fichier par bloc)
    if len(pdf) > 0:
        table = pa.Table.from_pandas(pdf[["user_id", "timestamp", "kwh"]])
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)
        blob_name = f"smart_meters/block_{i:03d}.parquet"
        out_blob = silver_bucket.blob(blob_name)
        out_blob.upload_from_file(buf, content_type="application/octet-stream")

    # Liberer memoire
    del pdf, content
    if (i + 1) % 20 == 0:
        gc.collect()
        print(f"  {i+1}/{len(sm_blobs)} blocs traites ({total_rows_out:,} lignes)")

gc.collect()
print(f"Smart Meters Silver: termine")
print(f"  Lignes brutes: {total_rows_in:,}")
print(f"  Lignes nettoyees: {total_rows_out:,}")
print(f"  Foyers uniques: {len(all_users):,}")

# --- Unity Catalog : enregistrer smart_meters Silver ---
# Strategie optimisee : lire directement depuis GCS avec Spark (bien plus rapide)
try:
    print("Enregistrement smart_meters dans Unity Catalog (Spark direct read)...")

    # Lire TOUS les parquets depuis GCS en une seule operation Spark
    gcs_path = f"gs://{BUCKET_SILVER}/smart_meters/*.parquet"
    print(f"  Lecture depuis {gcs_path}")

    sm_spark = spark.read.parquet(gcs_path)
    sm_count = sm_spark.count()
    print(f"  {sm_count:,} lignes lues depuis GCS Silver")

    # Ecrire dans Unity Catalog en une seule fois (overwrite)
    print(f"  Ecriture vers {UC_CATALOG}.silver.smart_meters...")
    sm_spark.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{UC_CATALOG}.silver.smart_meters")

    print(f"✓ {UC_CATALOG}.silver.smart_meters : {sm_count:,} lignes enregistrees")
    del sm_spark
    gc.collect()
except Exception as e:
    print(f"  Smart Meters UC Silver: erreur -- {e}")
    print(f"  -> Les donnees restent disponibles dans GCS Silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Nettoyage Weather (DarkSky)
# MAGIC
# MAGIC - Interpolation de l'horaire vers le demi-horaire (alignement 30 min)
# MAGIC - Suppression des colonnes inutiles (icon, summary, etc.)
# MAGIC - Interpolation lineaire des valeurs manquantes

# COMMAND ----------

# Lire le CSV Weather (petit dataset, Pandas OK)
print("Lecture Weather depuis Bronze...")
df_weather_pd = read_csv_from_gcs(bronze_bucket, "weather/")
print(f"Weather brut: {len(df_weather_pd):,} lignes")

df_weather = spark.createDataFrame(df_weather_pd)
del df_weather_pd
gc.collect()
df_weather.printSchema()

# COMMAND ----------

# DBTITLE Nettoyage Weather

# Colonnes a garder (features utiles pour la prediction)
WEATHER_COLS = [
    "time", "temperature", "humidity", "windSpeed", "windBearing",
    "pressure", "visibility", "dewPoint", "apparentTemperature"
]

# Garder uniquement les colonnes utiles (certaines peuvent ne pas exister)
existing_cols = [c for c in WEATHER_COLS if c in df_weather.columns]
df_weather = df_weather.select(*existing_cols)

# Renommer time -> timestamp
df_weather = df_weather.withColumnRenamed("time", "timestamp")
df_weather = df_weather.withColumn("timestamp", F.to_timestamp("timestamp"))

# Convertir toutes les colonnes numeriques
numeric_cols = [c for c in df_weather.columns if c != "timestamp"]
for col_name in numeric_cols:
    df_weather = df_weather.withColumn(col_name, F.col(col_name).cast(DoubleType()))

# Dedoublonner
df_weather = df_weather.dropDuplicates(["timestamp"])

# Trier par timestamp
df_weather = df_weather.orderBy("timestamp")

print(f"Weather apres nettoyage colonnes: {df_weather.count():,} lignes")

# COMMAND ----------

# DBTITLE Interpolation horaire -> demi-horaire

# Les donnees weather sont horaires, on doit interpoler vers 30 min
# Strategie : pour chaque point manquant a :30, on fait la moyenne des :00 adjacents

# Generer la grille 30-min
w_min_ts = df_weather.agg(F.min("timestamp")).collect()[0][0]
w_max_ts = df_weather.agg(F.max("timestamp")).collect()[0][0]

weather_grid = spark.sql(f"""
    SELECT explode(sequence(
        timestamp('{w_min_ts}'),
        timestamp('{w_max_ts}'),
        interval 30 minutes
    )) AS timestamp
""")

# Left join : les points :30 auront des nulls
df_weather = weather_grid.join(df_weather, "timestamp", "left")

# Interpolation lineaire via moyenne des voisins
# On utilise une window lag/lead pour interpoler
window_time = Window.orderBy("timestamp")

for col_name in numeric_cols:
    df_weather = df_weather.withColumn(
        col_name,
        F.when(
            F.col(col_name).isNotNull(),
            F.col(col_name)
        ).otherwise(
            (F.lag(col_name, 1).over(window_time) + F.lead(col_name, 1).over(window_time)) / 2
        )
    )

# Supprimer les lignes restantes avec des nulls (bords)
df_weather = df_weather.na.drop()

print(f"Weather interpolee 30-min: {df_weather.count():,} lignes")

# COMMAND ----------

upload_parquet_to_gcs(df_weather, "weather")
print("Weather Silver: termine")

# --- Unity Catalog ---
try:
    df_weather.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.silver.weather")
    print(f"  {UC_CATALOG}.silver.weather enregistre")
except Exception as e:
    print(f"  Weather UC Silver: erreur -- {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Nettoyage Households (ACORN)
# MAGIC
# MAGIC - Verification des LCLid existants
# MAGIC - Labellisation "ACORN-unknown" pour les profils manquants

# COMMAND ----------

# Lire le CSV Households depuis Bronze (uniquement informations_households.csv)
print("Lecture Households depuis Bronze...")
df_hh_pd = read_csv_from_gcs(
    bronze_bucket, "households/",
    filter_fn=lambda name: "informations_households" in name
)
print(f"Households brut: {len(df_hh_pd):,} lignes")

df_hh = spark.createDataFrame(df_hh_pd)
del df_hh_pd
gc.collect()
df_hh.printSchema()

# COMMAND ----------

# DBTITLE Nettoyage Households

# Renommer pour uniformite
df_hh = df_hh.withColumnRenamed("LCLid", "user_id") \
             .withColumnRenamed("Acorn", "acorn_group") \
             .withColumnRenamed("Acorn_grouped", "acorn_category")

# Garder uniquement les colonnes utiles
keep_cols = ["user_id", "acorn_group", "acorn_category"]
# Ajouter stdorToU (tariff type) si present
if "stdorToU" in df_hh.columns:
    df_hh = df_hh.withColumnRenamed("stdorToU", "tariff_type")
    keep_cols.append("tariff_type")

existing = [c for c in keep_cols if c in df_hh.columns]
df_hh = df_hh.select(*existing)

# Remplacer les ACORN manquants
df_hh = df_hh.fillna({
    "acorn_group": "ACORN-unknown",
    "acorn_category": "unknown"
})

# Dedoublonner par user_id
df_hh = df_hh.dropDuplicates(["user_id"])

print(f"Households nettoyes: {df_hh.count():,} lignes")
df_hh.groupBy("acorn_category").count().orderBy(F.desc("count")).show(10)

# COMMAND ----------

upload_parquet_to_gcs(df_hh, "households")
print("Households Silver: termine")

# --- Unity Catalog ---
try:
    df_hh.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.silver.households")
    print(f"  {UC_CATALOG}.silver.households enregistre")
except Exception as e:
    print(f"  Households UC Silver: erreur -- {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Nettoyage NESO (Demande nationale UK)
# MAGIC
# MAGIC - Aplatissement du JSON
# MAGIC - Alignement timezone UTC -> UK local (DST)
# MAGIC - Selection des colonnes pertinentes

# COMMAND ----------

# Lire le JSON NESO depuis Bronze
neso_raw = read_json_from_gcs(bronze_bucket, "neso/")

if neso_raw:
    df_neso_pd = pd.json_normalize(neso_raw)
    df_neso = spark.createDataFrame(df_neso_pd)
    del df_neso_pd
    gc.collect()
    print(f"NESO brut: {df_neso.count():,} lignes, colonnes: {df_neso.columns}")
    has_neso = True
else:
    print("NESO: aucun fichier trouve dans Bronze")
    has_neso = False

# COMMAND ----------

# DBTITLE Nettoyage NESO
if has_neso:
    # Identifier la colonne timestamp (varie selon le dataset)
    ts_candidates = [c for c in df_neso.columns if "date" in c.lower() or "time" in c.lower()]
    print(f"Colonnes timestamp candidates: {ts_candidates}")

    # Les colonnes utiles varient ; on garde les numeriques + timestamp
    # Typiquement : SETTLEMENT_DATE, SETTLEMENT_PERIOD, ND (National Demand), etc.
    df_neso = df_neso.withColumn(
        "timestamp",
        F.to_timestamp(F.col(ts_candidates[0]))
    )

    # Convertir en UTC -> UK local
    df_neso = df_neso.withColumn(
        "timestamp",
        F.from_utc_timestamp("timestamp", "Europe/London")
    )

    # Garder les colonnes numeriques pertinentes
    numeric_neso = [c for c in df_neso.columns
                    if c not in ts_candidates and c != "timestamp" and c != "_id"]

    select_cols = ["timestamp"] + numeric_neso
    df_neso = df_neso.select(*select_cols)

    # Dedoublonner et trier
    df_neso = df_neso.dropDuplicates(["timestamp"]).orderBy("timestamp")

    print(f"NESO nettoye: {df_neso.count():,} lignes")
    df_neso.show(5, truncate=False)

    upload_parquet_to_gcs(df_neso, "neso")
    print("NESO Silver: termine")

    # --- Unity Catalog ---
    try:
        df_neso.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.silver.neso_demand")
        print(f"  {UC_CATALOG}.silver.neso_demand enregistre")
    except Exception as e:
        print(f"  NESO UC Silver: erreur -- {e}")
else:
    print("NESO: skip (pas de donnees)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Nettoyage Elexon (Prix marche)
# MAGIC
# MAGIC - Aplatissement du JSON
# MAGIC - Alignement timezone

# COMMAND ----------

# Lire le JSON Elexon depuis Bronze
elexon_raw = read_json_from_gcs(bronze_bucket, "elexon/")

if elexon_raw:
    df_elexon_pd = pd.json_normalize(elexon_raw)
    df_elexon = spark.createDataFrame(df_elexon_pd)
    del df_elexon_pd
    gc.collect()
    print(f"Elexon brut: {df_elexon.count():,} lignes, colonnes: {df_elexon.columns}")
    has_elexon = True
else:
    print("Elexon: aucun fichier trouve dans Bronze")
    has_elexon = False

# COMMAND ----------

# DBTITLE Nettoyage Elexon
if has_elexon:
    # Colonnes typiques Elexon : settlementDate, settlementPeriod, systemSellPrice, systemBuyPrice
    ts_cols = [c for c in df_elexon.columns if "date" in c.lower() or "time" in c.lower()]
    period_cols = [c for c in df_elexon.columns if "period" in c.lower()]

    print(f"Colonnes date: {ts_cols}, Colonnes period: {period_cols}")

    # Construire un timestamp a partir de settlementDate + settlementPeriod
    # Chaque settlement period = 30 min, period 1 = 00:00, period 2 = 00:30, etc.
    if ts_cols and period_cols:
        df_elexon = df_elexon.withColumn(
            "settlement_date", F.to_date(F.col(ts_cols[0]))
        ).withColumn(
            "settlement_period", F.col(period_cols[0]).cast(IntegerType())
        )

        # Convertir period en minutes : (period - 1) * 30
        df_elexon = df_elexon.withColumn(
            "minutes_offset",
            (F.col("settlement_period") - 1) * 30
        )

        # Construire le timestamp
        df_elexon = df_elexon.withColumn(
            "timestamp",
            F.to_timestamp(
                F.concat(
                    F.date_format("settlement_date", "yyyy-MM-dd"),
                    F.lit(" "),
                    F.lpad(F.floor(F.col("minutes_offset") / 60).cast(StringType()), 2, "0"),
                    F.lit(":"),
                    F.lpad((F.col("minutes_offset") % 60).cast(StringType()), 2, "0"),
                    F.lit(":00")
                )
            )
        )

    # Garder les colonnes de prix
    price_cols = [c for c in df_elexon.columns
                  if "price" in c.lower() or "sell" in c.lower() or "buy" in c.lower()]
    select_cols = ["timestamp"] + [c for c in price_cols if c in df_elexon.columns]
    df_elexon = df_elexon.select(*select_cols)

    # Convertir les prix en Double
    for c in [col for col in df_elexon.columns if col != "timestamp"]:
        df_elexon = df_elexon.withColumn(c, F.col(c).cast(DoubleType()))

    df_elexon = df_elexon.dropDuplicates(["timestamp"]).orderBy("timestamp")
    df_elexon = df_elexon.filter(F.col("timestamp").isNotNull())

    print(f"Elexon nettoye: {df_elexon.count():,} lignes")
    df_elexon.show(5, truncate=False)

    upload_parquet_to_gcs(df_elexon, "elexon")
    print("Elexon Silver: termine")

    # --- Unity Catalog ---
    try:
        df_elexon.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.silver.elexon_prices")
        print(f"  {UC_CATALOG}.silver.elexon_prices enregistre")
    except Exception as e:
        print(f"  Elexon UC Silver: erreur -- {e}")
else:
    print("Elexon: skip (pas de donnees)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Nettoyage Holidays
# MAGIC
# MAGIC - Colonnes brutes : `Bank holidays, Type`
# MAGIC - Renommage vers `date, holiday_name`
# MAGIC - Formatage en colonne binaire `is_holiday`
# MAGIC - Generation de la serie complete de dates avec flag 0/1

# COMMAND ----------

# Lire le CSV Holidays
print("Lecture Holidays depuis Bronze...")
df_hol_pd = read_csv_from_gcs(bronze_bucket, "holidays/")
print(f"Holidays brut: {len(df_hol_pd):,} lignes")

df_hol = spark.createDataFrame(df_hol_pd)
del df_hol_pd
gc.collect()
df_hol.printSchema()
df_hol.show(5, truncate=False)

# COMMAND ----------

# DBTITLE Nettoyage Holidays

# Renommer les colonnes du fichier Kaggle : "Bank holidays" -> date, "Type" -> holiday_name
df_hol = df_hol.withColumnRenamed("Bank holidays", "date") \
               .withColumnRenamed("Type", "holiday_name")

# Convertir la date
df_hol = df_hol.withColumn("date", F.to_date("date"))

# Supprimer les lignes avec date nulle
df_hol = df_hol.filter(F.col("date").isNotNull())

# Generer toutes les dates de la periode du dataset
date_grid = spark.sql("""
    SELECT explode(sequence(
        date('2011-11-01'),
        date('2014-02-28'),
        interval 1 day
    )) AS date
""")

# Left join : les jours feries auront holiday_name, les autres null
df_holidays_full = date_grid.join(df_hol, "date", "left")

# Creer la colonne binaire is_holiday
df_holidays_full = df_holidays_full.withColumn(
    "is_holiday",
    F.when(F.col("holiday_name").isNotNull(), 1).otherwise(0)
)

# Garder date + is_holiday (+ le nom pour reference)
df_holidays_full = df_holidays_full.select("date", "is_holiday", "holiday_name")

print(f"Holidays complet: {df_holidays_full.count():,} jours")
print(f"  dont jours feries: {df_holidays_full.filter(F.col('is_holiday') == 1).count()}")

# COMMAND ----------

upload_parquet_to_gcs(df_holidays_full, "holidays")
print("Holidays Silver: termine")

# --- Unity Catalog ---
try:
    df_holidays_full.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.silver.holidays")
    print(f"  {UC_CATALOG}.silver.holidays enregistre")
except Exception as e:
    print(f"  Holidays UC Silver: erreur -- {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Verification finale Silver

# COMMAND ----------

print(f"=== Contenu de gs://{BUCKET_SILVER}/ ===\n")

silver_prefixes = ["smart_meters", "weather", "households", "neso", "elexon", "holidays"]

for prefix in silver_prefixes:
    blobs = list(silver_bucket.list_blobs(prefix=f"{prefix}/"))
    real_blobs = [b for b in blobs if b.size > 1]
    total_size_mb = sum(b.size for b in real_blobs) / (1024 * 1024)

    # Compter les fichiers Parquet
    parquet_files = [b for b in real_blobs if b.name.endswith(".parquet")]

    print(f"  {prefix}/")
    print(f"    Fichiers parquet: {len(parquet_files)}")
    print(f"    Taille totale: {total_size_mb:.1f} MB")
    print()

print("Nettoyage Silver termine. Toutes les tables sont en Parquet dans GCS.")

# Verifier les tables UC Silver
print(f"\n=== Tables Unity Catalog ({UC_CATALOG}.silver) ===\n")
try:
    uc_silver_tables = spark.sql(f"SHOW TABLES IN {UC_CATALOG}.silver").collect()
    for t in uc_silver_tables:
        print(f"  {t.tableName}")
    print(f"\n  Total : {len(uc_silver_tables)} tables Silver dans UC")
except Exception as e:
    print(f"  UC verification: {e}")

# Passer le catalogue au notebook suivant
try:
    dbutils.jobs.taskValues.set(key="uc_catalog", value=UC_CATALOG)
except Exception:
    pass
