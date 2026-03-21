# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 4 --- Inference (Gold)
# MAGIC
# MAGIC Charge le meilleur modele depuis MLflow, genere les predictions
# MAGIC pour tous les foyers, et ecrit les resultats en Parquet dans GCS Gold.
# MAGIC
# MAGIC **Snowpipe** detecte automatiquement les nouveaux fichiers dans Gold
# MAGIC et les ingere dans Snowflake (aucun code Snowflake ici).
# MAGIC
# MAGIC **Entrees :**
# MAGIC - Modele XGBoost depuis MLflow (via `model_uri` du Notebook 3)
# MAGIC - Tables Silver depuis GCS (features pour l'inference)
# MAGIC
# MAGIC **Sorties :**
# MAGIC - `gs://ml-energy-consumption-gold/predictions/` (Parquet)
# MAGIC - Colonnes : USER_ID, TIMESTAMP, KWH_PREDICTED, MODEL_VERSION, PREDICTION_DATE

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Installation des dependances

# COMMAND ----------

%pip install google-cloud-storage xgboost pyarrow scikit-learn --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import json
import os
import io
import gc
import tempfile
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date

from google.cloud import storage
from google.oauth2 import service_account

# ---------- MLflow : configurer AVANT tout import/appel ----------
os.environ["MLFLOW_REGISTRY_URI"] = "databricks-uc"

import xgboost as xgb
import mlflow
import mlflow.xgboost

mlflow.set_registry_uri("databricks-uc")

# ---------- Secrets ----------
GCP_SA_KEY_JSON = dbutils.secrets.get(scope="ml-energy", key="gcp-sa-key")

# ---------- Unity Catalog ----------
try:
    UC_CATALOG = dbutils.jobs.taskValues.get(
        taskKey="03_training", key="uc_catalog", debugValue="ml_energy"
    )
except Exception:
    UC_CATALOG = "ml_energy"

print(f"Unity Catalog: {UC_CATALOG}")

# ---------- Buckets ----------
BUCKET_SILVER = "ml-energy-consumption-silver"
BUCKET_GOLD = "ml-energy-consumption-gold"

# ---------- GCS Client ----------
gcp_credentials = service_account.Credentials.from_service_account_info(
    json.loads(GCP_SA_KEY_JSON)
)
# Augmenter le timeout pour eviter ReadTimeout sur gros fichiers (300s = 5 min)
from google.cloud.storage import Client
from google.api_core import retry
gcs_client = Client(credentials=gcp_credentials, project=gcp_credentials.project_id)
gcs_client._http.timeout = 300  # 5 minutes au lieu de 60s

silver_bucket = gcs_client.bucket(BUCKET_SILVER)
gold_bucket = gcs_client.bucket(BUCKET_GOLD)

# ---------- Recuperer les valeurs du Notebook 3 ----------
try:
    model_uri = dbutils.jobs.taskValues.get(
        taskKey="03_training", key="model_uri", debugValue=""
    )
    model_version = dbutils.jobs.taskValues.get(
        taskKey="03_training", key="model_version", debugValue="v1.0_manual"
    )
    selected_features_json = dbutils.jobs.taskValues.get(
        taskKey="03_training", key="selected_features", debugValue="[]"
    )
    quality_gate = dbutils.jobs.taskValues.get(
        taskKey="03_training", key="quality_gate", debugValue="PASSED"
    )
    selected_features = json.loads(selected_features_json)
except Exception:
    # Mode standalone (execution manuelle sans Workflow)
    print("Mode standalone : pas de taskValues disponibles")
    print("Chargement du dernier modele depuis Unity Catalog ou Model Registry...")
    # Tenter d'abord le UC Model Registry (3-level namespace)
    model_uri = f"models:/{UC_CATALOG}.gold.energy_consumption_xgboost/latest"
    model_version = "v1.0_manual"
    selected_features = []  # Sera charge depuis l'artefact MLflow
    quality_gate = "PASSED"

print(f"Model URI: {model_uri}")
print(f"Model Version: {model_version}")
print(f"Quality Gate: {quality_gate}")
print(f"Features selectionnees: {len(selected_features)}")

# COMMAND ----------

# DBTITLE Verifier Quality Gate

if quality_gate != "PASSED":
    print("QUALITY GATE FAILED -- Inference annulee.")
    dbutils.notebook.exit("Quality Gate FAILED - inference skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Chargement du modele MLflow

# COMMAND ----------

# Charger le modele
print(f"Chargement du modele depuis: {model_uri}")
try:
    model = mlflow.xgboost.load_model(model_uri)
    print(f"✓ Modele charge avec succes")
except Exception as e:
    print(f"❌ ERREUR lors du chargement du modele:")
    print(f"   {type(e).__name__}: {str(e)}")
    print(f"   Model URI: {model_uri}")
    raise

# Si features pas dispo via taskValues, les recuperer depuis l'artefact
if not selected_features:
    # Trouver le run_id depuis le model URI
    if model_uri.startswith("runs:/"):
        run_id = model_uri.split("/")[1]
    else:
        # Model Registry : recuperer le run_id du dernier modele
        try:
            client = mlflow.tracking.MlflowClient()
            latest = client.get_latest_versions("ml_energy_xgboost", stages=["None", "Production"])
            if latest:
                run_id = latest[0].run_id
            else:
                raise ValueError("Impossible de trouver le modele dans le Registry")
        except Exception as e:
            print(f"Warning: impossible de recuperer depuis le Registry: {e}")
            # Fallback: utiliser les features par defaut
            selected_features = []

    if not selected_features:
        try:
            # Telecharger la liste des features
            artifact_path = mlflow.artifacts.download_artifacts(
                run_id=run_id, artifact_path="selected_features.txt"
            )
            with open(artifact_path, "r") as f:
                selected_features = [line.strip() for line in f if line.strip()]
        except Exception as e:
            print(f"Warning: impossible de telecharger les features: {e}")

print(f"Modele charge avec {len(selected_features)} features")
print(f"Features: {selected_features[:10]}{'...' if len(selected_features) > 10 else ''}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Chargement et preparation des donnees Silver (100% Pandas)

# COMMAND ----------

def read_parquet_from_gcs_pandas(prefix: str, max_blobs: int = None):
    """Lit les fichiers Parquet depuis GCS Silver et retourne un Pandas DataFrame."""
    blobs = list(silver_bucket.list_blobs(prefix=f"{prefix}/"))
    parquet_blobs = [b for b in blobs if b.name.endswith(".parquet") and b.size > 1]

    if not parquet_blobs:
        raise FileNotFoundError(f"Aucun fichier parquet dans gs://{BUCKET_SILVER}/{prefix}/")

    if max_blobs:
        parquet_blobs = parquet_blobs[:max_blobs]

    dfs = []
    for blob in parquet_blobs:
        # Ajouter timeout pour eviter ReadTimeout
        content = blob.download_as_bytes(timeout=300)
        table = pq.read_table(io.BytesIO(content))
        dfs.append(table.to_pandas())

    return pd.concat(dfs, ignore_index=True)

# COMMAND ----------

# DBTITLE Charger les tables Silver

print("Chargement des tables Silver pour inference...")

# Smart Meters -- charger bloc par bloc avec echantillonnage
# Pour l'inference, on traite TOUS les users (pas d'echantillonnage)
# Mais on traite en chunks pour eviter OOM

try:
    sm_blobs = list(silver_bucket.list_blobs(prefix="smart_meters/"))
    sm_parquet_blobs = [b for b in sm_blobs if b.name.endswith(".parquet") and b.size > 1]
    print(f"✓ Smart Meters: {len(sm_parquet_blobs)} blocs Parquet dans Silver (total)")

    # Trier par taille (traiter les plus petits d'abord pour limiter OOM)
    sm_parquet_blobs = sorted(sm_parquet_blobs, key=lambda b: b.size)

    # LIMITER LE NOMBRE DE BLOCS pour demo/test (sinon trop long)
    # Pour production : enlever cette limite ou augmenter
    MAX_BLOCKS_FOR_INFERENCE = 20  # Traiter seulement 20 blocs (~18% du dataset)
    if len(sm_parquet_blobs) > MAX_BLOCKS_FOR_INFERENCE:
        print(f"⚠️  Limitation à {MAX_BLOCKS_FOR_INFERENCE} blocs pour eviter timeout")
        sm_parquet_blobs = sm_parquet_blobs[:MAX_BLOCKS_FOR_INFERENCE]

    print(f"   Blocs à traiter: {len(sm_parquet_blobs)}")
    print(f"   Taille min: {sm_parquet_blobs[0].size / 1024 / 1024:.1f} MB")
    print(f"   Taille max: {sm_parquet_blobs[-1].size / 1024 / 1024:.1f} MB")
except Exception as e:
    print(f"❌ ERREUR lors du listing des fichiers Smart Meters:")
    print(f"   {type(e).__name__}: {str(e)}")
    raise

# Weather (petit dataset)
df_weather = read_parquet_from_gcs_pandas("weather")
df_weather["timestamp"] = pd.to_datetime(df_weather["timestamp"], errors="coerce")
df_weather = df_weather.dropna(subset=["timestamp"])
df_weather = df_weather.drop_duplicates(subset=["timestamp"])
print(f"  Weather: {len(df_weather):,} lignes")

# Households (petit dataset)
df_hh = read_parquet_from_gcs_pandas("households")
print(f"  Households: {len(df_hh):,} lignes")

# Encoder ACORN
acorn_cols_to_encode = []
for col in ["acorn_group", "acorn_category"]:
    if col in df_hh.columns:
        df_hh[f"{col}_idx"] = df_hh[col].astype("category").cat.codes.astype(float)
        acorn_cols_to_encode.append(f"{col}_idx")
if "tariff_type" in df_hh.columns:
    df_hh["tariff_type_idx"] = df_hh["tariff_type"].astype("category").cat.codes.astype(float)
    acorn_cols_to_encode.append("tariff_type_idx")

hh_join_cols = ["user_id"] + acorn_cols_to_encode
hh_join_cols = [c for c in hh_join_cols if c in df_hh.columns]

# Holidays (petit dataset)
df_hol = read_parquet_from_gcs_pandas("holidays")
df_hol["date"] = pd.to_datetime(df_hol["date"], errors="coerce").dt.strftime("%Y-%m-%d")
if "is_holiday" not in df_hol.columns:
    df_hol["is_holiday"] = 1
df_hol_dedup = df_hol[["date", "is_holiday"]].drop_duplicates(subset=["date"])
print(f"  Holidays: {len(df_hol):,} lignes")

# NESO (optionnel)
try:
    df_neso = read_parquet_from_gcs_pandas("neso")
    df_neso["timestamp"] = pd.to_datetime(df_neso["timestamp"], errors="coerce")
    neso_cols = [c for c in df_neso.columns if c != "timestamp"]
    df_neso = df_neso.rename(columns={c: f"neso_{c}" for c in neso_cols})
    df_neso = df_neso.drop_duplicates(subset=["timestamp"])
    has_neso = True
    print(f"  NESO: {len(df_neso):,} lignes")
except Exception:
    has_neso = False
    print("  NESO: non disponible")

# Elexon (optionnel)
try:
    df_elexon = read_parquet_from_gcs_pandas("elexon")
    df_elexon["timestamp"] = pd.to_datetime(df_elexon["timestamp"], errors="coerce")
    elexon_cols = [c for c in df_elexon.columns if c != "timestamp"]
    df_elexon = df_elexon.rename(columns={c: f"elexon_{c}" for c in elexon_cols})
    df_elexon = df_elexon.drop_duplicates(subset=["timestamp"])
    has_elexon = True
    print(f"  Elexon: {len(df_elexon):,} lignes")
except Exception:
    has_elexon = False
    print("  Elexon: non disponible")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Feature Engineering + Inference bloc par bloc
# MAGIC
# MAGIC Pour eviter OOM, on traite les Smart Meters bloc par bloc :
# MAGIC lire un bloc -> construire les features -> predire -> ecrire le Parquet Gold.

# COMMAND ----------

# DBTITLE Inference bloc par bloc

print("Inference bloc par bloc...")
print(f"  {len(sm_parquet_blobs)} blocs a traiter")
print(f"  Features attendues: {len(selected_features)}")

prediction_date = date.today().isoformat()
total_predictions = 0
total_blobs_written = 0
all_stats = []

# Liberer la memoire avant de commencer
gc.collect()

for bloc_idx, blob in enumerate(sm_parquet_blobs):
    # 1. Lire le bloc avec retry en cas de timeout
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Download avec timeout augmenté
            content = blob.download_as_bytes(timeout=300)
            table = pq.read_table(io.BytesIO(content))
            pdf = table.to_pandas()
            del content, table
            break  # Success, sortir de la boucle retry
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  ⚠️  Retry {attempt+1}/{max_retries} pour bloc {bloc_idx} (timeout): {str(e)[:100]}")
                gc.collect()  # Liberer memoire avant retry
                continue
            else:
                print(f"  ❌ Echec après {max_retries} tentatives pour bloc {bloc_idx}: {str(e)}")
                raise

    if len(pdf) == 0:
        continue

    # 2. Features temporelles
    pdf["timestamp"] = pd.to_datetime(pdf["timestamp"], errors="coerce")
    pdf = pdf.dropna(subset=["timestamp"])
    pdf = pdf.sort_values(["user_id", "timestamp"]).reset_index(drop=True)

    pdf["date"] = pdf["timestamp"].dt.date.astype(str)
    pdf["hour"] = pdf["timestamp"].dt.hour
    pdf["minute"] = pdf["timestamp"].dt.minute
    pdf["day_of_week"] = pdf["timestamp"].dt.dayofweek + 1
    pdf["day_of_month"] = pdf["timestamp"].dt.day
    pdf["month"] = pdf["timestamp"].dt.month
    pdf["week_of_year"] = pdf["timestamp"].dt.isocalendar().week.astype(int)
    pdf["is_weekend"] = pdf["day_of_week"].isin([6, 7]).astype(int)
    pdf["settlement_period"] = (pdf["hour"] * 2 + (pdf["minute"] // 30) + 1).astype(int)

    # --- Features cycliques ---
    pdf["hour_sin"] = np.sin(2 * np.pi * pdf["hour"] / 24).astype(np.float32)
    pdf["hour_cos"] = np.cos(2 * np.pi * pdf["hour"] / 24).astype(np.float32)
    pdf["dow_sin"] = np.sin(2 * np.pi * pdf["day_of_week"] / 7).astype(np.float32)
    pdf["dow_cos"] = np.cos(2 * np.pi * pdf["day_of_week"] / 7).astype(np.float32)
    pdf["month_sin"] = np.sin(2 * np.pi * pdf["month"] / 12).astype(np.float32)
    pdf["month_cos"] = np.cos(2 * np.pi * pdf["month"] / 12).astype(np.float32)

    # 3. Features lag (avec conversion en float32 pour economiser memoire)
    lag_periods = {
        "kwh_lag_1": 1, "kwh_lag_2": 2, "kwh_lag_4": 4,
        "kwh_lag_48": 48, "kwh_lag_336": 336,
    }
    for col_name, lag_n in lag_periods.items():
        pdf[col_name] = pdf.groupby("user_id")["kwh"].shift(lag_n).astype(np.float32)

    pdf["kwh_rolling_mean_24h"] = pdf.groupby("user_id")["kwh"].transform(
        lambda x: x.rolling(window=48, min_periods=1).mean().shift(1)
    ).astype(np.float32)
    pdf["kwh_rolling_std_24h"] = pdf.groupby("user_id")["kwh"].transform(
        lambda x: x.rolling(window=48, min_periods=1).std().shift(1)
    ).astype(np.float32)

    # 4. Jointures
    pdf = pdf.merge(df_weather, on="timestamp", how="left")

    # --- Features interaction temperature x heure ---
    if "temperature" in pdf.columns:
        pdf["temp_x_hour"] = (pdf["temperature"] * pdf["hour"]).astype(np.float32)
        pdf["temp_x_hour_sin"] = (pdf["temperature"] * pdf["hour_sin"]).astype(np.float32)
    if "apparentTemperature" in pdf.columns:
        pdf["apparent_temp_x_hour"] = (pdf["apparentTemperature"] * pdf["hour"]).astype(np.float32)

    pdf = pdf.merge(df_hh[hh_join_cols], on="user_id", how="left")
    pdf = pdf.merge(df_hol_dedup, on="date", how="left")
    pdf["is_holiday"] = pdf["is_holiday"].fillna(0).astype(int)

    if has_neso:
        pdf = pdf.merge(df_neso, on="timestamp", how="left")
    if has_elexon:
        pdf = pdf.merge(df_elexon, on="timestamp", how="left")

    # 5. Filtrer les lignes sans lag
    pdf = pdf.dropna(subset=["kwh_lag_336"])

    if len(pdf) == 0:
        del pdf
        continue

    # 6. Verifier que toutes les features sont presentes
    missing_features = [f for f in selected_features if f not in pdf.columns]
    if missing_features:
        for mf in missing_features:
            pdf[mf] = 0.0  # Remplir les features manquantes avec 0

    # 7. Convertir et predire
    for col in selected_features:
        pdf[col] = pd.to_numeric(pdf[col], errors="coerce").fillna(0)

    X = pdf[selected_features].values.astype(np.float32)
    predictions = model.predict(X)

    # 8. Construire les DataFrames predictions ET actuals (tables Snowflake separees)

    # --- Convertir timestamps en microseconds (fix Snowflake invalid_date) ---
    # Snowflake + PyArrow : datetime64[ns] cause des dates corrompues (SNOW-884196)
    # On convertit en datetime64[us] avant serialisation Parquet
    timestamps_us = pdf["timestamp"].values.astype("datetime64[us]")

    # --- PREDICTION_DATE doit etre un vrai type date, pas string ---
    pred_date_val = date.today()

    # DataFrame pour ENERGY_PREDICTIONS (Snowpipe -> Snowflake)
    df_pred = pd.DataFrame({
        "USER_ID": pdf["user_id"].values,
        "TIMESTAMP": timestamps_us,
        "KWH_PREDICTED": predictions.astype(np.float64),
        "MODEL_VERSION": model_version,
        "PREDICTION_DATE": pred_date_val,
    })
    df_pred["KWH_PREDICTED"] = df_pred["KWH_PREDICTED"].clip(lower=0)

    # DataFrame pour ENERGY_ACTUALS (Snowpipe -> Snowflake)
    df_actual = pd.DataFrame({
        "USER_ID": pdf["user_id"].values,
        "TIMESTAMP": timestamps_us,
        "KWH_ACTUAL": pdf["kwh"].values.astype(np.float64),
    })

    total_predictions += len(df_pred)
    all_stats.append(df_pred["KWH_PREDICTED"].describe())

    # 9. Ecrire les Parquet dans GCS Gold (2 chemins separes pour 2 Snowpipes)
    # --- Predictions ---
    pred_table = pa.Table.from_pandas(df_pred, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(pred_table, buf,
                    coerce_timestamps="us",
                    allow_truncated_timestamps=True)
    buf.seek(0)

    blob_name = f"predictions/PREDICTION_DATE={prediction_date}/block_{bloc_idx:03d}.parquet"
    out_blob = gold_bucket.blob(blob_name)
    out_blob.upload_from_file(buf, content_type="application/octet-stream")

    # --- Actuals ---
    actual_table = pa.Table.from_pandas(df_actual, preserve_index=False)
    buf2 = io.BytesIO()
    pq.write_table(actual_table, buf2,
                    coerce_timestamps="us",
                    allow_truncated_timestamps=True)
    buf2.seek(0)

    blob_name_act = f"actuals/PREDICTION_DATE={prediction_date}/block_{bloc_idx:03d}.parquet"
    out_blob_act = gold_bucket.blob(blob_name_act)
    out_blob_act.upload_from_file(buf2, content_type="application/octet-stream")

    total_blobs_written += 1

    del pdf, df_pred, df_actual, X, predictions, pred_table, actual_table, buf, buf2

    # GC plus frequent pour eviter OOM
    if (bloc_idx + 1) % 10 == 0:
        gc.collect()
        print(f"  {bloc_idx+1}/{len(sm_parquet_blobs)} blocs traites "
              f"({total_predictions:,} predictions, {gc.get_count()} objects)")

gc.collect()
print(f"\nInference terminee:")
print(f"  {total_blobs_written} fichiers Parquet ecrits dans Gold")
print(f"  {total_predictions:,} predictions au total")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4b. Unity Catalog -- Enregistrement des tables Gold
# MAGIC
# MAGIC Enregistrement des predictions et actuals comme tables Delta dans UC Gold.

# COMMAND ----------

# DBTITLE Enregistrement Gold dans Unity Catalog

print("Enregistrement des tables Gold dans Unity Catalog...")

try:
    from pyspark.sql import SparkSession
    spark_uc = SparkSession.builder.getOrCreate()

    # Lire les predictions depuis GCS Gold et enregistrer dans UC
    gold_pred_blobs = [b for b in gold_bucket.list_blobs(prefix="predictions/")
                       if b.name.endswith(".parquet") and b.size > 1]
    gold_act_blobs = [b for b in gold_bucket.list_blobs(prefix="actuals/")
                      if b.name.endswith(".parquet") and b.size > 1]

    # --- Predictions ---
    if gold_pred_blobs:
        pred_dfs = []
        for blob in gold_pred_blobs:
            content = blob.download_as_bytes()
            table = pq.read_table(io.BytesIO(content))
            pred_dfs.append(table.to_pandas())
            del content, table
        if pred_dfs:
            pred_all = pd.concat(pred_dfs, ignore_index=True)
            del pred_dfs
            gc.collect()
            pred_spark = spark_uc.createDataFrame(pred_all)
            del pred_all
            gc.collect()
            pred_spark.write.mode("overwrite").saveAsTable(
                f"{UC_CATALOG}.gold.energy_predictions"
            )
            print(f"  {UC_CATALOG}.gold.energy_predictions : {pred_spark.count():,} lignes")
            del pred_spark
            gc.collect()

    # --- Actuals ---
    if gold_act_blobs:
        act_dfs = []
        for blob in gold_act_blobs:
            content = blob.download_as_bytes()
            table = pq.read_table(io.BytesIO(content))
            act_dfs.append(table.to_pandas())
            del content, table
        if act_dfs:
            act_all = pd.concat(act_dfs, ignore_index=True)
            del act_dfs
            gc.collect()
            act_spark = spark_uc.createDataFrame(act_all)
            del act_all
            gc.collect()
            act_spark.write.mode("overwrite").saveAsTable(
                f"{UC_CATALOG}.gold.energy_actuals"
            )
            print(f"  {UC_CATALOG}.gold.energy_actuals : {act_spark.count():,} lignes")
            del act_spark
            gc.collect()

    print("Enregistrement Gold UC termine.")

except Exception as e:
    print(f"  Gold UC: erreur -- {e}")
    print(f"  -> Les donnees restent disponibles dans GCS Gold pour Snowpipe")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verification

# COMMAND ----------

# Lister les fichiers dans Gold
print(f"=== Contenu de gs://{BUCKET_GOLD}/predictions/ ===\n")

blobs = list(gold_bucket.list_blobs(prefix="predictions/"))
parquet_blobs = [b for b in blobs if b.name.endswith(".parquet")]
total_size_mb = sum(b.size for b in parquet_blobs) / (1024 * 1024)

print(f"  Fichiers Parquet: {len(parquet_blobs)}")
print(f"  Taille totale: {total_size_mb:.1f} MB")

for blob in parquet_blobs[:10]:
    print(f"    {blob.name} ({blob.size / 1024:.1f} KB)")
if len(parquet_blobs) > 10:
    print(f"    ... et {len(parquet_blobs) - 10} autres fichiers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resume

# COMMAND ----------

# Calculer des stats globales sur les predictions
if all_stats:
    combined_stats = pd.DataFrame(all_stats)
    avg_mean = combined_stats["mean"].mean()
    avg_min = combined_stats["min"].min()
    avg_max = combined_stats["max"].max()
else:
    avg_mean = avg_min = avg_max = 0

print(f"""
{'='*60}
RESUME INFERENCE
{'='*60}

Model Version:         {model_version}
Predictions generees:  {total_predictions:,}
Fichiers Parquet:      {total_blobs_written}

Stats predictions (kWh):
  Min:    {avg_min:.4f}
  Max:    {avg_max:.4f}
  Mean:   {avg_mean:.4f}

Sortie:
  gs://{BUCKET_GOLD}/predictions/ ({total_blobs_written} fichiers Parquet)
  -> Snowpipe auto-ingestion vers Snowflake

Unity Catalog:
  {UC_CATALOG}.gold.energy_predictions
  {UC_CATALOG}.gold.energy_actuals
  {UC_CATALOG}.gold.walk_forward_metrics
  {UC_CATALOG}.gold.feature_importance

{'='*60}
""")

# Verification finale UC
try:
    from pyspark.sql import SparkSession
    spark_final = SparkSession.builder.getOrCreate()
    print(f"=== Tables Unity Catalog ===\n")
    for schema in ["bronze", "silver", "gold"]:
        try:
            tables = spark_final.sql(f"SHOW TABLES IN {UC_CATALOG}.{schema}").collect()
            print(f"  {UC_CATALOG}.{schema}: {len(tables)} tables")
            for t in tables:
                print(f"    - {t.tableName}")
        except Exception:
            print(f"  {UC_CATALOG}.{schema}: non accessible")
    print()
except Exception as e:
    print(f"UC verification: {e}")
