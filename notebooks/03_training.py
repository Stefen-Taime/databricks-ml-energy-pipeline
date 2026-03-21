# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3 --- Training
# MAGIC
# MAGIC Charge les tables Silver depuis GCS, effectue le Feature Engineering,
# MAGIC la selection automatique des features (2 passes), l'entrainement XGBoost
# MAGIC avec validation Walk-Forward, et logge tout dans MLflow.
# MAGIC
# MAGIC **Pipeline :**
# MAGIC 1. Feature Engineering (jointure de toutes les tables Silver) -- 100% Pandas
# MAGIC 2. Passe 1 : Entrainement exploratoire sur toutes les features
# MAGIC 3. Quality Gate : Elimination des features a importance quasi nulle
# MAGIC 4. Passe 2 : Reentrainement sur les features retenues
# MAGIC 5. Walk-Forward Validation (4 rounds : nov 2013 -> fev 2014)
# MAGIC 6. Quality Gate finale : MAPE < 15%, R2 > 0.75
# MAGIC 7. MLflow : log metriques, hyperparams, modele, artefacts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Installation des dependances

# COMMAND ----------

%pip install google-cloud-storage xgboost scikit-learn pyarrow --quiet

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
import pyarrow.parquet as pq
from datetime import datetime

from google.cloud import storage
from google.oauth2 import service_account

import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, mean_absolute_percentage_error

# ---------- MLflow : configurer AVANT tout import/appel ----------
# Sur Databricks Serverless, spark.mlflow.modelRegistryUri n'est pas
# accessible via Spark Connect. On fixe le registry URI via env var
# pour eviter que MLflow ne tente de lire cette config Spark.
os.environ["MLFLOW_REGISTRY_URI"] = "databricks-uc"

import mlflow
import mlflow.xgboost

mlflow.set_registry_uri("databricks-uc")

# ---------- Secrets ----------
GCP_SA_KEY_JSON = dbutils.secrets.get(scope="ml-energy", key="gcp-sa-key")

# ---------- Unity Catalog ----------
try:
    UC_CATALOG = dbutils.jobs.taskValues.get(
        taskKey="02_nettoyage_silver", key="uc_catalog", debugValue="ml_energy"
    )
except Exception:
    UC_CATALOG = "ml_energy"

print(f"Unity Catalog: {UC_CATALOG}")

# ---------- Buckets ----------
BUCKET_SILVER = "ml-energy-consumption-silver"

# ---------- GCS Client ----------
gcp_credentials = service_account.Credentials.from_service_account_info(
    json.loads(GCP_SA_KEY_JSON)
)
gcs_client = storage.Client(credentials=gcp_credentials, project=gcp_credentials.project_id)
silver_bucket = gcs_client.bucket(BUCKET_SILVER)

# ---------- MLflow Experiment ----------
# Utiliser un chemin sous le repertoire utilisateur pour eviter
# RESOURCE_DOES_NOT_EXIST sur le workspace
# NOTE: Update this path to match your Databricks username/email
import os
username = os.environ.get("DATABRICKS_USER", "your-email@example.com")
mlflow.set_experiment(f"/Users/{username}/ml-energy-training")

# ---------- Model Config ----------
MODEL_VERSION = f"v1.0_{datetime.now().strftime('%Y%m%d_%H%M')}"
TARGET_COL = "kwh"
WMAPE_THRESHOLD = 0.35  # 35% -- Seuil realiste pour prediction demi-horaire multi-foyers
R2_THRESHOLD = 0.70     # Seuil minimum acceptable pour un modele de consommation energie


def weighted_mape(y_true, y_pred):
    """Weighted MAPE : sum(|y_true - y_pred|) / sum(|y_true|).
    Standard industrie energie, insensible aux petites valeurs contrairement au MAPE."""
    return np.sum(np.abs(y_true - y_pred)) / np.sum(np.abs(y_true))

# Echantillonnage : limiter le nombre de foyers pour le training
# (le modele apprend des patterns generaux, pas besoin de TOUS les foyers)
MAX_USERS_TRAINING = 100

print(f"Model version: {MODEL_VERSION}")
print(f"Max users for training: {MAX_USERS_TRAINING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Chargement des tables Silver (100% Pandas, bloc par bloc)

# COMMAND ----------

# DBTITLE Fonctions utilitaires de chargement

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
        content = blob.download_as_bytes()
        table = pq.read_table(io.BytesIO(content))
        dfs.append(table.to_pandas())

    return pd.concat(dfs, ignore_index=True)


def read_sm_sampled(max_users: int = MAX_USERS_TRAINING):
    """
    Charge les Smart Meters bloc par bloc, echantillonne les users,
    et retourne un DataFrame Pandas contenant seulement les users selectionnes.
    """
    blobs = list(silver_bucket.list_blobs(prefix="smart_meters/"))
    parquet_blobs = [b for b in blobs if b.name.endswith(".parquet") and b.size > 1]

    if not parquet_blobs:
        raise FileNotFoundError(f"Aucun fichier parquet dans gs://{BUCKET_SILVER}/smart_meters/")

    print(f"  Smart Meters: {len(parquet_blobs)} blocs Parquet a scanner")

    # Premier passage : collecter tous les user_ids
    all_users = set()
    for blob in parquet_blobs:
        content = blob.download_as_bytes()
        table = pq.read_table(io.BytesIO(content), columns=["user_id"])
        all_users.update(table.to_pandas()["user_id"].unique())
        del content, table

    print(f"  Smart Meters: {len(all_users)} users au total")

    # Echantillonner les users
    all_users_list = sorted(all_users)
    if len(all_users_list) > max_users:
        rng = np.random.RandomState(42)
        sampled_users = set(rng.choice(all_users_list, size=max_users, replace=False))
        print(f"  Smart Meters: echantillon de {max_users} users")
    else:
        sampled_users = all_users
        print(f"  Smart Meters: tous les {len(sampled_users)} users retenus")

    # Deuxieme passage : charger uniquement les users selectionnes
    dfs = []
    for i, blob in enumerate(parquet_blobs):
        content = blob.download_as_bytes()
        table = pq.read_table(io.BytesIO(content))
        pdf = table.to_pandas()
        pdf = pdf[pdf["user_id"].isin(sampled_users)]
        if len(pdf) > 0:
            dfs.append(pdf)
        del content, table, pdf
        if (i + 1) % 30 == 0:
            gc.collect()
            print(f"    {i+1}/{len(parquet_blobs)} blocs traites...")

    gc.collect()
    result = pd.concat(dfs, ignore_index=True)
    del dfs
    gc.collect()

    # Convertir les colonnes numeriques en float32 pour economiser la memoire
    for col in result.select_dtypes(include=["float64"]).columns:
        result[col] = result[col].astype(np.float32)

    print(f"  Smart Meters charge: {len(result):,} lignes, {result['user_id'].nunique()} users")
    print(f"  Memoire: {result.memory_usage(deep=True).sum() / (1024*1024):.1f} MB")
    return result

# COMMAND ----------

# DBTITLE Charger toutes les tables Silver

print("Chargement des tables Silver...")

# Smart Meters (echantillonne)
df_sm = read_sm_sampled(MAX_USERS_TRAINING)

# Weather (petit dataset)
df_weather = read_parquet_from_gcs_pandas("weather")
print(f"  Weather: {len(df_weather):,} lignes")

# Households (petit dataset)
df_hh = read_parquet_from_gcs_pandas("households")
print(f"  Households: {len(df_hh):,} lignes")

# Holidays (petit dataset)
df_hol = read_parquet_from_gcs_pandas("holidays")
print(f"  Holidays: {len(df_hol):,} lignes")

# NESO (optionnel)
try:
    df_neso = read_parquet_from_gcs_pandas("neso")
    has_neso = True
    print(f"  NESO: {len(df_neso):,} lignes")
except Exception:
    has_neso = False
    print("  NESO: non disponible (skip)")

# Elexon (optionnel)
try:
    df_elexon = read_parquet_from_gcs_pandas("elexon")
    has_elexon = True
    print(f"  Elexon: {len(df_elexon):,} lignes")
except Exception:
    has_elexon = False
    print("  Elexon: non disponible (skip)")

gc.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Feature Engineering (100% Pandas)
# MAGIC
# MAGIC Tout en Pandas pour eviter les problemes de memoire Spark sur serverless.

# COMMAND ----------

# DBTITLE Features temporelles

print("Construction des features temporelles...")

df_sm["timestamp"] = pd.to_datetime(df_sm["timestamp"], errors="coerce")
df_sm = df_sm.dropna(subset=["timestamp"])
df_sm = df_sm.sort_values(["user_id", "timestamp"]).reset_index(drop=True)

df_sm["date"] = df_sm["timestamp"].dt.date.astype(str)
df_sm["hour"] = df_sm["timestamp"].dt.hour
df_sm["minute"] = df_sm["timestamp"].dt.minute
df_sm["day_of_week"] = df_sm["timestamp"].dt.dayofweek + 1  # 1=lundi, 7=dimanche
df_sm["day_of_month"] = df_sm["timestamp"].dt.day
df_sm["month"] = df_sm["timestamp"].dt.month
df_sm["week_of_year"] = df_sm["timestamp"].dt.isocalendar().week.astype(int)
df_sm["is_weekend"] = df_sm["day_of_week"].isin([6, 7]).astype(int)

# Settlement period (1-48 pour chaque demi-heure)
df_sm["settlement_period"] = (df_sm["hour"] * 2 + (df_sm["minute"] // 30) + 1).astype(int)

# --- Features cycliques (encodage sin/cos pour capturer la periodicite) ---
df_sm["hour_sin"] = np.sin(2 * np.pi * df_sm["hour"] / 24).astype(np.float32)
df_sm["hour_cos"] = np.cos(2 * np.pi * df_sm["hour"] / 24).astype(np.float32)
df_sm["dow_sin"] = np.sin(2 * np.pi * df_sm["day_of_week"] / 7).astype(np.float32)
df_sm["dow_cos"] = np.cos(2 * np.pi * df_sm["day_of_week"] / 7).astype(np.float32)
df_sm["month_sin"] = np.sin(2 * np.pi * df_sm["month"] / 12).astype(np.float32)
df_sm["month_cos"] = np.cos(2 * np.pi * df_sm["month"] / 12).astype(np.float32)

# --- Filtrage des consommations quasi-nulles (bruit, compteur inactif) ---
rows_before = len(df_sm)
df_sm = df_sm[df_sm["kwh"] >= 0.01].reset_index(drop=True)
rows_removed = rows_before - len(df_sm)
print(f"  Filtre kwh < 0.01 : {rows_removed:,} lignes supprimees ({rows_removed/rows_before*100:.1f}%)")

print(f"  {len(df_sm.columns)} colonnes apres features temporelles + cycliques")

# COMMAND ----------

# DBTITLE Features lag (historique de consommation)

print("Construction des features lag par user...")

# Lag features : consommation passee du meme foyer
lag_periods = {
    "kwh_lag_1": 1,       # 30 min
    "kwh_lag_2": 2,       # 1h
    "kwh_lag_4": 4,       # 2h
    "kwh_lag_48": 48,     # 24h
    "kwh_lag_336": 336,   # 7 jours
}

for col_name, lag_n in lag_periods.items():
    df_sm[col_name] = df_sm.groupby("user_id")["kwh"].shift(lag_n)

# Rolling mean (24h = 48 points)
df_sm["kwh_rolling_mean_24h"] = df_sm.groupby("user_id")["kwh"].transform(
    lambda x: x.rolling(window=48, min_periods=1).mean().shift(1)
)

# Rolling std (24h)
df_sm["kwh_rolling_std_24h"] = df_sm.groupby("user_id")["kwh"].transform(
    lambda x: x.rolling(window=48, min_periods=1).std().shift(1)
)

print("  Features lag ajoutees")

# COMMAND ----------

# DBTITLE Jointure Weather

print("Jointure Weather...")

# Aligner les timestamps weather
df_weather["timestamp"] = pd.to_datetime(df_weather["timestamp"], errors="coerce")
df_weather = df_weather.dropna(subset=["timestamp"])
df_weather = df_weather.drop_duplicates(subset=["timestamp"])

df = df_sm.merge(df_weather, on="timestamp", how="left")
del df_sm, df_weather
gc.collect()

# Convertir en float32
for col in df.select_dtypes(include=["float64"]).columns:
    df[col] = df[col].astype(np.float32)

# --- Features interaction temperature x heure ---
if "temperature" in df.columns:
    df["temp_x_hour"] = (df["temperature"] * df["hour"]).astype(np.float32)
    df["temp_x_hour_sin"] = (df["temperature"] * df["hour_sin"]).astype(np.float32)
if "apparentTemperature" in df.columns:
    df["apparent_temp_x_hour"] = (df["apparentTemperature"] * df["hour"]).astype(np.float32)

print(f"  {len(df.columns)} colonnes apres jointure Weather + interactions")
print(f"  Memoire df: {df.memory_usage(deep=True).sum() / (1024*1024):.1f} MB")

# COMMAND ----------

# DBTITLE Jointure Households (ACORN)

print("Jointure Households...")

# Encoder ACORN en labels numeriques
acorn_cols_to_encode = []
for col in ["acorn_group", "acorn_category"]:
    if col in df_hh.columns:
        df_hh[f"{col}_idx"] = df_hh[col].astype("category").cat.codes.astype(float)
        acorn_cols_to_encode.append(f"{col}_idx")

if "tariff_type" in df_hh.columns:
    df_hh["tariff_type_idx"] = df_hh["tariff_type"].astype("category").cat.codes.astype(float)
    acorn_cols_to_encode.append("tariff_type_idx")

# Selectionner les colonnes a joindre (user_id + colonnes numeriques)
hh_join_cols = ["user_id"] + acorn_cols_to_encode
hh_join_cols = [c for c in hh_join_cols if c in df_hh.columns]
df = df.merge(df_hh[hh_join_cols], on="user_id", how="left")
del df_hh

print(f"  {len(df.columns)} colonnes apres jointure Households")

# COMMAND ----------

# DBTITLE Jointure Holidays

print("Jointure Holidays...")

df_hol["date"] = pd.to_datetime(df_hol["date"], errors="coerce").dt.strftime("%Y-%m-%d")
if "is_holiday" not in df_hol.columns:
    df_hol["is_holiday"] = 1

df_hol_dedup = df_hol[["date", "is_holiday"]].drop_duplicates(subset=["date"])
df = df.merge(df_hol_dedup, on="date", how="left")
df["is_holiday"] = df["is_holiday"].fillna(0).astype(int)
del df_hol, df_hol_dedup
gc.collect()

print(f"  {len(df.columns)} colonnes apres jointure Holidays")

# COMMAND ----------

# DBTITLE Jointure NESO / Elexon (optionnel)

if has_neso:
    df_neso["timestamp"] = pd.to_datetime(df_neso["timestamp"], errors="coerce")
    neso_cols = [c for c in df_neso.columns if c != "timestamp"]
    df_neso = df_neso.rename(columns={c: f"neso_{c}" for c in neso_cols})
    df_neso = df_neso.drop_duplicates(subset=["timestamp"])
    df = df.merge(df_neso, on="timestamp", how="left")
    print(f"  NESO joint: {len(neso_cols)} colonnes ajoutees")
    del df_neso

if has_elexon:
    df_elexon["timestamp"] = pd.to_datetime(df_elexon["timestamp"], errors="coerce")
    elexon_cols = [c for c in df_elexon.columns if c != "timestamp"]
    df_elexon = df_elexon.rename(columns={c: f"elexon_{c}" for c in elexon_cols})
    df_elexon = df_elexon.drop_duplicates(subset=["timestamp"])
    df = df.merge(df_elexon, on="timestamp", how="left")
    print(f"  Elexon joint: {len(elexon_cols)} colonnes ajoutees")
    del df_elexon

gc.collect()

# COMMAND ----------

# DBTITLE Preparation du dataset final

# Supprimer les lignes avec des lag null (debut de serie)
df = df.dropna(subset=["kwh_lag_336"])

print(f"Dataset final: {len(df):,} lignes, {len(df.columns)} colonnes")

# Colonnes a exclure de l'entrainement (identifiants, cible, texte)
EXCLUDE_COLS = [
    "user_id", "timestamp", "date", TARGET_COL,
    "acorn_group", "acorn_category", "tariff_type", "holiday_name"
]

feature_cols = [c for c in df.columns if c not in EXCLUDE_COLS
                and not c.endswith("_string")
                and df[c].dtype in ["int64", "float64", "int32", "float32", "int", "float"]]

print(f"\nFeatures pour l'entrainement: {len(feature_cols)}")
print(feature_cols)

# Convertir les colonnes en float32 (pour XGBoost -- economise la memoire)
for col in feature_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce").astype(np.float32)

gc.collect()
print(f"  Memoire totale du DataFrame: {df.memory_usage(deep=True).sum() / (1024*1024):.1f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Walk-Forward Validation

# COMMAND ----------

# DBTITLE Definition des rounds Walk-Forward

WALK_FORWARD_ROUNDS = [
    {
        "round": 1,
        "train_end": "2013-10-31",
        "test_start": "2013-11-01",
        "test_end": "2013-11-30",
        "test_label": "2013-11",
    },
    {
        "round": 2,
        "train_end": "2013-11-30",
        "test_start": "2013-12-01",
        "test_end": "2013-12-31",
        "test_label": "2013-12",
    },
    {
        "round": 3,
        "train_end": "2013-12-31",
        "test_start": "2014-01-01",
        "test_end": "2014-01-31",
        "test_label": "2014-01",
    },
    {
        "round": 4,
        "train_end": "2014-01-31",
        "test_start": "2014-02-01",
        "test_end": "2014-02-28",
        "test_label": "2014-02",
    },
]

TRAIN_START = "2012-07-01"

print("Walk-Forward Rounds:")
for r in WALK_FORWARD_ROUNDS:
    print(f"  Round {r['round']}: Train [{TRAIN_START} -> {r['train_end']}] "
          f"-> Test [{r['test_start']} -> {r['test_end']}]")

# COMMAND ----------

# DBTITLE XGBoost hyperparametres

XGBOOST_PARAMS = {
    "objective": "reg:squarederror",
    "eval_metric": "rmse",
    "max_depth": 8,
    "learning_rate": 0.05,
    "n_estimators": 500,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 5,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "random_state": 42,
    "n_jobs": 2,
    "early_stopping_rounds": 50,
}

print("Hyperparametres XGBoost:")
for k, v in XGBOOST_PARAMS.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Passe 1 --- Entrainement exploratoire (toutes les features)

# COMMAND ----------

# DBTITLE Passe 1 : Entrainement sur Round 1 pour la selection

# Round 1 pour la selection de features
r1 = WALK_FORWARD_ROUNDS[0]

train_mask = (df["date"] >= TRAIN_START) & (df["date"] <= r1["train_end"])
test_mask = (df["date"] >= r1["test_start"]) & (df["date"] <= r1["test_end"])

X_train_p1 = df.loc[train_mask, feature_cols].values.astype(np.float32)
y_train_p1 = df.loc[train_mask, TARGET_COL].values.astype(np.float32)
X_test_p1 = df.loc[test_mask, feature_cols].values.astype(np.float32)
y_test_p1 = df.loc[test_mask, TARGET_COL].values.astype(np.float32)

print(f"Passe 1 - Train: {X_train_p1.shape}, Test: {X_test_p1.shape}")

# Entrainement
model_p1 = xgb.XGBRegressor(**{
    k: v for k, v in XGBOOST_PARAMS.items() if k != "early_stopping_rounds"
})
model_p1.set_params(early_stopping_rounds=XGBOOST_PARAMS["early_stopping_rounds"])

model_p1.fit(
    X_train_p1, y_train_p1,
    eval_set=[(X_test_p1, y_test_p1)],
    verbose=50
)

# Metriques Passe 1
y_pred_p1 = model_p1.predict(X_test_p1)
wmape_p1 = weighted_mape(y_test_p1, y_pred_p1)
rmse_p1 = np.sqrt(mean_squared_error(y_test_p1, y_pred_p1))
r2_p1 = r2_score(y_test_p1, y_pred_p1)

print(f"\nPasse 1 - Metriques sur Round 1:")
print(f"  WMAPE: {wmape_p1:.4f} ({wmape_p1 * 100:.2f}%)")
print(f"  RMSE: {rmse_p1:.4f}")
print(f"  R2:   {r2_p1:.4f}")

del X_train_p1, y_train_p1, X_test_p1, y_test_p1
gc.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Quality Gate --- Selection des features

# COMMAND ----------

# DBTITLE Feature importance et selection

# Extraire l'importance des features
importance = model_p1.feature_importances_
feature_importance = pd.DataFrame({
    "feature": feature_cols,
    "importance": importance
}).sort_values("importance", ascending=False)

print("Top 20 features (Passe 1):")
print(feature_importance.head(20).to_string(index=False))

# Seuil : eliminer les features avec importance < 1% de l'importance max
importance_threshold = feature_importance["importance"].max() * 0.01
selected_features = feature_importance[
    feature_importance["importance"] >= importance_threshold
]["feature"].tolist()

eliminated = [f for f in feature_cols if f not in selected_features]

print(f"\n--- Quality Gate Features ---")
print(f"Features totales: {len(feature_cols)}")
print(f"Features retenues: {len(selected_features)}")
print(f"Features eliminees ({len(eliminated)}): {eliminated}")

del model_p1
gc.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Passe 2 --- Walk-Forward complet avec features selectionnees

# COMMAND ----------

# DBTITLE Walk-Forward Validation (4 rounds)

all_round_metrics = []
tmp_dir = tempfile.mkdtemp()

with mlflow.start_run(run_name=f"walk_forward_{MODEL_VERSION}") as parent_run:
    # Log des hyperparametres et config globale
    mlflow.log_params({
        "model_type": "XGBRegressor",
        "model_version": MODEL_VERSION,
        "n_features_total": len(feature_cols),
        "n_features_selected": len(selected_features),
        "n_walk_forward_rounds": len(WALK_FORWARD_ROUNDS),
        "train_start": TRAIN_START,
        "wmape_pass1": round(wmape_p1, 4),
        "n_users_sampled": int(df["user_id"].nunique()),
        **{k: str(v) for k, v in XGBOOST_PARAMS.items()},
    })

    # Log de la liste des features selectionnees
    mlflow.log_text(
        "\n".join(selected_features),
        "selected_features.txt"
    )

    # Log du classement complet des features
    fi_path = os.path.join(tmp_dir, "feature_importance.csv")
    feature_importance.to_csv(fi_path, index=False)
    mlflow.log_artifact(fi_path)

    best_model = None
    best_wmape = float("inf")

    for wf_round in WALK_FORWARD_ROUNDS:
        round_num = wf_round["round"]
        print(f"\n{'='*60}")
        print(f"Walk-Forward Round {round_num}: "
              f"Train [{TRAIN_START} -> {wf_round['train_end']}] "
              f"-> Test [{wf_round['test_start']} -> {wf_round['test_end']}]")
        print(f"{'='*60}")

        with mlflow.start_run(
            run_name=f"round_{round_num}_{wf_round['test_label']}",
            nested=True
        ) as child_run:
            # Preparer les donnees
            train_mask = (
                (df["date"] >= TRAIN_START) &
                (df["date"] <= wf_round["train_end"])
            )
            test_mask = (
                (df["date"] >= wf_round["test_start"]) &
                (df["date"] <= wf_round["test_end"])
            )

            X_train = df.loc[train_mask, selected_features].values.astype(np.float32)
            y_train = df.loc[train_mask, TARGET_COL].values.astype(np.float32)
            X_test = df.loc[test_mask, selected_features].values.astype(np.float32)
            y_test = df.loc[test_mask, TARGET_COL].values.astype(np.float32)

            print(f"  Train: {X_train.shape[0]:,} lignes, Test: {X_test.shape[0]:,} lignes")

            # Entrainement
            model = xgb.XGBRegressor(**{
                k: v for k, v in XGBOOST_PARAMS.items()
                if k != "early_stopping_rounds"
            })
            model.set_params(early_stopping_rounds=XGBOOST_PARAMS["early_stopping_rounds"])

            model.fit(
                X_train, y_train,
                eval_set=[(X_test, y_test)],
                verbose=False
            )

            # Predictions
            y_pred = model.predict(X_test)

            # Metriques
            wmape = weighted_mape(y_test, y_pred)
            rmse = np.sqrt(mean_squared_error(y_test, y_pred))
            mae = mean_absolute_error(y_test, y_pred)
            r2 = r2_score(y_test, y_pred)

            round_metrics = {
                "round": round_num,
                "test_month": wf_round["test_label"],
                "train_end": wf_round["train_end"],
                "wmape": wmape,
                "rmse": rmse,
                "mae": mae,
                "r2": r2,
                "train_size": X_train.shape[0],
                "test_size": X_test.shape[0],
            }
            all_round_metrics.append(round_metrics)

            # Log dans MLflow (child run)
            mlflow.log_metrics({
                "wmape": wmape,
                "rmse": rmse,
                "mae": mae,
                "r2": r2,
                "train_size": X_train.shape[0],
                "test_size": X_test.shape[0],
            })
            mlflow.log_params({
                "round": round_num,
                "test_month": wf_round["test_label"],
                "train_end": wf_round["train_end"],
            })

            print(f"  WMAPE: {wmape:.4f} ({wmape * 100:.2f}%)")
            print(f"  RMSE: {rmse:.4f}")
            print(f"  MAE:  {mae:.4f}")
            print(f"  R2:   {r2:.4f}")

            # Garder le meilleur modele
            if wmape < best_wmape:
                best_wmape = wmape
                best_model = model
                best_round = round_num

            del X_train, y_train, X_test, y_test
            gc.collect()

    # --- Metriques globales ---
    df_metrics = pd.DataFrame(all_round_metrics)
    avg_wmape = df_metrics["wmape"].mean()
    avg_rmse = df_metrics["rmse"].mean()
    avg_mae = df_metrics["mae"].mean()
    avg_r2 = df_metrics["r2"].mean()

    print(f"\n{'='*60}")
    print(f"METRIQUES MOYENNES Walk-Forward ({len(WALK_FORWARD_ROUNDS)} rounds)")
    print(f"{'='*60}")
    print(f"  WMAPE moyen: {avg_wmape:.4f} ({avg_wmape * 100:.2f}%)")
    print(f"  RMSE moyen: {avg_rmse:.4f}")
    print(f"  MAE moyen:  {avg_mae:.4f}")
    print(f"  R2 moyen:   {avg_r2:.4f}")
    print(f"  Meilleur round: {best_round} (WMAPE: {best_wmape:.4f})")

    # Log metriques globales dans le parent run
    mlflow.log_metrics({
        "avg_wmape": avg_wmape,
        "avg_rmse": avg_rmse,
        "avg_mae": avg_mae,
        "avg_r2": avg_r2,
        "best_round": best_round,
        "best_wmape": best_wmape,
    })

    # Log le tableau des metriques
    metrics_path = os.path.join(tmp_dir, "walk_forward_metrics.csv")
    df_metrics.to_csv(metrics_path, index=False)
    mlflow.log_artifact(metrics_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Quality Gate finale

# COMMAND ----------

# DBTITLE Quality Gate

print(f"{'='*60}")
print(f"QUALITY GATE")
print(f"{'='*60}")
print(f"  WMAPE moyen:  {avg_wmape:.4f}  (seuil: < {WMAPE_THRESHOLD})")
print(f"  R2 moyen:    {avg_r2:.4f}  (seuil: > {R2_THRESHOLD})")

quality_gate_passed = (avg_wmape < WMAPE_THRESHOLD) and (avg_r2 > R2_THRESHOLD)

if quality_gate_passed:
    print(f"\n  QUALITY GATE: PASSED")
    print(f"  Le modele est valide pour la production.")
else:
    print(f"\n  QUALITY GATE: FAILED (seuils non atteints)")
    if avg_wmape >= WMAPE_THRESHOLD:
        print(f"  -> WMAPE trop eleve ({avg_wmape:.4f} >= {WMAPE_THRESHOLD})")
    if avg_r2 <= R2_THRESHOLD:
        print(f"  -> R2 trop bas ({avg_r2:.4f} <= {R2_THRESHOLD})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Enregistrement du meilleur modele dans MLflow

# COMMAND ----------

# DBTITLE Log du modele final

if quality_gate_passed:
    with mlflow.start_run(run_name=f"best_model_{MODEL_VERSION}") as final_run:
        # Log du modele XGBoost dans Unity Catalog Model Registry
        # Format UC 3-level namespace : catalog.schema.model_name
        UC_MODEL_NAME = f"{UC_CATALOG}.gold.energy_consumption_xgboost"
        try:
            mlflow.xgboost.log_model(
                best_model,
                artifact_path="model",
                registered_model_name=UC_MODEL_NAME,
            )
            print(f"  Modele enregistre dans UC: {UC_MODEL_NAME}")
        except Exception as e:
            print(f"  Warning: Enregistrement UC echoue: {e}")
            # Fallback : workspace Model Registry (nom simple)
            try:
                mlflow.xgboost.log_model(
                    best_model,
                    artifact_path="model",
                    registered_model_name="ml_energy_xgboost",
                )
                print(f"  Modele enregistre dans workspace registry: ml_energy_xgboost")
            except Exception as e2:
                print(f"  Warning: Workspace registry aussi echoue: {e2}")
                print(f"  -> Log du modele sans enregistrement dans le Registry")
                mlflow.xgboost.log_model(
                    best_model,
                    artifact_path="model",
                )

        # Log des metriques finales
        mlflow.log_metrics({
            "avg_wmape": avg_wmape,
            "avg_rmse": avg_rmse,
            "avg_mae": avg_mae,
            "avg_r2": avg_r2,
        })

        mlflow.log_params({
            "model_version": MODEL_VERSION,
            "quality_gate": "PASSED",
            "n_features": len(selected_features),
            "best_round": best_round,
        })

        # Log des artefacts
        mlflow.log_text("\n".join(selected_features), "selected_features.txt")
        final_metrics_path = os.path.join(tmp_dir, "final_metrics.csv")
        df_metrics.to_csv(final_metrics_path, index=False)
        mlflow.log_artifact(final_metrics_path)

        model_uri = f"runs:/{final_run.info.run_id}/model"
        print(f"Modele enregistre dans MLflow:")
        print(f"  Run ID: {final_run.info.run_id}")
        print(f"  Model URI: {model_uri}")

    # Sauvegarder le model URI pour le Notebook 4
    dbutils.jobs.taskValues.set(key="model_uri", value=model_uri)
    dbutils.jobs.taskValues.set(key="model_version", value=MODEL_VERSION)
    dbutils.jobs.taskValues.set(key="selected_features", value=json.dumps(selected_features))
    dbutils.jobs.taskValues.set(key="quality_gate", value="PASSED")
    dbutils.jobs.taskValues.set(key="uc_catalog", value=UC_CATALOG)

    # --- Unity Catalog : enregistrer les metriques et feature importance ---
    try:
        from pyspark.sql import SparkSession
        spark_uc = SparkSession.builder.getOrCreate()

        # Table des metriques Walk-Forward
        df_metrics["model_version"] = MODEL_VERSION
        df_metrics_spark = spark_uc.createDataFrame(df_metrics)
        df_metrics_spark.write.mode("overwrite").saveAsTable(
            f"{UC_CATALOG}.gold.walk_forward_metrics"
        )
        print(f"  {UC_CATALOG}.gold.walk_forward_metrics enregistre")

        # Table du feature importance
        feature_importance["model_version"] = MODEL_VERSION
        fi_spark = spark_uc.createDataFrame(feature_importance)
        fi_spark.write.mode("overwrite").saveAsTable(
            f"{UC_CATALOG}.gold.feature_importance"
        )
        print(f"  {UC_CATALOG}.gold.feature_importance enregistre")
    except Exception as e:
        print(f"  UC training tables: erreur -- {e}")
else:
    print("Quality Gate FAILED -- Modele NON enregistre.")
    dbutils.jobs.taskValues.set(key="quality_gate", value="FAILED")
    raise Exception(
        f"Quality Gate FAILED: WMAPE={avg_wmape:.4f} (seuil {WMAPE_THRESHOLD}), "
        f"R2={avg_r2:.4f} (seuil {R2_THRESHOLD})"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Resume

# COMMAND ----------

print(f"""
{'='*60}
RESUME TRAINING
{'='*60}

Model Version:        {MODEL_VERSION}
Quality Gate:         {'PASSED' if quality_gate_passed else 'FAILED'}
Users echantillonnes: {df['user_id'].nunique()}

Features:
  Total initiales:    {len(feature_cols)}
  Selectionnees:      {len(selected_features)}
  Eliminees:          {len(feature_cols) - len(selected_features)}

Walk-Forward ({len(WALK_FORWARD_ROUNDS)} rounds):
  WMAPE moyen:         {avg_wmape:.4f} ({avg_wmape * 100:.2f}%)
  RMSE moyen:         {avg_rmse:.4f}
  MAE moyen:          {avg_mae:.4f}
  R2 moyen:           {avg_r2:.4f}

Metriques par round:
""")

for _, row in df_metrics.iterrows():
    print(f"  Round {int(row['round'])} ({row['test_month']}): "
          f"WMAPE={row['wmape']:.4f}, RMSE={row['rmse']:.4f}, "
          f"MAE={row['mae']:.4f}, R2={row['r2']:.4f}")
