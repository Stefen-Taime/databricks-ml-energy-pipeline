# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1 --- Ingestion Bronze (APIs uniquement)
# MAGIC
# MAGIC Les fichiers statiques du dataset Kaggle (Smart Meters, Households, Weather,
# MAGIC Holidays) sont deja dans GCS Bronze via `terraform apply` (gsutil upload).
# MAGIC
# MAGIC Ce notebook ne gere que les **sources API dynamiques** :
# MAGIC - NESO Data Portal API (demande nationale UK)
# MAGIC - Elexon BMRS API (prix marche)
# MAGIC - Carbon Intensity API (mix generation -- optionnel, donnees depuis 2018)
# MAGIC
# MAGIC **Pre-requis :**
# MAGIC 1. `terraform apply` execute (buckets + donnees Kaggle deja dans Bronze)
# MAGIC 2. Configurer les secrets Databricks (voir cellule Configuration)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Installation des dependances

# COMMAND ----------

# DBTITLE Install dependencies
%pip install google-cloud-storage requests --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC
# MAGIC **IMPORTANT :** Les credentials ne sont JAMAIS en dur dans le code.
# MAGIC
# MAGIC Utiliser les **Databricks Secrets** pour stocker :
# MAGIC - `gcp-sa-key` : contenu JSON de la cle du Service Account GCP
# MAGIC - `elexon-api-key` : cle API Elexon BMRS (gratuite apres inscription)
# MAGIC
# MAGIC **Setup des secrets (une seule fois via Databricks CLI) :**
# MAGIC ```bash
# MAGIC # Creer le scope
# MAGIC databricks secrets create-scope ml-energy
# MAGIC
# MAGIC # Ajouter la cle GCP (coller le JSON complet)
# MAGIC databricks secrets put-secret ml-energy gcp-sa-key
# MAGIC
# MAGIC # Ajouter la cle Elexon
# MAGIC databricks secrets put-secret ml-energy elexon-api-key
# MAGIC ```

# COMMAND ----------

import json
import os
from datetime import datetime

from google.cloud import storage
from google.oauth2 import service_account

# ---------- Databricks Secrets ----------
GCP_SA_KEY_JSON = dbutils.secrets.get(scope="ml-energy", key="gcp-sa-key")
ELEXON_API_KEY = dbutils.secrets.get(scope="ml-energy", key="elexon-api-key")

# ---------- GCS Configuration ----------
BUCKET_BRONZE = "ml-energy-consumption-bronze"

# ---------- GCS Client ----------
gcp_credentials = service_account.Credentials.from_service_account_info(
    json.loads(GCP_SA_KEY_JSON)
)
gcs_client = storage.Client(credentials=gcp_credentials, project=gcp_credentials.project_id)
bronze_bucket = gcs_client.bucket(BUCKET_BRONZE)

print(f"GCS client initialise -- projet: {gcp_credentials.project_id}")
print(f"Bucket Bronze: {BUCKET_BRONZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verification des donnees statiques (deja dans Bronze via Terraform)

# COMMAND ----------

print(f"=== Donnees statiques dans gs://{BUCKET_BRONZE}/ ===\n")

for prefix in ["smart_meters", "households", "weather", "holidays"]:
    blobs = list(bronze_bucket.list_blobs(prefix=f"{prefix}/"))
    real_blobs = [b for b in blobs if b.size > 1]
    total_size_mb = sum(b.size for b in real_blobs) / (1024 * 1024)
    print(f"  {prefix}/ : {len(real_blobs)} fichiers, {total_size_mb:.1f} MB")

print("\nSi vide, verifier que `terraform apply` a ete execute.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fonction utilitaire

# COMMAND ----------

def blob_exists(gcs_prefix: str, filename: str):
    """Verifie si un fichier existe deja dans le bucket Bronze."""
    blob = bronze_bucket.blob(f"{gcs_prefix}/{filename}")
    return blob.exists()


def upload_json_to_gcs(data, gcs_prefix: str, filename: str):
    """Upload des donnees JSON vers GCS Bronze."""
    blob = bronze_bucket.blob(f"{gcs_prefix}/{filename}")
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
    print(f"  Uploaded: gs://{BUCKET_BRONZE}/{gcs_prefix}/{filename}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. NESO Data Portal API (Demande Nationale UK)
# MAGIC
# MAGIC API CKAN gratuite, sans cle.
# MAGIC Recupere les donnees de demande nationale demi-horaire.

# COMMAND ----------

import requests
import pandas as pd
import time

NESO_BASE_URL = "https://api.neso.energy/api/3/action/datastore_search"

# Historic Demand Data - resource_ids par annee (dataset 8f2fe0af)
# Periode alignee sur le dataset Smart Meters : Nov 2011 -> Fev 2014
NESO_RESOURCE_IDS = {
    "2011": "01522076-2691-4140-bfb8-c62284752efd",
    "2012": "4bf713a2-ea0c-44d3-a09a-63fc6a634b00",
    "2013": "2ff7aaff-8b42-4c1b-b234-9446573a1e27",
    "2014": "b9005225-49d3-40d1-921c-03ee2d83a2ff",
}

def fetch_neso_demand(resource_id: str, year: str, limit: int = 32000):
    """Recupere les donnees de demande nationale depuis NESO pour une annee."""
    all_records = []
    offset = 0

    while True:
        params = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset,
        }
        resp = requests.get(NESO_BASE_URL, params=params, timeout=120)
        resp.raise_for_status()
        result = resp.json()["result"]

        records = result["records"]
        if not records:
            break

        all_records.extend(records)
        offset += limit
        print(f"  NESO {year}: {len(all_records)} lignes recuperees...")

        if len(records) < limit:
            break

        # Respecter la limite NESO : max 2 requetes/min
        time.sleep(1)

    return all_records

if blob_exists("neso", "demand_national_raw.json"):
    print("NESO: donnees deja presentes dans Bronze -- SKIP")
else:
    print("Telechargement des donnees NESO (demande nationale 2011-2014)...")
    try:
        all_neso_records = []
        for year, resource_id in NESO_RESOURCE_IDS.items():
            print(f"  Annee {year}...")
            try:
                records = fetch_neso_demand(resource_id, year)
                all_neso_records.extend(records)
                print(f"  NESO {year}: {len(records)} lignes")
            except requests.exceptions.RequestException as e:
                print(f"  NESO {year}: erreur -- {e}")
                print(f"  -> On continue avec les autres annees")

        if all_neso_records:
            print(f"NESO: {len(all_neso_records)} lignes au total")
            upload_json_to_gcs(all_neso_records, "neso", "demand_national_raw.json")
            print("NESO: upload Bronze termine")
        else:
            print("NESO: aucune donnee recuperee -- API potentiellement indisponible")
            print("  -> Le pipeline continuera sans les donnees NESO")
    except Exception as e:
        print(f"NESO: erreur inattendue -- {e}")
        print("  -> Le pipeline continuera sans les donnees NESO")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Elexon BMRS API (Prix marche UK)
# MAGIC
# MAGIC API gratuite avec inscription (cle API requise).
# MAGIC Recupere les System Prices (prix d'imbalance) demi-horaires.

# COMMAND ----------

ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"

def fetch_elexon_prices(start_date: str, end_date: str):
    """
    Recupere les prix systeme (System Price) depuis Elexon BMRS.
    Format dates : YYYY-MM-DD
    """
    all_data = []

    current = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)

    while current <= end:
        settlement_date = current.strftime("%Y-%m-%d")

        url = f"{ELEXON_BASE_URL}/balancing/settlement/system-prices"
        params = {
            "settlement_date": settlement_date,
            "format": "json",
        }

        try:
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, list):
                all_data.extend(data)
            elif isinstance(data, dict) and "data" in data:
                all_data.extend(data["data"])
        except requests.exceptions.RequestException as e:
            print(f"  Elexon erreur pour {settlement_date}: {e}")

        current += pd.Timedelta(days=1)

        if len(all_data) % 5000 < 50:
            print(f"  Elexon: {len(all_data)} lignes, date courante: {settlement_date}")

    return all_data

# Periode alignee sur le dataset Smart Meters : Nov 2011 -> Fev 2014
if blob_exists("elexon", "system_prices_raw.json"):
    print("Elexon: donnees deja presentes dans Bronze -- SKIP")
else:
    print("Telechargement des prix Elexon BMRS...")
    try:
        elexon_data = fetch_elexon_prices("2011-11-01", "2014-02-28")
        if elexon_data:
            print(f"Elexon: {len(elexon_data)} lignes au total")
            upload_json_to_gcs(elexon_data, "elexon", "system_prices_raw.json")
            print("Elexon: upload Bronze termine")
        else:
            print("Elexon: aucune donnee recuperee")
            print("  -> Le pipeline continuera sans les donnees Elexon")
    except Exception as e:
        print(f"Elexon: erreur inattendue -- {e}")
        print("  -> Le pipeline continuera sans les donnees Elexon")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Carbon Intensity API (optionnel)
# MAGIC
# MAGIC API gratuite, sans cle.
# MAGIC **Note :** Cette API ne couvre que depuis ~2018. Pour la periode 2011-2014,
# MAGIC elle ne retournera pas de donnees. On tente quand meme et on skip si vide.

# COMMAND ----------

CARBON_BASE_URL = "https://api.carbonintensity.org.uk"

def fetch_carbon_intensity(start_date: str, end_date: str):
    """
    Recupere l'intensite carbone nationale par demi-heure.
    L'API accepte des plages de 14 jours max.
    """
    all_data = []

    current = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)

    while current < end:
        chunk_end = min(current + pd.Timedelta(days=14), end)

        from_str = current.strftime("%Y-%m-%dT%H:%MZ")
        to_str = chunk_end.strftime("%Y-%m-%dT%H:%MZ")

        url = f"{CARBON_BASE_URL}/intensity/{from_str}/{to_str}"

        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            result = resp.json()

            if "data" in result:
                all_data.extend(result["data"])
        except requests.exceptions.RequestException as e:
            print(f"  Carbon erreur pour {from_str}: {e}")

        current = chunk_end

        if len(all_data) % 2000 < 100:
            print(f"  Carbon Intensity: {len(all_data)} lignes...")

    return all_data

if blob_exists("carbon", "carbon_intensity_raw.json"):
    print("Carbon Intensity: donnees deja presentes dans Bronze -- SKIP")
else:
    print("Telechargement Carbon Intensity...")
    try:
        carbon_data = fetch_carbon_intensity("2012-01-01", "2014-02-28")
        if carbon_data:
            upload_json_to_gcs(carbon_data, "carbon", "carbon_intensity_raw.json")
            print(f"Carbon Intensity: {len(carbon_data)} lignes uploadees")
        else:
            print("Carbon Intensity: pas de donnees pour la periode 2011-2014 (API demarre ~2018)")
            print("  -> Cette source sera optionnelle pour le POC")
    except Exception as e:
        print(f"Carbon Intensity: erreur -- {e}")
        print("  -> Cette source sera optionnelle pour le POC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Unity Catalog -- Enregistrement des tables Bronze
# MAGIC
# MAGIC On enregistre les donnees brutes comme tables Delta dans Unity Catalog
# MAGIC pour beneficier du lineage, de la data discovery et de la gouvernance.
# MAGIC Le flux GCS -> Snowpipe reste intact (on ne casse rien).

# COMMAND ----------

# DBTITLE Unity Catalog : Creation du catalogue et schemas

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Strategie : tenter de creer un catalogue dedie, sinon utiliser "main"
UC_CATALOG = "ml_energy"

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {UC_CATALOG}")
    spark.sql(f"USE CATALOG {UC_CATALOG}")
    print(f"Catalogue '{UC_CATALOG}' cree/utilise avec succes")
except Exception as e:
    print(f"Impossible de creer le catalogue '{UC_CATALOG}': {e}")
    print("Fallback sur le catalogue 'main'")
    UC_CATALOG = "main"
    spark.sql(f"USE CATALOG {UC_CATALOG}")

# Creer les 3 schemas (bronze, silver, gold)
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{schema}")
    print(f"  Schema {UC_CATALOG}.{schema} OK")

print(f"\nUnity Catalog configure : {UC_CATALOG}.[bronze|silver|gold]")

# Passer le nom du catalogue au notebook suivant via taskValues
try:
    dbutils.jobs.taskValues.set(key="uc_catalog", value=UC_CATALOG)
except Exception:
    pass  # Mode standalone

# COMMAND ----------

# DBTITLE Enregistrement des tables Bronze dans Unity Catalog

import io

print("Enregistrement des tables Bronze dans Unity Catalog...\n")

# --- 1. NESO (JSON -> Spark -> UC) ---
if blob_exists("neso", "demand_national_raw.json"):
    try:
        blob = bronze_bucket.blob("neso/demand_national_raw.json")
        neso_data = json.loads(blob.download_as_bytes())
        import pandas as pd
        df_neso_pd = pd.json_normalize(neso_data)
        df_neso_spark = spark.createDataFrame(df_neso_pd)
        df_neso_spark.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.bronze.neso_demand_raw")
        print(f"  {UC_CATALOG}.bronze.neso_demand_raw : {df_neso_spark.count():,} lignes")
        del df_neso_pd, df_neso_spark
    except Exception as e:
        print(f"  NESO UC: erreur -- {e}")

# --- 2. Elexon (JSON -> Spark -> UC) ---
if blob_exists("elexon", "system_prices_raw.json"):
    try:
        blob = bronze_bucket.blob("elexon/system_prices_raw.json")
        elexon_data = json.loads(blob.download_as_bytes())
        import pandas as pd
        df_elexon_pd = pd.json_normalize(elexon_data)
        df_elexon_spark = spark.createDataFrame(df_elexon_pd)
        df_elexon_spark.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.bronze.elexon_prices_raw")
        print(f"  {UC_CATALOG}.bronze.elexon_prices_raw : {df_elexon_spark.count():,} lignes")
        del df_elexon_pd, df_elexon_spark
    except Exception as e:
        print(f"  Elexon UC: erreur -- {e}")

# --- 3. Smart Meters (trop gros pour tout charger, on enregistre un echantillon + metadata) ---
try:
    sm_blobs = [b for b in bronze_bucket.list_blobs(prefix="smart_meters/")
                if b.size > 1 and b.name.endswith(".csv")]
    sm_metadata = spark.createDataFrame([
        {"source": "smart_meters", "n_files": len(sm_blobs),
         "total_size_mb": round(sum(b.size for b in sm_blobs) / (1024*1024), 1),
         "gcs_path": f"gs://{BUCKET_BRONZE}/smart_meters/",
         "format": "CSV", "ingestion_date": str(datetime.now())}
    ])
    sm_metadata.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.bronze.smart_meters_metadata")
    print(f"  {UC_CATALOG}.bronze.smart_meters_metadata : {len(sm_blobs)} fichiers references")
except Exception as e:
    print(f"  Smart Meters UC metadata: erreur -- {e}")

# --- 4. Households (CSV petit, on peut charger) ---
try:
    hh_blobs = [b for b in bronze_bucket.list_blobs(prefix="households/")
                if b.size > 1 and b.name.endswith(".csv") and "informations" in b.name]
    if hh_blobs:
        import pandas as pd
        content = hh_blobs[0].download_as_bytes()
        df_hh_pd = pd.read_csv(io.BytesIO(content))
        df_hh_spark = spark.createDataFrame(df_hh_pd)
        df_hh_spark.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.bronze.households_raw")
        print(f"  {UC_CATALOG}.bronze.households_raw : {df_hh_spark.count():,} lignes")
        del df_hh_pd, df_hh_spark
except Exception as e:
    print(f"  Households UC: erreur -- {e}")

# --- 5. Weather (CSV petit) ---
try:
    w_blobs = [b for b in bronze_bucket.list_blobs(prefix="weather/")
               if b.size > 1 and b.name.endswith(".csv")]
    if w_blobs:
        import pandas as pd
        content = w_blobs[0].download_as_bytes()
        df_w_pd = pd.read_csv(io.BytesIO(content))
        df_w_spark = spark.createDataFrame(df_w_pd)
        df_w_spark.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.bronze.weather_raw")
        print(f"  {UC_CATALOG}.bronze.weather_raw : {df_w_spark.count():,} lignes")
        del df_w_pd, df_w_spark
except Exception as e:
    print(f"  Weather UC: erreur -- {e}")

# --- 6. Holidays (CSV petit) ---
try:
    hol_blobs = [b for b in bronze_bucket.list_blobs(prefix="holidays/")
                 if b.size > 1 and b.name.endswith(".csv")]
    if hol_blobs:
        import pandas as pd
        content = hol_blobs[0].download_as_bytes()
        df_hol_pd = pd.read_csv(io.BytesIO(content))
        df_hol_spark = spark.createDataFrame(df_hol_pd)
        df_hol_spark.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.bronze.holidays_raw")
        print(f"  {UC_CATALOG}.bronze.holidays_raw : {df_hol_spark.count():,} lignes")
        del df_hol_pd, df_hol_spark
except Exception as e:
    print(f"  Holidays UC: erreur -- {e}")

print("\nEnregistrement Bronze UC termine.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verification finale

# COMMAND ----------

print(f"=== Contenu complet de gs://{BUCKET_BRONZE}/ ===\n")

prefixes = ["smart_meters", "households", "weather", "holidays", "neso", "elexon", "carbon"]

for prefix in prefixes:
    blobs = list(bronze_bucket.list_blobs(prefix=f"{prefix}/"))
    real_blobs = [b for b in blobs if b.size > 1]
    total_size_mb = sum(b.size for b in real_blobs) / (1024 * 1024)
    print(f"  {prefix}/ : {len(real_blobs)} fichiers, {total_size_mb:.1f} MB")

# Verifier les tables UC
print(f"\n=== Tables Unity Catalog ({UC_CATALOG}.bronze) ===\n")
uc_tables = spark.sql(f"SHOW TABLES IN {UC_CATALOG}.bronze").collect()
for t in uc_tables:
    print(f"  {t.tableName}")

print("\nIngestion Bronze terminee.")
print("  Donnees statiques : via Terraform (gsutil)")
print("  Donnees API : via ce notebook")
print(f"  Unity Catalog : {UC_CATALOG}.bronze ({len(uc_tables)} tables)")
