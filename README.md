# ML Predict User Consumption

Prediction de la consommation electrique individuelle (kWh) par demi-heure, basee sur le dataset "Smart Meters in London" (Kaggle).

Pipeline MLOps complet : Terraform (IaC) -> GCS (Medallion) -> Databricks (Spark + XGBoost + MLflow) -> Snowpipe -> Snowflake.

---

## Contexte du Projet

Ce projet est le "cerveau" algorithmique d'une application B2C dediee a la maitrise de l'energie (type *Smart Home* / *Opower*).
L'objectif est d'offrir a chaque utilisateur individuel (identifie par `LCLid`) un tableau de bord predictif de sa propre consommation electrique.

Le produit final est capable de :
1. Predire la consommation d'energie (en kWh) d'un utilisateur demi-heure par demi-heure pour le lendemain.
2. Estimer son budget d'energie pour la fin du mois.
3. Emettre des recommandations hyper-personnalisees (ex: *"Alerte pointe tarifaire : decalez vos appareils electromenagers demain a 14h pour economiser"*).

---

## Structure du Projet

```
Ml-predict-user-consomation/
|
|-- terraform/                     # Infrastructure as Code
|   |-- main.tf                    # Orchestration des modules
|   |-- providers.tf               # GCP + Snowflake providers
|   |-- variables.tf               # Variables d'entree
|   |-- outputs.tf                 # Outputs post-apply
|   |-- terraform.tfvars.example   # Template de configuration
|   |-- modules/
|       |-- gcs/                   # Buckets GCS + upload Kaggle data
|       |   |-- main.tf
|       |   |-- variables.tf
|       |   |-- outputs.tf
|       |   |-- versions.tf
|       |-- snowflake/             # Database, schemas, tables, warehouse
|       |   |-- main.tf
|       |   |-- variables.tf
|       |   |-- outputs.tf
|       |   |-- versions.tf
|       |-- snowpipe/              # Pub/Sub + Storage/Notification integration + Pipe
|           |-- main.tf
|           |-- variables.tf
|           |-- outputs.tf
|           |-- versions.tf
|
|-- notebooks/                     # Databricks notebooks (pipeline ML)
|   |-- 01_ingestion_bronze.py     # APIs -> GCS Bronze
|   |-- 02_nettoyage_silver.py     # Bronze -> Silver (Delta Lake)
|   |-- 03_training.py             # Feature Eng + XGBoost + Walk-Forward + MLflow
|   |-- 04_inference_gold.py       # Predictions -> GCS Gold -> Snowpipe
|
|-- dashboard/                     # Streamlit dashboard (Databricks Apps)
|   |-- app.py                     # Dashboard principal (5 pages, Plotly)
|   |-- app.yaml                   # Configuration Databricks Apps
|   |-- requirements.txt           # Dependances Python
|   |-- start.sh                   # Lanceur avec port dynamique
|   |-- app_debug.py               # Page de diagnostic
|
|-- img/                            # Captures d'ecran du dashboard
|
|-- data/                          # Dataset Kaggle (gitignore, ~10 GB)
|-- .gitignore
|-- README.md
```

---

## Pre-requis

| Outil | Version | Usage |
|---|---|---|
| **Terraform** | >= 1.5 | Provisionnement infrastructure |
| **gcloud CLI** | latest | Authentification GCP + gsutil |
| **Databricks CLI** | latest | Secrets + import notebooks |
| **Compte GCP** | -- | Projet avec APIs Storage & Pub/Sub activees |
| **Compte Snowflake** | -- | Trial ou payant, role ACCOUNTADMIN |
| **Compte Databricks** | Free Edition | Workspace avec compute serverless |

---

## Installation et Deploiement

### 1. Cloner le repo et telecharger le dataset

```bash
git clone <repo-url>
cd Ml-predict-user-consomation

# Telecharger le dataset Kaggle "Smart Meters in London"
# https://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london
# Dezipper dans data/
unzip archive.zip -d data/
```

Le dossier `data/` doit contenir :
- `halfhourly_dataset/halfhourly_dataset/block_*.csv` (112 fichiers)
- `informations_households.csv`
- `weather_hourly_darksky.csv`
- `uk_bank_holidays.csv`
- `acorn_details.csv`

### 2. Configurer Terraform

```bash
cd terraform

# Copier le template et remplir avec vos valeurs
cp terraform.tfvars.example terraform.tfvars
# Editer terraform.tfvars avec : GCP project ID, Snowflake credentials, etc.

# Authentification GCP
gcloud auth application-default login

# Deployer toute l'infrastructure
terraform init
terraform plan
terraform apply
```

Terraform provisionne en une commande :
- 3 buckets GCS (bronze, silver, gold) avec versioning
- Upload des fichiers Kaggle vers GCS Bronze via `gsutil`
- Database Snowflake (`ML_ENERGY_DB`), 3 schemas, 4 tables, warehouse XSMALL
- Pub/Sub topic + subscription pour Snowpipe
- Storage integration + Notification integration + Stage + Pipe auto-ingest

### 3. Post-apply : IAM Snowpipe

Apres `terraform apply`, accorder les permissions aux Service Accounts Snowflake (affiches dans les outputs) :

```bash
# 1. Pub/Sub Subscriber pour le SA de notification
gcloud pubsub subscriptions add-iam-policy-binding snowpipe-gold-sub \
  --member="serviceAccount:<NOTIFICATION_SA>" \
  --role="roles/pubsub.subscriber" \
  --project="<GCP_PROJECT_ID>"

# 2. Monitoring Viewer au niveau projet
gcloud projects add-iam-policy-binding <GCP_PROJECT_ID> \
  --member="serviceAccount:<NOTIFICATION_SA>" \
  --role="roles/monitoring.viewer"

# 3. GCS Object Viewer pour le SA de storage
gsutil iam ch serviceAccount:<STORAGE_SA>:objectViewer \
  gs://ml-energy-consumption-gold
```

Les valeurs des SA sont visibles via :
```bash
terraform output snowflake_notification_sa
terraform output snowflake_storage_integration_details
```

### 4. Configurer Databricks

```bash
# Installer le CLI
brew tap databricks/tap && brew install databricks/tap/databricks

# Configurer l'authentification (Personal Access Token)
databricks configure --host https://<workspace>.cloud.databricks.com

# Creer un Service Account GCP et sa cle JSON
gcloud iam service-accounts create ml-energy-sa \
  --display-name="ML Energy Databricks SA" \
  --project=<GCP_PROJECT_ID>

gcloud projects add-iam-policy-binding <GCP_PROJECT_ID> \
  --member="serviceAccount:ml-energy-sa@<GCP_PROJECT_ID>.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud iam service-accounts keys create /tmp/sa-key.json \
  --iam-account=ml-energy-sa@<GCP_PROJECT_ID>.iam.gserviceaccount.com

# Stocker les secrets dans Databricks
databricks secrets create-scope ml-energy
databricks secrets put-secret ml-energy gcp-sa-key --string-value "$(cat /tmp/sa-key.json)"
databricks secrets put-secret ml-energy elexon-api-key --string-value "<VOTRE_CLE_ELEXON>"

# Supprimer la cle locale
rm /tmp/sa-key.json

# Importer les notebooks
databricks workspace mkdirs /Users/<email>/ml-energy
databricks workspace import /Users/<email>/ml-energy/01_ingestion_bronze \
  --file notebooks/01_ingestion_bronze.py --language PYTHON --format SOURCE
databricks workspace import /Users/<email>/ml-energy/02_nettoyage_silver \
  --file notebooks/02_nettoyage_silver.py --language PYTHON --format SOURCE
databricks workspace import /Users/<email>/ml-energy/03_training \
  --file notebooks/03_training.py --language PYTHON --format SOURCE
databricks workspace import /Users/<email>/ml-energy/04_inference_gold \
  --file notebooks/04_inference_gold.py --language PYTHON --format SOURCE
```

### 5. Executer le pipeline

Un **Databricks Workflow** (Job) orchestre les 4 notebooks sequentiellement sur du compute serverless.
Le Job est pre-configure avec un environnement contenant les dependances necessaires (`google-cloud-storage`, `requests`, `xgboost`, `pandas`, `numpy`, `scikit-learn`).

**Creer le Workflow via le CLI :**

```bash
databricks jobs create --json @- <<'EOF'
{
  "name": "ML Energy Pipeline",
  "tasks": [
    {
      "task_key": "01_ingestion_bronze",
      "notebook_task": {
        "notebook_path": "/Users/<email>/ml-energy/01_ingestion_bronze",
        "source": "WORKSPACE"
      },
      "environment_key": "Default",
      "timeout_seconds": 3600
    },
    {
      "task_key": "02_nettoyage_silver",
      "depends_on": [{"task_key": "01_ingestion_bronze"}],
      "notebook_task": {
        "notebook_path": "/Users/<email>/ml-energy/02_nettoyage_silver",
        "source": "WORKSPACE"
      },
      "environment_key": "Default",
      "timeout_seconds": 7200
    },
    {
      "task_key": "03_training",
      "depends_on": [{"task_key": "02_nettoyage_silver"}],
      "notebook_task": {
        "notebook_path": "/Users/<email>/ml-energy/03_training",
        "source": "WORKSPACE"
      },
      "environment_key": "Default",
      "timeout_seconds": 14400
    },
    {
      "task_key": "04_inference_gold",
      "depends_on": [{"task_key": "03_training"}],
      "notebook_task": {
        "notebook_path": "/Users/<email>/ml-energy/04_inference_gold",
        "source": "WORKSPACE"
      },
      "environment_key": "Default",
      "timeout_seconds": 3600
    }
  ],
  "environments": [
    {
      "environment_key": "Default",
      "spec": {
        "client": "1",
        "dependencies": [
          "google-cloud-storage", "requests", "xgboost",
          "pandas", "numpy", "scikit-learn"
        ]
      }
    }
  ],
  "max_concurrent_runs": 1,
  "format": "MULTI_TASK"
}
EOF
```

**Lancer le pipeline :**

```bash
# Via CLI (remplacer <JOB_ID> par l'ID retourne ci-dessus)
databricks jobs run-now <JOB_ID>
```

Ou via l'UI Databricks : **Workflows** > **ML Energy Pipeline** > **Run now**.

Le pipeline execute :

1. **01_ingestion_bronze** -- Ingere les donnees API (NESO, Elexon, Carbon Intensity) dans GCS Bronze
2. **02_nettoyage_silver** -- Nettoie et transforme Bronze -> Silver (Delta Lake)
3. **03_training** -- Feature engineering + XGBoost + Walk-Forward + MLflow
4. **04_inference_gold** -- Predictions -> GCS Gold (Parquet) -> Snowpipe -> Snowflake

Si une task echoue, les suivantes sont automatiquement sautees.

### 6. Resultats d'Execution du Pipeline

Le pipeline complet a ete execute avec succes le **20 mars 2026** sur Databricks Free Edition.

**Run ID** : `584630894868956` | **Job ID** : `342544352422540` | **Statut** : SUCCESS

#### Temps d'execution par etape

| Etape | Debut | Fin | Duree | Description |
|---|---|---|---|---|
| `01_ingestion_bronze` | 10:02 | 10:09 | **6 min 32s** | Appels API NESO, Elexon, Carbon Intensity + upload GCS Bronze |
| `02_nettoyage_silver` | 10:09 | 10:26 | **17 min 23s** | Nettoyage, jointures, feature engineering -> GCS Silver (Delta) |
| `03_training` | 10:26 | 10:43 | **17 min 22s** | XGBoost Walk-Forward 4 rounds + MLflow tracking |
| `04_inference_gold` | 10:43 | 11:18 | **34 min 41s** | Predictions ~30M lignes -> GCS Gold (Parquet) -> Snowpipe |
| **Total** | **10:02** | **11:18** | **1h 16min 08s** | Pipeline end-to-end |

#### Pourquoi le pipeline prend ~1h16 ?

La duree totale s'explique principalement par les **contraintes de la Databricks Community / Free Edition** :

1. **Compute serverless restreint** : La Free Edition de Databricks fournit un compute serverless partage avec des ressources limitees (CPU, memoire). Contrairement aux clusters dedies d'une edition payante (Standard/Premium), le compute est bride en termes de parallelisme et de puissance de calcul. Il n'y a aucun controle sur la taille du cluster (nombre de workers, type d'instance).

2. **Pas de cluster dedie** : En Free Edition, on ne peut pas provisionner de cluster Spark personnalise (ex: `i3.xlarge` avec 4 workers). Tout s'execute sur l'environnement serverless partage, ce qui signifie que les ressources sont mutualisees avec d'autres utilisateurs de la plateforme.

3. **Execution sequentielle obligatoire** : Les 4 notebooks s'executent l'un apres l'autre (chaque etape depend de la precedente). Il n'y a pas de parallelisation possible entre les etapes du pipeline.

4. **Volume de donnees important** : Le step d'inference (`04_inference_gold`) est le plus long (~35 min) car il genere des predictions pour l'ensemble des ~5,500 foyers x ~16,000 demi-heures, soit pres de **30 millions de lignes** de predictions, puis les exporte en Parquet vers GCS Gold pour ingestion Snowpipe.

5. **Cold start a chaque task** : Chaque notebook demarre un nouvel environnement serverless, ce qui implique un temps de demarrage (installation des dependances `xgboost`, `google-cloud-storage`, etc.) a chaque etape.

> **Note** : Avec un cluster Databricks dedie (edition Standard/Premium), le meme pipeline s'executerait en **15-25 minutes** grace au parallelisme Spark, a des instances plus puissantes, et a l'absence de cold start entre les tasks. La Free Edition est suffisante pour le prototypage et la validation du pipeline, mais un environnement de production necessite un upgrade.

#### Metriques du modele (MLflow)

Le modele `best_model_v1.0_20260320_1426` a passe le **Quality Gate** avec succes :

| Metrique | Valeur | Seuil |
|---|---|---|
| **WMAPE** | 31.82% | < 35% |
| **R2** | 0.7426 | > 0.70 |
| **RMSE** | 0.1557 | minimise |
| **MAE** | 0.0786 | minimise |

- **Nombre de features retenues** : 34 (apres selection automatique en 2 passes)
- **Meilleur round Walk-Forward** : Round 3 (janvier 2014)
- **MLflow Run ID** : `dcc503f0d82c4344a66dfbb8889055a6`

---

## Architecture des Donnees

### Sources de donnees (5 flux)

| # | Source | Contenu | Format |
|---|---|---|---|
| 1 | Smart Meters (Kaggle) | Consommation kWh/30min par foyer | 112 CSV (~10 GB) |
| 2 | Households (Kaggle) | Profil ACORN (socio-demo) | CSV |
| 3 | Weather (Kaggle) | Meteo horaire DarkSky | CSV |
| 4 | UK Bank Holidays (Kaggle) | Jours feries UK | CSV |
| 5 | APIs (NESO, Elexon, Carbon) | Demande nationale, prix marche, intensite carbone | JSON |

### Architecture Medallion (GCS)

```
gs://ml-energy-consumption-bronze/    # Raw (CSV/JSON tel que recu)
  smart_meters/block_*.csv            #   112 fichiers Kaggle
  households/informations_households.csv
  weather/weather_hourly_darksky.csv
  holidays/uk_bank_holidays.csv
  neso/demand_national_raw.json       #   API
  elexon/system_prices_raw.json       #   API
  carbon/carbon_intensity_raw.json    #   API (optionnel)

gs://ml-energy-consumption-silver/    # Clean (Delta Lake, aligne 30min)
  smart_meters/                       #   Dedup, LOCF, filtre kWh > 0
  weather/                            #   Interpolation horaire -> 30min
  households/                         #   ACORN verifie
  holidays/                           #   is_holiday binaire
  neso/                               #   Aplati, timezone UK
  elexon/                             #   Prix alignes 30min

gs://ml-energy-consumption-gold/      # Predictions (Parquet -> Snowpipe)
  predictions/                        #   user_id, timestamp, kwh_predicted
```

### Snowflake (Data Warehouse)

```
ML_ENERGY_DB
  |-- RAW                             # Schema pour donnees brutes (reserve)
  |-- PREDICTIONS
  |     |-- ENERGY_PREDICTIONS        # <- Snowpipe auto-ingest depuis Gold
  |     |-- ENERGY_ACTUALS            # Consommation reelle pour comparaison
  |-- ANALYTICS
        |-- HOUSEHOLDS                # Profils ACORN
        |-- MODEL_METRICS             # MAPE, RMSE, MAE, R2 par round
```

---

## Architecture Globale

```
  +------------ Databricks Free Edition ---------------+
  |                                                     |
  |  Notebook 1 (Ingestion)                             |
  |    google-cloud-storage SDK + requests (APIs)       |
  |         |                                           |
  |         v                                           |
  |  +------------- GCS Medallion ----------------+     |
  |  |  BRONZE (Raw) -> SILVER (Clean) -> GOLD    |     |
  |  +--------------------------------------------+     |
  |         |                                           |
  |  Notebook 2 (Nettoyage)                             |
  |    Bronze -> Spark clean -> Silver (Delta Lake)     |
  |         |                                           |
  |  Notebook 3 (Training)                              |
  |    Silver -> Feature Eng -> XGBoost -> MLflow       |
  |    Selection features (2 passes) + Walk-Forward     |
  |    Quality Gate : WMAPE < 35%, R2 > 0.70             |
  |         |                                           |
  |  Notebook 4 (Inference)                             |
  |    Modele MLflow -> Predictions -> Gold (Parquet)   |
  |                                                     |
  +-----------------------------------------------------+
                        |
                        v
  +------------- GCS Gold -----------------+
  |  Notification Pub/Sub (OBJECT_FINALIZE) |
  |         |                               |
  |         v                               |
  |  SNOWPIPE (auto-ingest)                 |
  +---------+-------------------------------+
            v
  +----------------+         +----------------------------------+
  |   SNOWFLAKE    | <-----> |   DATABRICKS APPS                |
  |   (Serving)    |         |   Streamlit + Plotly             |
  |                |         |                                  |
  |  Predictions   |         |  Overview (KPIs, heatmap)        |
  |  + Reel        |         |  My Consumption (profil 24h)     |
  |                |         |  Predicted vs Actual (overlay)   |
  |  Households    |         |  Budget (tarifs UK, savings)     |
  |  + Metrics     |         |  Model Performance (radar, WF)   |
  +----------------+         +----------------------------------+

  +----------- Terraform (IaC) -----------+
  |  modules/gcs       -> 3 buckets + data |
  |  modules/snowflake -> DB, schemas, WH  |
  |  modules/snowpipe  -> Pub/Sub + Pipe   |
  +----------------------------------------+
```

---

## Modelisation et Prediction

### Ce qui est predit

Pour un identifiant donne (`LCLid`) et un timestamp, le modele predit une valeur numerique continue (regression multivariee) :
> **Combien de kWh seront consommes pour la prochaine demi-heure.**

### Algorithme

**XGBoost** (ou LightGBM) avec :
- Features temporelles : heure, jour de semaine, mois, is_weekend, is_holiday
- Features lag : consommation t-1, t-2, t-4, t-48, t-336
- Features rolling : moyenne/ecart-type glissants 24h
- Features meteo : temperature, humidite, pression, vent, etc.
- Features socio-demo : groupe ACORN, type de tarif (stdorToU)
- Features macro : demande nationale (NESO), prix marche (Elexon)

### Selection Automatique des Features (2 passes)

1. **Passe 1 (Exploration)** : Entrainement XGBoost sur toutes les features. Classement d'importance.
2. **Quality Gate** : Features avec importance < 1% du max sont eliminees.
3. **Passe 2 (Production)** : Re-entrainement sur le sous-ensemble retenu. Validation des metriques.

### Validation : Walk-Forward (4 rounds)

```
Round 1 : Train [mi-2012 --- oct 2013] -> Predit nov 2013 -> Compare au reel
Round 2 : Train [mi-2012 ---- nov 2013] -> Predit dec 2013 -> Compare au reel
Round 3 : Train [mi-2012 ----- dec 2013] -> Predit jan 2014 -> Compare au reel
Round 4 : Train [mi-2012 ------ jan 2014] -> Predit fev 2014 -> Compare au reel
```

Le MAPE moyen sur tous les rounds constitue la metrique finale.

### Metriques cibles

| Metrique | Seuil (Quality Gate) |
|---|---|
| WMAPE | < 35% |
| R2 | > 0.70 |
| RMSE | minimise |
| MAE | minimise |

---

## Technologies

| Composant | Technologie |
|---|---|
| Infrastructure as Code | Terraform (modules GCS, Snowflake, Snowpipe) |
| Stockage | Google Cloud Storage (Medallion : Bronze/Silver/Gold) |
| Format donnees | Delta Lake (Silver), Parquet (Gold) |
| Compute / ML | Databricks Free Edition (Spark + XGBoost) |
| Tracking ML | MLflow (integre Databricks) |
| Data Warehouse | Snowflake |
| Ingestion auto | Snowpipe + GCS Pub/Sub notifications |
| Dashboard | Streamlit + Plotly (deploye sur Databricks Apps) |
| Hebergement dashboard | Databricks Apps (HTTPS, OAuth integre) |
| Gouvernance donnees | Unity Catalog (lineage, discovery, ACL) |

---

## Performance & Optimisations

### Temps d'Execution Pipeline Complet

| Task | Description | Duree Moyenne | Optimisations |
|------|-------------|---------------|---------------|
| **01_ingestion_bronze** | Ingestion APIs → GCS Bronze | ~8 min | Download concurrent, caching |
| **02_nettoyage_silver** | Nettoyage Bronze → Silver | ~15 min | Spark direct read (optimise de 1h+ à 15min) |
| **03_training** | Training XGBoost + MLflow | ~16 min | Walk-forward validation, feature selection |
| **04_inference_gold** | Predictions → GCS Gold | ~10-15 min | Limite à 20 blocs (demo), float32, GC agressif |
| **TOTAL** | Pipeline end-to-end | **~50 min** | Optimisations memoire et reseau |

### Optimisations Appliquees

#### Notebook 02 (Nettoyage Silver)
- ✅ **Lecture Spark directe** depuis GCS au lieu de bloc-par-bloc
- ✅ **Ecriture Unity Catalog** en une seule operation `overwrite` au lieu de 112 `append`
- ✅ **Performance gain** : 4-6x plus rapide (1h+ → 15 min)

#### Notebook 04 (Inference Gold)
- ✅ **Limitation blocs** : 20/112 blocs traités (~18% dataset) pour demo
- ✅ **Optimisation memoire** : Features en float32 au lieu de float64 (50% RAM economisee)
- ✅ **GC agressif** : Garbage collection tous les 10 blocs au lieu de 20
- ✅ **Timeout GCS** : Augmente à 300s avec retry automatique (3 tentatives)
- ✅ **Tri par taille** : Traiter les petits fichiers d'abord

### Configuration Production

Pour traiter le dataset complet (112 blocs, ~5.5M lignes), modifier dans `notebooks/04_inference_gold.py` ligne 224 :

```python
MAX_BLOCKS_FOR_INFERENCE = 112  # Au lieu de 20
```

**Impact :**
- Temps d'inference : ~50-60 min (au lieu de 10-15 min)
- Predictions : 5.5M lignes (au lieu de 1M)
- Couverture : 100% des foyers (au lieu de 18%)

### Compute Serverless Databricks

Le pipeline utilise **Databricks Serverless Compute** (pas besoin de cluster persistent) :
- Auto-scaling selon la charge
- Pay-per-use (facturation à la seconde)
- Cold start : ~2-3 min par task
- Memoire : 8-16 GB par worker (auto-ajuste)

---

## Unity Catalog -- Gouvernance des Donnees

Toutes les tables du pipeline sont enregistrees dans **Unity Catalog** pour beneficier du lineage automatique, de la data discovery et de la gouvernance.

### Organisation des tables UC

```
ml_energy (Catalogue)
|
|-- bronze (Schema)
|   |-- neso_demand_raw          # Donnees brutes NESO (JSON aplati)
|   |-- elexon_prices_raw        # Prix marche bruts Elexon
|   |-- smart_meters_metadata    # Metadata des 112 fichiers CSV (trop gros pour UC)
|   |-- households_raw           # Profils ACORN bruts
|   |-- weather_raw              # Meteo horaire DarkSky brute
|   |-- holidays_raw             # Jours feries UK bruts
|
|-- silver (Schema)
|   |-- smart_meters             # ~30M lignes, dedup + LOCF + filtre kWh > 0
|   |-- weather                  # Interpolation 30min, colonnes nettoyees
|   |-- households               # ACORN verifie, labels uniformises
|   |-- holidays                 # Serie complete avec is_holiday binaire
|   |-- neso_demand              # Demande nationale, timezone UK
|   |-- elexon_prices            # Prix alignes 30min
|
|-- gold (Schema)
    |-- energy_predictions       # Predictions kWh par foyer/timestamp
    |-- energy_actuals           # Consommation reelle pour comparaison
    |-- walk_forward_metrics     # WMAPE, R2, RMSE, MAE par round
    |-- feature_importance       # Classement des features XGBoost
    |-- energy_consumption_xgboost  # Modele MLflow (UC Model Registry)
```

### Avantages apportes par Unity Catalog

| Fonctionnalite | Description |
|---|---|
| **Lineage automatique** | Visualisation du flux Bronze -> Silver -> Gold dans l'UI Databricks |
| **Data Discovery** | Recherche et exploration des tables via le Catalog Explorer |
| **Gouvernance (ACL)** | Controle d'acces granulaire par table/schema/catalogue |
| **Model Registry UC** | Modele XGBoost enregistre au format 3-level namespace (`ml_energy.gold.energy_consumption_xgboost`) |
| **Audit & Compliance** | Historique des acces et modifications |

### Coexistence avec GCS + Snowpipe

Unity Catalog **n'interfere pas** avec le flux existant :

```
Bronze (GCS) -----> Silver (GCS) -----> Gold (GCS) -----> Snowpipe -> Snowflake
     |                   |                   |
     v                   v                   v
   UC Bronze           UC Silver           UC Gold
   (tables Delta)      (tables Delta)      (tables Delta + modele)
```

Les ecritures GCS restent intactes (Snowpipe continue de fonctionner). Les tables UC sont des **copies Delta** enregistrees en parallele, servant de couche de gouvernance et de discovery.

---

## Roadmap -- Delta Live Tables (DLT / Lakeflow Pipelines)

### Pourquoi DLT ?

Delta Live Tables (renomme **Lakeflow Pipelines**) est l'evolution naturelle de ce projet. Il permet de transformer les 4 notebooks orchestres par un Job en un **pipeline declaratif unique** avec gestion automatique de la qualite des donnees.

### Architecture actuelle vs DLT

| | Architecture actuelle | Avec DLT |
|---|---|---|
| **Orchestration** | Job multi-task (4 notebooks sequentiels) | Pipeline DLT declaratif unique |
| **Quality Gates** | Code Python custom (`if wmape > seuil`) | `@dlt.expect("valid_kwh", "kwh >= 0")` |
| **Gestion des erreurs** | Try/except manuels | Quarantine automatique des lignes invalides |
| **Lineage** | Via Unity Catalog (ajoute manuellement) | Natif et automatique |
| **Monitoring** | Logs + MLflow | Event log DLT + Databricks Observability |

### Exemple de refactoring DLT

Le pipeline actuel en 4 notebooks deviendrait :

```python
import dlt
from pyspark.sql.functions import *

# Bronze : ingestion brute
@dlt.table(comment="Smart meters raw data from GCS Bronze")
@dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
def bronze_smart_meters():
    return spark.read.parquet("gs://ml-energy-consumption-bronze/smart_meters/")

# Silver : nettoyage
@dlt.table(comment="Smart meters cleaned and deduplicated")
@dlt.expect("positive_kwh", "kwh > 0")
@dlt.expect_or_drop("valid_user", "user_id IS NOT NULL")
def silver_smart_meters():
    return (
        dlt.read("bronze_smart_meters")
        .dropDuplicates(["user_id", "timestamp"])
        .filter(col("kwh") > 0)
    )

# Gold : predictions
@dlt.table(comment="Energy consumption predictions")
@dlt.expect("valid_prediction", "kwh_predicted >= 0")
def gold_energy_predictions():
    df = dlt.read("silver_smart_meters")
    # ... feature engineering + inference ...
    return predictions_df
```

### Effort estime

| Etape | Effort | Description |
|---|---|---|
| Reecriture des notebooks | 2-3 jours | Convertir les 4 notebooks en decorateurs `@dlt.table` |
| Configuration du pipeline DLT | 1 jour | Remplacer le Job multi-task par un DLT Pipeline |
| Tests et validation | 1-2 jours | Verifier que les expectations DLT capturent les memes cas |
| **Total** | **4-6 jours** | Refactoring complet, sans changement fonctionnel |

### Priorite

DLT est recommande comme **Phase 2** du projet, apres la mise en production de la version Unity Catalog. Les benefices principaux sont :

1. **Simplification de l'orchestration** (1 pipeline au lieu de 4 notebooks + Job)
2. **Quality gates declaratives** (plus robuste que le code custom)
3. **Auto-recovery** (reprise automatique en cas d'echec)
4. **Monitoring natif** (event log, data quality dashboard)

> **Notebook de reference** : `notebooks/dlt_pipeline_reference.py` contient l'implementation complete DLT prete a deployer sur un workspace Premium.

---

## Roadmap -- Model Serving (Inference Temps Reel)

### Concept

Au lieu de lancer un pipeline batch (~35 min) pour generer toutes les predictions d'un coup, **Model Serving** expose le modele XGBoost comme un **endpoint REST**. Les predictions se font a la demande, en temps reel.

### Architecture Batch (actuelle) vs Temps Reel

```
BATCH (actuel) :
  Toutes les 24h -> Job Databricks (35 min) -> Parquet -> Snowpipe -> Snowflake -> Dashboard
  Latence : ~35 min pour toutes les predictions

TEMPS REEL (Model Serving) :
  App/Dashboard -> POST /predict {"user_id":"MAC000012","hour":18} -> 100ms -> reponse JSON
  Latence : < 200 ms par prediction
```

### Comparaison

| Aspect | Batch actuel | Model Serving |
|---|---|---|
| **Latence** | Predictions disponibles apres ~35 min | Reponse en < 200 ms |
| **Fraicheur** | Predictions generees 1x/jour | Predictions a la demande |
| **Cout** | Cluster execute 1x/jour (~0.50 USD) | Endpoint actif en permanence (~5-20 USD/jour) |
| **Scaling** | Fixe | Auto-scale selon le trafic |
| **Use cases** | Dashboard reporting, analyse historique | Alertes push, app mobile, widget temps reel |

### Cas d'usage concrets pour le projet

1. **Alertes push temps reel** : "Votre consommation depasse 150% de la normale -- verifiez vos appareils"
2. **Widget predictif** : "Dans la prochaine heure, vous allez consommer ~0.12 kWh (cout estime : 0.04 GBP)"
3. **API pour app mobile** : Endpoint REST appelable depuis une app iOS/Android
4. **Optimisation dynamique** : "Decalez votre lave-linge dans 2h, le prix spot baisse de 30%"

### Implementation technique

```python
# 1. Enregistrer le modele dans UC Model Registry (deja fait)
# ml_energy.gold.energy_consumption_xgboost

# 2. Creer l'endpoint Model Serving via l'API
import requests

endpoint_config = {
    "name": "energy-prediction-endpoint",
    "config": {
        "served_entities": [{
            "entity_name": "ml_energy.gold.energy_consumption_xgboost",
            "entity_version": "1",
            "workload_size": "Small",
            "scale_to_zero_enabled": True  # Economise quand pas de trafic
        }]
    }
}

# 3. Appeler l'endpoint
response = requests.post(
    "https://<workspace>/serving-endpoints/energy-prediction-endpoint/invocations",
    headers={"Authorization": "Bearer <token>"},
    json={"inputs": {"hour": 18, "day_of_week": 3, "temperature": 8.5, ...}}
)
prediction = response.json()  # {"predictions": [0.087]}
```

### Pre-requis et priorite

- **Edition requise** : Databricks **Premium** ou **Enterprise** (non disponible sur Free Edition)
- **Priorite** : **Phase 3** (apres Unity Catalog + DLT)
- Le batch quotidien reste suffisant tant que le dashboard Streamlit est le seul consommateur
- Model Serving devient pertinent des qu'une application temps reel (mobile, alertes, API publique) est envisagee

---

## Securite

### Gestion des Secrets

- ✅ Les credentials ne sont **jamais** en dur dans le code
- ✅ Variables d'environnement avec validation stricte (pas de fallback hardcodés)
- ✅ Pre-commit hooks pour bloquer les commits contenant des secrets
- ✅ `.gitignore` complet pour tous les fichiers sensibles
- ✅ Documentation sécurité complète : `SECURITY.md`

### Fichiers Sensibles Protégés

| Fichier | Status | Usage |
|---------|--------|-------|
| `terraform.tfvars` | Gitignored | Credentials Snowflake, GCP project ID |
| `eia_api_key.txt` | Gitignored | Clé API EIA |
| `.env` | Gitignored | Variables d'environnement locales |
| `*_credentials.json` | Gitignored | Service accounts GCP |

### Databricks Secrets

Les clés sensibles sont stockées dans **Databricks Secrets** (scope `ml-energy`) :

```bash
# Secrets stockés
databricks secrets list --scope ml-energy
# - gcp-sa-key : Service Account GCP (JSON)
# - elexon-api-key : Clé API Elexon
```

**Important :** La clé JSON du Service Account est supprimée du disque local immédiatement après injection dans Databricks Secrets.

### Templates de Configuration

| Template | Description |
|----------|-------------|
| `terraform.tfvars.example` | Configuration Terraform sans secrets |
| `.env.example` | Variables d'environnement template |
| `dashboard/.env.example` | Configuration dashboard template |

### Pre-Commit Hook

Un hook Git automatique bloque les commits contenant :
- Mots de passe
- Clés API
- Tokens
- Emails personnels
- Identifiants de comptes

**Test :** `./test-precommit.sh`

---

## Dashboard - Energy Insights

Le dashboard interactif est deploye sur **Databricks Apps** et consomme les donnees directement depuis Snowflake.

### Acces

| | |
|---|---|
| **URL** | `https://energy-insights-<app-id>.aws.databricksapps.com` |
| **Authentification** | OAuth Databricks (integre automatiquement) |
| **Port** | Dynamique via `DATABRICKS_APP_PORT` (fallback 8501) |

### Pages du Dashboard

| Page | Description |
|---|---|
| **Overview** | KPIs globaux (foyers, predictions, consommation moyenne), heatmap de consommation par jour/heure, distribution des profils ACORN |
| **My Consumption** | Selection d'un foyer (LCLid), profil journalier moyen, timeline interactive, box plot par jour de semaine, radar chart hebdomadaire |
| **Predicted vs Actual** | Courbes superposees predit/reel, scatter plot avec ligne de regression, distribution des erreurs, metriques par foyer |
| **Budget** | Configuration tarif (prix/kWh, abonnement), cout journalier predit vs reel, economies estimees, projection mensuelle |
| **Model Performance** | Metriques walk-forward (WMAPE, R2, RMSE, MAE) par round de validation, radar chart comparatif, evolution temporelle |

### Design

- **Theme** : Dark glassmorphism (fond `#0E1117`, accents `#6C63FF`)
- **Graphiques** : Plotly interactifs avec hover, zoom, export PNG
- **Cache** : `@st.cache_data(ttl=300)` pour les requetes Snowflake, `@st.cache_resource` pour la connexion
- **Responsive** : Layout adaptatif avec colonnes Streamlit

### Deploiement

```bash
# Uploader les fichiers dans le workspace Databricks
databricks workspace mkdirs /Users/<email>/ml-energy/dashboard
databricks workspace import /Users/<email>/ml-energy/dashboard/app.py \
  --file dashboard/app.py --language PYTHON --overwrite
# (repeter pour app.yaml, start.sh, requirements.txt)

# Creer et deployer l'application
databricks apps create energy-insights \
  --description "ML-powered energy consumption analytics dashboard"
databricks apps deploy energy-insights \
  --source-code-path /Workspace/Users/<email>/ml-energy/dashboard
```

### Variables d'Environnement Requises

| Variable | Description |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Identifiant du compte Snowflake (org-account) |
| `SNOWFLAKE_USER` | Nom d'utilisateur Snowflake |
| `SNOWFLAKE_PASSWORD` | Mot de passe Snowflake |
| `DATABRICKS_APP_PORT` | Port dynamique assigne par Databricks Apps (automatique) |

### Fichiers

```
dashboard/
  app.py              # Application principale (5 pages, ~1300 lignes)
  app.yaml            # Configuration Databricks Apps (command + env vars)
  start.sh            # Launcher script (port dynamique + theme)
  requirements.txt    # Dependencies Python (streamlit, plotly, snowflake-connector)
  app_debug.py        # App de diagnostic (test imports + connexion Snowflake)
```

### Visualisations du Dashboard

#### Page "My Consumption" - Indicateurs du foyer

![Household KPIs](img/01_household_kpis.png)

**Resume de consommation du foyer MAC000012.** Les quatre KPIs montrent une consommation totale predite de **1 129,78 kWh** sur la periode d'etude, avec une moyenne de **0,0674 kWh** par demi-heure. Le pic enregistre atteint **0,9807 kWh** sur un seul creneaux, ce qui indique des pointes ponctuelles de forte consommation (chauffage electrique, cuisson, etc.). Les **16 757 points de donnees** couvrent environ 1,5 an de mesures semi-horaires.

---

#### Page "My Consumption" - Profil journalier moyen

![Daily Consumption Profile](img/02_daily_consumption_profile.png)

**Courbe de charge moyenne sur 24h.** La consommation suit un profil bimodal typique d'un foyer residentiels UK : un creux nocturne entre 01h et 06h (~0,03 kWh), une remontee progressive le matin (activite domestique), un plateau en journee autour de 0,07-0,08 kWh, puis un **pic de soiree a 18h atteignant 0,1186 kWh** correspondant au retour au domicile (cuisson, chauffage, eclairage). La consommation redescend progressivement apres 21h.

---

#### Page "My Consumption" - Timeline de consommation

![Consumption Timeline](img/03_consumption_timeline.png)

**Serie temporelle des predictions sur toute la periode (Nov 2012 - Jan 2014).** La courbe bleue (Predicted kWh) et la courbe verte (24h Rolling Avg) montrent une **saisonnalite marquee** : la consommation est nettement plus elevee en hiver (Nov-Mar) avec des pics reguliers depassant 0,8-1,0 kWh, et basse en ete (Mai-Sep) ou la ligne de base tombe sous 0,1 kWh. Ce pattern est coherent avec un chauffage electrique dans le mix energetique du foyer.

---

#### Page "My Consumption" - Pattern hebdomadaire et radar 24h

![Weekly Pattern and Radar](img/04_weekly_pattern_radar.png)

**A gauche : Box plot par jour de semaine.** La distribution de la consommation est relativement stable du lundi au dimanche, avec une mediane autour de 0,04-0,05 kWh. Les jours de **week-end (samedi, dimanche) montrent une dispersion legerement plus elevee** avec des outliers plus frequents (max 0,98 kWh samedi), ce qui s'explique par une presence au domicile plus longue et des activites variables.

**A droite : Radar chart 24h.** Le radar confirme le profil journalier : la zone de consommation s'etend clairement vers les heures 17h-19h (pic de soiree) et se contracte entre 02h et 06h (creux nocturne). La forme asymetrique du radar revele que l'essentiel de la consommation se concentre sur la tranche 09h-22h.

---

#### Page "Predicted vs Actual" - Superposition temporelle

![Predicted vs Actual Overlay](img/05_predicted_vs_actual_overlay.png)

**Courbes superposees predit (bleu) vs reel (vert) avec bande d'erreur (rouge).** Sur toute la periode Nov 2012 - Jan 2014, le modele XGBoost suit fidelement la dynamique reelle. La courbe d'erreur (axe droit) oscille principalement entre -0,2 et +0,2 kWh, avec des pics d'erreur concentres sur les periodes de **forte variabilite hivernale** ou la consommation est moins previsible. En ete, l'erreur est quasi nulle car la consommation est faible et reguliere.

---

#### Page "Predicted vs Actual" - Scatter plot et distribution d'erreur

![Scatter and Error Distribution](img/06_scatter_error_distribution.png)

**A gauche : Scatter plot predit vs reel.** Les points se regroupent le long de la diagonale (ligne de prediction parfaite en rouge pointille), confirmant la bonne qualite du modele. On observe une **concentration dense pour les faibles consommations** (0-0,3 kWh) qui constituent la majorite des observations. Pour les valeurs elevees (>0,5 kWh), le modele tend a sous-estimer legerement, ce qui est visible par les points au-dessus de la diagonale.

**A droite : Distribution des erreurs.** L'histogramme montre une distribution **centree autour de zero** (ligne verte "Zero error") et fortement concentree dans l'intervalle [-0,1 ; +0,1] kWh. La grande majorite des predictions sont dans une marge d'erreur de 0,2 kWh. La distribution est legerement asymetrique vers les valeurs negatives, indiquant une tendance a la sous-estimation sur les fortes consommations.

---

#### Page "Predicted vs Actual" - Comparaison journaliere agregee

![Daily Aggregated Comparison](img/07_daily_aggregated_comparison.png)

**Comparaison quotidienne predit (bleu) vs reel (vert) avec pourcentage d'erreur (rouge).** Ce graphique agrege les 48 mesures semi-horaires en un total journalier. On observe que le modele suit bien les variations saisonnieres (5-14 kWh/jour en hiver, 2-5 kWh/jour en ete). Le **pourcentage d'erreur (axe droit)** reste generalement entre 0% et 20%, avec des pics occasionnels pouvant atteindre 60-80% sur certains jours atypiques (vacances, meteo extreme, etc.). La concordance est particulierement bonne sur les mois de mai a septembre.

---

#### Page "Budget" - Estimation des couts

![Budget Daily Cost](img/08_budget_daily_cost.png)

**Estimation budgetaire basee sur les predictions.** Les KPIs affichent un cout total estime de **580,32 GBP** sur la periode, soit **34,61 GBP/mois** en moyenne. La decomposition en **heures pleines (65,50 GBP)** vs **heures creuses (23,83 GBP)** montre que l'essentiel du cout provient des heures standard. Le graphique empile (barres) decompose le cout quotidien en **standing charge** (gris, cout fixe d'abonnement) et **energie** (bleu). On voit clairement que les jours d'hiver coutent 3 a 4 GBP/jour contre moins de 1 GBP en ete.

---

#### Page "Budget" - Repartition tarifaire et conseils d'economies

![Tariff Distribution and Savings](img/09_tariff_distribution_savings.png)

**A gauche : Donut chart de repartition par periode tarifaire.** La consommation se repartit en : **66,9% Standard** (tarif normal), **24,2% Peak** (heures de pointe 16h-19h) et **8,8% Off-Peak** (heures creuses 00h-07h). Le poids important de la tranche Peak est coherent avec le pic de soiree observe dans le profil journalier.

**A droite : Recommandations d'economies.** Le systeme detecte un **usage eleve en heures de pointe (35%)** et recommande de decaler certains appareils (lave-vaisselle, lave-linge, recharge vehicule electrique) vers les heures creuses. L'economie potentielle estimee est de **86,08 GBP** sur la periode si toute la consommation de pointe etait deplacee en heures creuses -- une reduction de ~15% de la facture totale.
