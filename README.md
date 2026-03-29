# Measuring Success of F&B Business

---

## [Tableau Dashboard](https://public.tableau.com/app/profile/hsin.yen.wu4563/viz/Restuarant_success_analysis/Dashboard1?publish=yes)

## What this pipeline does

The pipeline runs in this order:

1. Read raw datasets from Bronze (`gs://mgmt405_dataset/bronze/...`)
2. Transform to Silver files using PySpark jobs submitted in dataproc cluster
3. Load Silver parquet files into Snowflake and create silver tables
4. Build Gold wide tables
5. Visualise on tableau via snowflake connector

---

## GCP + Composer setup (professor demo flow)

### 1) Create GCS bucket and Bronze folder

Create bucket:

- `mgmt405_dataset`

Create folder:

- `gs://mgmt405_dataset/bronze/`

Download the raw data from Google Drive and upload it under Bronze:

- [mgmt405_dataset - Google Drive](https://drive.google.com/drive/folders/1S4ADySitmFwKGfLZyP8wcWqQj9qSp1yT?usp=sharing)

Expected Bronze layout:

```text
gs://mgmt405_dataset/bronze/
  yelp_data/
    yelp_academic_dataset_business.json
    yelp_academic_dataset_review.json
  weather_data/
    PA/...
    LA/...
  acs_datasets/
    ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021.csv
```

### 2) Create `scripts` folder in bucket and upload ETL scripts

Create:

- `gs://mgmt405_dataset/scripts/`

Upload all files from repo `gcp/scripts/` into that bucket folder:

- `yelp_cannibalisation.py`
- `business_agg_pyspark.py`
- `Script_Philadelphia_New_Orleans_ACS.py`
- `weather_business_spark_final.py`
- `snowflake_rollback`
- `load_yelp_to_snowflake.py`
- `snowflake_silver_to_gold.py`

These are referenced directly by the DAG.

### 3) Create Dataproc Spark cluster

Create a Dataproc cluster named `**sparkexplorationv3**` in the same GCP project and region as your Composer environment.

In GCP Console → Dataproc → Clusters → **Create Cluster**:

- **Cluster name:** `sparkexplorationv3`
- **Region:** choose the same region as your data, scripts and composer environment (e.g. `us-west1`)
- **Cluster type:** Standard
- Leave other settings as default

Alternatively, create it via `gcloud`:

```bash
gcloud dataproc clusters create sparkexplorationv3 \
  --region=us-west1 \
  --num-workers=2 \
  --image-version=2.1-debian11
```

> **Why this matters:** When the Airflow DAG runs PySpark scripts, it submits them as Spark jobs to this Dataproc cluster (`sparkexplorationv3`) using the `DataprocSubmitJobOperator`. The cluster must be running before you trigger the DAG.

### 4) Create Cloud Composer environment and deploy DAG

Create a Cloud Composer environment (you can configure a custom bucket during setup).  
After Composer is created, use its DAGs bucket/folder and upload:

- repo file: `gcp/dags/mgmt405_etl.py`

This DAG appears in Airflow UI as:

- `restaurants_etl`

### 5) Create Snowflake account and GCS storage integration

Run in Snowflake:

```sql
CREATE OR REPLACE STORAGE INTEGRATION GCS_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://mgmt405_dataset/');

DESC INTEGRATION GCS_INT;
```

Then complete the GCP side trust/permissions using the integration details from `DESC INTEGRATION GCS_INT` so Snowflake can access the bucket.

### 6) Set Composer environment variables

In Cloud Composer, set these environment variables:

- `SNOWFLAKE_USER`
- `SNOWFLAKE_PAT`
- `SNOWFLAKE_ACCOUNT`

Use your Snowflake credentials/connection values.

### 7) Trigger and monitor DAG in Airflow UI

From Composer, open Airflow UI:

1. Confirm DAG `restaurants_etl` is visible
2. Trigger the DAG
3. Monitor task execution end-to-end

Expected flow:

- Bronze in GCS -> Silver outputs in GCS
- Silver loaded into Snowflake
- Snowflake Silver transformed into Gold tables

---

## Alternative 1: Run ETL locally (no Airflow required)

This path executes the full pipeline directly from your machine.

### Local prerequisites

1. Ensure local Bronze data exists in repo root:

```text
bronze/
  yelp_data/
  weather_data/
  acs_datasets/
```

1. Create local `.env` in repo root (see `.env.template` for reference):

```bash
SNOWFLAKE_ACCOUNT="<your_snowflake_account>"
SNOWFLAKE_USER="<your_snowflake_user>"
SNOWFLAKE_PASSWORD="<your_snowflake_password>"
SNOWFLAKE_WAREHOUSE="<your_snowflake_warehouse>"
SNOWFLAKE_DATABASE="<your_snowflake_database>"
SNOWFLAKE_SCHEMA="<your_snowflake_schema>"
SNOWFLAKE_ROLE="<your_snowflake_role>"
```

1. Running the script

### Run

```bash
chmod +x run_local_etl.sh
./run_local_etl.sh
```

---

## Alternative 2: Run end-to-end with local Airflow

If you want orchestration locally with Airflow:

```bash
chmod +x run_etl_with_airflow.sh
./run_etl_with_airflow.sh
```

This script sets up local Airflow, copies the DAG, starts services, and triggers `restaurants_etl` locally.

---

## Troubleshooting

- If a Composer task cannot find a script, verify it exists in `gs://mgmt405_dataset/scripts/`.
- If DAG is not visible, verify `gcp/dags/mgmt405_etl.py` is uploaded to Composer DAGs folder.
- If Snowflake load fails, verify Composer env vars are set and valid.
- If Snowflake cannot read GCS, re-check storage integration trust/permissions on both sides.
- If local runs fail with missing files, verify Bronze layout and `scripts/` folder in repo root.

