from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = "mgmt405"
REGION = "us-west1"
CLUSTER_NAME = "sparkexplorationv3"
BUCKET = "gs://mgmt405_dataset"

with DAG(
    dag_id="restaurants_etl",
    start_date=datetime(2026, 3, 6),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "dataproc", "yelp", "snowflake"],
) as dag:

    starting_etl_jobs = EmptyOperator(task_id="start_pipeline")

    run_yelp_cannibalisation = DataprocSubmitJobOperator(
        task_id="run_yelp_cannibalisation",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://mgmt405_dataset/scripts/yelp_cannibalisation.py",
                "args": [
                    "--bronze-business",
                    "gs://mgmt405_dataset/bronze/yelp_data/yelp_academic_dataset_business.json",
                    "--bronze-review",
                    "gs://mgmt405_dataset/bronze/yelp_data/yelp_academic_dataset_review.json",
                    "--silver-business",
                    "gs://mgmt405_dataset/silver/yelp_business_2city",
                    "--silver-review",
                    "gs://mgmt405_dataset/silver/yelp_review_2city",
                    "--write-mode",
                    "overwrite",
                ],
            },
        },
    )

    run_weather_agg = DataprocSubmitJobOperator(
        task_id="run_weather_agg",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://mgmt405_dataset/scripts/weather_business_spark_final.py",
                "properties": {
                    "spark.driver.memory": "4g",
                    "spark.driver.memoryOverhead": "1536m",
                    "spark.executor.cores": "2",
                    "spark.executor.memory": "3g",
                    "spark.executor.memoryOverhead": "1536m",
                    "spark.sql.shuffle.partitions": "200",
                },
            },
        },
    )

    run_acs_aggregation = DataprocSubmitJobOperator(
        task_id="run_acs_aggregation",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://mgmt405_dataset/scripts/Script_Philadelphia_New_Orleans_ACS.py",
                "args": [],
            },
        },
    )

    run_business_agg = DataprocSubmitJobOperator(
        task_id="run_business_agg",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://mgmt405_dataset/scripts/business_agg_pyspark.py",
                "args": [
                    "--bronze-business",
                    "gs://mgmt405_dataset/bronze/yelp_data/yelp_academic_dataset_business.json",
                    "--silver-business",
                    "gs://mgmt405_dataset/silver/yelp_business_zip_summary",
                    "--write-mode",
                    "overwrite",
                ],
            },
        },
    )

    wait_for_silver_jobs = EmptyOperator(task_id="wait_for_bronze_to_silver_jobs")

    run_snowflake_load = BashOperator(
        task_id="load_data_to_snowflake",
        bash_command="""
        set -euo pipefail

        echo "Copying Snowflake load script from GCS..."
        gcloud storage cp \
          gs://mgmt405_dataset/scripts/load_yelp_to_snowflake.py \
          /tmp/load_yelp_to_snowflake.py

        echo "Running Snowflake load script..."
        python /tmp/load_yelp_to_snowflake.py
        """,
    )

    run_snowflake_silver_to_gold = BashOperator(
        task_id="snowflake_silver_to_gold",
        bash_command="""
        set -euo pipefail

        echo "Copying Snowflake silver-to-gold script from GCS..."
        gcloud storage cp \
          gs://mgmt405_dataset/scripts/snowflake_silver_to_gold.py \
          /tmp/snowflake_silver_to_gold.py

        echo "Running Snowflake silver-to-gold script..."
        python /tmp/snowflake_silver_to_gold.py
        """,
    )

    rollback_pipeline = BashOperator(
        task_id="rollback_pipeline",
        trigger_rule=TriggerRule.ONE_FAILED,
        bash_command="""
        set -euo pipefail

        echo "Pipeline failure detected. Starting rollback..."

        echo "Removing silver files from GCS..."
        gcloud storage rm --recursive gs://mgmt405_dataset/silver/** || true

        echo "Copying Snowflake rollback script from GCS..."
        gcloud storage cp \
          gs://mgmt405_dataset/scripts/snowflake_rollback.py \
          /tmp/snowflake_rollback.py

        echo "Running Snowflake rollback script..."
        python /tmp/snowflake_rollback.py || true

        echo "Rollback completed."
        """,
    )

    pipeline_done = EmptyOperator(task_id="pipeline_done")

    starting_etl_jobs >> [run_yelp_cannibalisation, run_business_agg, run_acs_aggregation] >> run_weather_agg
    run_weather_agg >> wait_for_silver_jobs >> run_snowflake_load >> run_snowflake_silver_to_gold >> pipeline_done

    [
        run_yelp_cannibalisation,
        run_business_agg,
        run_acs_aggregation,
        run_weather_agg,
        wait_for_silver_jobs,
        run_snowflake_load,
        run_snowflake_silver_to_gold,
    ] >> rollback_pipeline