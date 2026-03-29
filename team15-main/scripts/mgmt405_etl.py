from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


def _discover_project_root() -> Path:
    here = Path(__file__).resolve().parent
    for candidate in [here] + list(here.parents):
        if (candidate / "bronze").exists() and (candidate / "scripts").exists():
            return candidate
    return Path.cwd()


PROJECT_ROOT = _discover_project_root()
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
BRONZE_DIR = PROJECT_ROOT / "bronze"
SILVER_DIR = PROJECT_ROOT / "silver"
CHECKPOINT_DIR = SILVER_DIR / "checkpoints"

YELP_BUSINESS_JSON = BRONZE_DIR / "yelp_data" / "yelp_academic_dataset_business.json"
YELP_REVIEW_JSON = BRONZE_DIR / "yelp_data" / "yelp_academic_dataset_review.json"
ACS_INPUT_CSV = BRONZE_DIR / "acs_datasets" / "ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021.csv"
WEATHER_BASE = BRONZE_DIR / "weather_data"

with DAG(
    dag_id="restaurants_etl",
    start_date=datetime(2026, 3, 6),
    schedule=None,
    catchup=False,
    tags=["etl", "local", "spark", "snowflake"],
) as dag:
    starting_etl_jobs = EmptyOperator(task_id="start_pipeline")

    run_yelp_cannibalisation = BashOperator(
        task_id="run_yelp_cannibalisation",
        bash_command=f"""
        set -euo pipefail
        mkdir -p "{SILVER_DIR}" "{CHECKPOINT_DIR}"
        spark-submit "{SCRIPTS_DIR / 'yelp_cannibalisation.py'}" \
          --bronze-business "{YELP_BUSINESS_JSON}" \
          --bronze-review "{YELP_REVIEW_JSON}" \
          --silver-business "{SILVER_DIR / 'yelp_business_2city'}" \
          --silver-review "{SILVER_DIR / 'yelp_review_2city'}" \
          --write-mode overwrite
        """,
    )

    run_business_agg = BashOperator(
        task_id="run_business_agg",
        bash_command=f"""
        set -euo pipefail
        mkdir -p "{SILVER_DIR}"
        spark-submit "{SCRIPTS_DIR / 'business_agg_pyspark.py'}" \
          --bronze-business "{YELP_BUSINESS_JSON}" \
          --silver-business "{SILVER_DIR / 'yelp_business_zip_summary'}" \
          --write-mode overwrite
        """,
    )

    run_acs_aggregation = BashOperator(
        task_id="run_acs_aggregation",
        bash_command=f"""
        set -euo pipefail
        mkdir -p "{SILVER_DIR / 'acs'}"
        spark-submit "{SCRIPTS_DIR / 'Script_Philadelphia_New_Orleans_ACS.py'}" \
          --input-csv "{ACS_INPUT_CSV}" \
          --output-path "{SILVER_DIR / 'acs' / 'ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021_WITH_CUSTOM_COLUMNS'}" \
          --write-mode overwrite
        """,
    )

    run_weather_agg = BashOperator(
        task_id="run_weather_agg",
        bash_command=f"""
        set -euo pipefail
        mkdir -p "{SILVER_DIR}" "{CHECKPOINT_DIR}"
        spark-submit "{SCRIPTS_DIR / 'weather_business_spark_final.py'}" \
          --bronze-weather-base "{WEATHER_BASE}" \
          --bronze-business "{YELP_BUSINESS_JSON}" \
          --bronze-review "{YELP_REVIEW_JSON}" \
          --silver-weather-week "{SILVER_DIR / 'weather_week_agg'}" \
          --silver-business-mapped "{SILVER_DIR / 'business_category_mapped'}" \
          --silver-review-week "{SILVER_DIR / 'review_week_agg'}" \
          --checkpoint-dir "{CHECKPOINT_DIR}"
        """,
    )

    wait_for_silver_jobs = EmptyOperator(task_id="wait_for_bronze_to_silver_jobs")

    run_snowflake_load = BashOperator(
        task_id="load_data_to_snowflake",
        bash_command=f"""
        set -euo pipefail
        python "{SCRIPTS_DIR / 'load_yelp_to_snowflake.py'}" \
          --source local \
          --local-silver-root "{SILVER_DIR}"
        """,
    )

    run_snowflake_silver_to_gold = BashOperator(
        task_id="snowflake_silver_to_gold",
        bash_command=f"""
        set -euo pipefail
        python "{SCRIPTS_DIR / 'snowflake_silver_to_gold.py'}"
        """,
    )

    rollback_pipeline = BashOperator(
        task_id="rollback_pipeline",
        trigger_rule=TriggerRule.ONE_FAILED,
        bash_command=f"""
        set -euo pipefail

        echo "Pipeline failure detected. Starting rollback..."

        echo "Removing local silver files..."
        rm -rf "{SILVER_DIR}"/* || true

        echo "Running Snowflake rollback script..."
        python "{SCRIPTS_DIR / 'snowflake_rollback.py'}" \
          --local-silver-root "{SILVER_DIR}" || true

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
