#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"
cd "${PROJECT_ROOT}"

show_help() {
  cat <<'EOF'
Usage: ./run_local_etl.sh [options]

Options:
  --snowflake-user <value>       Snowflake user
  --snowflake-account <value>    Snowflake account
  --snowflake-pat <value>        Snowflake personal access token
  --skip-install                 Skip pip dependency installation
  -h, --help                     Show this help

Credentials can be provided in this order of precedence:
  1) CLI flags
  2) Environment variables
  3) .env file at project root
EOF
}

SKIP_INSTALL="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --snowflake-user)
      export SNOWFLAKE_USER="$2"
      shift 2
      ;;
    --snowflake-account)
      export SNOWFLAKE_ACCOUNT="$2"
      shift 2
      ;;
    --snowflake-pat)
      export SNOWFLAKE_PAT="$2"
      shift 2
      ;;
    --skip-install)
      SKIP_INSTALL="true"
      shift
      ;;
    -h|--help)
      show_help
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      show_help
      exit 1
      ;;
  esac
done

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

if [[ ! -d "bronze" ]]; then
  echo "ERROR: bronze/ folder not found."
  echo "Download bronze data first:"
  echo "https://drive.google.com/drive/folders/1S4ADySitmFwKGfLZyP8wcWqQj9qSp1yT?usp=sharing"
  exit 1
fi

if [[ ! -f "bronze/yelp_data/yelp_academic_dataset_business.json" ]]; then
  echo "ERROR: Missing bronze/yelp_data/yelp_academic_dataset_business.json"
  exit 1
fi

if [[ ! -f "bronze/yelp_data/yelp_academic_dataset_review.json" ]]; then
  echo "ERROR: Missing bronze/yelp_data/yelp_academic_dataset_review.json"
  exit 1
fi

if [[ ! -f "bronze/acs_datasets/ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021.csv" ]]; then
  echo "ERROR: Missing bronze/acs_datasets/ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021.csv"
  exit 1
fi

if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
python -m pip install --upgrade pip

if [[ "${SKIP_INSTALL}" != "true" ]]; then
  pip install "pyspark" "snowflake-connector-python"
fi

if [[ -z "${SNOWFLAKE_USER:-}" || -z "${SNOWFLAKE_ACCOUNT:-}" || -z "${SNOWFLAKE_PAT:-}" ]]; then
  echo "ERROR: Snowflake credentials are missing."
  echo "Set SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, and SNOWFLAKE_PAT in .env or environment variables."
  exit 1
fi

SILVER_DIR="${PROJECT_ROOT}/silver"
CHECKPOINT_DIR="${SILVER_DIR}/checkpoints"

rollback() {
  echo
  echo "============================================================"
  echo "Pipeline failure detected. Starting rollback..."
  echo "============================================================"

  echo "Removing local silver files..."
  rm -rf "${SILVER_DIR:?}"/* || true

  echo "Running Snowflake rollback script..."
  python "${PROJECT_ROOT}/scripts/snowflake_rollback.py" \
    --local-silver-root "${SILVER_DIR}" || true

  echo "Rollback completed."
}

trap rollback ERR

mkdir -p "${SILVER_DIR}" "${CHECKPOINT_DIR}"

echo "Step 1/6: Yelp bronze -> silver"
spark-submit "${PROJECT_ROOT}/scripts/yelp_cannibalisation.py" \
  --bronze-business "${PROJECT_ROOT}/bronze/yelp_data/yelp_academic_dataset_business.json" \
  --bronze-review "${PROJECT_ROOT}/bronze/yelp_data/yelp_academic_dataset_review.json" \
  --silver-business "${SILVER_DIR}/yelp_business_2city" \
  --silver-review "${SILVER_DIR}/yelp_review_2city" \
  --write-mode overwrite

echo "Step 2/6: Yelp ZIP aggregation"
spark-submit "${PROJECT_ROOT}/scripts/business_agg_pyspark.py" \
  --bronze-business "${PROJECT_ROOT}/bronze/yelp_data/yelp_academic_dataset_business.json" \
  --silver-business "${SILVER_DIR}/yelp_business_zip_summary" \
  --write-mode overwrite

echo "Step 3/6: ACS bronze -> silver"
spark-submit "${PROJECT_ROOT}/scripts/Script_Philadelphia_New_Orleans_ACS.py" \
  --input-csv "${PROJECT_ROOT}/bronze/acs_datasets/ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021.csv" \
  --output-path "${SILVER_DIR}/acs/ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021_WITH_CUSTOM_COLUMNS" \
  --write-mode overwrite

echo "Step 4/6: Weather + business + review transformations"
spark-submit "${PROJECT_ROOT}/scripts/weather_business_spark_final.py" \
  --bronze-weather-base "${PROJECT_ROOT}/bronze/weather_data" \
  --bronze-business "${PROJECT_ROOT}/bronze/yelp_data/yelp_academic_dataset_business.json" \
  --bronze-review "${PROJECT_ROOT}/bronze/yelp_data/yelp_academic_dataset_review.json" \
  --silver-weather-week "${SILVER_DIR}/weather_week_agg" \
  --silver-business-mapped "${SILVER_DIR}/business_category_mapped" \
  --silver-review-week "${SILVER_DIR}/review_week_agg" \
  --checkpoint-dir "${CHECKPOINT_DIR}"

echo "Step 5/6: Load silver parquet to Snowflake"
python "${PROJECT_ROOT}/scripts/load_yelp_to_snowflake.py" \
  --source local \
  --local-silver-root "${SILVER_DIR}"

echo "Step 6/6: Snowflake silver -> gold"
python "${PROJECT_ROOT}/scripts/snowflake_silver_to_gold.py"

echo
echo "============================================================"
echo "Local ETL completed"
echo "============================================================"
