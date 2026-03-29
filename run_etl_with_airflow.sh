#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"
cd "${PROJECT_ROOT}"

AIRFLOW_VERSION="2.10.5"
PYTHON_VERSION="3.9"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

show_help() {
  cat <<'EOF'
Usage: ./run_etl_with_airflow.sh [options]

Options:
  --snowflake-user <value>       Snowflake user
  --snowflake-account <value>    Snowflake account
  --snowflake-pat <value>        Snowflake personal access token
  --run-id <value>               Airflow DAG run id (optional)
  --skip-install                 Skip pip dependency installation
  -h, --help                     Show this help

Credentials can be provided in this order of precedence:
  1) CLI flags
  2) Environment variables
  3) .env file at project root
EOF
}

RUN_ID=""
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
    --run-id)
      RUN_ID="$2"
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
  pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
  pip install "apache-airflow-providers-google" "pyspark" "snowflake-connector-python"
fi

export AIRFLOW_HOME="${PROJECT_ROOT}/.airflow"
mkdir -p "${AIRFLOW_HOME}/dags"

if [[ -z "${SNOWFLAKE_USER:-}" || -z "${SNOWFLAKE_ACCOUNT:-}" || -z "${SNOWFLAKE_PAT:-}" ]]; then
  echo "ERROR: Snowflake credentials are missing."
  echo "Set SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, and SNOWFLAKE_PAT in .env or environment variables."
  exit 1
fi

if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="local_e2e_$(date +%Y%m%d_%H%M%S)"
fi

airflow db migrate
airflow users create \
  --username admin \
  --firstname Local \
  --lastname Admin \
  --role Admin \
  --email admin@example.com \
  --password admin || true

cp "scripts/mgmt405_etl.py" "${AIRFLOW_HOME}/dags/restaurants_etl.py"

pkill -f "airflow webserver" || true
pkill -f "airflow scheduler" || true
pkill -f "airflow dag-processor" || true
pkill -f "airflow triggerer" || true
sleep 2

nohup airflow webserver --port 8080 > "${AIRFLOW_HOME}/webserver.out" 2>&1 &
nohup airflow scheduler > "${AIRFLOW_HOME}/scheduler.out" 2>&1 &

sleep 8

airflow dags unpause restaurants_etl || true
airflow dags trigger restaurants_etl --run-id "${RUN_ID}"

echo
echo "============================================================"
echo "Airflow pipeline triggered."
echo "Run ID: ${RUN_ID}"
echo "Airflow UI: http://localhost:8080 (admin/admin)"
echo "Status command:"
echo "  source .venv/bin/activate && export AIRFLOW_HOME=\"${AIRFLOW_HOME}\" && airflow tasks states-for-dag-run restaurants_etl ${RUN_ID}"
echo "Webserver log: ${AIRFLOW_HOME}/webserver.out"
echo "Scheduler log: ${AIRFLOW_HOME}/scheduler.out"
echo "============================================================"
