import os
import sys
import argparse
from pathlib import Path
import snowflake.connector


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if value.startswith(("'", '"')) and value.endswith(("'", '"')):
            value = value[1:-1]

        # Keep already-exported env vars as source of truth.
        os.environ.setdefault(key, value)


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def run_query(cur, label: str, query: str) -> None:
    print(f"\n{'=' * 80}", flush=True)
    print(f"START: {label}", flush=True)
    print(f"QUERY:\n{query.strip()}", flush=True)
    print(f"{'=' * 80}", flush=True)

    cur.execute(query)

    print(f"STATUS: {cur.sfqid}", flush=True)

    try:
        rows = cur.fetchall()
        if rows:
            print(f"RESULT ({len(rows)} rows):", flush=True)
            for row in rows:
                print(row, flush=True)
        else:
            print("RESULT: Query returned no rows.", flush=True)
    except Exception:
        print("RESULT: No fetchable result set for this statement.", flush=True)

    print(f"END: {label}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load silver datasets into Snowflake GOLD schema.")
    parser.add_argument(
        "--source",
        choices=["gcs", "local"],
        default="gcs",
        help="Read silver parquet from GCS external stage or from local filesystem.",
    )
    parser.add_argument(
        "--local-silver-root",
        default="silver",
        help="Local silver root dir used when --source=local.",
    )
    return parser.parse_args()


def list_parquet_files(dir_path: Path) -> list[Path]:
    return sorted([p for p in dir_path.glob("*.parquet") if p.is_file()])


def load_sql_statements(sql_path: Path, **format_kwargs: str) -> list[tuple[str, str]]:
    sql_text = sql_path.read_text(encoding="utf-8")
    if format_kwargs:
        sql_text = sql_text.format(**format_kwargs)

    statements: list[tuple[str, str]] = []
    for idx, statement in enumerate(sql_text.split(";"), start=1):
        query = statement.strip()
        if not query:
            continue
        first_line = query.splitlines()[0].strip()
        label = f"sql_{idx:02d}_{first_line[:40].replace(' ', '_').lower()}"
        statements.append((label, query))

    return statements


def main() -> None:
    conn = None
    cur = None
    args = parse_args()

    try:
        repo_root = Path(__file__).resolve().parent.parent
        load_env_file(repo_root / ".env")

        print("Reading Snowflake credentials from environment variables...", flush=True)
        user = get_required_env("SNOWFLAKE_USER")
        password = get_required_env("SNOWFLAKE_PAT")
        account = get_required_env("SNOWFLAKE_ACCOUNT")
        role = get_required_env("SNOWFLAKE_ROLE")
        warehouse = get_required_env("SNOWFLAKE_WAREHOUSE")
        database = get_required_env("SNOWFLAKE_DATABASE")
        schema = get_required_env("SNOWFLAKE_SCHEMA")

        print(f"Connecting to Snowflake account: {account}", flush=True)
        print(f"Using Snowflake user: {user}", flush=True)

        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
        )
        cur = conn.cursor()

        print("Connected to Snowflake successfully.", flush=True)

        setup_queries = [
            ("use_role", f"USE ROLE {role}"),
            ("use_warehouse", f"USE WAREHOUSE {warehouse}"),
            ("use_database", f"USE DATABASE {database}"),
            ("use_schema", f"USE SCHEMA {schema}"),
        ]
        for label, query in setup_queries:
            run_query(cur, label, query)

        if args.source == "gcs":
            run_query(
                cur,
                "create_stage_gcs",
                """
                CREATE OR REPLACE STAGE GCS_SILVER_STAGE
                  STORAGE_INTEGRATION = GCS_INT
                  URL = 'gcs://mgmt405_dataset/silver/'
                  FILE_FORMAT = (TYPE = PARQUET)
                """,
            )
            stage_prefix = "@GCS_SILVER_STAGE"
        else:
            run_query(
                cur,
                "create_stage_local",
                """
                CREATE OR REPLACE STAGE LOCAL_SILVER_STAGE
                  FILE_FORMAT = (TYPE = PARQUET)
                """,
            )
            stage_prefix = "@LOCAL_SILVER_STAGE"

            local_root = Path(args.local_silver_root).expanduser().resolve()
            datasets = [
                ("yelp_business_zip_summary", local_root / "yelp_business_zip_summary"),
                ("acs/ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021_WITH_CUSTOM_COLUMNS", local_root / "acs" / "ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021_WITH_CUSTOM_COLUMNS"),
                ("business_category_mapped", local_root / "business_category_mapped"),
                ("review_week_agg", local_root / "review_week_agg"),
                ("weather_week_agg", local_root / "weather_week_agg"),
                ("yelp_business_2city", local_root / "yelp_business_2city"),
                ("yelp_review_2city", local_root / "yelp_review_2city"),
            ]

            for ds_name, ds_path in datasets:
                if not ds_path.exists():
                    raise FileNotFoundError(f"Local dataset path not found: {ds_path}")
                parquet_files = list_parquet_files(ds_path)
                if not parquet_files:
                    raise FileNotFoundError(f"No parquet files found in: {ds_path}")

                run_query(cur, f"remove_stage_files_{ds_name}", f"REMOVE {stage_prefix}/{ds_name}")
                for idx, parquet_file in enumerate(parquet_files, start=1):
                    local_file_uri = f"file://{parquet_file.resolve()}"
                    put_query = (
                        f"PUT '{local_file_uri}' "
                        f"{stage_prefix}/{ds_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                    )
                    run_query(cur, f"put_{ds_name}_{idx}", put_query)

        sql_file = repo_root / "snowflake" / "load_yelp_to_snowflake.sql"
        object_queries = load_sql_statements(sql_file, stage_prefix=stage_prefix)
        for label, query in object_queries:
            run_query(cur, label, query)

        print("\nAll Snowflake queries finished successfully.", flush=True)

    except Exception as e:
        print("\nERROR OCCURRED DURING SNOWFLAKE LOAD", flush=True)
        print(repr(e), flush=True)
        sys.exit(1)

    finally:
        if cur is not None:
            print("Closing Snowflake cursor...", flush=True)
            cur.close()
        if conn is not None:
            print("Closing Snowflake connection...", flush=True)
            conn.close()
        print("Script finished.", flush=True)


if __name__ == "__main__":
    main()
