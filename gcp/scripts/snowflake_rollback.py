import os
import sys
import snowflake.connector


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


def main() -> None:
    conn = None
    cur = None

    try:
        print("Reading Snowflake credentials from environment variables...", flush=True)
        user = get_required_env("SNOWFLAKE_USER")
        password = get_required_env("SNOWFLAKE_PAT")
        account = get_required_env("SNOWFLAKE_ACCOUNT")

        print(f"Connecting to Snowflake account: {account}", flush=True)
        print(f"Using Snowflake user: {user}", flush=True)

        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
        )
        cur = conn.cursor()

        print("Connected to Snowflake successfully.", flush=True)

        queries = [
            ("use_role", "USE ROLE ACCOUNTADMIN"),
            ("use_warehouse", "USE WAREHOUSE MSBA405_WH"),
            ("use_database", "USE DATABASE MSBA405_DB"),
            ("use_schema", "USE SCHEMA GOLD"),

            # Gold tables
            ("drop_gold_cannibalization_zip_cuisine_window_weighted", "DROP TABLE IF EXISTS GOLD_CANNIBALIZATION_ZIP_CUISINE_WINDOW_WEIGHTED"),
            ("drop_fact_weekly_activity", "DROP TABLE IF EXISTS FACT_WEEKLY_ACTIVITY"),
            ("drop_dim_business", "DROP TABLE IF EXISTS DIM_BUSINESS"),
            ("drop_dim_date", "DROP TABLE IF EXISTS DIM_DATE"),
            ("drop_gold_yelp_weather_business_panel", "DROP TABLE IF EXISTS GOLD_YELP_WEATHER_BUSINESS_PANEL"),
            ("drop_gold_table_acs_business", "DROP TABLE IF EXISTS GOLD_TABLE_ACS_BUSINESS"),

            # Snowflake-loaded tables from silver files
            ("drop_yelp_review_2city", "DROP TABLE IF EXISTS YELP_REVIEW_2CITY"),
            ("drop_yelp_business_2city", "DROP TABLE IF EXISTS YELP_BUSINESS_2CITY"),
            ("drop_weather_week_agg", "DROP TABLE IF EXISTS WEATHER_WEEK_AGG"),
            ("drop_review_week_agg", "DROP TABLE IF EXISTS REVIEW_WEEK_AGG"),
            ("drop_business_category_mapped", "DROP TABLE IF EXISTS BUSINESS_CATEGORY_MAPPED"),
            ("drop_silver_acs_philadelphia_new_orleans", "DROP TABLE IF EXISTS SILVER_ACS_PHILADELPHIA_NEW_ORLEANS"),
            ("drop_yelp_business_acs", "DROP TABLE IF EXISTS YELP_BUSINESS_ACS"),

            # Supporting objects
            ("drop_stage", "DROP STAGE IF EXISTS GCS_SILVER_STAGE"),
            ("drop_file_format", "DROP FILE FORMAT IF EXISTS FF_PARQUET"),
        ]

        for label, query in queries:
            run_query(cur, label, query)

        print("\nSnowflake rollback finished successfully.", flush=True)

    except Exception as e:
        print("\nERROR OCCURRED DURING SNOWFLAKE ROLLBACK", flush=True)
        print(repr(e), flush=True)
        sys.exit(1)

    finally:
        if cur is not None:
            print("Closing Snowflake cursor...", flush=True)
            cur.close()
        if conn is not None:
            print("Closing Snowflake connection...", flush=True)
            conn.close()
        print("Rollback script finished.", flush=True)


if __name__ == "__main__":
    main()