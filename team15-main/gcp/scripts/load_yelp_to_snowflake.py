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

            (
                "create_stage",
                """
                CREATE OR REPLACE STAGE GCS_SILVER_STAGE
                  STORAGE_INTEGRATION = GCS_INT
                  URL = 'gcs://mgmt405_dataset/silver/'
                  FILE_FORMAT = (TYPE = PARQUET)
                """
            ),

            (
                "create_table_yelp_business_acs",
                """
                CREATE OR REPLACE TABLE YELP_BUSINESS_ACS (
                  postal_code       STRING,
                  n_business        NUMBER,
                  mean_stars        FLOAT,
                  total_reviews     NUMBER,
                  zip_avg_price     FLOAT,
                  share_3plus       FLOAT,
                  weighted_rating   FLOAT
                )
                """
            ),

            (
                "copy_into_yelp_business_acs",
                """
                COPY INTO YELP_BUSINESS_ACS
                FROM @GCS_SILVER_STAGE/yelp_business_zip_summary/
                FILE_FORMAT = (TYPE = PARQUET)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PATTERN = '.*part-.*'
                """
            ),

            (
                "create_file_format_ff_parquet",
                """
                CREATE OR REPLACE FILE FORMAT FF_PARQUET
                TYPE = PARQUET
                """
            ),

            (
                "create_table_silver_acs_philadelphia_new_orleans",
                """
                CREATE OR REPLACE TABLE SILVER_ACS_PHILADELPHIA_NEW_ORLEANS
                USING TEMPLATE (
                  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                  FROM TABLE(
                    INFER_SCHEMA(
                      LOCATION => '@GCS_SILVER_STAGE/acs/ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021_WITH_CUSTOM_COLUMNS/',
                      FILE_FORMAT => 'FF_PARQUET'
                    )
                  )
                )
                """
            ),

            (
                "copy_into_silver_acs_philadelphia_new_orleans",
                """
                COPY INTO SILVER_ACS_PHILADELPHIA_NEW_ORLEANS
                FROM @GCS_SILVER_STAGE/acs/ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021_WITH_CUSTOM_COLUMNS/
                FILE_FORMAT = (FORMAT_NAME = FF_PARQUET)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PATTERN = '.*part-.*'
                """
            ),

            (
                "create_table_business_category_mapped",
                """
                CREATE OR REPLACE TABLE BUSINESS_CATEGORY_MAPPED (
                  business_id       STRING,
                  name              STRING,
                  city              STRING,
                  state             STRING,
                  postal_code       STRING,
                  categories        STRING,
                  stars             FLOAT,
                  latitude          DOUBLE,
                  review_count      NUMBER,
                  is_open           NUMBER,
                  longitude         DOUBLE,
                  station_id        STRING,
                  dist_km           DOUBLE,
                  mapped_category   STRING
                )
                """
            ),

            (
                "copy_into_business_category_mapped",
                """
                COPY INTO BUSINESS_CATEGORY_MAPPED
                FROM @GCS_SILVER_STAGE/business_category_mapped/
                FILE_FORMAT = (TYPE = PARQUET)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PATTERN = '.*part-.*'
                """
            ),

            (
                "create_table_review_week_agg",
                """
                CREATE OR REPLACE TABLE REVIEW_WEEK_AGG (
                  business_id       STRING,
                  week              TIMESTAMP_NTZ,
                  review_cnt        NUMBER,
                  avg_stars         FLOAT
                )
                """
            ),

            (
                "copy_into_review_week_agg",
                """
                COPY INTO REVIEW_WEEK_AGG
                FROM @GCS_SILVER_STAGE/review_week_agg/
                FILE_FORMAT = (TYPE = PARQUET)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PATTERN = '.*part-.*'
                """
            ),

            (
                "create_table_weather_week_agg",
                """
                CREATE OR REPLACE TABLE WEATHER_WEEK_AGG (
                  state             STRING,
                  station_id        STRING,
                  week              TIMESTAMP_NTZ,
                  tmax_mean         FLOAT,
                  tmin_mean         FLOAT,
                  prcp_sum          FLOAT,
                  prcp_mean         FLOAT,
                  temp_mean         FLOAT,
                  temp_range        FLOAT,
                  heavy_rain_week   NUMBER,
                  hot_week          NUMBER,
                  freeze_week       NUMBER
                )
                """
            ),

            (
                "copy_into_weather_week_agg",
                """
                COPY INTO WEATHER_WEEK_AGG
                FROM @GCS_SILVER_STAGE/weather_week_agg/
                FILE_FORMAT = (TYPE = PARQUET)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PATTERN = '.*part-.*'
                """
            ),

            (
                "create_table_yelp_business_2city",
                """
                CREATE OR REPLACE TABLE YELP_BUSINESS_2CITY (
                  business_id       STRING,
                  name              STRING,
                  city              STRING,
                  state             STRING,
                  zipcode           STRING,
                  postal_code       STRING,
                  stars             FLOAT,
                  review_count      NUMBER,
                  is_open           NUMBER,
                  categories        STRING,
                  primary_cuisine   STRING
                )
                """
            ),

            (
                "copy_into_yelp_business_2city",
                """
                COPY INTO YELP_BUSINESS_2CITY
                FROM @GCS_SILVER_STAGE/yelp_business_2city/
                FILE_FORMAT = (TYPE = PARQUET)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PATTERN = '.*part-.*'
                """
            ),

            (
                "create_table_yelp_review_2city",
                """
                CREATE OR REPLACE TABLE YELP_REVIEW_2CITY (
                  review_id         STRING,
                  user_id           STRING,
                  business_id       STRING,
                  stars             FLOAT,
                  review_ts         TIMESTAMP_NTZ,
                  useful            NUMBER,
                  funny             NUMBER,
                  cool              NUMBER,
                  city              STRING,
                  state             STRING,
                  zipcode           STRING,
                  primary_cuisine   STRING
                )
                """
            ),

            (
                "copy_into_yelp_review_2city",
                """
                COPY INTO YELP_REVIEW_2CITY
                FROM @GCS_SILVER_STAGE/yelp_review_2city/
                FILE_FORMAT = (TYPE = PARQUET)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PATTERN = '.*part-.*'
                """
            ),
        ]

        for label, query in queries:
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