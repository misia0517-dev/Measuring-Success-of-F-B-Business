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
                "create_gold_table_acs_business",
                """
                CREATE OR REPLACE TABLE GOLD_TABLE_ACS_BUSINESS AS
                SELECT
                    b.*,
                    a."zcta",
                    a."total_population",
                    a."median_household_income",
                    a."total_housing_units",
                    a."total_households",
                    a."median_age_total",
                    a."city",
                    a."state_id",
                    a."pct_low_income",
                    a."pct_lower_middle",
                    a."pct_upper_middle",
                    a."pct_high_income",
                    a."pct_kids",
                    a."pct_young_adults",
                    a."pct_middle_age",
                    a."pct_seniors"
                FROM YELP_BUSINESS_ACS b
                JOIN SILVER_ACS_PHILADELPHIA_NEW_ORLEANS a
                    ON b.postal_code = TO_VARCHAR(a."zcta")
                """
            ),

            (
                "create_gold_yelp_weather_business_panel",
                """
                CREATE OR REPLACE TABLE GOLD_YELP_WEATHER_BUSINESS_PANEL AS
                SELECT 
                    r.week AS review_week,
                    MONTH(r.week) AS review_month,
                    YEAR(r.week) AS review_year,
                    b.business_id,
                    b.name AS business_name,
                    b.city,
                    b.state,
                    b.postal_code,
                    b.station_id,
                    b.categories,
                    b.mapped_category,
                    r.review_cnt AS weekly_review_count,
                    r.avg_stars AS weekly_avg_stars,
                    w.temp_mean,
                    w.tmax_mean,
                    w.tmin_mean,
                    w.temp_range,
                    w.prcp_sum,
                    w.prcp_mean,
                    w.heavy_rain_week,
                    w.hot_week,
                    w.freeze_week
                FROM REVIEW_WEEK_AGG r
                INNER JOIN BUSINESS_CATEGORY_MAPPED b
                    ON r.business_id = b.business_id
                LEFT JOIN WEATHER_WEEK_AGG w
                    ON b.station_id = w.station_id 
                    AND r.week = w.week
                """
            ),

            (
                "create_dim_date",
                """
                CREATE OR REPLACE TABLE DIM_DATE AS
                SELECT DISTINCT
                    REVIEW_WEEK,
                    REVIEW_MONTH,
                    REVIEW_YEAR
                FROM GOLD_YELP_WEATHER_BUSINESS_PANEL
                """
            ),

            (
                "create_dim_business",
                """
                CREATE OR REPLACE TABLE DIM_BUSINESS AS
                SELECT DISTINCT
                    BUSINESS_ID,
                    BUSINESS_NAME,
                    CITY,
                    STATE,
                    POSTAL_CODE,
                    STATION_ID,
                    CATEGORIES,
                    MAPPED_CATEGORY
                FROM GOLD_YELP_WEATHER_BUSINESS_PANEL
                """
            ),

            (
                "create_fact_weekly_activity",
                """
                CREATE OR REPLACE TABLE FACT_WEEKLY_ACTIVITY AS
                SELECT
                    BUSINESS_ID,
                    REVIEW_WEEK,
                    WEEKLY_REVIEW_COUNT,
                    WEEKLY_AVG_STARS,
                    TEMP_MEAN,
                    TMAX_MEAN,
                    TMIN_MEAN,
                    TEMP_RANGE,
                    PRCP_SUM,
                    PRCP_MEAN,
                    HEAVY_RAIN_WEEK,
                    HOT_WEEK,
                    FREEZE_WEEK
                FROM GOLD_YELP_WEATHER_BUSINESS_PANEL
                """
            ),

            ("set_min_lag_days", "SET MIN_LAG_DAYS = 180"),
            ("set_min_incumbents", "SET MIN_INCUMBENTS = 2"),
            ("set_min_lifetime_reviews", "SET MIN_LIFETIME_REVIEWS = 10"),
            ("set_min_biz_per_group", "SET MIN_BIZ_PER_GROUP = 8"),
            ("set_pre_days", "SET PRE_DAYS = 365"),
            ("set_min_pre_reviews", "SET MIN_PRE_REVIEWS = 5"),
            ("set_min_post_reviews", "SET MIN_POST_REVIEWS = 3"),
            ("set_baseline_rating", "SET BASELINE_RATING = 3.5"),
            ("set_damping_m", "SET DAMPING_M = 100"),

            (
                "create_gold_cannibalization_zip_cuisine_window_weighted",
                """
                CREATE OR REPLACE TABLE GOLD_CANNIBALIZATION_ZIP_CUISINE_WINDOW_WEIGHTED AS
                WITH

                BUSINESS_ENTRY AS (
                    SELECT
                        B.BUSINESS_ID,
                        B.CITY,
                        B.STATE,
                        B.ZIPCODE,
                        B.PRIMARY_CUISINE,
                        MIN(R.REVIEW_TS) AS ENTRY_DATE,
                        COUNT(*) AS LIFETIME_REVIEW_CNT
                    FROM YELP_BUSINESS_2CITY B
                    JOIN YELP_REVIEW_2CITY R
                      ON B.BUSINESS_ID = R.BUSINESS_ID
                    WHERE B.PRIMARY_CUISINE <> 'Other'
                    GROUP BY
                        B.BUSINESS_ID, B.CITY, B.STATE, B.ZIPCODE, B.PRIMARY_CUISINE
                    HAVING COUNT(*) >= $MIN_LIFETIME_REVIEWS
                ),

                ELIGIBLE_GROUPS AS (
                    SELECT
                        CITY,
                        STATE,
                        ZIPCODE,
                        PRIMARY_CUISINE
                    FROM BUSINESS_ENTRY
                    GROUP BY CITY, STATE, ZIPCODE, PRIMARY_CUISINE
                    HAVING COUNT(DISTINCT BUSINESS_ID) >= $MIN_BIZ_PER_GROUP
                ),

                BUSINESS_ENTRY_ELIGIBLE AS (
                    SELECT E.*
                    FROM BUSINESS_ENTRY E
                    JOIN ELIGIBLE_GROUPS G
                      ON E.CITY = G.CITY
                     AND E.STATE = G.STATE
                     AND E.ZIPCODE = G.ZIPCODE
                     AND E.PRIMARY_CUISINE = G.PRIMARY_CUISINE
                ),

                EVENT_PAIRS AS (
                    SELECT
                        E.CITY,
                        E.STATE,
                        E.ZIPCODE,
                        E.PRIMARY_CUISINE,
                        E.BUSINESS_ID AS ENTRANT_BUSINESS_ID,
                        E.ENTRY_DATE AS ENTRANT_DATE,
                        I.BUSINESS_ID AS INCUMBENT_BUSINESS_ID
                    FROM BUSINESS_ENTRY_ELIGIBLE E
                    JOIN BUSINESS_ENTRY_ELIGIBLE I
                      ON E.CITY = I.CITY
                     AND E.STATE = I.STATE
                     AND E.ZIPCODE = I.ZIPCODE
                     AND E.PRIMARY_CUISINE = I.PRIMARY_CUISINE
                     AND I.ENTRY_DATE <= DATEADD(DAY, -$MIN_LAG_DAYS, E.ENTRY_DATE)
                ),

                VALID_EVENTS AS (
                    SELECT
                        CITY,
                        STATE,
                        ZIPCODE,
                        PRIMARY_CUISINE,
                        ENTRANT_BUSINESS_ID,
                        ENTRANT_DATE,
                        COUNT(DISTINCT INCUMBENT_BUSINESS_ID) AS N_INCUMBENTS
                    FROM EVENT_PAIRS
                    GROUP BY CITY, STATE, ZIPCODE, PRIMARY_CUISINE, ENTRANT_BUSINESS_ID, ENTRANT_DATE
                    HAVING COUNT(DISTINCT INCUMBENT_BUSINESS_ID) >= $MIN_INCUMBENTS
                ),

                EVENT_INCUMBENTS AS (
                    SELECT
                        P.CITY,
                        P.STATE,
                        P.ZIPCODE,
                        P.PRIMARY_CUISINE,
                        P.ENTRANT_BUSINESS_ID,
                        P.ENTRANT_DATE,
                        P.INCUMBENT_BUSINESS_ID,
                        E.N_INCUMBENTS
                    FROM EVENT_PAIRS P
                    JOIN VALID_EVENTS E
                      ON P.CITY = E.CITY
                     AND P.STATE = E.STATE
                     AND P.ZIPCODE = E.ZIPCODE
                     AND P.PRIMARY_CUISINE = E.PRIMARY_CUISINE
                     AND P.ENTRANT_BUSINESS_ID = E.ENTRANT_BUSINESS_ID
                     AND P.ENTRANT_DATE = E.ENTRANT_DATE
                ),

                EVENT_REVIEWS AS (
                    SELECT
                        EI.CITY,
                        EI.STATE,
                        EI.ZIPCODE,
                        EI.PRIMARY_CUISINE,
                        EI.ENTRANT_BUSINESS_ID,
                        EI.ENTRANT_DATE,
                        EI.N_INCUMBENTS,
                        R.REVIEW_TS,
                        R.STARS
                    FROM EVENT_INCUMBENTS EI
                    JOIN YELP_REVIEW_2CITY R
                      ON EI.INCUMBENT_BUSINESS_ID = R.BUSINESS_ID
                ),

                PRE_METRICS AS (
                    SELECT
                        CITY,
                        STATE,
                        ZIPCODE,
                        PRIMARY_CUISINE,
                        ENTRANT_BUSINESS_ID,
                        ENTRANT_DATE,
                        MAX(N_INCUMBENTS) AS N_INCUMBENTS,
                        COUNT(*) AS PRE_REVIEW_COUNT,
                        AVG(STARS) AS PRE_AVG_RATING,
                        COUNT(*) / 12.0 AS PRE_REVIEWS_PER_MONTH,
                        ((AVG(STARS) * COUNT(*)) + ($BASELINE_RATING * $DAMPING_M)) / (COUNT(*) + $DAMPING_M) AS PRE_WEIGHTED_RATING
                    FROM EVENT_REVIEWS
                    WHERE REVIEW_TS >= DATEADD(DAY, -$PRE_DAYS, ENTRANT_DATE)
                      AND REVIEW_TS < ENTRANT_DATE
                    GROUP BY CITY, STATE, ZIPCODE, PRIMARY_CUISINE, ENTRANT_BUSINESS_ID, ENTRANT_DATE
                    HAVING COUNT(*) >= $MIN_PRE_REVIEWS
                ),

                POST_LABELED AS (
                    SELECT
                        CITY,
                        STATE,
                        ZIPCODE,
                        PRIMARY_CUISINE,
                        ENTRANT_BUSINESS_ID,
                        ENTRANT_DATE,
                        N_INCUMBENTS,
                        STARS,
                        CASE
                            WHEN REVIEW_TS >= ENTRANT_DATE
                             AND REVIEW_TS < DATEADD(DAY, 90, ENTRANT_DATE)
                                THEN 'post_0_90'
                            WHEN REVIEW_TS >= DATEADD(DAY, 90, ENTRANT_DATE)
                             AND REVIEW_TS < DATEADD(DAY, 180, ENTRANT_DATE)
                                THEN 'post_90_180'
                            WHEN REVIEW_TS >= DATEADD(DAY, 180, ENTRANT_DATE)
                             AND REVIEW_TS < DATEADD(DAY, 365, ENTRANT_DATE)
                                THEN 'post_180_365'
                            ELSE NULL
                        END AS WINDOW
                    FROM EVENT_REVIEWS
                ),

                POST_METRICS AS (
                    SELECT
                        CITY,
                        STATE,
                        ZIPCODE,
                        PRIMARY_CUISINE,
                        ENTRANT_BUSINESS_ID,
                        ENTRANT_DATE,
                        WINDOW,
                        MAX(N_INCUMBENTS) AS N_INCUMBENTS,
                        COUNT(*) AS POST_REVIEW_COUNT,
                        AVG(STARS) AS POST_AVG_RATING,
                        CASE
                            WHEN WINDOW = 'post_180_365' THEN COUNT(*) / (185.0 / 30.0)
                            ELSE COUNT(*) / (90.0 / 30.0)
                        END AS POST_REVIEWS_PER_MONTH,
                        ((AVG(STARS) * COUNT(*)) + ($BASELINE_RATING * $DAMPING_M)) / (COUNT(*) + $DAMPING_M) AS POST_WEIGHTED_RATING
                    FROM POST_LABELED
                    WHERE WINDOW IS NOT NULL
                    GROUP BY CITY, STATE, ZIPCODE, PRIMARY_CUISINE, ENTRANT_BUSINESS_ID, ENTRANT_DATE, WINDOW
                    HAVING COUNT(*) >= $MIN_POST_REVIEWS
                ),

                EVENT_METRICS AS (
                    SELECT
                        POST.CITY,
                        POST.STATE,
                        POST.ZIPCODE,
                        POST.PRIMARY_CUISINE,
                        POST.ENTRANT_BUSINESS_ID,
                        POST.ENTRANT_DATE,
                        POST.WINDOW,
                        POST.N_INCUMBENTS,
                        POST.POST_REVIEWS_PER_MONTH - PRE.PRE_REVIEWS_PER_MONTH AS DELTA_REVIEWS_PER_MONTH,
                        POST.POST_AVG_RATING - PRE.PRE_AVG_RATING AS DELTA_AVG_RATING,
                        POST.POST_WEIGHTED_RATING - PRE.PRE_WEIGHTED_RATING AS DELTA_WEIGHTED_RATING
                    FROM POST_METRICS POST
                    JOIN PRE_METRICS PRE
                      ON POST.CITY = PRE.CITY
                     AND POST.STATE = PRE.STATE
                     AND POST.ZIPCODE = PRE.ZIPCODE
                     AND POST.PRIMARY_CUISINE = PRE.PRIMARY_CUISINE
                     AND POST.ENTRANT_BUSINESS_ID = PRE.ENTRANT_BUSINESS_ID
                     AND POST.ENTRANT_DATE = PRE.ENTRANT_DATE
                )

                SELECT
                    CITY,
                    STATE,
                    ZIPCODE,
                    PRIMARY_CUISINE,
                    WINDOW,
                    AVG(DELTA_REVIEWS_PER_MONTH) AS MEAN_DELTA_REVIEWS,
                    AVG(DELTA_AVG_RATING) AS MEAN_DELTA_RATING,
                    AVG(DELTA_WEIGHTED_RATING) AS MEAN_DELTA_WEIGHTED_RATING,
                    COUNT(*) AS N_EVENTS
                FROM EVENT_METRICS
                GROUP BY CITY, STATE, ZIPCODE, PRIMARY_CUISINE, WINDOW
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