CREATE OR REPLACE TABLE YELP_BUSINESS_ACS (
  postal_code       STRING,
  n_business        NUMBER,
  mean_stars        FLOAT,
  total_reviews     NUMBER,
  zip_avg_price     FLOAT,
  share_3plus       FLOAT,
  weighted_rating   FLOAT
);

COPY INTO YELP_BUSINESS_ACS
FROM {stage_prefix}/yelp_business_zip_summary/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*part-.*';

CREATE OR REPLACE FILE FORMAT FF_PARQUET
TYPE = PARQUET;

CREATE OR REPLACE TABLE SILVER_ACS_PHILADELPHIA_NEW_ORLEANS
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '{stage_prefix}/acs/ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021_WITH_CUSTOM_COLUMNS/',
      FILE_FORMAT => 'FF_PARQUET'
    )
  )
);

COPY INTO SILVER_ACS_PHILADELPHIA_NEW_ORLEANS
FROM {stage_prefix}/acs/ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021_WITH_CUSTOM_COLUMNS/
FILE_FORMAT = (FORMAT_NAME = FF_PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*part-.*';

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
);

COPY INTO BUSINESS_CATEGORY_MAPPED
FROM {stage_prefix}/business_category_mapped/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*part-.*';

CREATE OR REPLACE TABLE REVIEW_WEEK_AGG (
  business_id       STRING,
  week              TIMESTAMP_NTZ,
  review_cnt        NUMBER,
  avg_stars         FLOAT
);

COPY INTO REVIEW_WEEK_AGG
FROM {stage_prefix}/review_week_agg/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*part-.*';

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
);

COPY INTO WEATHER_WEEK_AGG
FROM {stage_prefix}/weather_week_agg/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*part-.*';

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
);

COPY INTO YELP_BUSINESS_2CITY
FROM {stage_prefix}/yelp_business_2city/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*part-.*';

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
);

COPY INTO YELP_REVIEW_2CITY
FROM {stage_prefix}/yelp_review_2city/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*part-.*';


