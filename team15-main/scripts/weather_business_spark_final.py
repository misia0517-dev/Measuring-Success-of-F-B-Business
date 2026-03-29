import argparse

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


def haversine_km(lat1, lon1, lat2, lon2):
    R = F.lit(6371.0)
    lat1r = F.radians(lat1)
    lon1r = F.radians(lon1)
    lat2r = F.radians(lat2)
    lon2r = F.radians(lon2)
    dlat = lat2r - lat1r
    dlon = lon2r - lon1r
    a = (
        F.pow(F.sin(dlat / 2.0), 2)
        + F.cos(lat1r) * F.cos(lat2r) * F.pow(F.sin(dlon / 2.0), 2)
    )
    c = 2.0 * F.asin(F.sqrt(a))
    return R * c


def parse_args():
    parser = argparse.ArgumentParser(description="Weather + business + reviews silver aggregations")
    parser.add_argument(
        "--bronze-weather-base",
        default="gs://mgmt405_dataset/bronze/weather_data",
        help="Base path for weather CSV folders (local path or gs://)",
    )
    parser.add_argument(
        "--bronze-business",
        default="gs://mgmt405_dataset/bronze/yelp_data/yelp_academic_dataset_business.json",
        help="Path to bronze business JSON (local path or gs://)",
    )
    parser.add_argument(
        "--bronze-review",
        default="gs://mgmt405_dataset/bronze/yelp_data/yelp_academic_dataset_review.json",
        help="Path to bronze review JSON (local path or gs://)",
    )
    parser.add_argument(
        "--silver-weather-week",
        default="gs://mgmt405_dataset/silver/weather_week_agg",
        help="Output path for weather weekly aggregation parquet",
    )
    parser.add_argument(
        "--silver-business-mapped",
        default="gs://mgmt405_dataset/silver/business_category_mapped",
        help="Output path for mapped business parquet",
    )
    parser.add_argument(
        "--silver-review-week",
        default="gs://mgmt405_dataset/silver/review_week_agg",
        help="Output path for review weekly aggregation parquet",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default="",
        help="Checkpoint dir for Spark lineage cuts. Required for local Spark runs.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("YelpWeatherAnalysis")
        .getOrCreate()
    )

    # Helps cut lineage safely when intermediate DataFrames become too large/complex.
    # For local Spark runs, pass --checkpoint-dir.
    if args.checkpoint_dir:
        spark.sparkContext.setCheckpointDir(args.checkpoint_dir)
    else:
        raise ValueError("Missing required --checkpoint-dir for this pipeline.")

    BASE_PATH = args.bronze_weather_base
    BIZ_PATH = args.bronze_business
    REV_PATH = args.bronze_review

    STATES = ["PA", "LA"]
    CITIES = ["Philadelphia", "New Orleans"]
    START_DATE = "2017-01-01"
    END_DATE = "2021-12-31"

    PRIORITY = [
        "Cajun/Creole", "Seafood", "French", "Italian", "Mexican",
        "Chinese", "Pizza", "Burgers", "Breakfast & Brunch",
        "American (New)", "American (Traditional)", "Sandwiches", "Fast Food"
    ]

    # -------------------------------------------------------------------------
    # 1) WEATHER
    # -------------------------------------------------------------------------
    weather_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.csv")
        .csv(BASE_PATH)
        .withColumn("input_path", F.input_file_name())
    )

    weather_df = weather_df.withColumn(
        "state",
        F.regexp_extract(
            F.col("input_path"),
            r"weather_data[\\/](.*?)[\\/]",
            1
        )
    ).drop("input_path")

    weather = (
        weather_df
        .withColumn("DATE", F.to_date("DATE"))
        .withColumn("PRCP", F.col("PRCP").cast("double"))
        .withColumn("TMAX", F.col("TMAX").cast("double"))
        .withColumn("TMIN", F.col("TMIN").cast("double"))
        .withColumn("LATITUDE", F.col("LATITUDE").cast("double"))
        .withColumn("LONGITUDE", F.col("LONGITUDE").cast("double"))
        .filter(F.col("state").isin(STATES))
        .filter((F.col("DATE") >= F.lit(START_DATE)) & (F.col("DATE") <= F.lit(END_DATE)))
    )

    stations = (
        weather
        .select(
            F.col("state"),
            F.col("STATION").alias("station_id"),
            F.col("LATITUDE").alias("st_lat"),
            F.col("LONGITUDE").alias("st_lon")
        )
        .dropna()
        .dropDuplicates(["state", "station_id"])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # Force materialization, then checkpoint to cut lineage
    stations.count()
    stations = stations.checkpoint(eager=True)

    weather_week = (
        weather
        .withColumn("week", F.date_trunc("week", F.col("DATE")))
        .groupBy("state", F.col("STATION").alias("station_id"), "week")
        .agg(
            F.avg("TMAX").alias("tmax_mean"),
            F.avg("TMIN").alias("tmin_mean"),
            F.sum("PRCP").alias("prcp_sum"),
            F.avg("PRCP").alias("prcp_mean"),
        )
        .withColumn("temp_mean", (F.col("tmax_mean") + F.col("tmin_mean")) / 2.0)
        .withColumn("temp_range", F.col("tmax_mean") - F.col("tmin_mean"))
        .withColumn("heavy_rain_week", (F.col("prcp_sum") > 50.0).cast("int"))
        .withColumn("hot_week", (F.col("tmax_mean") > 32.0).cast("int"))
        .withColumn("freeze_week", (F.col("tmin_mean") < 0.0).cast("int"))
    )

    (
        weather_week.write
        .mode("overwrite")
        .parquet(args.silver_weather_week)
    )

    # -------------------------------------------------------------------------
    # 2) BUSINESS
    # -------------------------------------------------------------------------
    biz_schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("categories", StringType(), True),
        StructField("stars", DoubleType(), True),
        StructField("review_count", LongType(), True),
        StructField("is_open", LongType(), True),
    ])

    biz = (
        spark.read
        .schema(biz_schema)
        .json(BIZ_PATH)
        .select(
            "business_id", "name", "city", "state", "postal_code",
            "latitude", "longitude", "categories", "stars",
            "review_count", "is_open"
        )
        .filter(F.col("city").isin(CITIES))
        .withColumn("categories", F.coalesce(F.col("categories"), F.lit("")))
    )

    biz_rest = biz.filter(F.lower(F.col("categories")).contains("restaurant"))
    biz_use = biz_rest.dropna(subset=["latitude", "longitude"])

    biz_station = (
        biz_use
        .join(F.broadcast(stations), on="state", how="inner")
        .withColumn(
            "dist_km",
            haversine_km(
                F.col("latitude"),
                F.col("longitude"),
                F.col("st_lat"),
                F.col("st_lon")
            )
        )
        .select(
            "business_id", "name", "city", "state", "postal_code",
            "categories", "stars", "latitude", "review_count",
            "is_open", "longitude", "station_id", "dist_km"
        )
    )

    w = Window.partitionBy("business_id").orderBy(F.col("dist_km").asc())

    biz_nearest = (
        biz_station
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Done with stations now
    stations.unpersist(blocking=True)

    mapping_expr = None
    for p in PRIORITY:
        condition = F.lower(F.col("categories")).contains(p.lower())
        if mapping_expr is None:
            mapping_expr = F.when(condition, p)
        else:
            mapping_expr = mapping_expr.when(condition, p)

    mapping_expr = mapping_expr.otherwise("Other")

    biz_mapped = biz_nearest.withColumn("mapped_category", mapping_expr)

    # Force lineage cut before downstream review processing
    biz_mapped = biz_mapped.checkpoint(eager=True)

    (
        biz_mapped.write
        .mode("overwrite")
        .parquet(args.silver_business_mapped)
    )

    # Re-read intentionally to break lineage for the next phase
    biz_clean = spark.read.parquet(args.silver_business_mapped)

    # -------------------------------------------------------------------------
    # 3) REVIEWS
    # -------------------------------------------------------------------------
    rev_schema = StructType([
        StructField("review_id", StringType(), True),
        StructField("business_id", StringType(), True),
        StructField("stars", DoubleType(), True),
        StructField("date", StringType(), True),
    ])

    rev = (
        spark.read
        .schema(rev_schema)
        .json(REV_PATH)
        .select("review_id", "business_id", "stars", "date")
        .withColumn("date_ts", F.to_timestamp("date"))
        .withColumn("date", F.to_date("date_ts"))
        .drop("date_ts")
        .filter((F.col("date") >= F.lit(START_DATE)) & (F.col("date") <= F.lit(END_DATE)))
    )

    rev_small = rev.join(
        F.broadcast(biz_clean.select("business_id").distinct()),
        on="business_id",
        how="inner"
    )

    rev_week = (
        rev_small
        .withColumn("week", F.date_trunc("week", F.col("date")))
        .groupBy("business_id", "week")
        .agg(
            F.count("*").alias("review_cnt"),
            F.avg("stars").alias("avg_stars"),
        )
    )

    (
        rev_week.write
        .mode("overwrite")
        .parquet(args.silver_review_week)
    )

    spark.stop()


if __name__ == "__main__":
    main()
