#!/usr/bin/env python3
"""
Yelp bronze -> silver PySpark pipeline for Dataproc / spark-submit.

Reads bronze Yelp business + review JSON files from GCS, filters to
restaurant businesses in New Orleans, LA and Philadelphia, PA, derives a
primary cuisine, and writes curated silver parquet datasets back to GCS.

Example:
  gcloud dataproc jobs submit pyspark gs://YOUR_BUCKET/code/yelp_bronze_to_silver.py \
    --cluster=sparkexplorationv3 \
    --region=us-west1 \
    -- \
    --bronze-business gs://YOUR_BUCKET/bronze/yelp_academic_dataset_business.json \
    --bronze-review gs://YOUR_BUCKET/bronze/yelp_academic_dataset_review.json \
    --silver-business gs://YOUR_BUCKET/silver/yelp_business_2city \
    --silver-review gs://YOUR_BUCKET/silver/yelp_review_2city \
    --write-mode overwrite
"""

import argparse
import sys
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


APP_NAME = "yelp-bronze-to-silver"
VALID_WRITE_MODES = {"overwrite", "append", "error", "errorifexists", "ignore"}


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Yelp bronze to silver PySpark pipeline")
    parser.add_argument(
        "--bronze-business",
        required=True,
        help="GCS path to bronze business JSON",
    )
    parser.add_argument(
        "--bronze-review",
        required=True,
        help="GCS path to bronze review JSON",
    )
    parser.add_argument(
        "--silver-business",
        required=True,
        help="GCS output path for silver business parquet",
    )
    parser.add_argument(
        "--silver-review",
        required=True,
        help="GCS output path for silver review parquet",
    )
    parser.add_argument(
        "--write-mode",
        default="overwrite",
        help="Spark write mode: overwrite, append, error, errorifexists, ignore",
    )
    return parser.parse_args(list(argv))


def validate_args(args: argparse.Namespace) -> None:
    if args.write_mode.lower() not in VALID_WRITE_MODES:
        raise ValueError(
            f"Invalid --write-mode '{args.write_mode}'. "
            f"Expected one of: {', '.join(sorted(VALID_WRITE_MODES))}"
        )


def create_spark() -> SparkSession:
    return SparkSession.builder.appName(APP_NAME).getOrCreate()


def build_primary_cuisine_expr() -> F.Column:
    return (
        F.when(F.col("categories").contains("Cajun/Creole"), F.lit("Cajun/Creole"))
        .when(F.col("categories").contains("Seafood"), F.lit("Seafood"))
        .when(F.col("categories").contains("French"), F.lit("French"))
        .when(F.col("categories").contains("Italian"), F.lit("Italian"))
        .when(F.col("categories").contains("Mexican"), F.lit("Mexican"))
        .when(F.col("categories").contains("Chinese"), F.lit("Chinese"))
        .when(F.col("categories").contains("Pizza"), F.lit("Pizza"))
        .when(F.col("categories").contains("Burgers"), F.lit("Burgers"))
        .when(F.col("categories").contains("Breakfast & Brunch"), F.lit("Breakfast & Brunch"))
        .when(F.col("categories").contains("American (New)"), F.lit("American"))
        .when(F.col("categories").contains("American (Traditional)"), F.lit("American"))
        .when(F.col("categories").contains("Sandwiches"), F.lit("Sandwiches"))
        .when(F.col("categories").contains("Fast Food"), F.lit("Fast Food"))
        .otherwise(F.lit("Other"))
    )


def transform_business(business_df: DataFrame) -> DataFrame:
    biz = (
        business_df.withColumn("categories", F.coalesce(F.col("categories"), F.lit("")))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("state", F.trim(F.col("state")))
        .withColumn("postal_code", F.coalesce(F.col("postal_code"), F.lit("")))
        .withColumn("zipcode", F.substring(F.col("postal_code"), 1, 5))
        .withColumn("is_restaurant", F.lower(F.col("categories")).contains("restaurants"))
        .filter(F.col("is_restaurant") == True)
        .filter(
            ((F.col("city") == "New Orleans") & (F.col("state") == "LA"))
            | ((F.col("city") == "Philadelphia") & (F.col("state") == "PA"))
        )
    )

    return biz.withColumn("primary_cuisine", build_primary_cuisine_expr()).select(
        "business_id",
        "name",
        "city",
        "state",
        "zipcode",
        "postal_code",
        "stars",
        "review_count",
        "is_open",
        "categories",
        "primary_cuisine",
    )


def transform_review(review_df: DataFrame, biz_silver_df: DataFrame) -> DataFrame:
    review_silver = (
        review_df.select(
            "review_id",
            "user_id",
            "business_id",
            "stars",
            "date",
            "useful",
            "funny",
            "cool",
        )
        .withColumn("review_ts", F.to_timestamp("date"))
        .drop("date")
        .filter(F.col("review_ts").isNotNull())
    )

    return (
        review_silver.alias("r")
        .join(
            biz_silver_df.select(
                "business_id", "city", "state", "zipcode", "primary_cuisine"
            ).alias("b"),
            on="business_id",
            how="inner",
        )
        .select(
            "review_id",
            "user_id",
            "business_id",
            "stars",
            "review_ts",
            "useful",
            "funny",
            "cool",
            "city",
            "state",
            "zipcode",
            "primary_cuisine",
        )
    )


def main(argv: Iterable[str]) -> int:
    args = parse_args(argv)
    validate_args(args)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Reading bronze business data from: {args.bronze_business}")
        print(f"Reading bronze review data from:   {args.bronze_review}")

        business_df = spark.read.json(args.bronze_business)
        review_df = spark.read.json(args.bronze_review)

        biz_silver_df = transform_business(business_df)
        review_silver_df = transform_review(review_df, biz_silver_df)

        business_count = biz_silver_df.count()
        review_count = review_silver_df.count()

        print(f"Silver business row count: {business_count}")
        print(f"Silver review row count:   {review_count}")

        write_mode = args.write_mode.lower()

        biz_silver_df.write.mode(write_mode).parquet(args.silver_business)
        review_silver_df.write.mode(write_mode).parquet(args.silver_review)

        print(f"Saved silver business parquet to: {args.silver_business}")
        print(f"Saved silver review parquet to:   {args.silver_review}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))