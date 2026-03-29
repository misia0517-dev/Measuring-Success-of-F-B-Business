#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(description="Build ZIP-level Yelp business summary from bronze to silver")
    parser.add_argument("--bronze-business", required=True, help="Input path to bronze Yelp business JSON")
    parser.add_argument("--silver-business", required=True, help="Output path to silver ZIP summary")
    parser.add_argument("--write-mode", default="overwrite", help="Spark write mode: overwrite / append / error / ignore")
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("YelpBusinessZipSummary")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # -----------------------------
    # 1. Load bronze business data
    # -----------------------------
    business_df = spark.read.json(args.bronze_business)

    # -----------------------------
    # 2. Extract price range
    # Yelp JSON usually parses attributes as struct
    # -----------------------------
    business_df = business_df.withColumn(
        "price_range",
        F.col("attributes.RestaurantsPriceRange2").cast("int")
    )

    # -----------------------------
    # 3. Filter target cities and select needed columns
    # -----------------------------
    business_data = (
    business_df
    .filter(
        (F.col("city").isin("Philadelphia", "New Orleans")) &
        (F.lower(F.col("categories")).contains("restaurants"))
    )
    .select(
        "business_id",
        "name",
        "city",
        "state",
        "postal_code",
        "latitude",
        "longitude",
        "stars",
        "review_count",
        "categories",
        "price_range"
    )
)

    # -----------------------------
    # 4. Clean ZIP and nulls
    # Keep only 5-digit ZIP codes
    # -----------------------------
    business_data = (
        business_data
        .withColumn("postal_code", F.regexp_extract(F.col("postal_code"), r"^\d{5}", 0))
        .filter(F.col("postal_code") != "")
        .dropna(subset=["postal_code", "stars", "review_count", "price_range"])
    )

    # -----------------------------
    # 5. Add helper columns
    # -----------------------------
    business_data = (
        business_data
        .withColumn("price_x_review", F.col("price_range") * F.col("review_count"))
        .withColumn("is_3plus", F.when(F.col("price_range") >= 3, 1).otherwise(0))
        .withColumn("is_3plus_x_review", F.col("is_3plus") * F.col("review_count"))
    )

    # -----------------------------
    # 6. ZIP-level aggregation
    # -----------------------------
    df_zip = business_data.groupBy("postal_code").agg(
        F.count("*").alias("n_business"),
        F.avg("stars").alias("mean_stars"),
        F.sum("review_count").alias("total_reviews"),
        (F.sum("price_x_review") / F.sum("review_count")).alias("zip_avg_price"),
        (F.sum("is_3plus_x_review") / F.sum("review_count")).alias("share_3plus")
    )

    # -----------------------------
    # 7. Add Bayesian-smoothed rating
    # weighted_rating =
    # ((mean_stars * total_reviews) + (3.8 * 814)) / (total_reviews + 814)
    # -----------------------------
    BASELINE_RATING = 3.8
    REVIEW_COUNT_BASELINE = 814

    df_zip = df_zip.withColumn(
        "weighted_rating",
        (
            (F.col("mean_stars") * F.col("total_reviews") +
             F.lit(BASELINE_RATING * REVIEW_COUNT_BASELINE))
            /
            (F.col("total_reviews") + F.lit(REVIEW_COUNT_BASELINE))
        )
    )

    # -----------------------------
    # 8. Reorder columns
    # -----------------------------
    df_zip = df_zip.select(
        "postal_code",
        "n_business",
        "mean_stars",
        "total_reviews",
        "zip_avg_price",
        "share_3plus",
        "weighted_rating"
    )

    # -----------------------------
    # 9. Write to silver
    # Output as parquet for Spark-friendly downstream use
    # -----------------------------
    (
        df_zip.write
        .mode(args.write_mode)
        .parquet(args.silver_business)
    )

    spark.stop()


if __name__ == "__main__":
    main()

