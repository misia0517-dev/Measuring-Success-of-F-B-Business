#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ACS_ETL_Pipeline") \
    .getOrCreate()

# -----------------------------
# Read Bronze Table
# -----------------------------
df = spark.read.option("header", True).option("inferSchema", True) \
    .csv("gs://mgmt405_dataset/bronze/acs_datasets/ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021.csv")

print("Input row count:", df.count())
print("Input columns:")
print(df.columns)

# -----------------------------
# Filter invalid rows
# -----------------------------
cols_check = [
    "total_population",
    "median_household_income",
    "total_housing_units",
    "median_age_total",
    "median_age_male",
    "median_age_female",
    "age_total",
    "age_male_total"
]

condition = None
for c in cols_check:
    cond = F.col(c) > 0
    condition = cond if condition is None else (condition & cond)

df_filtered = df.filter(condition)

# -----------------------------
# Age buckets
# -----------------------------
df_filtered = df_filtered.withColumn(
    "age_0_17",
    F.col("age_male_under_5") +
    F.col("age_male_5_9") +
    F.col("age_male_10_14") +
    F.col("age_male_15_17") +
    F.col("age_female_under_5") +
    F.col("age_female_5_9") +
    F.col("age_female_10_14") +
    F.col("age_female_15_17")
)

df_filtered = df_filtered.withColumn(
    "age_18_34",
    F.col("age_male_18_19") +
    F.col("age_male_20") +
    F.col("age_male_21") +
    F.col("age_male_22_24") +
    F.col("age_male_25_29") +
    F.col("age_male_30_34") +
    F.col("age_female_18_19") +
    F.col("age_female_20") +
    F.col("age_female_21") +
    F.col("age_female_22_24") +
    F.col("age_female_25_29") +
    F.col("age_female_30_34")
)

df_filtered = df_filtered.withColumn(
    "age_35_54",
    F.col("age_male_35_39") +
    F.col("age_male_40_44") +
    F.col("age_male_45_49") +
    F.col("age_male_50_54") +
    F.col("age_female_35_39") +
    F.col("age_female_40_44") +
    F.col("age_female_45_49") +
    F.col("age_female_50_54")
)

df_filtered = df_filtered.withColumn(
    "age_55_plus",
    F.col("age_male_55_59") +
    F.col("age_male_60_61") +
    F.col("age_male_62_64") +
    F.col("age_male_65_66") +
    F.col("age_male_67_69") +
    F.col("age_male_70_74") +
    F.col("age_male_75_79") +
    F.col("age_male_80_84") +
    F.col("age_male_85_plus") +
    F.col("age_female_55_59") +
    F.col("age_female_60_61") +
    F.col("age_female_62_64") +
    F.col("age_female_65_66") +
    F.col("age_female_67_69") +
    F.col("age_female_70_74") +
    F.col("age_female_75_79") +
    F.col("age_female_80_84") +
    F.col("age_female_85_plus")
)

# -----------------------------
# Age shares
# -----------------------------
df_filtered = df_filtered \
    .withColumn("pct_kids", F.col("age_0_17") / F.col("total_population")) \
    .withColumn("pct_young_adults", F.col("age_18_34") / F.col("total_population")) \
    .withColumn("pct_middle_age", F.col("age_35_54") / F.col("total_population")) \
    .withColumn("pct_seniors", F.col("age_55_plus") / F.col("total_population"))

# -----------------------------
# Income groups
# -----------------------------
df_filtered = df_filtered.withColumn(
    "low_income_hh",
    F.col("hh_under_10k") +
    F.col("hh_10k_15k") +
    F.col("hh_15k_20k") +
    F.col("hh_20k_25k") +
    F.col("hh_25k_30k") +
    F.col("hh_30k_35k")
)

df_filtered = df_filtered.withColumn(
    "lower_middle_hh",
    F.col("hh_35k_40k") +
    F.col("hh_40k_45k") +
    F.col("hh_45k_50k") +
    F.col("hh_50k_60k") +
    F.col("hh_60k_75k")
)

df_filtered = df_filtered.withColumn(
    "upper_middle_hh",
    F.col("hh_75k_100k") +
    F.col("hh_100k_125k") +
    F.col("hh_125k_150k")
)

df_filtered = df_filtered.withColumn(
    "high_income_hh",
    F.col("hh_150k_200k") + F.col("hh_200k_plus")
)

# -----------------------------
# Income shares
# -----------------------------
df_filtered = df_filtered \
    .withColumn("pct_low_income", F.col("low_income_hh") / F.col("total_households")) \
    .withColumn("pct_lower_middle", F.col("lower_middle_hh") / F.col("total_households")) \
    .withColumn("pct_upper_middle", F.col("upper_middle_hh") / F.col("total_households")) \
    .withColumn("pct_high_income", F.col("high_income_hh") / F.col("total_households"))

# -----------------------------
# Debug before write
# -----------------------------
print("Row count after transformations:", df_filtered.count())
df_filtered.printSchema()
df_filtered.show(5, truncate=False)

# -----------------------------
# Write Silver Table (Parquet)
# -----------------------------
output_path = "gs://mgmt405_dataset/silver/acs/ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021_WITH_CUSTOM_COLUMNS"

df_filtered.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(output_path)

print(f"Write completed to: {output_path}")

spark.stop()


# In[ ]:




