from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    max,
    min,
    stddev,
    sum,
    when,
    col,
    hour,
    dayofweek,
    lit
)


spark = (
    SparkSession.builder
    .appName("GridGuard-Feature-Engineering")
    .master("local[2]")
    .getOrCreate()
)


input_path = "data/parquet/smart_meter"
output_path = "data/processed/meter_features"


# --------------------------------------------------
# 1. Read processed Parquet data
# --------------------------------------------------

df = spark.read.parquet(input_path)

print("\n===== INPUT DATA =====")
df.show(10, truncate=False)


# --------------------------------------------------
# 2. Create time-based features
# --------------------------------------------------

df = (
    df
    .withColumn("hour", hour(col("timestamp")))
    .withColumn("day_of_week", dayofweek(col("timestamp")))
)


# --------------------------------------------------
# 3. Identify night-time readings
# --------------------------------------------------

df = df.withColumn(
    "is_night",
    when(
        (col("hour") >= 0) & (col("hour") < 6),
        1
    ).otherwise(0)
)


# --------------------------------------------------
# 4. Identify weekend readings
# Spark:
# 1 = Sunday
# 7 = Saturday
# --------------------------------------------------

df = df.withColumn(
    "is_weekend",
    when(
        (col("day_of_week") == 1) |
        (col("day_of_week") == 7),
        1
    ).otherwise(0)
)


# --------------------------------------------------
# 5. Aggregate meter-level features
# --------------------------------------------------

features = (
    df.groupBy("meter_id")
    .agg(
        avg("consumption").alias("avg_consumption"),

        max("consumption").alias("peak_consumption"),

        min("consumption").alias("min_consumption"),

        stddev("consumption").alias(
            "consumption_std"
        ),

        sum(
            when(
                col("is_night") == 1,
                col("consumption")
            ).otherwise(0)
        ).alias("night_consumption"),

        sum("consumption").alias(
            "total_consumption"
        ),

        sum(
            when(
                col("is_weekend") == 1,
                col("consumption")
            ).otherwise(0)
        ).alias("weekend_consumption"),

        sum(
            when(
                col("is_weekend") == 1,
                1
            ).otherwise(0)
        ).alias("weekend_readings"),

        sum(
            when(
                col("is_night") == 1,
                1
            ).otherwise(0)
        ).alias("night_readings"),

        sum(lit(1)).alias(
            "total_readings"
        )
    )
)


# --------------------------------------------------
# 6. Calculate ratios
# --------------------------------------------------

features = (
    features

    .withColumn(
        "night_consumption_ratio",
        col("night_consumption") /
        col("total_consumption")
    )

    .withColumn(
        "weekend_consumption_ratio",
        col("weekend_consumption") /
        col("total_consumption")
    )
)


# --------------------------------------------------
# 7. Select final features
# --------------------------------------------------

final_features = features.select(
    "meter_id",
    "avg_consumption",
    "peak_consumption",
    "min_consumption",
    "consumption_std",
    "night_consumption_ratio",
    "weekend_consumption_ratio"
)


print("\n===== METER FEATURES =====")

final_features.show(
    truncate=False
)


print("\n===== FEATURE SCHEMA =====")

final_features.printSchema()


# --------------------------------------------------
# 8. Save features
# --------------------------------------------------

final_features.write \
    .mode("overwrite") \
    .parquet(output_path)


print("\nFeatures written to:")
print(output_path)


spark.stop()