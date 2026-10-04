from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    hour,
    dayofweek,
    to_date,
    when,
    lit,
    avg,
    min,
    max,
    stddev,
    sum,
    count,
    percentile_approx
)


# ============================================================
# CONFIGURATION
# ============================================================

INPUT_PATH = "data/real/processed/standardized_1m"

OUTPUT_PATH = "data/real/processed/daily_features_1m"


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("GridGuard-Real-Feature-Engineering")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("\n" + "=" * 70)
print("GRIDGUARD — REAL DATA FEATURE ENGINEERING")
print("=" * 70)


# ============================================================
# 1. READ STANDARDIZED DATA
# ============================================================

print("\n" + "=" * 70)
print("1. READING STANDARDIZED PARQUET")
print("=" * 70)

df = spark.read.parquet(INPUT_PATH)

print("\nInput schema:")
df.printSchema()

total_rows = df.count()

print(
    f"\nInput rows: {total_rows:,}"
)


# ============================================================
# 2. REMOVE INVALID CORE RECORDS
# ============================================================

print("\n" + "=" * 70)
print("2. FILTERING INVALID RECORDS")
print("=" * 70)


# We cannot calculate consumption features if the
# timestamp or consumption is missing.

df_valid = (
    df
    .filter(col("meter_id").isNotNull())
    .filter(col("timestamp").isNotNull())
    .filter(col("consumption").isNotNull())
)


valid_rows = df_valid.count()

print(
    f"\nValid rows for feature engineering: "
    f"{valid_rows:,}"
)

print(
    f"Excluded rows: "
    f"{total_rows - valid_rows:,}"
)


# ============================================================
# 3. ADD TEMPORAL FEATURES
# ============================================================

print("\n" + "=" * 70)
print("3. CREATING TEMPORAL FEATURES")
print("=" * 70)


df_features = (
    df_valid

    # Calendar date
    .withColumn(
        "date",
        to_date(col("timestamp"))
    )

    # Hour: 0–23
    .withColumn(
        "hour",
        hour(col("timestamp"))
    )

    # Spark dayofweek:
    # 1 = Sunday
    # 7 = Saturday
    .withColumn(
        "day_of_week",
        dayofweek(col("timestamp"))
    )

    # Weekend flag
    .withColumn(
        "is_weekend",
        when(
            dayofweek(col("timestamp")).isin(1, 7),
            1
        ).otherwise(0)
    )

    # Night period:
    # 00:00–06:00
    # 22:00–23:59
    .withColumn(
        "is_night",
        when(
            (hour(col("timestamp")) < 6) |
            (hour(col("timestamp")) >= 22),
            1
        ).otherwise(0)
    )

    # Peak-hour period
    # 17:00–21:59
    .withColumn(
        "is_peak",
        when(
            (hour(col("timestamp")) >= 17) &
            (hour(col("timestamp")) < 22),
            1
        ).otherwise(0)
    )

    # Daytime
    .withColumn(
        "is_daytime",
        when(
            (hour(col("timestamp")) >= 6) &
            (hour(col("timestamp")) < 22),
            1
        ).otherwise(0)
    )
)


# ============================================================
# 4. DAILY METER FEATURES
# ============================================================

print("\n" + "=" * 70)
print("4. CALCULATING DAILY METER FEATURES")
print("=" * 70)


daily_features = (
    df_features
    .groupBy(
        "meter_id",
        "date"
    )
    .agg(

        # Number of half-hour readings
        count("consumption")
        .alias("reading_count"),

        # Total daily consumption
        sum("consumption")
        .alias("daily_consumption"),

        # Average half-hour consumption
        avg("consumption")
        .alias("avg_consumption"),

        # Minimum
        min("consumption")
        .alias("min_consumption"),

        # Maximum
        max("consumption")
        .alias("max_consumption"),

        # Standard deviation
        stddev("consumption")
        .alias("consumption_std"),

        # Median
        percentile_approx(
            "consumption",
            0.50,
            10000
        ).alias("consumption_median"),

        # Night consumption
        sum(
            when(
                col("is_night") == 1,
                col("consumption")
            ).otherwise(0)
        ).alias("night_consumption"),

        # Daytime consumption
        sum(
            when(
                col("is_daytime") == 1,
                col("consumption")
            ).otherwise(0)
        ).alias("daytime_consumption"),

        # Peak consumption
        sum(
            when(
                col("is_peak") == 1,
                col("consumption")
            ).otherwise(0)
        ).alias("peak_consumption"),

        # Weekend consumption
        sum(
            when(
                col("is_weekend") == 1,
                col("consumption")
            ).otherwise(0)
        ).alias("weekend_consumption")
    )
)


# ============================================================
# 5. DERIVE RATIOS
# ============================================================

print("\n" + "=" * 70)
print("5. CALCULATING CONSUMPTION RATIOS")
print("=" * 70)


daily_features = (
    daily_features

    # Night ratio
    .withColumn(
        "night_consumption_ratio",
        when(
            col("daily_consumption") > 0,
            col("night_consumption") /
            col("daily_consumption")
        ).otherwise(0)
    )

    # Peak ratio
    .withColumn(
        "peak_consumption_ratio",
        when(
            col("daily_consumption") > 0,
            col("peak_consumption") /
            col("daily_consumption")
        ).otherwise(0)
    )

    # Weekend ratio
    .withColumn(
        "weekend_consumption_ratio",
        when(
            col("daily_consumption") > 0,
            col("weekend_consumption") /
            col("daily_consumption")
        ).otherwise(0)
    )
)


# ============================================================
# 6. SELECT FINAL FEATURES
# ============================================================

daily_features = daily_features.select(
    "meter_id",
    "date",
    "reading_count",
    "daily_consumption",
    "avg_consumption",
    "min_consumption",
    "max_consumption",
    "consumption_std",
    "consumption_median",
    "night_consumption",
    "daytime_consumption",
    "peak_consumption",
    "weekend_consumption",
    "night_consumption_ratio",
    "peak_consumption_ratio",
    "weekend_consumption_ratio"
)


# ============================================================
# 7. DISPLAY RESULTS
# ============================================================

print("\n" + "=" * 70)
print("6. FEATURE SUMMARY")
print("=" * 70)


daily_count = daily_features.count()

print(
    f"\nDaily meter records: "
    f"{daily_count:,}"
)


print("\nSample daily features:")

(
    daily_features
    .orderBy(
        "meter_id",
        "date"
    )
    .show(
        20,
        truncate=False
    )
)


# ============================================================
# 8. SCHEMA
# ============================================================

print("\nFinal feature schema:")

daily_features.printSchema()


# ============================================================
# 9. WRITE PARQUET
# ============================================================

print("\n" + "=" * 70)
print("7. WRITING DAILY FEATURES")
print("=" * 70)


(
    daily_features
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)


print("\nDaily feature Parquet written successfully.")


# ============================================================
# 10. VERIFY OUTPUT
# ============================================================

print("\n" + "=" * 70)
print("8. VERIFYING OUTPUT")
print("=" * 70)


verification_df = (
    spark.read
    .parquet(OUTPUT_PATH)
)


verification_count = (
    verification_df.count()
)


print(
    f"\nOutput daily records: "
    f"{verification_count:,}"
)


if verification_count == daily_count:
    print(
        "\nVerification PASSED."
    )
else:
    print(
        "\nWARNING: Row count mismatch."
    )


print("\nOutput schema:")

verification_df.printSchema()


# ============================================================
# COMPLETE
# ============================================================

print("\n" + "=" * 70)
print("FEATURE ENGINEERING COMPLETE")
print("=" * 70)

print(
    f"\nInput readings: "
    f"{total_rows:,}"
)

print(
    f"Valid readings: "
    f"{valid_rows:,}"
)

print(
    f"Daily feature records: "
    f"{verification_count:,}"
)

print(
    f"\nOutput:"
)

print(
    OUTPUT_PATH
)


spark.stop()