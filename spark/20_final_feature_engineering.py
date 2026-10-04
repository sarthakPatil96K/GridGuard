"""
GridGuard - Step 11A
Final Feature Engineering

Creates robust daily and weekly consumption features
from the standardized real smart-meter dataset.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURATION
# ============================================================

INPUT_PATH = "data/real/processed/standardized_1m"

DAILY_OUTPUT = "data/real/processed/final_daily_features_1m"

WEEKLY_OUTPUT = "data/real/processed/final_weekly_features_1m"


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("GridGuard-Final-Feature-Engineering")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 75)
print("GRIDGUARD - FINAL FEATURE ENGINEERING")
print("=" * 75)


# ============================================================
# READ DATA
# ============================================================

print("\nReading standardized smart-meter data...")

df = spark.read.parquet(INPUT_PATH)

input_count = df.count()

print(f"Input rows: {input_count:,}")


# ============================================================
# CLEAN
# ============================================================

df = (
    df
    .filter(
        F.col("meter_id").isNotNull()
        & F.col("timestamp").isNotNull()
        & F.col("consumption").isNotNull()
    )
    .filter(
        F.col("consumption") >= 0
    )
)


valid_count = df.count()

print(f"Valid rows: {valid_count:,}")


# ============================================================
# TIME FEATURES
# ============================================================

df = (
    df
    .withColumn(
        "date",
        F.to_date("timestamp")
    )
    .withColumn(
        "week_start",
        F.to_date(
            F.date_trunc(
                "week",
                F.col("timestamp")
            )
        )
    )
    .withColumn(
        "month",
        F.month("timestamp")
    )
    .withColumn(
        "day_of_week",
        F.dayofweek("timestamp")
    )
    .withColumn(
        "hour",
        F.hour("timestamp")
    )
)


# ============================================================
# TIME PERIOD FLAGS
# ============================================================

df = (
    df

    # Sunday = 1, Saturday = 7
    .withColumn(
        "is_weekend",
        F.when(
            F.col("day_of_week").isin(1, 7),
            1
        ).otherwise(0)
    )

    # 22:00–23:59 and 00:00–05:59
    .withColumn(
        "is_night",
        F.when(
            (F.col("hour") >= 22)
            | (F.col("hour") < 6),
            1
        ).otherwise(0)
    )

    # 17:00–22:00
    .withColumn(
        "is_peak",
        F.when(
            (F.col("hour") >= 17)
            & (F.col("hour") < 22),
            1
        ).otherwise(0)
    )

    # 06:00–22:00
    .withColumn(
        "is_daytime",
        F.when(
            (F.col("hour") >= 6)
            & (F.col("hour") < 22),
            1
        ).otherwise(0)
    )
)


# ============================================================
# DAILY FEATURES
# ============================================================

print("\nCalculating daily features...")

daily = (
    df
    .groupBy(
        "meter_id",
        "date"
    )
    .agg(

        # Number of half-hour readings
        F.count("*")
        .alias("reading_count"),

        # Total daily consumption
        F.sum("consumption")
        .alias("daily_consumption"),

        # Basic statistics
        F.avg("consumption")
        .alias("avg_consumption"),

        F.min("consumption")
        .alias("min_consumption"),

        F.max("consumption")
        .alias("max_consumption"),

        F.stddev("consumption")
        .alias("consumption_std"),

        F.expr(
            "percentile_approx(consumption, 0.5)"
        )
        .alias("consumption_median"),

        # Period consumption
        F.sum(
            F.when(
                F.col("is_night") == 1,
                F.col("consumption")
            ).otherwise(0)
        )
        .alias("night_consumption"),

        F.sum(
            F.when(
                F.col("is_peak") == 1,
                F.col("consumption")
            ).otherwise(0)
        )
        .alias("peak_consumption"),

        F.sum(
            F.when(
                F.col("is_daytime") == 1,
                F.col("consumption")
            ).otherwise(0)
        )
        .alias("daytime_consumption"),

        F.sum(
            F.when(
                F.col("is_weekend") == 1,
                F.col("consumption")
            ).otherwise(0)
        )
        .alias("weekend_consumption"),
    )
)


# ============================================================
# DAILY RATIOS
# ============================================================

daily = (
    daily

    .withColumn(
        "night_consumption_ratio",
        F.when(
            F.col("daily_consumption") > 0,
            F.col("night_consumption")
            / F.col("daily_consumption")
        ).otherwise(0)
    )

    .withColumn(
        "peak_consumption_ratio",
        F.when(
            F.col("daily_consumption") > 0,
            F.col("peak_consumption")
            / F.col("daily_consumption")
        ).otherwise(0)
    )

    .withColumn(
        "daytime_consumption_ratio",
        F.when(
            F.col("daily_consumption") > 0,
            F.col("daytime_consumption")
            / F.col("daily_consumption")
        ).otherwise(0)
    )

    .withColumn(
        "weekend_day",
        F.when(
            F.dayofweek("date").isin(1, 7),
            1
        ).otherwise(0)
    )
)


# ============================================================
# WEEKLY FEATURES
# ============================================================

print("\nCalculating weekly features...")

weekly = (
    df
    .groupBy(
        "meter_id",
        "week_start"
    )
    .agg(

        F.sum("consumption")
        .alias("weekly_consumption"),

        F.sum(
            F.when(
                F.col("is_weekend") == 1,
                F.col("consumption")
            ).otherwise(0)
        )
        .alias("weekend_consumption"),

        F.sum(
            F.when(
                F.col("is_night") == 1,
                F.col("consumption")
            ).otherwise(0)
        )
        .alias("night_consumption"),

        F.sum(
            F.when(
                F.col("is_peak") == 1,
                F.col("consumption")
            ).otherwise(0)
        )
        .alias("peak_consumption"),

        F.count("*")
        .alias("weekly_reading_count"),
    )
)


# ============================================================
# WEEKLY RATIOS
# ============================================================

weekly = (
    weekly

    .withColumn(
        "weekend_consumption_ratio",
        F.when(
            F.col("weekly_consumption") > 0,
            F.col("weekend_consumption")
            / F.col("weekly_consumption")
        ).otherwise(0)
    )

    .withColumn(
        "night_consumption_ratio",
        F.when(
            F.col("weekly_consumption") > 0,
            F.col("night_consumption")
            / F.col("weekly_consumption")
        ).otherwise(0)
    )

    .withColumn(
        "peak_consumption_ratio",
        F.when(
            F.col("weekly_consumption") > 0,
            F.col("peak_consumption")
            / F.col("weekly_consumption")
        ).otherwise(0)
    )
)


# ============================================================
# WRITE DAILY
# ============================================================

print("\nWriting daily features...")

(
    daily
    .write
    .mode("overwrite")
    .parquet(DAILY_OUTPUT)
)


# ============================================================
# WRITE WEEKLY
# ============================================================

print("Writing weekly features...")

(
    weekly
    .write
    .mode("overwrite")
    .parquet(WEEKLY_OUTPUT)
)


# ============================================================
# VERIFICATION
# ============================================================

daily_count = daily.count()
weekly_count = weekly.count()

print("\n" + "=" * 75)
print("FINAL FEATURE ENGINEERING RESULTS")
print("=" * 75)

print(
    f"Input rows          : {input_count:,}"
)

print(
    f"Valid readings      : {valid_count:,}"
)

print(
    f"Daily feature rows  : {daily_count:,}"
)

print(
    f"Weekly feature rows : {weekly_count:,}"
)


# ============================================================
# SCHEMA
# ============================================================

print("\nDaily feature schema:")

daily.printSchema()

print("\nWeekly feature schema:")

weekly.printSchema()


# ============================================================
# SAMPLE
# ============================================================

print("\nDaily feature sample:")

(
    daily
    .orderBy(
        "meter_id",
        "date"
    )
    .show(
        10,
        truncate=False
    )
)


print("\nWeekly feature sample:")

(
    weekly
    .orderBy(
        "meter_id",
        "week_start"
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# FINAL
# ============================================================

spark.stop()

print("\n" + "=" * 75)
print("STEP 11A COMPLETED")
print("=" * 75)