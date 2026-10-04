from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# ============================================================
# GridGuard - Real Data BDA Analysis
# ============================================================

RAW_FILE = r"C:\projects\GridGuard\data\raw\LCL-June2015v2_0.csv"

SILVER_PATH = r"C:\projects\GridGuard\data\parquet\silver"
GOLD_PATH = r"C:\projects\GridGuard\data\parquet\gold"

# ------------------------------------------------------------
# 1. Spark Session
# ------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("GridGuard-Real-BDA")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 70)
print("GRIDGUARD - BIG DATA ANALYTICS PIPELINE")
print("=" * 70)


# ------------------------------------------------------------
# 2. INGESTION
# ------------------------------------------------------------

print("\n[1] Loading real smart-meter data...")

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(RAW_FILE)
)

print("Raw columns:")
print(df.columns)

raw_count = df.count()

print(f"Raw records: {raw_count:,}")
print(f"Raw meters: {df.select('LCLid').distinct().count():,}")


# ------------------------------------------------------------
# 3. STANDARDIZE COLUMN NAMES
# ------------------------------------------------------------

df = df.withColumnRenamed("LCLid", "meter_id")
df = df.withColumnRenamed("stdorToU", "tariff_type")
df = df.withColumnRenamed("DateTime", "datetime")
df = df.withColumnRenamed(
    "KWH/hh (per half hour) ",
    "consumption"
)


# ------------------------------------------------------------
# 4. CLEANING
# ------------------------------------------------------------

print("\n[2] Cleaning data...")

df = (
    df
    .withColumn(
        "datetime",
        F.to_timestamp("datetime")
    )
    .withColumn(
        "consumption",
        F.when(
            F.trim(F.col("consumption")).isin("", "Null", "NULL", "null"),
            None
        ).otherwise(
            F.col("consumption").cast(DoubleType())
        )
    )
)

# Remove invalid timestamps
df = df.filter(F.col("datetime").isNotNull())

# Remove negative consumption
df = df.filter(
    (F.col("consumption").isNull()) |
    (F.col("consumption") >= 0)
)

# Remove duplicate meter/timestamp observations
df = df.dropDuplicates(["meter_id", "datetime"])

clean_count = df.count()

print(f"Clean records: {clean_count:,}")
print(f"Records removed: {raw_count - clean_count:,}")


# ------------------------------------------------------------
# 5. DATA QUALITY ANALYSIS
# ------------------------------------------------------------

print("\n[3] Data quality analysis...")

null_consumption = df.filter(
    F.col("consumption").isNull()
).count()

negative_consumption = df.filter(
    F.col("consumption") < 0
).count()

duplicate_check = (
    df.groupBy("meter_id", "datetime")
      .count()
      .filter(F.col("count") > 1)
      .count()
)

print(f"Null consumption values : {null_consumption:,}")
print(f"Negative consumption    : {negative_consumption:,}")
print(f"Duplicate meter/times   : {duplicate_check:,}")


# ------------------------------------------------------------
# 6. TEMPORAL FEATURES
# ------------------------------------------------------------

print("\n[4] Creating temporal features...")

df = (
    df
    .withColumn("date", F.to_date("datetime"))
    .withColumn("hour", F.hour("datetime"))
    .withColumn("day_of_week", F.dayofweek("datetime"))
    .withColumn("day_name", F.date_format("datetime", "EEEE"))
    .withColumn("month", F.month("datetime"))
    .withColumn("year", F.year("datetime"))
    .withColumn(
        "is_weekend",
        F.when(F.dayofweek("datetime").isin(1, 7), 1).otherwise(0)
    )
    .withColumn(
        "is_night",
        F.when(
            (F.hour("datetime") >= 0) &
            (F.hour("datetime") < 6),
            1
        ).otherwise(0)
    )
)


# ------------------------------------------------------------
# 7. SILVER DATASET
# ------------------------------------------------------------

print("\n[5] Writing Silver Parquet...")

(
    df
    .select(
        "meter_id",
        "tariff_type",
        "datetime",
        "date",
        "hour",
        "day_of_week",
        "day_name",
        "month",
        "year",
        "is_weekend",
        "is_night",
        "consumption"
    )
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(SILVER_PATH)
)

print(f"Silver dataset written to:\n{SILVER_PATH}")


# ------------------------------------------------------------
# 8. DAILY AGGREGATION
# ------------------------------------------------------------

print("\n[6] Performing distributed daily aggregation...")

daily = (
    df
    .groupBy("meter_id", "date")
    .agg(
        F.sum("consumption").alias("daily_consumption"),
        F.avg("consumption").alias("avg_half_hour_consumption"),
        F.max("consumption").alias("peak_consumption"),
        F.min("consumption").alias("min_consumption"),
        F.stddev("consumption").alias("consumption_std"),
        F.count("consumption").alias("valid_readings"),

        F.sum(
            F.when(F.col("is_night") == 1, F.col("consumption"))
             .otherwise(0)
        ).alias("night_consumption"),

        F.sum(
            F.when(F.col("is_weekend") == 1, F.col("consumption"))
             .otherwise(0)
        ).alias("weekend_consumption")
    )
)


# ------------------------------------------------------------
# 9. BEHAVIORAL FEATURES
# ------------------------------------------------------------

print("\n[7] Creating behavioral features...")

daily = (
    daily
    .withColumn(
        "night_ratio",
        F.when(
            F.col("daily_consumption") > 0,
            F.col("night_consumption") /
            F.col("daily_consumption")
        ).otherwise(0)
    )
)


# ------------------------------------------------------------
# 10. HISTORICAL BASELINE
# ------------------------------------------------------------

print("\n[8] Calculating historical baselines...")

meter_window = Window.partitionBy("meter_id")

daily = (
    daily
    .withColumn(
        "historical_mean",
        F.avg("daily_consumption").over(meter_window)
    )
    .withColumn(
        "historical_std",
        F.stddev("daily_consumption").over(meter_window)
    )
    .withColumn(
        "historical_z",
        F.when(
            F.col("historical_std") > 0,
            (
                F.col("daily_consumption")
                - F.col("historical_mean")
            ) / F.col("historical_std")
        ).otherwise(0)
    )
)


# ------------------------------------------------------------
# 11. ROLLING ANALYSIS
# ------------------------------------------------------------

print("\n[9] Calculating rolling behavioral statistics...")

rolling_window = (
    Window
    .partitionBy("meter_id")
    .orderBy("date")
    .rowsBetween(-7, -1)
)

daily = (
    daily
    .withColumn(
        "rolling_mean",
        F.avg("daily_consumption").over(rolling_window)
    )
    .withColumn(
        "rolling_std",
        F.stddev("daily_consumption").over(rolling_window)
    )
    .withColumn(
        "rolling_deviation",
        F.when(
            F.col("rolling_mean") > 0,
            (
                F.col("daily_consumption")
                - F.col("rolling_mean")
            ) / F.col("rolling_mean")
        ).otherwise(0)
    )
)


# ------------------------------------------------------------
# 12. PEER ANALYSIS
# ------------------------------------------------------------

print("\n[10] Performing peer comparison...")

peer_stats = (
    daily
    .groupBy("date")
    .agg(
        F.avg("daily_consumption").alias("peer_mean"),
        F.stddev("daily_consumption").alias("peer_std")
    )
)

daily = daily.join(
    peer_stats,
    on="date",
    how="left"
)

daily = daily.withColumn(
    "peer_z",
    F.when(
        F.col("peer_std") > 0,
        (
            F.col("daily_consumption")
            - F.col("peer_mean")
        ) / F.col("peer_std")
    ).otherwise(0)
)


# ------------------------------------------------------------
# 13. TREND
# ------------------------------------------------------------

print("\n[11] Calculating consumption trend...")

trend_window = (
    Window
    .partitionBy("meter_id")
    .orderBy("date")
)

daily = (
    daily
    .withColumn(
        "previous_consumption",
        F.lag("daily_consumption").over(trend_window)
    )
    .withColumn(
        "consumption_change",
        F.when(
            F.col("previous_consumption") > 0,
            (
                F.col("daily_consumption")
                - F.col("previous_consumption")
            ) / F.col("previous_consumption")
        ).otherwise(0)
    )
)


# ------------------------------------------------------------
# 14. GOLD DATASET
# ------------------------------------------------------------

print("\n[12] Creating Gold analytical dataset...")

gold = daily.select(
    "meter_id",
    "date",

    "daily_consumption",
    "avg_half_hour_consumption",
    "peak_consumption",
    "min_consumption",
    "consumption_std",

    "historical_mean",
    "historical_std",
    "historical_z",

    "rolling_mean",
    "rolling_std",
    "rolling_deviation",

    "peer_mean",
    "peer_std",
    "peer_z",

    "night_ratio",
    "weekend_consumption",

    "previous_consumption",
    "consumption_change",

    "valid_readings"
)


(
    gold
    .write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(GOLD_PATH)
)

print(f"Gold dataset written to:\n{GOLD_PATH}")


# ------------------------------------------------------------
# 15. BDA SUMMARY
# ------------------------------------------------------------

print("\n" + "=" * 70)
print("GRIDGUARD BDA SUMMARY")
print("=" * 70)

print(f"Raw records       : {raw_count:,}")
print(f"Clean records     : {clean_count:,}")
print(f"Unique meters     : {df.select('meter_id').distinct().count():,}")
print(f"Daily records     : {gold.count():,}")

print("\nTop meters by total consumption:")

(
    gold
    .groupBy("meter_id")
    .agg(
        F.sum("daily_consumption").alias("total_consumption"),
        F.avg("daily_consumption").alias("avg_daily_consumption"),
        F.stddev("daily_consumption").alias("volatility")
    )
    .orderBy(F.desc("total_consumption"))
    .show(10, truncate=False)
)

print("\nAverage consumption by month:")

(
    gold
    .withColumn("month", F.month("date"))
    .groupBy("month")
    .agg(
        F.avg("daily_consumption")
        .alias("average_daily_consumption")
    )
    .orderBy("month")
    .show(12)
)

print("\nBDA pipeline completed successfully.")

spark.stop()