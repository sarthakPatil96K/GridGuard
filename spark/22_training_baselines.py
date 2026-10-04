from pyspark.sql import SparkSession, functions as F

# ============================================================
# GRIDGUARD - STEP 11C
# TRAINING-ONLY BASELINE CONSTRUCTION
# ============================================================

TRAIN_INPUT = "data/real/processed/temporal_train_1m"

METER_BASELINE_OUTPUT = (
    "data/real/processed/temporal_meter_baselines_1m"
)

SEASONAL_BASELINE_OUTPUT = (
    "data/real/processed/temporal_seasonal_baselines_1m"
)

PEER_BASELINE_OUTPUT = (
    "data/real/processed/temporal_peer_baselines_1m"
)

spark = (
    SparkSession.builder
    .appName("GridGuard_Training_Baselines")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 75)
print("GRIDGUARD - STEP 11C")
print("TRAINING-ONLY BASELINE CONSTRUCTION")
print("=" * 75)


# ============================================================
# 1. READ TRAINING DATA
# ============================================================

print("\nReading temporal training data...")

train = spark.read.parquet(TRAIN_INPUT)

train = (
    train
    .filter(F.col("meter_id").isNotNull())
    .filter(F.col("date").isNotNull())
    .filter(F.col("daily_consumption").isNotNull())
    .filter(F.col("daily_consumption") >= 0)
)

train_count = train.count()

print(f"Training rows: {train_count:,}")


print("\nTraining date range:")

train.select(
    F.min("date").alias("training_start"),
    F.max("date").alias("training_end")
).show(truncate=False)


# ============================================================
# 2. ADD SEASONAL FEATURES
# ============================================================

train = (
    train
    .withColumn(
        "month",
        F.month("date")
    )
    .withColumn(
        "day_of_week",
        F.dayofweek("date")
    )
)


# ============================================================
# 3. METER-LEVEL BASELINES
# ============================================================

print("\nCalculating meter-level training baselines...")

meter_baselines = (
    train
    .groupBy("meter_id")
    .agg(
        F.count("*").alias("training_days"),

        F.avg("daily_consumption")
         .alias("baseline_avg"),

        F.stddev("daily_consumption")
         .alias("baseline_std"),

        F.min("daily_consumption")
         .alias("baseline_min"),

        F.max("daily_consumption")
         .alias("baseline_max"),

        F.expr(
            "percentile_approx(daily_consumption, 0.50)"
        ).alias("baseline_median"),

        F.expr(
            "percentile_approx(daily_consumption, 0.05)"
        ).alias("baseline_p05"),

        F.expr(
            "percentile_approx(daily_consumption, 0.95)"
        ).alias("baseline_p95")
    )
    .withColumn(
        "baseline_std",
        F.coalesce(
            F.col("baseline_std"),
            F.lit(0.0)
        )
    )
)

meter_baselines = meter_baselines.cache()

print("\nMeter baseline count:")
print(meter_baselines.count())


print("\nSample meter baselines:")

(
    meter_baselines
    .orderBy("meter_id")
    .show(30, truncate=False)
)


# ============================================================
# 4. WRITE METER BASELINES
# ============================================================

print("\nWriting meter baselines...")

(
    meter_baselines
    .write
    .mode("overwrite")
    .parquet(METER_BASELINE_OUTPUT)
)


# ============================================================
# 5. SEASONAL BASELINES
# ============================================================

print("\nCalculating seasonal training baselines...")

seasonal_baselines = (
    train
    .groupBy(
        "meter_id",
        "month",
        "day_of_week"
    )
    .agg(
        F.count("*").alias("seasonal_days"),

        F.avg("daily_consumption")
         .alias("seasonal_avg"),

        F.stddev("daily_consumption")
         .alias("seasonal_std"),

        F.expr(
            "percentile_approx(daily_consumption, 0.50)"
        ).alias("seasonal_median"),

        F.expr(
            "percentile_approx(daily_consumption, 0.05)"
        ).alias("seasonal_p05"),

        F.expr(
            "percentile_approx(daily_consumption, 0.95)"
        ).alias("seasonal_p95")
    )
    .withColumn(
        "seasonal_std",
        F.coalesce(
            F.col("seasonal_std"),
            F.lit(0.0)
        )
    )
)


print("\nSeasonal baseline rows:")

seasonal_count = seasonal_baselines.count()

print(seasonal_count)


print("\nSample seasonal baselines:")

(
    seasonal_baselines
    .orderBy(
        "meter_id",
        "month",
        "day_of_week"
    )
    .show(30, truncate=False)
)


# ============================================================
# 6. WRITE SEASONAL BASELINES
# ============================================================

print("\nWriting seasonal baselines...")

(
    seasonal_baselines
    .write
    .mode("overwrite")
    .parquet(SEASONAL_BASELINE_OUTPUT)
)


# ============================================================
# 7. CREATE PEER GROUPS
# ============================================================

print("\nCreating training-only peer groups...")

# Peer groups are based on training-period average consumption.
#
# This is a project-defined approximation because the current
# dataset does not provide reliable household zone / consumer
# category metadata.

peer_stats = (
    meter_baselines
    .select(
        "meter_id",
        "baseline_avg"
    )
)


# Approximate quintile boundaries.

quantiles = peer_stats.approxQuantile(
    "baseline_avg",
    [0.20, 0.40, 0.60, 0.80],
    0.01
)

q20, q40, q60, q80 = quantiles

print("\nPeer-group thresholds:")
print(f"20th percentile: {q20}")
print(f"40th percentile: {q40}")
print(f"60th percentile: {q60}")
print(f"80th percentile: {q80}")


peer_groups = (
    peer_stats
    .withColumn(
        "peer_group",
        F.when(
            F.col("baseline_avg") <= q20,
            "P1_LOW"
        )
        .when(
            F.col("baseline_avg") <= q40,
            "P2_LOW_MEDIUM"
        )
        .when(
            F.col("baseline_avg") <= q60,
            "P3_MEDIUM"
        )
        .when(
            F.col("baseline_avg") <= q80,
            "P4_MEDIUM_HIGH"
        )
        .otherwise(
            "P5_HIGH"
        )
    )
)


print("\nPeer-group distribution:")

(
    peer_groups
    .groupBy("peer_group")
    .agg(
        F.count("*").alias("meter_count"),
        F.avg("baseline_avg").alias("average_consumption")
    )
    .orderBy("peer_group")
    .show(truncate=False)
)


# ============================================================
# 8. PEER BASELINE
# ============================================================

print("\nCalculating peer baselines from TRAINING data only...")

peer_baselines = (
    peer_groups
    .groupBy("peer_group")
    .agg(
        F.count("*").alias("peer_meter_count"),

        F.avg("baseline_avg")
         .alias("peer_average_consumption"),

        F.expr(
            "percentile_approx(baseline_avg, 0.50)"
        ).alias("peer_median_consumption"),

        F.stddev("baseline_avg")
         .alias("peer_std_consumption")
    )
    .withColumn(
        "peer_std_consumption",
        F.coalesce(
            F.col("peer_std_consumption"),
            F.lit(0.0)
        )
    )
)


print("\nPeer baselines:")

peer_baselines.show(truncate=False)


# ============================================================
# 9. WRITE PEER BASELINES
# ============================================================

print("\nWriting peer baselines...")

(
    peer_baselines
    .write
    .mode("overwrite")
    .parquet(PEER_BASELINE_OUTPUT)
)


# ============================================================
# 10. FINAL VALIDATION
# ============================================================

print("\n" + "=" * 75)
print("STEP 11C VALIDATION")
print("=" * 75)

print(f"Training rows              : {train_count:,}")
print(
    f"Meter baseline records     : "
    f"{meter_baselines.count():,}"
)
print(
    f"Seasonal baseline records  : "
    f"{seasonal_count:,}"
)
print(
    f"Peer baseline records      : "
    f"{peer_baselines.count():,}"
)

print("\nOutput paths:")

print(
    f"Meter baselines    : "
    f"{METER_BASELINE_OUTPUT}"
)

print(
    f"Seasonal baselines : "
    f"{SEASONAL_BASELINE_OUTPUT}"
)

print(
    f"Peer baselines     : "
    f"{PEER_BASELINE_OUTPUT}"
)

print("\n" + "=" * 75)
print("STEP 11C COMPLETED")
print("=" * 75)

spark.stop()