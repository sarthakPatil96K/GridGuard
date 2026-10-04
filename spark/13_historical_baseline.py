from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 1. Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("GridGuard_Historical_Baseline")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. Paths
# ============================================================

INPUT_PATH = "data/real/processed/daily_features_1m"

OUTPUT_PATH = (
    "data/real/processed/historical_deviations_1m"
)

BASELINE_PATH = (
    "data/real/processed/meter_baselines_1m"
)


# ============================================================
# 3. Read daily features
# ============================================================

print("=" * 70)
print("GRIDGUARD - HISTORICAL BASELINE ANALYSIS")
print("=" * 70)

print("\nReading daily features...")

df = spark.read.parquet(INPUT_PATH)

input_count = df.count()

print(f"Input rows: {input_count}")

print("\nInput schema:")
df.printSchema()


# ============================================================
# 4. Select required columns
# ============================================================

df = (
    df
    .select(
        "meter_id",
        "date",
        "daily_consumption",
        "avg_consumption",
        "max_consumption",
        "min_consumption",
        "consumption_std",
        "night_consumption_ratio",
        "peak_consumption_ratio",
        "weekend_consumption_ratio"
    )
    .filter(
        F.col("meter_id").isNotNull()
        & F.col("date").isNotNull()
        & F.col("daily_consumption").isNotNull()
    )
)


# ============================================================
# 5. Calculate meter-specific historical baseline
# ============================================================

print("\nCalculating meter-level historical baselines...")

meter_baseline = (
    df
    .groupBy("meter_id")
    .agg(
        F.count("*").alias("historical_days"),

        F.avg("daily_consumption")
        .alias("historical_avg_consumption"),

        F.stddev("daily_consumption")
        .alias("historical_std_consumption"),

        F.min("daily_consumption")
        .alias("historical_min_consumption"),

        F.max("daily_consumption")
        .alias("historical_max_consumption"),

        F.expr(
            "percentile_approx(daily_consumption, 0.50)"
        ).alias("historical_median_consumption"),

        F.expr(
            "percentile_approx(daily_consumption, 0.95)"
        ).alias("historical_p95_consumption"),

        F.expr(
            "percentile_approx(daily_consumption, 0.05)"
        ).alias("historical_p05_consumption")
    )
)


# ============================================================
# 6. Handle NULL / zero standard deviation
# ============================================================

meter_baseline = (
    meter_baseline
    .withColumn(
        "historical_std_consumption",
        F.coalesce(
            F.col("historical_std_consumption"),
            F.lit(0.0)
        )
    )
)


# ============================================================
# 7. Join historical baseline to daily records
# ============================================================

daily = (
    df
    .join(
        meter_baseline,
        on="meter_id",
        how="left"
    )
)


# ============================================================
# 8. Calculate absolute deviation
# ============================================================

daily = (
    daily
    .withColumn(
        "absolute_deviation",
        F.abs(
            F.col("daily_consumption")
            - F.col("historical_avg_consumption")
        )
    )
)


# ============================================================
# 9. Calculate historical Z-score
# ============================================================

EPSILON = 0.000001

daily = (
    daily
    .withColumn(
        "historical_z_score",
        F.when(
            F.col("historical_std_consumption") > EPSILON,

            (
                F.col("daily_consumption")
                - F.col("historical_avg_consumption")
            )
            /
            F.col("historical_std_consumption")

        ).otherwise(F.lit(0.0))
    )
)


# ============================================================
# 10. Absolute Z-score
# ============================================================

daily = (
    daily
    .withColumn(
        "absolute_historical_z_score",
        F.abs(
            F.col("historical_z_score")
        )
    )
)


# ============================================================
# 11. Relative historical deviation
# ============================================================

daily = (
    daily
    .withColumn(
        "relative_historical_deviation",

        F.when(
            F.abs(
                F.col("historical_avg_consumption")
            ) > EPSILON,

            F.abs(
                F.col("daily_consumption")
                - F.col("historical_avg_consumption")
            )
            /
            F.abs(
                F.col("historical_avg_consumption")
            )

        ).otherwise(F.lit(0.0))
    )
)


# ============================================================
# 12. Historical anomaly flag
# ============================================================

# Project-defined threshold:
# |Z| >= 3 → historical anomaly

daily = (
    daily
    .withColumn(
        "historical_anomaly",

        F.when(
            F.col("absolute_historical_z_score") >= 3,
            F.lit(1)
        ).otherwise(
            F.lit(0)
        )
    )
)


# ============================================================
# 13. Deviation direction
# ============================================================

daily = (
    daily
    .withColumn(
        "deviation_direction",

        F.when(
            F.col("historical_z_score") >= 3,
            F.lit("HIGH")
        )

        .when(
            F.col("historical_z_score") <= -3,
            F.lit("LOW")
        )

        .otherwise(
            F.lit("NORMAL")
        )
    )
)


# ============================================================
# 14. Historical deviation score
# ============================================================

# Convert |Z| to a 0–100 project-defined score.
#
# |Z| >= 5 → 100
# |Z| = 0  → 0

daily = (
    daily
    .withColumn(
        "historical_deviation_score",

        (
            F.least(
                F.col("absolute_historical_z_score"),
                F.lit(5.0)
            )
            / 5.0
            * 100.0
        )
    )
)


# ============================================================
# 15. Final result
# ============================================================

result = daily.select(

    "meter_id",
    "date",

    "daily_consumption",

    "avg_consumption",
    "max_consumption",
    "min_consumption",
    "consumption_std",

    "night_consumption_ratio",
    "peak_consumption_ratio",
    "weekend_consumption_ratio",

    "historical_days",

    "historical_avg_consumption",
    "historical_std_consumption",
    "historical_min_consumption",
    "historical_max_consumption",
    "historical_median_consumption",
    "historical_p05_consumption",
    "historical_p95_consumption",

    "absolute_deviation",

    "historical_z_score",
    "absolute_historical_z_score",

    "relative_historical_deviation",

    "historical_anomaly",

    "deviation_direction",

    "historical_deviation_score"
)


# ============================================================
# 16. Save daily deviation results
# ============================================================

print("\nSaving historical deviation results...")

(
    result
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)


# ============================================================
# 17. Save meter baselines
# ============================================================

print("Saving meter baselines...")

(
    meter_baseline
    .write
    .mode("overwrite")
    .parquet(BASELINE_PATH)
)


# ============================================================
# 18. Verification
# ============================================================

print("\n" + "=" * 70)
print("VERIFICATION")
print("=" * 70)

output_count = result.count()

print(f"\nOutput rows: {output_count}")


# ============================================================
# 19. Meter baseline summary
# ============================================================

print("\nMeter baseline summary:")

(
    meter_baseline
    .orderBy("meter_id")
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# 20. Highest historical deviations
# ============================================================

print("\nHighest historical deviations:")

(
    result
    .select(
        "meter_id",
        "date",
        "daily_consumption",
        "historical_avg_consumption",
        "historical_std_consumption",
        "historical_z_score",
        "relative_historical_deviation",
        "historical_anomaly",
        "deviation_direction",
        "historical_deviation_score"
    )
    .orderBy(
        F.desc(
            "absolute_historical_z_score"
        )
    )
    .show(
        20,
        truncate=False
    )
)


# ============================================================
# 21. Anomaly distribution
# ============================================================

print("\nHistorical anomaly distribution:")

(
    result
    .groupBy("historical_anomaly")
    .count()
    .orderBy("historical_anomaly")
    .show()
)


# ============================================================
# 22. Direction distribution
# ============================================================

print("\nDeviation direction distribution:")

(
    result
    .groupBy("deviation_direction")
    .count()
    .orderBy("deviation_direction")
    .show()
)


# ============================================================
# 23. High-deviation records
# ============================================================

print("\nRecords with |Z| >= 3:")

(
    result
    .filter(
        F.col("absolute_historical_z_score") >= 3
    )
    .select(
        "meter_id",
        "date",
        "daily_consumption",
        "historical_avg_consumption",
        "historical_z_score",
        "relative_historical_deviation",
        "historical_deviation_score"
    )
    .orderBy(
        F.desc(
            "absolute_historical_z_score"
        )
    )
    .show(
        20,
        truncate=False
    )
)


# ============================================================
# 24. Final verification
# ============================================================

print("\nChecking row-count preservation...")

if output_count == input_count:
    print("Row-count verification PASSED.")
else:
    print(
        "WARNING: Row-count mismatch!"
        f" Input={input_count}, Output={output_count}"
    )


print("\n" + "=" * 70)
print("STEP 4 COMPLETED")
print("=" * 70)

print("\nDaily deviation output:")
print(OUTPUT_PATH)

print("\nMeter baseline output:")
print(BASELINE_PATH)


spark.stop()