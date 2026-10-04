# ============================================================
# GridGuard - Step 34
# Meter-Level Investigation Priority
#
# Converts daily anomaly evidence into a meter-level
# investigation priority score.
#
# IMPORTANT:
# This score is NOT theft probability.
# It represents investigation priority based on:
#   - Severity
#   - Persistence
#   - Anomaly Frequency
#   - Model Agreement
#   - Recent Activity
# ============================================================

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 1. PYSPARK CONFIGURATION
# ============================================================

PYTHON_EXECUTABLE = sys.executable

os.environ["PYSPARK_PYTHON"] = PYTHON_EXECUTABLE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXECUTABLE

spark = (
    SparkSession.builder
    .appName("GridGuard_Meter_Investigation_Priority")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.pyspark.python", PYTHON_EXECUTABLE)
    .config("spark.pyspark.driver.python", PYTHON_EXECUTABLE)
    .config("spark.python.worker.reuse", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. PATHS
# ============================================================

INPUT_PATH = r"C:\projects\GridGuard\data\parquet\isolation_forest"

OUTPUT_PATH = (
    r"C:\projects\GridGuard\data\parquet"
    r"\meter_investigation_priority"
)

CSV_OUTPUT_PATH = (
    r"C:\projects\GridGuard\data\processed"
    r"\meter_investigation_priority_csv"
)


# ============================================================
# 3. READ STEP 32 OUTPUT
# ============================================================

print("\n" + "=" * 70)
print("GRIDGUARD - METER LEVEL INVESTIGATION PRIORITY")
print("=" * 70)

df = spark.read.parquet(INPUT_PATH)

print("\nInput schema:")
df.printSchema()

total_events = df.count()

print(f"\nTotal analytical events: {total_events:,}")


# ============================================================
# 4. VALIDATE REQUIRED COLUMNS
# ============================================================

required_columns = [
    "meter_id",
    "date",
    "ensemble_score",
    "ensemble_anomaly",
    "contextual_anomaly",
    "if_anomaly",
    "risk_level"
]

missing_columns = [
    column
    for column in required_columns
    if column not in df.columns
]

if missing_columns:
    raise ValueError(
        f"Missing required columns: {missing_columns}"
    )


# ============================================================
# 5. EVENT-LEVEL SEVERITY
# ============================================================
#
# ensemble_score is 0-1.
# Convert to 0-100 for easier interpretation.
#
# This is NOT probability.
#
# ============================================================

df = df.withColumn(
    "event_risk_score",
    F.least(
        F.greatest(
            F.col("ensemble_score") * 100.0,
            F.lit(0.0)
        ),
        F.lit(100.0)
    )
)


# ============================================================
# 6. MODEL AGREEMENT
# ============================================================
#
# Step 32 stores anomaly flags as INTEGER/LONG:
#
# contextual_anomaly = 0/1
# if_anomaly         = 0/1
# ensemble_anomaly   = 0/1
#
# Agreement is counted ONLY when:
#
# ensemble anomaly = 1
# AND contextual anomaly = 1
# AND IF anomaly = 1
#
# ============================================================

df = df.withColumn(
    "model_agreement_flag",
    F.when(
        (F.col("ensemble_anomaly") == 1)
        & (F.col("contextual_anomaly") == 1)
        & (F.col("if_anomaly") == 1),
        1
    ).otherwise(0)
)


# ============================================================
# 7. ENSEMBLE ANOMALY FLAG
# ============================================================

df = df.withColumn(
    "ensemble_anomaly_flag",
    F.when(
        F.col("ensemble_anomaly") == 1,
        1
    ).otherwise(0)
)


# ============================================================
# 8. ANALYSIS PERIOD
# ============================================================

period = df.select(
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date")
).collect()[0]

min_date = period["min_date"]
max_date = period["max_date"]

print("\nAnalysis period:")
print(f"Start date : {min_date}")
print(f"End date   : {max_date}")


# ============================================================
# 9. RECENT ACTIVITY WINDOW
# ============================================================
#
# Last 30 days of the complete analysis period.
#
# ============================================================

recent_start_date = (
    spark.range(1)
    .select(
        F.date_sub(
            F.lit(max_date),
            30
        ).alias("recent_start")
    )
    .collect()[0]["recent_start"]
)

df = df.withColumn(
    "recent_anomaly_flag",
    F.when(
        (F.col("date") >= F.lit(recent_start_date))
        & (F.col("ensemble_anomaly") == 1),
        1
    ).otherwise(0)
)


# ============================================================
# 10. HIGH-RISK EVENT FLAG
# ============================================================

df = df.withColumn(
    "high_risk_flag",
    F.when(
        F.col("risk_level").isin(
            "HIGH",
            "VERY HIGH",
            "CRITICAL"
        ),
        1
    ).otherwise(0)
)


# ============================================================
# 11. METER-LEVEL AGGREGATION
# ============================================================

meter_df = (
    df.groupBy("meter_id")
    .agg(

        # Number of analyzed days
        F.count("*").alias(
            "analyzed_days"
        ),

        # Ensemble anomaly days
        F.sum(
            "ensemble_anomaly_flag"
        ).alias(
            "anomaly_days"
        ),

        # High-risk days
        F.sum(
            "high_risk_flag"
        ).alias(
            "high_risk_days"
        ),

        # Average severity of anomalous days
        F.avg(
            F.when(
                F.col("ensemble_anomaly") == 1,
                F.col("event_risk_score")
            )
        ).alias(
            "avg_event_risk_score"
        ),

        # Maximum anomaly severity
        F.max(
            F.when(
                F.col("ensemble_anomaly") == 1,
                F.col("event_risk_score")
            )
        ).alias(
            "max_event_risk_score"
        ),

        # Days where both detection approaches agree
        F.sum(
            "model_agreement_flag"
        ).alias(
            "model_agreement_days"
        ),

        # Recent anomaly days
        F.sum(
            "recent_anomaly_flag"
        ).alias(
            "recent_anomaly_days"
        )
    )
)


# ============================================================
# 12. NULL HANDLING
# ============================================================

meter_df = (
    meter_df
    .withColumn(
        "anomaly_days",
        F.coalesce(
            F.col("anomaly_days"),
            F.lit(0)
        )
    )
    .withColumn(
        "high_risk_days",
        F.coalesce(
            F.col("high_risk_days"),
            F.lit(0)
        )
    )
    .withColumn(
        "model_agreement_days",
        F.coalesce(
            F.col("model_agreement_days"),
            F.lit(0)
        )
    )
    .withColumn(
        "recent_anomaly_days",
        F.coalesce(
            F.col("recent_anomaly_days"),
            F.lit(0)
        )
    )
)


# ============================================================
# 13. FREQUENCY SCORE
# ============================================================
#
# 10 anomaly days = 100 points.
#
# 1  -> 10
# 5  -> 50
# 10 -> 100
# 20 -> 100
#
# ============================================================

meter_df = meter_df.withColumn(
    "frequency_score",
    F.least(
        (
            F.col("anomaly_days") / 10.0
        ) * 100.0,
        F.lit(100.0)
    )
)


# ============================================================
# 14. PERSISTENCE SCORE
# ============================================================
#
# Percentage of analyzed days flagged by the ensemble.
#
# Example:
#
# 1 / 100 days  = 1%
# 10 / 100 days = 10%
# 50 / 100 days = 50%
#
# ============================================================

meter_df = meter_df.withColumn(
    "anomaly_rate",
    F.when(
        F.col("analyzed_days") > 0,
        (
            F.col("anomaly_days")
            / F.col("analyzed_days")
        ) * 100.0
    ).otherwise(0.0)
)

meter_df = meter_df.withColumn(
    "persistence_score",
    F.least(
        F.greatest(
            F.col("anomaly_rate"),
            F.lit(0.0)
        ),
        F.lit(100.0)
    )
)


# ============================================================
# 15. MODEL AGREEMENT SCORE
# ============================================================
#
# Agreement rate =
#
# model agreement days
# -------------------- × 100
# ensemble anomaly days
#
# ============================================================

meter_df = meter_df.withColumn(
    "agreement_rate",
    F.when(
        F.col("anomaly_days") > 0,
        (
            F.col("model_agreement_days")
            / F.col("anomaly_days")
        ) * 100.0
    ).otherwise(0.0)
)

meter_df = meter_df.withColumn(
    "agreement_score",
    F.least(
        F.greatest(
            F.col("agreement_rate"),
            F.lit(0.0)
        ),
        F.lit(100.0)
    )
)


# ============================================================
# 16. SEVERITY SCORE
# ============================================================

meter_df = meter_df.withColumn(
    "severity_score",
    F.coalesce(
        F.col("avg_event_risk_score"),
        F.lit(0.0)
    )
)

meter_df = meter_df.withColumn(
    "severity_score",
    F.least(
        F.greatest(
            F.col("severity_score"),
            F.lit(0.0)
        ),
        F.lit(100.0)
    )
)


# ============================================================
# 17. RECENT ACTIVITY SCORE
# ============================================================
#
# Five recent anomaly days = 100.
#
# ============================================================

meter_df = meter_df.withColumn(
    "recent_activity_score",
    F.least(
        (
            F.col("recent_anomaly_days")
            / 5.0
        ) * 100.0,
        F.lit(100.0)
    )
)


# ============================================================
# 18. FINAL INVESTIGATION PRIORITY SCORE
# ============================================================
#
# Severity          = 25%
# Persistence       = 25%
# Frequency         = 20%
# Model Agreement   = 20%
# Recent Activity   = 10%
#
# Total             = 100%
#
# ============================================================

meter_df = meter_df.withColumn(
    "investigation_priority_score",
    (
        F.col("severity_score") * 0.25
        + F.col("persistence_score") * 0.25
        + F.col("frequency_score") * 0.20
        + F.col("agreement_score") * 0.20
        + F.col("recent_activity_score") * 0.10
    )
)


# ============================================================
# 19. SCORE SAFETY CLAMP
# ============================================================

meter_df = meter_df.withColumn(
    "investigation_priority_score",
    F.least(
        F.greatest(
            F.col("investigation_priority_score"),
            F.lit(0.0)
        ),
        F.lit(100.0)
    )
)


# ============================================================
# 20. PRIORITY LEVEL
# ============================================================

meter_df = meter_df.withColumn(
    "priority_level",
    F.when(
        F.col("investigation_priority_score") >= 80,
        "VERY HIGH"
    )
    .when(
        F.col("investigation_priority_score") >= 60,
        "HIGH"
    )
    .when(
        F.col("investigation_priority_score") >= 40,
        "MEDIUM"
    )
    .when(
        F.col("investigation_priority_score") >= 20,
        "LOW"
    )
    .otherwise(
        "NORMAL"
    )
)


# ============================================================
# 21. ROUND OUTPUT VALUES
# ============================================================

output_df = (
    meter_df
    .withColumn(
        "avg_event_risk_score",
        F.round(
            F.col("avg_event_risk_score"),
            2
        )
    )
    .withColumn(
        "max_event_risk_score",
        F.round(
            F.col("max_event_risk_score"),
            2
        )
    )
    .withColumn(
        "anomaly_rate",
        F.round(
            F.col("anomaly_rate"),
            2
        )
    )
    .withColumn(
        "severity_score",
        F.round(
            F.col("severity_score"),
            2
        )
    )
    .withColumn(
        "persistence_score",
        F.round(
            F.col("persistence_score"),
            2
        )
    )
    .withColumn(
        "frequency_score",
        F.round(
            F.col("frequency_score"),
            2
        )
    )
    .withColumn(
        "agreement_rate",
        F.round(
            F.col("agreement_rate"),
            2
        )
    )
    .withColumn(
        "agreement_score",
        F.round(
            F.col("agreement_score"),
            2
        )
    )
    .withColumn(
        "recent_activity_score",
        F.round(
            F.col("recent_activity_score"),
            2
        )
    )
    .withColumn(
        "investigation_priority_score",
        F.round(
            F.col("investigation_priority_score"),
            2
        )
    )
)


# ============================================================
# 22. FINAL COLUMN ORDER
# ============================================================

output_df = output_df.select(
    "meter_id",
    "analyzed_days",
    "anomaly_days",
    "high_risk_days",
    "avg_event_risk_score",
    "max_event_risk_score",
    "anomaly_rate",
    "persistence_score",
    "frequency_score",
    "model_agreement_days",
    "agreement_rate",
    "agreement_score",
    "recent_anomaly_days",
    "recent_activity_score",
    "severity_score",
    "investigation_priority_score",
    "priority_level"
)


# ============================================================
# 23. VALIDATION
# ============================================================

invalid_agreement = (
    output_df
    .filter(
        (F.col("agreement_rate") < 0)
        | (F.col("agreement_rate") > 100)
    )
    .count()
)

if invalid_agreement > 0:
    raise ValueError(
        f"Invalid agreement rate found for "
        f"{invalid_agreement} meters."
    )

invalid_priority = (
    output_df
    .filter(
        (F.col("investigation_priority_score") < 0)
        | (F.col("investigation_priority_score") > 100)
    )
    .count()
)

if invalid_priority > 0:
    raise ValueError(
        f"Invalid investigation priority score found for "
        f"{invalid_priority} meters."
    )

print(
    "\nModel agreement validation: PASSED"
)

print(
    "Investigation priority range validation: PASSED"
)


# ============================================================
# 24. TOP METERS
# ============================================================

print("\n" + "=" * 70)
print("TOP METERS BY INVESTIGATION PRIORITY")
print("=" * 70)

(
    output_df
    .orderBy(
        F.desc("investigation_priority_score")
    )
    .show(
        15,
        truncate=False
    )
)


# ============================================================
# 25. PRIORITY DISTRIBUTION
# ============================================================

print("\n" + "=" * 70)
print("INVESTIGATION PRIORITY DISTRIBUTION")
print("=" * 70)

distribution = (
    output_df
    .groupBy("priority_level")
    .count()
    .orderBy(
        F.desc("count")
    )
)

distribution.show(
    truncate=False
)


# ============================================================
# 26. MODEL AGREEMENT VALIDATION TABLE
# ============================================================

print("\n" + "=" * 70)
print("MODEL AGREEMENT VALIDATION")
print("=" * 70)

(
    output_df
    .select(
        "meter_id",
        "anomaly_days",
        "model_agreement_days",
        "agreement_rate"
    )
    .orderBy(
        F.desc("agreement_rate")
    )
    .show(
        15,
        truncate=False
    )
)


# ============================================================
# 27. ONE-OFF ANOMALIES
# ============================================================

print("\n" + "=" * 70)
print("ONE-OFF ANOMALY METERS")
print("=" * 70)

(
    output_df
    .filter(
        F.col("anomaly_days") == 1
    )
    .orderBy(
        F.desc("investigation_priority_score")
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# 28. PERSISTENT ANOMALY METERS
# ============================================================

print("\n" + "=" * 70)
print("PERSISTENT ANOMALY METERS")
print("=" * 70)

(
    output_df
    .filter(
        F.col("anomaly_days") >= 10
    )
    .orderBy(
        F.desc("investigation_priority_score")
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# 29. SAVE PARQUET
# ============================================================

(
    output_df
    .write
    .mode("overwrite")
    .parquet(
        OUTPUT_PATH
    )
)

print(
    f"\nParquet output saved to:\n{OUTPUT_PATH}"
)


# ============================================================
# 30. SAVE CSV
# ============================================================

(
    output_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option(
        "header",
        True
    )
    .csv(
        CSV_OUTPUT_PATH
    )
)

print(
    f"\nCSV output saved to:\n{CSV_OUTPUT_PATH}"
)


# ============================================================
# 31. FINAL SUMMARY
# ============================================================

total_meters = output_df.count()

print("\n" + "=" * 70)
print("FINAL SUMMARY")
print("=" * 70)

print(
    f"Analysis period : {min_date} -> {max_date}"
)

print(
    f"Total meters    : {total_meters}"
)

print("\nPriority distribution:")

distribution.show(
    truncate=False
)

print("\nTop 10 investigation priorities:")

(
    output_df
    .orderBy(
        F.desc("investigation_priority_score")
    )
    .select(
        "meter_id",
        "anomaly_days",
        "high_risk_days",
        "avg_event_risk_score",
        "max_event_risk_score",
        "persistence_score",
        "frequency_score",
        "agreement_score",
        "recent_activity_score",
        "investigation_priority_score",
        "priority_level"
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# 32. STOP SPARK
# ============================================================

spark.stop()

print(
    "\nStep 34 completed successfully."
)