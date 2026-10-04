from pyspark.sql import SparkSession, functions as F
import pandas as pd
import numpy as np
import os


# ============================================================
# CONFIGURATION
# ============================================================

INPUT_PATH = (
    "data/real/processed/"
    "contextual_detection_results_1m"
)

OUTPUT_PATH = (
    "data/real/processed/"
    "risk_scoring_results_1m"
)

os.makedirs(OUTPUT_PATH, exist_ok=True)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("GridGuard_Risk_Scoring")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 75)
print("GRIDGUARD - EXPLAINABLE RISK SCORING")
print("=" * 75)


# ============================================================
# 1. READ CONTEXTUAL DETECTION RESULTS
# ============================================================

print("\nReading Step 8 contextual detection results...")

df = spark.read.parquet(
    os.path.join(
        INPUT_PATH,
        "contextual_detection_results.parquet"
    )
)

print(f"Input rows: {df.count():,}")


# ============================================================
# 2. CHECK REQUIRED COLUMNS
# ============================================================

required_columns = [
    "meter_id",
    "date",
    "daily_consumption",
    "ground_truth",
    "anomaly_type",

    "historical_z",
    "seasonal_z",
    "rolling_z",
    "peer_z",

    "relative_deviation",

    "historical_signal",
    "seasonal_signal",
    "rolling_signal",
    "peer_signal",
    "relative_signal",

    "contextual_score",
    "if_anomaly_score",
    "ensemble_score"
]

missing = [
    col for col in required_columns
    if col not in df.columns
]

if missing:
    print("\nERROR: Missing columns:")
    for col in missing:
        print(f"  {col}")

    spark.stop()
    raise ValueError(
        f"Missing required columns: {missing}"
    )


# ============================================================
# 3. PREPARE FEATURES
# ============================================================

print("\nPreparing risk components...")


# ------------------------------------------------------------
# Historical component
# ------------------------------------------------------------

df = df.withColumn(
    "historical_component",
    F.least(
        F.abs(F.col("historical_z")) / 6.0,
        F.lit(1.0)
    )
)


# ------------------------------------------------------------
# Seasonal component
# ------------------------------------------------------------

df = df.withColumn(
    "seasonal_component",
    F.least(
        F.abs(F.col("seasonal_z")) / 6.0,
        F.lit(1.0)
    )
)


# ------------------------------------------------------------
# Rolling component
# ------------------------------------------------------------

df = df.withColumn(
    "rolling_component",
    F.least(
        F.abs(F.col("rolling_z")) / 6.0,
        F.lit(1.0)
    )
)


# ------------------------------------------------------------
# Peer component
# ------------------------------------------------------------

df = df.withColumn(
    "peer_component",
    F.least(
        F.abs(F.col("peer_z")) / 4.0,
        F.lit(1.0)
    )
)


# ------------------------------------------------------------
# Magnitude component
# ------------------------------------------------------------

df = df.withColumn(
    "magnitude_component",
    F.least(
        F.abs(F.col("relative_deviation")) / 3.0,
        F.lit(1.0)
    )
)


# ============================================================
# 4. PERSISTENCE COMPONENT
# ============================================================

print("\nCalculating persistence component...")


# Candidate anomaly evidence.
#
# We use contextual signals rather than the final ensemble
# prediction because risk should preserve the underlying
# evidence.

df = df.withColumn(
    "evidence_signal",
    (
        F.col("historical_signal")
        +
        F.col("seasonal_signal")
        +
        F.col("rolling_signal")
        +
        F.col("peer_signal")
        +
        F.col("relative_signal")
    )
)


# Window ordered by time for each meter.

from pyspark.sql.window import Window

persistence_window = (
    Window
    .partitionBy("meter_id")
    .orderBy("date")
    .rowsBetween(-4, 0)
)


df = df.withColumn(
    "persistence_count",
    F.sum(
        F.when(
            F.col("evidence_signal") >= 2,
            1
        ).otherwise(0)
    ).over(persistence_window)
)


df = df.withColumn(
    "persistence_component",
    F.least(
        F.col("persistence_count") / 5.0,
        F.lit(1.0)
    )
)


# ============================================================
# 5. ML COMPONENT
# ============================================================

print("\nPreparing ML anomaly component...")


df = df.withColumn(
    "ml_component",
    F.coalesce(
        F.col("if_anomaly_score"),
        F.lit(0.0)
    )
)


# ============================================================
# 6. FINAL RISK SCORE
# ============================================================

print("\nCalculating explainable risk score...")


# Project-defined weights:
#
# Historical deviation       20%
# Seasonal deviation         15%
# Rolling deviation          15%
# Peer deviation             15%
# Persistence                15%
# ML anomaly                 10%
# Anomaly magnitude          10%
#
# Total                     100%

df = df.withColumn(
    "risk_score_raw",

    (
        F.lit(0.20)
        * F.col("historical_component")

        +

        F.lit(0.15)
        * F.col("seasonal_component")

        +

        F.lit(0.15)
        * F.col("rolling_component")

        +

        F.lit(0.15)
        * F.col("peer_component")

        +

        F.lit(0.15)
        * F.col("persistence_component")

        +

        F.lit(0.10)
        * F.col("ml_component")

        +

        F.lit(0.10)
        * F.col("magnitude_component")
    )
)


df = df.withColumn(
    "risk_score",
    F.round(
        F.col("risk_score_raw") * 100,
        2
    )
)


# ============================================================
# 7. RISK LEVEL
# ============================================================

df = df.withColumn(
    "risk_level",

    F.when(
        F.col("risk_score") >= 80,
        "CRITICAL"
    )
    .when(
        F.col("risk_score") >= 60,
        "HIGH"
    )
    .when(
        F.col("risk_score") >= 30,
        "MEDIUM"
    )
    .otherwise(
        "LOW"
    )
)


# ============================================================
# 8. EXPLAINABLE REASONS
# ============================================================

print("\nGenerating risk explanations...")


df = df.withColumn(
    "historical_reason",
    F.when(
        F.col("historical_component") >= 0.75,
        "strong historical deviation"
    ).otherwise(None)
)


df = df.withColumn(
    "seasonal_reason",
    F.when(
        F.col("seasonal_component") >= 0.75,
        "strong seasonal deviation"
    ).otherwise(None)
)


df = df.withColumn(
    "rolling_reason",
    F.when(
        F.col("rolling_component") >= 0.75,
        "strong recent deviation"
    ).otherwise(None)
)


df = df.withColumn(
    "peer_reason",
    F.when(
        F.col("peer_component") >= 0.75,
        "unusual compared with peer meters"
    ).otherwise(None)
)


df = df.withColumn(
    "persistence_reason",
    F.when(
        F.col("persistence_component") >= 0.60,
        "persistent abnormal behavior"
    ).otherwise(None)
)


df = df.withColumn(
    "ml_reason",
    F.when(
        F.col("ml_component") >= 0.70,
        "machine-learning anomaly detected"
    ).otherwise(None)
)


df = df.withColumn(
    "magnitude_reason",
    F.when(
        F.col("magnitude_component") >= 0.75,
        "large consumption deviation"
    ).otherwise(None)
)


# Build a readable explanation.

df = df.withColumn(
    "risk_reason",

    F.concat_ws(
        "; ",

        F.col("historical_reason"),
        F.col("seasonal_reason"),
        F.col("rolling_reason"),
        F.col("peer_reason"),
        F.col("persistence_reason"),
        F.col("ml_reason"),
        F.col("magnitude_reason")
    )
)


# ============================================================
# 9. INVESTIGATION PRIORITY
# ============================================================

df = df.withColumn(
    "investigation_priority",

    F.when(
        F.col("risk_score") >= 80,
        "IMMEDIATE_REVIEW"
    )
    .when(
        F.col("risk_score") >= 60,
        "HIGH_PRIORITY_REVIEW"
    )
    .when(
        F.col("risk_score") >= 30,
        "ROUTINE_REVIEW"
    )
    .otherwise(
        "MONITOR"
    )
)


# ============================================================
# 10. SELECT FINAL READING-LEVEL OUTPUT
# ============================================================

result = df.select(
    "meter_id",
    "date",
    "daily_consumption",
    "ground_truth",
    "anomaly_type",

    "historical_z",
    "seasonal_z",
    "rolling_z",
    "peer_z",

    "relative_deviation",

    "historical_component",
    "seasonal_component",
    "rolling_component",
    "peer_component",
    "persistence_component",
    "ml_component",
    "magnitude_component",

    "persistence_count",

    "risk_score",
    "risk_level",
    "investigation_priority",
    "risk_reason",

    "contextual_score",
    "ensemble_score"
)


# ============================================================
# 11. DISPLAY HIGH-RISK READINGS
# ============================================================

print("\n")
print("=" * 75)
print("HIGH-RISK READINGS")
print("=" * 75)


high_risk = (
    result
    .filter(
        F.col("risk_score") >= 60
    )
    .orderBy(
        F.col("risk_score").desc()
    )
)


high_risk.select(
    "meter_id",
    "date",
    "daily_consumption",
    "risk_score",
    "risk_level",
    "investigation_priority",
    "risk_reason",
    "ground_truth",
    "anomaly_type"
).show(
    30,
    truncate=False
)


# ============================================================
# 12. RISK DISTRIBUTION
# ============================================================

print("\n")
print("=" * 75)
print("RISK DISTRIBUTION")
print("=" * 75)


risk_distribution = (
    result
    .groupBy("risk_level")
    .count()
    .orderBy(
        F.when(
            F.col("risk_level") == "CRITICAL",
            1
        )
        .when(
            F.col("risk_level") == "HIGH",
            2
        )
        .when(
            F.col("risk_level") == "MEDIUM",
            3
        )
        .otherwise(4)
    )
)


risk_distribution.show()


# ============================================================
# 13. GROUND-TRUTH RISK ANALYSIS
# ============================================================

print("\n")
print("=" * 75)
print("GROUND-TRUTH ANOMALY RISK ANALYSIS")
print("=" * 75)


truth_risk = (
    result
    .filter(
        F.col("ground_truth") == True
    )
    .select(
        "meter_id",
        "date",
        "anomaly_type",
        "daily_consumption",
        "risk_score",
        "risk_level",
        "risk_reason"
    )
    .orderBy(
        "meter_id",
        "date"
    )
)


truth_risk.show(
    30,
    truncate=False
)


# ============================================================
# 14. METER-LEVEL SUMMARY
# ============================================================

print("\n")
print("=" * 75)
print("METER-LEVEL RISK SUMMARY")
print("=" * 75)


meter_summary = (
    result
    .groupBy("meter_id")
    .agg(

        F.round(
            F.avg("risk_score"),
            2
        ).alias("average_risk"),

        F.round(
            F.max("risk_score"),
            2
        ).alias("maximum_risk"),

        F.sum(
            F.when(
                F.col("risk_level") == "HIGH",
                1
            ).otherwise(0)
        ).alias("high_risk_days"),

        F.sum(
            F.when(
                F.col("risk_level") == "CRITICAL",
                1
            ).otherwise(0)
        ).alias("critical_risk_days"),

        F.sum(
            F.when(
                F.col("risk_score") >= 60,
                1
            ).otherwise(0)
        ).alias("investigation_days"),

        F.sum(
            F.when(
                F.col("ground_truth") == True,
                1
            ).otherwise(0)
        ).alias("ground_truth_anomalies"),

        F.count("*").alias("total_days")
    )
)


meter_summary = meter_summary.withColumn(
    "overall_risk_level",

    F.when(
        F.col("maximum_risk") >= 80,
        "CRITICAL"
    )
    .when(
        F.col("maximum_risk") >= 60,
        "HIGH"
    )
    .when(
        F.col("average_risk") >= 30,
        "MEDIUM"
    )
    .otherwise(
        "LOW"
    )
)


meter_summary = meter_summary.orderBy(
    F.col("maximum_risk").desc()
)


meter_summary.show(
    30,
    truncate=False
)


# ============================================================
# 15. SAVE SPARK PARQUET
# ============================================================

print("\nSaving reading-level risk results...")


result.write.mode("overwrite").parquet(
    os.path.join(
        OUTPUT_PATH,
        "risk_results.parquet"
    )
)


meter_summary.write.mode("overwrite").parquet(
    os.path.join(
        OUTPUT_PATH,
        "meter_risk_summary.parquet"
    )
)


# ============================================================
# 16. EXPORT SMALL REPORTS
# ============================================================

print("\nExporting summary CSV files...")


risk_distribution_pd = (
    risk_distribution
    .toPandas()
)

risk_distribution_pd.to_csv(
    os.path.join(
        OUTPUT_PATH,
        "risk_distribution.csv"
    ),
    index=False
)


truth_risk_pd = (
    truth_risk
    .toPandas()
)

truth_risk_pd.to_csv(
    os.path.join(
        OUTPUT_PATH,
        "ground_truth_risk_analysis.csv"
    ),
    index=False
)


meter_summary_pd = (
    meter_summary
    .toPandas()
)

meter_summary_pd.to_csv(
    os.path.join(
        OUTPUT_PATH,
        "meter_risk_summary.csv"
    ),
    index=False
)


# ============================================================
# 17. FINAL VERIFICATION
# ============================================================

print("\n")
print("=" * 75)
print("FINAL VERIFICATION")
print("=" * 75)


input_count = df.count()
output_count = result.count()

print(
    f"Input rows              : {input_count:,}"
)

print(
    f"Output rows             : {output_count:,}"
)

print(
    f"Ground-truth anomalies  : "
    f"{result.filter(F.col('ground_truth') == True).count():,}"
)

print(
    f"High-risk readings      : "
    f"{result.filter(F.col('risk_score') >= 60).count():,}"
)

print(
    f"Critical readings       : "
    f"{result.filter(F.col('risk_score') >= 80).count():,}"
)


if input_count == output_count:

    print(
        "Row-count verification PASSED."
    )

else:

    print(
        "WARNING: Row-count mismatch!"
    )


print("\nOutput directory:")
print(OUTPUT_PATH)

print("\nGenerated files:")
print("  risk_results.parquet")
print("  meter_risk_summary.parquet")
print("  risk_distribution.csv")
print("  ground_truth_risk_analysis.csv")
print("  meter_risk_summary.csv")


print("\n")
print("=" * 75)
print("STEP 9 COMPLETED")
print("=" * 75)


spark.stop()