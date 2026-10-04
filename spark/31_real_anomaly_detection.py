from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.stat import Correlation
from pyspark.sql.types import DoubleType

# ============================================================
# GridGuard - Real Data Anomaly Detection
# ============================================================

GOLD_PATH = r"C:\projects\GridGuard\data\parquet\gold"
ANALYSIS_PATH = r"C:\projects\GridGuard\data\parquet\investigation_priority"

spark = (
    SparkSession.builder
    .appName("GridGuard-Real-Anomaly-Detection")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 70)
print("GRIDGUARD - REAL DATA ANOMALY DETECTION")
print("=" * 70)


# ============================================================
# 1. LOAD GOLD DATA
# ============================================================

print("\n[1] Loading Gold analytical dataset...")

df = spark.read.parquet(GOLD_PATH)

print(f"Records: {df.count():,}")
print(f"Meters : {df.select('meter_id').distinct().count():,}")


# ============================================================
# 2. CLEAN ANALYTICAL FEATURES
# ============================================================

print("\n[2] Preparing analytical features...")

feature_columns = [
    "historical_z",
    "rolling_deviation",
    "peer_z",
    "night_ratio",
    "consumption_change",
    "consumption_std"
]

for column in feature_columns:
    df = df.withColumn(
        column,
        F.coalesce(F.col(column).cast(DoubleType()), F.lit(0.0))
    )


# ============================================================
# 3. CAP EXTREME VALUES
# ============================================================
# Prevent one extreme observation from dominating the
# composite score.

df = (
    df
    .withColumn(
        "historical_signal",
        F.least(F.abs(F.col("historical_z")) / 3.0, F.lit(1.0))
    )
    .withColumn(
        "rolling_signal",
        F.least(F.abs(F.col("rolling_deviation")) / 0.50, F.lit(1.0))
    )
    .withColumn(
        "peer_signal",
        F.least(F.abs(F.col("peer_z")) / 3.0, F.lit(1.0))
    )
    .withColumn(
        "night_signal",
        F.least(F.col("night_ratio") / 0.50, F.lit(1.0))
    )
    .withColumn(
        "change_signal",
        F.least(F.abs(F.col("consumption_change")) / 0.50, F.lit(1.0))
    )
)


# ============================================================
# 4. PERSISTENCE ANALYSIS
# ============================================================

print("\n[3] Calculating persistence of unusual behavior...")

persistence_window = (
    Window
    .partitionBy("meter_id")
    .orderBy("date")
    .rowsBetween(-6, 0)
)

df = df.withColumn(
    "abnormal_signal",
    F.when(
        (
            (F.abs(F.col("historical_z")) >= 2.0)
            |
            (F.abs(F.col("rolling_deviation")) >= 0.30)
            |
            (F.abs(F.col("peer_z")) >= 2.0)
        ),
        1
    ).otherwise(0)
)

df = df.withColumn(
    "persistence_count",
    F.sum("abnormal_signal").over(persistence_window)
)

df = df.withColumn(
    "persistence_signal",
    F.least(
        F.col("persistence_count") / 5.0,
        F.lit(1.0)
    )
)


# ============================================================
# 5. CONTEXTUAL SCORE
# ============================================================

print("\n[4] Calculating contextual anomaly score...")

df = df.withColumn(
    "contextual_score",
    (
        F.col("historical_signal") * 0.25
        + F.col("peer_signal") * 0.20
        + F.col("rolling_signal") * 0.20
        + F.col("change_signal") * 0.15
        + F.col("night_signal") * 0.10
        + F.col("persistence_signal") * 0.10
    )
)


# ============================================================
# 6. SIGNAL COUNT
# ============================================================

df = df.withColumn(
    "signal_count",
    (
        F.when(F.col("historical_signal") >= 0.60, 1).otherwise(0)
        + F.when(F.col("peer_signal") >= 0.60, 1).otherwise(0)
        + F.when(F.col("rolling_signal") >= 0.60, 1).otherwise(0)
        + F.when(F.col("change_signal") >= 0.60, 1).otherwise(0)
        + F.when(F.col("night_signal") >= 0.60, 1).otherwise(0)
        + F.when(F.col("persistence_signal") >= 0.60, 1).otherwise(0)
    )
)


# ============================================================
# 7. RULE-BASED ANOMALY FLAG
# ============================================================

df = df.withColumn(
    "contextual_anomaly",
    F.when(
        (
            (F.col("contextual_score") >= 0.65)
            &
            (F.col("signal_count") >= 2)
        ),
        1
    ).otherwise(0)
)


# ============================================================
# 8. RISK / INVESTIGATION PRIORITY
# ============================================================

print("\n[5] Calculating Investigation Priority Score...")

df = df.withColumn(
    "investigation_score",
    F.round(
        F.col("contextual_score") * 100,
        2
    )
)

df = df.withColumn(
    "risk_level",
    F.when(
        F.col("investigation_score") >= 81,
        "HIGH"
    )
    .when(
        F.col("investigation_score") >= 61,
        "MEDIUM"
    )
    .when(
        F.col("investigation_score") >= 31,
        "LOW"
    )
    .otherwise("NORMAL")
)


# ============================================================
# 9. EXPLANATION
# ============================================================

df = df.withColumn(
    "primary_reason",
    F.when(
        F.col("historical_signal") >= 0.80,
        "Strong historical deviation"
    )
    .when(
        F.col("peer_signal") >= 0.80,
        "Strong peer deviation"
    )
    .when(
        F.col("rolling_signal") >= 0.80,
        "Recent consumption shift"
    )
    .when(
        F.col("persistence_signal") >= 0.80,
        "Persistent unusual behavior"
    )
    .when(
        F.col("night_signal") >= 0.80,
        "Unusual night consumption"
    )
    .otherwise(
        "Multiple behavioral deviations"
    )
)


# ============================================================
# 10. SAVE RESULTS
# ============================================================

print("\n[6] Writing investigation-priority dataset...")

result = df.select(
    "meter_id",
    "date",
    "daily_consumption",

    "historical_mean",
    "historical_z",

    "rolling_mean",
    "rolling_deviation",

    "peer_mean",
    "peer_z",

    "night_ratio",
    "consumption_change",
    "consumption_std",

    "persistence_count",
    "signal_count",

    "historical_signal",
    "rolling_signal",
    "peer_signal",
    "night_signal",
    "change_signal",
    "persistence_signal",

    "contextual_score",
    "investigation_score",
    "risk_level",
    "contextual_anomaly",
    "primary_reason"
)

(
    result
    .write
    .mode("overwrite")
    .partitionBy("risk_level")
    .parquet(ANALYSIS_PATH)
)


# ============================================================
# 11. RESULTS
# ============================================================

print("\n" + "=" * 70)
print("REAL-DATA INVESTIGATION RESULTS")
print("=" * 70)

total = result.count()

print(f"Total analytical events : {total:,}")

print("\nRisk distribution:")

(
    result
    .groupBy("risk_level")
    .count()
    .orderBy(
        F.when(F.col("risk_level") == "HIGH", 1)
         .when(F.col("risk_level") == "MEDIUM", 2)
         .when(F.col("risk_level") == "LOW", 3)
         .otherwise(4)
    )
    .show()
)


print("\nTop 20 investigation candidates:")

(
    result
    .filter(F.col("contextual_anomaly") == 1)
    .orderBy(F.desc("investigation_score"))
    .select(
        "meter_id",
        "date",
        "daily_consumption",
        "investigation_score",
        "risk_level",
        "signal_count",
        "primary_reason"
    )
    .show(20, truncate=False)
)


print("\nMost frequent anomaly reasons:")

(
    result
    .filter(F.col("contextual_anomaly") == 1)
    .groupBy("primary_reason")
    .count()
    .orderBy(F.desc("count"))
    .show(truncate=False)
)


# ============================================================
# 12. METER-LEVEL PRIORITY
# ============================================================

print("\n[7] Creating meter-level investigation ranking...")

meter_priority = (
    result
    .groupBy("meter_id")
    .agg(
        F.max("investigation_score").alias("max_score"),
        F.avg("investigation_score").alias("average_score"),
        F.sum("contextual_anomaly").alias("unusual_event_count"),
        F.max("signal_count").alias("max_signal_count"),
        F.max(
            F.when(
                F.col("risk_level") == "HIGH",
                1
            ).otherwise(0)
        ).alias("has_high_risk")
    )
    .withColumn(
        "priority_level",
        F.when(
            F.col("has_high_risk") == 1,
            "HIGH"
        )
        .when(
            F.col("average_score") >= 40,
            "MEDIUM"
        )
        .otherwise("LOW")
    )
    .orderBy(F.desc("max_score"))
)

print("\nTop meters requiring investigation:")

meter_priority.show(20, truncate=False)


# ============================================================
# 13. FINAL SUMMARY
# ============================================================

high = result.filter(
    F.col("risk_level") == "HIGH"
).count()

medium = result.filter(
    F.col("risk_level") == "MEDIUM"
).count()

low = result.filter(
    F.col("risk_level") == "LOW"
).count()

normal = result.filter(
    F.col("risk_level") == "NORMAL"
).count()

print("\n" + "=" * 70)
print("GRIDGUARD REAL-DATA BDA COMPLETE")
print("=" * 70)

print(f"Total events       : {total:,}")
print(f"HIGH priority      : {high:,}")
print(f"MEDIUM priority    : {medium:,}")
print(f"LOW priority       : {low:,}")
print(f"NORMAL             : {normal:,}")

print(f"\nOutput:")
print(ANALYSIS_PATH)

print("\nIMPORTANT:")
print("These results are derived from real smart-meter observations.")
print("No synthetic anomalies were injected in this analysis.")

spark.stop()