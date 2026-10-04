import os
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


# ============================================================
# CONFIGURATION
# ============================================================

GOLD_PATH = r"C:\projects\GridGuard\data\parquet\gold"
OUTPUT_PATH = r"C:\projects\GridGuard\data\parquet\isolation_forest"

os.makedirs(OUTPUT_PATH, exist_ok=True)


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("GridGuard-Isolation-Forest")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 75)
print("GRIDGUARD - ISOLATION FOREST ANOMALY DETECTION")
print("=" * 75)


# ============================================================
# 1. LOAD GOLD DATA
# ============================================================

print("\n[1] Loading Gold dataset...")

df = spark.read.parquet(GOLD_PATH)

print(f"Records : {df.count():,}")
print(f"Meters  : {df.select('meter_id').distinct().count():,}")


# ============================================================
# 2. CREATE LEAKAGE-SAFE HISTORICAL BASELINE
# ============================================================

print("\n[2] Creating leakage-safe historical baseline...")

meter_window = (
    Window
    .partitionBy("meter_id")
    .orderBy("date")
    .rowsBetween(Window.unboundedPreceding, -1)
)

df = (
    df
    .withColumn(
        "prior_mean",
        F.avg("daily_consumption").over(meter_window)
    )
    .withColumn(
        "prior_std",
        F.stddev("daily_consumption").over(meter_window)
    )
)

df = df.withColumn(
    "prior_historical_z",
    F.when(
        (F.col("prior_std") > 0) &
        F.col("prior_mean").isNotNull(),
        (
            F.col("daily_consumption") -
            F.col("prior_mean")
        ) / F.col("prior_std")
    ).otherwise(0.0)
)


# ============================================================
# 3. PRIOR 7-DAY BASELINE
# ============================================================

print("\n[3] Creating prior 7-day rolling baseline...")

rolling_window = (
    Window
    .partitionBy("meter_id")
    .orderBy("date")
    .rowsBetween(-7, -1)
)

df = (
    df
    .withColumn(
        "prior_7d_mean",
        F.avg("daily_consumption").over(rolling_window)
    )
    .withColumn(
        "prior_7d_std",
        F.stddev("daily_consumption").over(rolling_window)
    )
)

df = df.withColumn(
    "prior_rolling_deviation",
    F.when(
        F.col("prior_7d_mean") > 0,
        (
            F.col("daily_consumption") -
            F.col("prior_7d_mean")
        ) / F.col("prior_7d_mean")
    ).otherwise(0.0)
)


# ============================================================
# 4. DAILY CHANGE
# ============================================================

print("\n[4] Calculating consumption change...")

trend_window = (
    Window
    .partitionBy("meter_id")
    .orderBy("date")
)

df = (
    df
    .withColumn(
        "previous_consumption",
        F.lag("daily_consumption").over(trend_window)
    )
    .withColumn(
        "daily_change",
        F.when(
            F.col("previous_consumption") > 0,
            (
                F.col("daily_consumption") -
                F.col("previous_consumption")
            ) / F.col("previous_consumption")
        ).otherwise(0.0)
    )
)


# ============================================================
# 5. PEER BASELINE
# ============================================================

print("\n[5] Creating daily peer baseline...")

peer_window = Window.partitionBy("date")

df = (
    df
    .withColumn(
        "current_peer_mean",
        F.avg("daily_consumption").over(peer_window)
    )
    .withColumn(
        "current_peer_std",
        F.stddev("daily_consumption").over(peer_window)
    )
)

df = df.withColumn(
    "current_peer_z",
    F.when(
        F.col("current_peer_std") > 0,
        (
            F.col("daily_consumption") -
            F.col("current_peer_mean")
        ) / F.col("current_peer_std")
    ).otherwise(0.0)
)


# ============================================================
# 6. CLEAN ML FEATURES
# ============================================================

print("\n[6] Preparing Isolation Forest features...")

ml_columns = [
    "daily_consumption",
    "prior_historical_z",
    "prior_rolling_deviation",
    "current_peer_z",
    "night_ratio",
    "daily_change",
    "consumption_std"
]

for column in ml_columns:
    df = df.withColumn(
        column,
        F.coalesce(
            F.col(column).cast("double"),
            F.lit(0.0)
        )
    )


# ============================================================
# 7. COLLECT COMPACT GOLD DATA
# ============================================================

print("\n[7] Collecting compact analytical dataset...")

pdf = (
    df
    .select(
        "meter_id",
        "date",
        *ml_columns
    )
    .orderBy("date", "meter_id")
    .toPandas()
)

print(f"Rows sent to Isolation Forest: {len(pdf):,}")


# ============================================================
# 8. STANDARDIZE FEATURES
# ============================================================

print("\n[8] Standardizing ML features...")

X = pdf[ml_columns].replace(
    [np.inf, -np.inf],
    np.nan
).fillna(0.0)

scaler = StandardScaler()

X_scaled = scaler.fit_transform(X)


# ============================================================
# 9. ISOLATION FOREST
# ============================================================

print("\n[9] Training Isolation Forest...")

model = IsolationForest(
    n_estimators=200,
    contamination="auto",
    random_state=42,
    n_jobs=-1
)

model.fit(X_scaled)


# ============================================================
# 10. MODEL OUTPUT
# ============================================================

print("\n[10] Generating Isolation Forest scores...")

raw_prediction = model.predict(X_scaled)

raw_score = model.decision_function(X_scaled)


# Isolation Forest:
#
# prediction:
#   -1 = anomaly
#    1 = normal
#
# decision_function:
#   lower = more anomalous
#
# Convert it to a convenient 0-100 anomaly score.

min_score = raw_score.min()
max_score = raw_score.max()

if max_score > min_score:
    if_score = (
        (max_score - raw_score) /
        (max_score - min_score)
    ) * 100
else:
    if_score = np.zeros(len(raw_score))


pdf["if_raw_score"] = raw_score
pdf["if_score"] = np.round(if_score, 2)
pdf["if_prediction"] = np.where(
    raw_prediction == -1,
    1,
    0
)


# ============================================================
# 11. ISOLATION FOREST THRESHOLD
# ============================================================

# Use the 95th percentile of the anomaly score
# as the ML outlier threshold.

if_threshold = np.percentile(
    pdf["if_score"],
    95
)

pdf["if_anomaly"] = (
    pdf["if_score"] >= if_threshold
).astype(int)


print(
    f"\nIsolation Forest threshold: "
    f"{if_threshold:.2f}"
)


# ============================================================
# 12. CONVERT BACK TO SPARK
# ============================================================

print("\n[11] Returning ML results to Spark...")

ml_pdf = pdf[
    [
        "meter_id",
        "date",
        "if_raw_score",
        "if_score",
        "if_prediction",
        "if_anomaly"
    ]
]

ml_df = spark.createDataFrame(ml_pdf)


# ============================================================
# 13. JOIN WITH ORIGINAL DATA
# ============================================================

result = df.join(
    ml_df,
    on=["meter_id", "date"],
    how="left"
)


# ============================================================
# 14. CONTEXTUAL SCORE
# ============================================================

print("\n[12] Recalculating contextual evidence...")

result = (
    result
    .withColumn(
        "historical_signal",
        F.least(
            F.abs(F.col("prior_historical_z")) / 3.0,
            F.lit(1.0)
        )
    )
    .withColumn(
        "rolling_signal",
        F.least(
            F.abs(F.col("prior_rolling_deviation")) / 0.50,
            F.lit(1.0)
        )
    )
    .withColumn(
        "peer_signal",
        F.least(
            F.abs(F.col("current_peer_z")) / 3.0,
            F.lit(1.0)
        )
    )
    .withColumn(
        "change_signal",
        F.least(
            F.abs(F.col("daily_change")) / 0.50,
            F.lit(1.0)
        )
    )
    .withColumn(
        "night_signal",
        F.least(
            F.col("night_ratio") / 0.50,
            F.lit(1.0)
        )
    )
)


# ============================================================
# 15. SIGNAL COUNT
# ============================================================

result = result.withColumn(
    "signal_count",
    (
        F.when(F.col("historical_signal") >= 0.60, 1).otherwise(0)
        +
        F.when(F.col("rolling_signal") >= 0.60, 1).otherwise(0)
        +
        F.when(F.col("peer_signal") >= 0.60, 1).otherwise(0)
        +
        F.when(F.col("change_signal") >= 0.60, 1).otherwise(0)
        +
        F.when(F.col("night_signal") >= 0.60, 1).otherwise(0)
    )
)


# ============================================================
# 16. CONTEXTUAL ANOMALY
# ============================================================

result = result.withColumn(
    "contextual_score",
    F.round(
        (
            F.col("historical_signal") * 0.30
            +
            F.col("peer_signal") * 0.20
            +
            F.col("rolling_signal") * 0.20
            +
            F.col("change_signal") * 0.20
            +
            F.col("night_signal") * 0.10
        ),
        4
    )
)

result = result.withColumn(
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
# 17. GRIDGUARD ENSEMBLE
# ============================================================

print("\n[13] Building GridGuard ensemble score...")

result = result.withColumn(
    "if_normalized",
    F.col("if_score") / 100.0
)

result = result.withColumn(
    "ensemble_score",
    F.round(
        (
            F.col("contextual_score") * 0.70
            +
            F.col("if_normalized") * 0.30
        ),
        4
    )
)


# ============================================================
# 18. ENSEMBLE ANOMALY
# ============================================================

result = result.withColumn(
    "ensemble_anomaly",
    F.when(
        (
            (F.col("ensemble_score") >= 0.65)
            &
            (
                (F.col("contextual_anomaly") == 1)
                |
                (F.col("if_anomaly") == 1)
            )
        ),
        1
    ).otherwise(0)
)


# ============================================================
# 19. INVESTIGATION SCORE
# ============================================================

result = result.withColumn(
    "investigation_score",
    F.round(
        F.col("ensemble_score") * 100,
        2
    )
)

result = result.withColumn(
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
# 20. EXPLANATION
# ============================================================

result = result.withColumn(
    "primary_reason",
    F.when(
        (F.col("contextual_anomaly") == 1) &
        (F.col("if_anomaly") == 1),
        "Multiple signals + ML outlier"
    )
    .when(
        F.col("contextual_anomaly") == 1,
        "Behavioral deviation"
    )
    .when(
        F.col("if_anomaly") == 1,
        "Multivariate ML outlier"
    )
    .otherwise(
        "Normal behavior"
    )
)


# ============================================================
# 21. SAVE FINAL ANALYTICAL DATASET
# ============================================================

print("\n[14] Saving GridGuard ensemble dataset...")

final_result = result.select(
    "meter_id",
    "date",
    "daily_consumption",

    "prior_historical_z",
    "prior_rolling_deviation",
    "current_peer_z",

    "night_ratio",
    "daily_change",
    "consumption_std",

    "historical_signal",
    "rolling_signal",
    "peer_signal",
    "change_signal",
    "night_signal",

    "signal_count",

    "contextual_score",
    "contextual_anomaly",

    "if_raw_score",
    "if_score",
    "if_prediction",
    "if_anomaly",

    "ensemble_score",
    "ensemble_anomaly",

    "investigation_score",
    "risk_level",
    "primary_reason"
)

(
    final_result
    .write
    .mode("overwrite")
    .partitionBy("risk_level")
    .parquet(OUTPUT_PATH)
)


# ============================================================
# 22. MODEL COMPARISON
# ============================================================

print("\n" + "=" * 75)
print("MODEL COMPARISON")
print("=" * 75)

contextual_count = (
    final_result
    .filter(F.col("contextual_anomaly") == 1)
    .count()
)

if_count = (
    final_result
    .filter(F.col("if_anomaly") == 1)
    .count()
)

ensemble_count = (
    final_result
    .filter(F.col("ensemble_anomaly") == 1)
    .count()
)

print(
    f"\nContextual anomalies : {contextual_count:,}"
)

print(
    f"Isolation Forest     : {if_count:,}"
)

print(
    f"Ensemble anomalies   : {ensemble_count:,}"
)


# ============================================================
# 23. OVERLAP ANALYSIS
# ============================================================

print("\nModel overlap:")

(
    final_result
    .groupBy(
        "contextual_anomaly",
        "if_anomaly"
    )
    .count()
    .orderBy(
        "contextual_anomaly",
        "if_anomaly"
    )
    .show()
)


# ============================================================
# 24. RISK DISTRIBUTION
# ============================================================

print("\nFinal risk distribution:")

(
    final_result
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


# ============================================================
# 25. TOP INVESTIGATION CANDIDATES
# ============================================================

print("\nTop 20 GridGuard investigation candidates:")

(
    final_result
    .filter(F.col("ensemble_anomaly") == 1)
    .orderBy(F.desc("investigation_score"))
    .select(
        "meter_id",
        "date",
        "daily_consumption",
        "contextual_score",
        "if_score",
        "ensemble_score",
        "investigation_score",
        "risk_level",
        "primary_reason"
    )
    .show(20, truncate=False)
)


# ============================================================
# 26. METER-LEVEL RANKING
# ============================================================

print("\nTop meters by investigation priority:")

(
    final_result
    .groupBy("meter_id")
    .agg(
        F.max("investigation_score")
        .alias("max_score"),

        F.avg("investigation_score")
        .alias("average_score"),

        F.sum("ensemble_anomaly")
        .alias("anomaly_events"),

        F.max("if_score")
        .alias("max_if_score")
    )
    .orderBy(F.desc("max_score"))
    .show(20, truncate=False)
)


# ============================================================
# 27. FINAL SUMMARY
# ============================================================

print("\n" + "=" * 75)
print("GRIDGUARD ISOLATION FOREST ANALYSIS COMPLETE")
print("=" * 75)

print(f"Total events        : {final_result.count():,}")
print(f"Contextual events   : {contextual_count:,}")
print(f"IF events           : {if_count:,}")
print(f"Ensemble events     : {ensemble_count:,}")

print("\nOutput:")
print(OUTPUT_PATH)

print("\nNOTE:")
print("Isolation Forest is an unsupervised outlier detector.")
print("Risk score represents investigation priority,")
print("not probability or confirmation of electricity theft.")

spark.stop()