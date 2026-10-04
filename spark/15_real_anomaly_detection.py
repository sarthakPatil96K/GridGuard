from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from sklearn.ensemble import IsolationForest
import pandas as pd
import os


# ============================================================
# GRIDGUARD - REAL DATA ANOMALY DETECTION
# ============================================================

INPUT_PATH = "data/real/processed/anomaly_injected_1m"
BASELINE_PATH = "data/real/processed/meter_baselines_1m"
CLEAN_PATH = "data/real/processed/historical_deviations_1m"

OUTPUT_PATH = "data/real/processed/anomaly_detection_results_1m"


# ============================================================
# 1. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("GridGuard-Real-Anomaly-Detection")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("GRIDGUARD - REAL DATA ANOMALY DETECTION")
print("=" * 70)


# ============================================================
# 2. READ EVALUATION DATA
# ============================================================

print("\nReading injected evaluation dataset...")

df = spark.read.parquet(INPUT_PATH)

input_count = df.count()

print(f"Input rows: {input_count:,}")


# ============================================================
# 3. READ METER BASELINES
# ============================================================

print("\nReading clean meter baselines...")

baseline = spark.read.parquet(BASELINE_PATH)

baseline_count = baseline.count()

print(f"Baseline meters: {baseline_count:,}")

print("\nBaseline schema:")
baseline.printSchema()


# ============================================================
# 4. STANDARDIZE BASELINE COLUMN NAMES
# ============================================================

# Actual columns created by Step 4:
#
# historical_avg_consumption
# historical_std_consumption
# historical_median_consumption
# historical_p05_consumption
# historical_p95_consumption
#
# Rename them to simpler names for this pipeline.

baseline = baseline.select(
    "meter_id",
    F.col("historical_avg_consumption").alias("baseline_avg"),
    F.col("historical_std_consumption").alias("baseline_std"),
    F.col("historical_median_consumption").alias("baseline_median"),
    F.col("historical_p05_consumption").alias("baseline_p05"),
    F.col("historical_p95_consumption").alias("baseline_p95")
)


# ============================================================
# 5. JOIN BASELINES TO EVALUATION DATA
# ============================================================

print("\nJoining meter baselines...")

df = df.join(
    baseline,
    on="meter_id",
    how="left"
)


# ============================================================
# 6. CHECK BASELINE JOIN
# ============================================================

missing_baseline = df.filter(
    F.col("baseline_avg").isNull()
).count()

print(f"Meters without baseline: {missing_baseline}")

if missing_baseline > 0:
    print("WARNING: Some records do not have a baseline.")


# ============================================================
# 7. HISTORICAL DEVIATION FEATURES
# ============================================================

print("\nCalculating historical deviation features...")

df = df.withColumn(
    "safe_std",
    F.when(
        F.col("baseline_std").isNull() |
        (F.col("baseline_std") < 0.000001),
        F.lit(0.000001)
    ).otherwise(F.col("baseline_std"))
)


# Z-score
df = df.withColumn(
    "historical_z_score",
    (
        F.col("daily_consumption") -
        F.col("baseline_avg")
    ) / F.col("safe_std")
)


# Relative deviation from historical average
df = df.withColumn(
    "relative_deviation",
    F.when(
        F.col("baseline_avg") > 0,
        (
            F.col("daily_consumption") -
            F.col("baseline_avg")
        ) / F.col("baseline_avg")
    ).otherwise(0.0)
)


# Distance from historical median
df = df.withColumn(
    "median_deviation",
    F.when(
        F.col("baseline_median") > 0,
        (
            F.col("daily_consumption") -
            F.col("baseline_median")
        ) / F.col("baseline_median")
    ).otherwise(0.0)
)


# ============================================================
# 8. PREVIOUS DAY / TREND FEATURES
# ============================================================

print("\nCalculating trend features...")

window_spec = (
    Window
    .partitionBy("meter_id")
    .orderBy("date")
)

df = df.withColumn(
    "previous_consumption",
    F.lag("daily_consumption").over(window_spec)
)


df = df.withColumn(
    "trend_change",
    F.when(
        F.col("previous_consumption").isNotNull() &
        (F.col("previous_consumption") > 0),

        (
            F.col("daily_consumption") -
            F.col("previous_consumption")
        ) / F.col("previous_consumption")

    ).otherwise(0.0)
)


# ============================================================
# 9. STATISTICAL ANOMALY DETECTION
# ============================================================

print("\nRunning statistical detection...")


# Strong historical deviation
df = df.withColumn(
    "z_anomaly",

    F.when(
        F.abs(F.col("historical_z_score")) >= 3,
        1
    ).otherwise(0)
)


# At least 50% change from historical baseline
df = df.withColumn(
    "relative_anomaly",

    F.when(
        F.abs(F.col("relative_deviation")) >= 0.50,
        1
    ).otherwise(0)
)


# At least 50% change from previous day
df = df.withColumn(
    "trend_anomaly",

    F.when(
        F.abs(F.col("trend_change")) >= 0.50,
        1
    ).otherwise(0)
)


# ============================================================
# 10. STATISTICAL SCORE
# ============================================================

df = df.withColumn(
    "statistical_score",

    (
        F.col("z_anomaly") * 0.50 +
        F.col("relative_anomaly") * 0.30 +
        F.col("trend_anomaly") * 0.20
    )
)


df = df.withColumn(
    "statistical_prediction",

    F.when(
        F.col("statistical_score") >= 0.50,
        1
    ).otherwise(0)
)


# ============================================================
# 11. PREPARE CLEAN TRAINING DATA
# ============================================================

print("\nReading clean historical data for Isolation Forest...")

clean_df = spark.read.parquet(CLEAN_PATH)

print(f"Clean historical rows: {clean_df.count():,}")


# The clean historical dataset already contains
# meter-specific historical deviation information.
#
# We use the same meter baselines to construct the
# ML training features.

clean_df = clean_df.join(
    baseline,
    on="meter_id",
    how="left"
)


clean_df = clean_df.withColumn(
    "safe_std",
    F.when(
        F.col("baseline_std").isNull() |
        (F.col("baseline_std") < 0.000001),
        F.lit(0.000001)
    ).otherwise(F.col("baseline_std"))
)


clean_df = clean_df.withColumn(
    "historical_z_score",

    (
        F.col("daily_consumption") -
        F.col("baseline_avg")
    ) / F.col("safe_std")
)


clean_df = clean_df.withColumn(
    "relative_deviation",

    F.when(
        F.col("baseline_avg") > 0,

        (
            F.col("daily_consumption") -
            F.col("baseline_avg")
        ) / F.col("baseline_avg")

    ).otherwise(0.0)
)


clean_df = clean_df.withColumn(
    "previous_consumption",
    F.lag("daily_consumption").over(window_spec)
)


clean_df = clean_df.withColumn(
    "trend_change",

    F.when(
        F.col("previous_consumption").isNotNull() &
        (F.col("previous_consumption") > 0),

        (
            F.col("daily_consumption") -
            F.col("previous_consumption")
        ) / F.col("previous_consumption")

    ).otherwise(0.0)
)


# ============================================================
# 12. CONVERT DEVELOPMENT DATA TO PANDAS
# ============================================================

print("\nPreparing Isolation Forest training data...")

train_pdf = clean_df.select(
    "historical_z_score",
    "relative_deviation",
    "trend_change"
).dropna().toPandas()


eval_pdf = df.select(
    "meter_id",
    "date",
    "daily_consumption",
    "original_daily_consumption",
    "anomaly_type",
    "ground_truth",
    "historical_z_score",
    "relative_deviation",
    "trend_change",
    "statistical_prediction",
    "statistical_score"
).toPandas()


print(f"Training rows: {len(train_pdf):,}")
print(f"Evaluation rows: {len(eval_pdf):,}")


# ============================================================
# 13. PREPARE ML FEATURES
# ============================================================

feature_columns = [
    "historical_z_score",
    "relative_deviation",
    "trend_change"
]


X_train = train_pdf[
    feature_columns
].fillna(0)


X_eval = eval_pdf[
    feature_columns
].fillna(0)


# ============================================================
# 14. ISOLATION FOREST
# ============================================================

print("\nTraining Isolation Forest...")

model = IsolationForest(
    n_estimators=200,
    contamination=0.01,
    random_state=42,
    n_jobs=-1
)

model.fit(X_train)


if_predictions = model.predict(X_eval)


eval_pdf["if_prediction"] = (
    if_predictions == -1
).astype(int)


# ============================================================
# 15. ISOLATION FOREST SCORE
# ============================================================

# Lower decision_function values are more anomalous.

raw_scores = model.decision_function(X_eval)

eval_pdf["if_raw_score"] = raw_scores


# Convert to an anomaly score where higher = more anomalous.

score_min = raw_scores.min()
score_max = raw_scores.max()

if score_max > score_min:

    eval_pdf["if_anomaly_score"] = (
        (score_max - raw_scores) /
        (score_max - score_min)
    )

else:

    eval_pdf["if_anomaly_score"] = 0.0


# ============================================================
# 16. GRIDGUARD ENSEMBLE
# ============================================================

print("\nCalculating GridGuard ensemble...")


eval_pdf["ensemble_score"] = (
    0.5 * eval_pdf["statistical_prediction"] +
    0.5 * eval_pdf["if_prediction"]
)


eval_pdf["ensemble_prediction"] = (
    eval_pdf["ensemble_score"] >= 0.5
).astype(int)


# ============================================================
# 17. EVALUATION FUNCTION
# ============================================================

def evaluate_model(name, prediction_column):

    actual = eval_pdf["ground_truth"].astype(int)

    predicted = eval_pdf[
        prediction_column
    ].astype(int)

    tp = (
        (actual == 1) &
        (predicted == 1)
    ).sum()

    tn = (
        (actual == 0) &
        (predicted == 0)
    ).sum()

    fp = (
        (actual == 0) &
        (predicted == 1)
    ).sum()

    fn = (
        (actual == 1) &
        (predicted == 0)
    ).sum()

    precision = (
        tp / (tp + fp)
        if (tp + fp) > 0
        else 0
    )

    recall = (
        tp / (tp + fn)
        if (tp + fn) > 0
        else 0
    )

    f1 = (
        2 * precision * recall /
        (precision + recall)
        if (precision + recall) > 0
        else 0
    )

    accuracy = (
        (tp + tn) /
        (tp + tn + fp + fn)
    )

    print("\n" + name)
    print("-" * 50)

    print(f"TP        : {tp}")
    print(f"TN        : {tn}")
    print(f"FP        : {fp}")
    print(f"FN        : {fn}")

    print(f"Precision : {precision:.4f}")
    print(f"Recall    : {recall:.4f}")
    print(f"F1 Score  : {f1:.4f}")
    print(f"Accuracy  : {accuracy:.4f}")

    return {
        "model": name,
        "TP": tp,
        "TN": tn,
        "FP": fp,
        "FN": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": accuracy
    }


# ============================================================
# 18. MODEL COMPARISON
# ============================================================

print("\n")
print("=" * 70)
print("MODEL EVALUATION")
print("=" * 70)


results = []

results.append(
    evaluate_model(
        "Statistical Detection",
        "statistical_prediction"
    )
)

results.append(
    evaluate_model(
        "Isolation Forest",
        "if_prediction"
    )
)

results.append(
    evaluate_model(
        "GridGuard Ensemble",
        "ensemble_prediction"
    )
)


# ============================================================
# 19. DETECTED ANOMALIES
# ============================================================

print("\n")
print("=" * 70)
print("GRIDGUARD DETECTED ANOMALIES")
print("=" * 70)


detected = eval_pdf[
    eval_pdf["ensemble_prediction"] == 1
].sort_values(
    ["meter_id", "date"]
)


if len(detected) > 0:

    display_columns = [
        "meter_id",
        "date",
        "daily_consumption",
        "original_daily_consumption",
        "anomaly_type",
        "ground_truth",
        "historical_z_score",
        "relative_deviation",
        "trend_change",
        "statistical_prediction",
        "if_prediction",
        "ensemble_prediction"
    ]

    print(
        detected[
            display_columns
        ].to_string(index=False)
    )

else:

    print("No anomalies detected.")


# ============================================================
# 20. ANOMALY-TYPE EVALUATION
# ============================================================

print("\n")
print("=" * 70)
print("ANOMALY TYPE RESULTS")
print("=" * 70)


anomaly_types = (
    eval_pdf[
        eval_pdf["ground_truth"] == True
    ]
    .groupby("anomaly_type")
    .agg(
        total=("ground_truth", "count"),
        detected=("ensemble_prediction", "sum")
    )
    .reset_index()
)


anomaly_types["detection_rate"] = (
    anomaly_types["detected"] /
    anomaly_types["total"]
)


print(
    anomaly_types.to_string(index=False)
)


# ============================================================
# 21. SAVE RESULTS
# ============================================================

print("\nSaving detection results...")

os.makedirs(
    OUTPUT_PATH,
    exist_ok=True
)


result_columns = [
    "meter_id",
    "date",
    "daily_consumption",
    "original_daily_consumption",
    "anomaly_type",
    "ground_truth",
    "historical_z_score",
    "relative_deviation",
    "trend_change",
    "statistical_prediction",
    "statistical_score",
    "if_prediction",
    "if_raw_score",
    "if_anomaly_score",
    "ensemble_score",
    "ensemble_prediction"
]


eval_pdf[
    result_columns
].to_parquet(
    OUTPUT_PATH + "/detection_results.parquet",
    index=False
)


pd.DataFrame(results).to_csv(
    OUTPUT_PATH + "/model_comparison.csv",
    index=False
)


anomaly_types.to_csv(
    OUTPUT_PATH + "/anomaly_type_results.csv",
    index=False
)


# ============================================================
# 22. FINAL VERIFICATION
# ============================================================

saved = pd.read_parquet(
    OUTPUT_PATH + "/detection_results.parquet"
)


print("\n")
print("=" * 70)
print("FINAL VERIFICATION")
print("=" * 70)

print(f"Input rows       : {input_count:,}")
print(f"Output rows      : {len(saved):,}")
print(f"Ground truth     : {int(eval_pdf['ground_truth'].sum())}")
print(
    f"Predicted anomaly: "
    f"{int(eval_pdf['ensemble_prediction'].sum())}"
)


if input_count == len(saved):

    print("Row-count verification PASSED.")

else:

    print("WARNING: Row-count mismatch!")


print("\nOutput directory:")
print(OUTPUT_PATH)

print("\nGenerated files:")
print("  detection_results.parquet")
print("  model_comparison.csv")
print("  anomaly_type_results.csv")


print("\n")
print("=" * 70)
print("STEP 6 COMPLETED")
print("=" * 70)


spark.stop()