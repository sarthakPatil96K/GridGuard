from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from sklearn.ensemble import IsolationForest
import pandas as pd
import numpy as np
import os


# ============================================================
# CONFIGURATION
# ============================================================

INPUT_PATH = "data/real/processed/anomaly_injected_1m"
BASELINE_PATH = "data/real/processed/meter_baselines_1m"
CLEAN_PATH = "data/real/processed/historical_deviations_1m"

OUTPUT_PATH = "data/real/processed/contextual_detection_results_1m"

os.makedirs(OUTPUT_PATH, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("GridGuard_Contextual_Anomaly_Detection")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 75)
print("GRIDGUARD - CONTEXTUAL ANOMALY DETECTION")
print("=" * 75)


# ============================================================
# 1. READ EVALUATION DATA
# ============================================================

print("\nReading injected evaluation dataset...")

evaluation = (
    spark.read
    .parquet(INPUT_PATH)
    .withColumn("date", F.to_date("date"))
)

evaluation = evaluation.filter(
    F.col("meter_id").isNotNull()
    & F.col("date").isNotNull()
    & F.col("daily_consumption").isNotNull()
)

evaluation_count = evaluation.count()

print(f"Evaluation rows: {evaluation_count:,}")


# ============================================================
# 2. READ CLEAN HISTORICAL DATA
# ============================================================

print("\nReading clean historical data...")

clean = (
    spark.read
    .parquet(CLEAN_PATH)
    .withColumn("date", F.to_date("date"))
)

clean = clean.filter(
    F.col("meter_id").isNotNull()
    & F.col("date").isNotNull()
    & F.col("daily_consumption").isNotNull()
)

clean_count = clean.count()

print(f"Clean historical rows: {clean_count:,}")


# ============================================================
# 3. READ METER BASELINES
# ============================================================

print("\nReading meter baselines...")

baseline = spark.read.parquet(BASELINE_PATH)

baseline = baseline.select(
    "meter_id",
    F.col("historical_avg_consumption").alias("baseline_avg"),
    F.col("historical_std_consumption").alias("baseline_std"),
    F.col("historical_median_consumption").alias("baseline_median"),
    F.col("historical_p05_consumption").alias("baseline_p05"),
    F.col("historical_p95_consumption").alias("baseline_p95")
)

print(f"Baseline meters: {baseline.count():,}")


# ============================================================
# 4. CREATE PEER GROUPS
# ============================================================

print("\nCreating consumption-based peer groups...")

# We do not have reliable zone/consumer-type information
# in this dataset, so peer groups are initially based on
# historical average consumption.

baseline = baseline.withColumn(
    "peer_group",
    F.when(F.col("baseline_avg") < 2, "P1_LOW")
    .when(F.col("baseline_avg") < 5, "P2_LOW_MEDIUM")
    .when(F.col("baseline_avg") < 10, "P3_MEDIUM")
    .when(F.col("baseline_avg") < 20, "P4_MEDIUM_HIGH")
    .when(F.col("baseline_avg") < 40, "P5_HIGH")
    .otherwise("P6_VERY_HIGH")
)

peer_group_counts = (
    baseline
    .groupBy("peer_group")
    .count()
    .orderBy("peer_group")
)

print("\nPeer group distribution:")
peer_group_counts.show(truncate=False)


# ============================================================
# 5. ADD BASELINE FEATURES
# ============================================================

print("\nJoining meter baselines...")

evaluation = evaluation.join(
    baseline,
    on="meter_id",
    how="left"
)

missing_baseline = evaluation.filter(
    F.col("baseline_avg").isNull()
).count()

print(f"Records without baseline: {missing_baseline}")


# ============================================================
# 6. BASIC HISTORICAL FEATURES
# ============================================================

print("\nCalculating historical deviation...")

evaluation = (
    evaluation
    .withColumn(
        "historical_z",
        F.when(
            F.col("baseline_std") > 0,
            (
                F.col("daily_consumption")
                - F.col("baseline_avg")
            ) / F.col("baseline_std")
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "relative_deviation",
        F.when(
            F.abs(F.col("baseline_avg")) > 1e-8,
            (
                F.col("daily_consumption")
                - F.col("baseline_avg")
            ) / F.col("baseline_avg")
        ).otherwise(F.lit(0.0))
    )
)


# ============================================================
# 7. SEASONAL BASELINE
# ============================================================

print("\nCalculating seasonal baselines...")

# Month and day-of-week
evaluation = (
    evaluation
    .withColumn("month", F.month("date"))
    .withColumn("day_of_week", F.dayofweek("date"))
)

clean_seasonal = (
    clean
    .withColumn("month", F.month("date"))
    .withColumn("day_of_week", F.dayofweek("date"))
)

seasonal_baseline = (
    clean_seasonal
    .groupBy(
        "meter_id",
        "month",
        "day_of_week"
    )
    .agg(
        F.avg("daily_consumption").alias("seasonal_avg"),
        F.stddev("daily_consumption").alias("seasonal_std"),
        F.expr(
            "percentile_approx(daily_consumption, 0.5)"
        ).alias("seasonal_median"),
        F.expr(
            "percentile_approx(daily_consumption, 0.05)"
        ).alias("seasonal_p05"),
        F.expr(
            "percentile_approx(daily_consumption, 0.95)"
        ).alias("seasonal_p95")
    )
)

evaluation = evaluation.join(
    seasonal_baseline,
    on=["meter_id", "month", "day_of_week"],
    how="left"
)

evaluation = evaluation.withColumn(
    "seasonal_z",
    F.when(
        F.col("seasonal_std") > 0,
        (
            F.col("daily_consumption")
            - F.col("seasonal_avg")
        ) / F.col("seasonal_std")
    ).otherwise(F.lit(0.0))
)

evaluation = evaluation.withColumn(
    "seasonal_relative_deviation",
    F.when(
        F.abs(F.col("seasonal_avg")) > 1e-8,
        (
            F.col("daily_consumption")
            - F.col("seasonal_avg")
        ) / F.col("seasonal_avg")
    ).otherwise(F.lit(0.0))
)


# ============================================================
# 8. ROLLING HISTORICAL BASELINE
# ============================================================

print("\nCalculating rolling historical baseline...")

# IMPORTANT:
# Use CLEAN historical data to calculate the rolling baseline.
# This prevents injected anomalies from contaminating the baseline.

rolling_window = (
    Window
    .partitionBy("meter_id")
    .orderBy("date")
    .rowsBetween(-30, -1)
)

clean_rolling = (
    clean
    .withColumn(
        "rolling_avg",
        F.avg("daily_consumption").over(rolling_window)
    )
    .withColumn(
        "rolling_std",
        F.stddev("daily_consumption").over(rolling_window)
    )
)

rolling_features = clean_rolling.select(
    "meter_id",
    "date",
    "rolling_avg",
    "rolling_std"
)

evaluation = evaluation.join(
    rolling_features,
    on=["meter_id", "date"],
    how="left"
)

evaluation = evaluation.withColumn(
    "rolling_z",
    F.when(
        F.col("rolling_std") > 0,
        (
            F.col("daily_consumption")
            - F.col("rolling_avg")
        ) / F.col("rolling_std")
    ).otherwise(F.lit(0.0))
)


# ============================================================
# 9. PEER COMPARISON
# ============================================================

print("\nCalculating peer-group comparison...")

evaluation = evaluation.join(
    baseline.select(
        "meter_id",
        "peer_group"
    ),
    on="meter_id",
    how="left"
)

peer_daily = (
    clean
    .join(
        baseline.select(
            "meter_id",
            "peer_group"
        ),
        on="meter_id",
        how="left"
    )
    .groupBy(
        "date",
        "peer_group"
    )
    .agg(
        F.expr(
            "percentile_approx(daily_consumption, 0.5)"
        ).alias("peer_median"),
        F.avg("daily_consumption").alias("peer_avg"),
        F.stddev("daily_consumption").alias("peer_std"),
        F.countDistinct("meter_id").alias("peer_count")
    )
)

evaluation = evaluation.join(
    peer_daily,
    on=["date", "peer_group"],
    how="left"
)

evaluation = evaluation.withColumn(
    "peer_deviation",
    F.when(
        F.abs(F.col("peer_median")) > 1e-8,
        (
            F.col("daily_consumption")
            - F.col("peer_median")
        ) / F.col("peer_median")
    ).otherwise(F.lit(0.0))
)

evaluation = evaluation.withColumn(
    "peer_z",
    F.when(
        F.col("peer_std") > 0,
        (
            F.col("daily_consumption")
            - F.col("peer_avg")
        ) / F.col("peer_std")
    ).otherwise(F.lit(0.0))
)


# ============================================================
# 10. CONTEXTUAL SIGNALS
# ============================================================

print("\nGenerating contextual signals...")

evaluation = (
    evaluation

    # Historical signal
    .withColumn(
        "historical_signal",
        F.when(
            F.abs(F.col("historical_z")) >= 3,
            1
        ).otherwise(0)
    )

    # Seasonal signal
    .withColumn(
        "seasonal_signal",
        F.when(
            F.abs(F.col("seasonal_z")) >= 3,
            1
        ).otherwise(0)
    )

    # Rolling signal
    .withColumn(
        "rolling_signal",
        F.when(
            F.abs(F.col("rolling_z")) >= 3,
            1
        ).otherwise(0)
    )

    # Peer signal
    .withColumn(
        "peer_signal",
        F.when(
            F.abs(F.col("peer_z")) >= 3,
            1
        ).otherwise(0)
    )

    # Large meter-specific deviation
    .withColumn(
        "relative_signal",
        F.when(
            F.abs(F.col("relative_deviation")) >= 1.0,
            1
        ).otherwise(0)
    )
)


# ============================================================
# 11. CONTEXTUAL EVIDENCE SCORE
# ============================================================

print("\nBuilding contextual evidence score...")

evaluation = evaluation.withColumn(
    "contextual_score",

    (
        F.lit(0.25) * F.col("historical_signal")
        +
        F.lit(0.20) * F.col("seasonal_signal")
        +
        F.lit(0.20) * F.col("rolling_signal")
        +
        F.lit(0.20) * F.col("peer_signal")
        +
        F.lit(0.15) * F.col("relative_signal")
    )
)


# ============================================================
# 12. CONTEXTUAL ANOMALY DECISION
# ============================================================

# Require evidence from at least TWO independent contextual
# signals.

evaluation = evaluation.withColumn(
    "contextual_signal_count",

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

evaluation = evaluation.withColumn(
    "contextual_prediction",
    F.when(
        F.col("contextual_signal_count") >= 2,
        1
    ).otherwise(0)
)


# ============================================================
# 13. ISOLATION FOREST
# ============================================================

print("\nPreparing Isolation Forest dataset...")

ml_columns = [
    "daily_consumption",
    "historical_z",
    "relative_deviation",
    "seasonal_z",
    "seasonal_relative_deviation",
    "rolling_z",
    "peer_z",
    "peer_deviation"
]

ml_df = evaluation.select(
    "meter_id",
    "date",
    "ground_truth",
    "anomaly_type",
    *ml_columns
)

ml_pd = ml_df.toPandas()

# Replace infinite values
for col in ml_columns:
    ml_pd[col] = pd.to_numeric(
        ml_pd[col],
        errors="coerce"
    )

ml_pd[ml_columns] = (
    ml_pd[ml_columns]
    .replace([np.inf, -np.inf], np.nan)
    .fillna(0)
)

print(f"ML evaluation rows: {len(ml_pd):,}")


# ============================================================
# 14. TRAIN ISOLATION FOREST
# ============================================================

print("\nTraining contextual Isolation Forest...")

# Lower contamination than previous experiment.
# The real dataset contains relatively few unusual observations.

model = IsolationForest(
    n_estimators=250,
    contamination=0.005,
    random_state=42,
    n_jobs=-1
)

X = ml_pd[ml_columns]

model.fit(X)

ml_pd["if_prediction"] = (
    model.predict(X) == -1
).astype(int)

decision = model.decision_function(X)

# Convert decision function to anomaly score.
# Lower decision = more anomalous.

score_min = decision.min()
score_max = decision.max()

if score_max > score_min:
    ml_pd["if_anomaly_score"] = (
        (score_max - decision)
        / (score_max - score_min)
    )
else:
    ml_pd["if_anomaly_score"] = 0.0


# ============================================================
# 15. GRIDGUARD CONTEXTUAL ENSEMBLE
# ============================================================

print("\nBuilding GridGuard contextual ensemble...")

result_pd = ml_pd.copy()

result_pd["contextual_score"] = (
    evaluation
    .select("contextual_score")
    .toPandas()["contextual_score"]
    .fillna(0)
    .astype(float)
)

result_pd["contextual_signal_count"] = (
    evaluation
    .select("contextual_signal_count")
    .toPandas()["contextual_signal_count"]
    .fillna(0)
    .astype(int)
)

result_pd["contextual_prediction"] = (
    evaluation
    .select("contextual_prediction")
    .toPandas()["contextual_prediction"]
    .fillna(0)
    .astype(int)
)

result_pd["historical_signal"] = (
    evaluation
    .select("historical_signal")
    .toPandas()["historical_signal"]
)

result_pd["seasonal_signal"] = (
    evaluation
    .select("seasonal_signal")
    .toPandas()["seasonal_signal"]
)

result_pd["rolling_signal"] = (
    evaluation
    .select("rolling_signal")
    .toPandas()["rolling_signal"]
)

result_pd["peer_signal"] = (
    evaluation
    .select("peer_signal")
    .toPandas()["peer_signal"]
)

result_pd["relative_signal"] = (
    evaluation
    .select("relative_signal")
    .toPandas()["relative_signal"]
)


# Contextual ensemble:
#
# 70% contextual evidence
# 30% ML evidence

result_pd["ensemble_score"] = (
    0.70 * result_pd["contextual_score"]
    +
    0.30 * result_pd["if_anomaly_score"]
)

result_pd["ensemble_prediction"] = (
    result_pd["ensemble_score"] >= 0.60
).astype(int)


# ============================================================
# 16. EVALUATION FUNCTION
# ============================================================

def evaluate_model(name, actual, predicted):

    tp = int(((actual == 1) & (predicted == 1)).sum())
    tn = int(((actual == 0) & (predicted == 0)).sum())
    fp = int(((actual == 0) & (predicted == 1)).sum())
    fn = int(((actual == 1) & (predicted == 0)).sum())

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
        2 * precision * recall / (precision + recall)
        if (precision + recall) > 0
        else 0
    )

    accuracy = (
        (tp + tn)
        / (tp + tn + fp + fn)
    )

    print(f"\n{name}")
    print("-" * 55)
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
# 17. MODEL EVALUATION
# ============================================================

print("\n")
print("=" * 75)
print("MODEL EVALUATION")
print("=" * 75)

actual = result_pd["ground_truth"].astype(int)

metrics = []

metrics.append(
    evaluate_model(
        "Contextual Statistical Detector",
        actual,
        result_pd["contextual_prediction"]
    )
)

metrics.append(
    evaluate_model(
        "Isolation Forest",
        actual,
        result_pd["if_prediction"]
    )
)

metrics.append(
    evaluate_model(
        "GridGuard Contextual Ensemble",
        actual,
        result_pd["ensemble_prediction"]
    )
)


# ============================================================
# 18. ANOMALY TYPE ANALYSIS
# ============================================================

print("\n")
print("=" * 75)
print("ANOMALY TYPE DETECTION")
print("=" * 75)

ground_truth = result_pd[
    result_pd["ground_truth"] == 1
].copy()

type_results = []

for anomaly_type, group in ground_truth.groupby("anomaly_type"):

    total = len(group)

    detected = int(
        group["ensemble_prediction"].sum()
    )

    rate = (
        detected / total
        if total > 0
        else 0
    )

    type_results.append({
        "anomaly_type": anomaly_type,
        "total": total,
        "detected": detected,
        "detection_rate": rate
    })

type_df = pd.DataFrame(type_results)

print(
    type_df.to_string(index=False)
)


# ============================================================
# 19. FALSE POSITIVE ANALYSIS
# ============================================================

print("\n")
print("=" * 75)
print("FALSE POSITIVE ANALYSIS")
print("=" * 75)

false_positive = result_pd[
    (result_pd["ground_truth"] == 0)
    &
    (result_pd["ensemble_prediction"] == 1)
].copy()

print(
    f"False positives: {len(false_positive):,}"
)

if len(false_positive) > 0:

    display_columns = [
        "meter_id",
        "date",
        "daily_consumption",
        "historical_z",
        "seasonal_z",
        "rolling_z",
        "peer_z",
        "relative_deviation",
        "contextual_score",
        "if_anomaly_score",
        "ensemble_score"
    ]

    print("\nTop false positives:")

    print(
        false_positive
        .sort_values(
            "ensemble_score",
            ascending=False
        )[display_columns]
        .head(20)
        .to_string(index=False)
    )


# ============================================================
# 20. DETECTED GROUND-TRUTH ANOMALIES
# ============================================================

print("\n")
print("=" * 75)
print("DETECTED GROUND-TRUTH ANOMALIES")
print("=" * 75)

detected_truth = result_pd[
    (result_pd["ground_truth"] == 1)
    &
    (result_pd["ensemble_prediction"] == 1)
].copy()

truth_columns = [
    "meter_id",
    "date",
    "daily_consumption",
    "anomaly_type",
    "historical_z",
    "seasonal_z",
    "rolling_z",
    "peer_z",
    "relative_deviation",
    "contextual_score",
    "if_anomaly_score",
    "ensemble_score"
]

if len(detected_truth) > 0:

    print(
        detected_truth[
            truth_columns
        ]
        .sort_values(["meter_id", "date"])
        .to_string(index=False)
    )

else:

    print("No injected anomalies detected.")


# ============================================================
# 21. SAVE RESULTS
# ============================================================

print("\nSaving results...")

result_pd.to_parquet(
    os.path.join(
        OUTPUT_PATH,
        "contextual_detection_results.parquet"
    ),
    index=False
)

pd.DataFrame(metrics).to_csv(
    os.path.join(
        OUTPUT_PATH,
        "model_comparison.csv"
    ),
    index=False
)

type_df.to_csv(
    os.path.join(
        OUTPUT_PATH,
        "anomaly_type_results.csv"
    ),
    index=False
)


# ============================================================
# 22. FINAL VERIFICATION
# ============================================================

print("\n")
print("=" * 75)
print("FINAL VERIFICATION")
print("=" * 75)

output_rows = len(result_pd)

print(
    f"Input rows              : {evaluation_count:,}"
)

print(
    f"Output rows             : {output_rows:,}"
)

print(
    f"Ground-truth anomalies  : "
    f"{int(result_pd['ground_truth'].sum()):,}"
)

print(
    f"Predicted anomalies     : "
    f"{int(result_pd['ensemble_prediction'].sum()):,}"
)

if evaluation_count == output_rows:

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
print("  contextual_detection_results.parquet")
print("  model_comparison.csv")
print("  anomaly_type_results.csv")

print("\n")
print("=" * 75)
print("STEP 8 COMPLETED")
print("=" * 75)

spark.stop()