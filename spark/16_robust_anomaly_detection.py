from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from sklearn.ensemble import IsolationForest
import pandas as pd
import numpy as np
import os


# ============================================================
# GRIDGUARD
# STEP 7 - ROBUST ANOMALY DETECTION
# ============================================================

INPUT_PATH = "data/real/processed/anomaly_injected_1m"
BASELINE_PATH = "data/real/processed/meter_baselines_1m"
CLEAN_PATH = "data/real/processed/historical_deviations_1m"

OUTPUT_PATH = "data/real/processed/robust_detection_results_1m"


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("GridGuard-Robust-Anomaly-Detection")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 75)
print("GRIDGUARD - ROBUST ANOMALY DETECTION")
print("=" * 75)


# ============================================================
# 1. READ EVALUATION DATA
# ============================================================

print("\nReading injected evaluation dataset...")

df = spark.read.parquet(INPUT_PATH)

input_count = df.count()

print(f"Evaluation rows: {input_count:,}")


# ============================================================
# 2. READ BASELINES
# ============================================================

print("\nReading meter baselines...")

baseline = spark.read.parquet(BASELINE_PATH)

print(f"Baseline meters: {baseline.count():,}")


# ============================================================
# 3. STANDARDIZE BASELINE COLUMNS
# ============================================================

baseline = baseline.select(
    "meter_id",

    F.col(
        "historical_avg_consumption"
    ).alias("baseline_avg"),

    F.col(
        "historical_std_consumption"
    ).alias("baseline_std"),

    F.col(
        "historical_median_consumption"
    ).alias("baseline_median"),

    F.col(
        "historical_p05_consumption"
    ).alias("baseline_p05"),

    F.col(
        "historical_p95_consumption"
    ).alias("baseline_p95")
)


# ============================================================
# 4. JOIN BASELINES
# ============================================================

print("\nJoining baselines...")

df = df.join(
    baseline,
    on="meter_id",
    how="left"
)


missing_baseline = df.filter(
    F.col("baseline_avg").isNull()
).count()

print(
    f"Meters/records without baseline: "
    f"{missing_baseline}"
)


# ============================================================
# 5. ROBUST HISTORICAL FEATURES
# ============================================================

print("\nCalculating robust historical features...")


# Prevent division by zero

df = df.withColumn(
    "safe_std",

    F.when(
        F.col("baseline_std").isNull() |
        (F.col("baseline_std") < 0.000001),

        F.lit(0.000001)

    ).otherwise(
        F.col("baseline_std")
    )
)


# ------------------------------------------------------------
# Z-SCORE
# ------------------------------------------------------------

df = df.withColumn(
    "historical_z",

    (
        F.col("daily_consumption") -
        F.col("baseline_avg")
    ) / F.col("safe_std")
)


# ------------------------------------------------------------
# RELATIVE DEVIATION
# ------------------------------------------------------------

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


# ------------------------------------------------------------
# MEDIAN DEVIATION
# ------------------------------------------------------------

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


# ------------------------------------------------------------
# PERCENTILE RANGE
# ------------------------------------------------------------

df = df.withColumn(
    "outside_percentile_range",

    F.when(
        (
            F.col("daily_consumption") <
            F.col("baseline_p05")
        ) |

        (
            F.col("daily_consumption") >
            F.col("baseline_p95")
        ),

        1

    ).otherwise(0)
)


# ============================================================
# 6. PREVIOUS DAY FEATURES
# ============================================================

print("\nCalculating temporal features...")


window_spec = (
    Window
    .partitionBy("meter_id")
    .orderBy("date")
)


df = df.withColumn(
    "previous_consumption",

    F.lag(
        "daily_consumption"
    ).over(window_spec)
)


df = df.withColumn(
    "trend_change",

    F.when(

        F.col(
            "previous_consumption"
        ).isNotNull() &

        (
            F.col(
                "previous_consumption"
            ) > 0
        ),

        (
            F.col("daily_consumption") -
            F.col("previous_consumption")
        ) /
        F.col("previous_consumption")

    ).otherwise(0.0)
)


# ============================================================
# 7. INITIAL ROBUST SIGNALS
# ============================================================

print("\nGenerating robust anomaly signals...")


# ------------------------------------------------------------
# SIGNAL 1 - STRONG HISTORICAL DEVIATION
# ------------------------------------------------------------

df = df.withColumn(
    "historical_signal",

    F.when(
        F.abs(
            F.col("historical_z")
        ) >= 3,

        1

    ).otherwise(0)
)


# ------------------------------------------------------------
# SIGNAL 2 - OUTSIDE HISTORICAL PERCENTILE RANGE
# ------------------------------------------------------------

df = df.withColumn(
    "percentile_signal",

    F.col(
        "outside_percentile_range"
    )
)


# ------------------------------------------------------------
# SIGNAL 3 - LARGE RELATIVE CHANGE
#
# We use 100% rather than 50%.
#
# This prevents ordinary consumption fluctuations
# from immediately becoming anomalies.
# ------------------------------------------------------------

df = df.withColumn(
    "relative_signal",

    F.when(
        F.abs(
            F.col("relative_deviation")
        ) >= 1.0,

        1

    ).otherwise(0)
)


# ------------------------------------------------------------
# SIGNAL 4 - LARGE DAY-TO-DAY CHANGE
# ------------------------------------------------------------

df = df.withColumn(
    "trend_signal",

    F.when(
        F.abs(
            F.col("trend_change")
        ) >= 1.0,

        1

    ).otherwise(0)
)


# ============================================================
# 8. PERSISTENCE DETECTION
# ============================================================

print("\nCalculating persistence...")


# An abnormal observation is a candidate if it is
# substantially outside its normal operating range.

df = df.withColumn(
    "candidate_signal",

    F.when(

        (
            F.abs(
                F.col("historical_z")
            ) >= 2.5
        )

        |

        (
            F.abs(
                F.col("relative_deviation")
            ) >= 0.75
        ),

        1

    ).otherwise(0)
)


# ------------------------------------------------------------
# Previous candidate observations
# ------------------------------------------------------------

df = df.withColumn(
    "previous_candidate_1",

    F.lag(
        "candidate_signal",
        1
    ).over(window_spec)
)


df = df.withColumn(
    "previous_candidate_2",

    F.lag(
        "candidate_signal",
        2
    ).over(window_spec)
)


df = df.withColumn(
    "previous_candidate_3",

    F.lag(
        "candidate_signal",
        3
    ).over(window_spec)
)


df = df.withColumn(
    "previous_candidate_4",

    F.lag(
        "candidate_signal",
        4
    ).over(window_spec)
)


# ------------------------------------------------------------
# Persistence score
# ------------------------------------------------------------

df = df.withColumn(
    "persistence_count",

    F.col("candidate_signal") +

    F.coalesce(
        F.col("previous_candidate_1"),
        F.lit(0)
    ) +

    F.coalesce(
        F.col("previous_candidate_2"),
        F.lit(0)
    ) +

    F.coalesce(
        F.col("previous_candidate_3"),
        F.lit(0)
    ) +

    F.coalesce(
        F.col("previous_candidate_4"),
        F.lit(0)
    )
)


df = df.withColumn(
    "persistence_signal",

    F.when(
        F.col("persistence_count") >= 3,
        1
    ).otherwise(0)
)


# ============================================================
# 9. BUILD STATISTICAL EVIDENCE SCORE
# ============================================================

print("\nBuilding statistical evidence score...")


df = df.withColumn(
    "robust_score",

    F.least(

        F.lit(1.0),

        (
            F.col("historical_signal") * 0.30 +

            F.col("percentile_signal") * 0.15 +

            F.col("relative_signal") * 0.20 +

            F.col("trend_signal") * 0.15 +

            F.col("persistence_signal") * 0.20
        )

    )
)


# ============================================================
# 10. ROBUST STATISTICAL PREDICTION
# ============================================================

# Require at least two independent signals.
#
# OR:
# persistent abnormal behavior + one additional signal.

df = df.withColumn(
    "robust_prediction",

    F.when(

        (
            (
                F.col("historical_signal") +
                F.col("percentile_signal") +
                F.col("relative_signal") +
                F.col("trend_signal")
            ) >= 2
        )

        |

        (
            (
                F.col("persistence_signal") == 1
            )

            &

            (
                (
                    F.col("historical_signal") +
                    F.col("percentile_signal") +
                    F.col("relative_signal")
                ) >= 1
            )
        ),

        1

    ).otherwise(0)
)


# ============================================================
# 11. PREPARE CLEAN DATA FOR ISOLATION FOREST
# ============================================================

print("\nReading clean historical data...")

clean_df = spark.read.parquet(CLEAN_PATH)

clean_count = clean_df.count()

print(
    f"Clean training rows: "
    f"{clean_count:,}"
)


clean_df = clean_df.join(
    baseline,
    on="meter_id",
    how="left"
)


clean_df = clean_df.withColumn(
    "safe_std",

    F.when(
        F.col("baseline_std").isNull() |
        (
            F.col("baseline_std") <
            0.000001
        ),

        F.lit(0.000001)

    ).otherwise(
        F.col("baseline_std")
    )
)


clean_df = clean_df.withColumn(
    "historical_z",

    (
        F.col("daily_consumption") -
        F.col("baseline_avg")
    ) /
    F.col("safe_std")
)


clean_df = clean_df.withColumn(
    "relative_deviation",

    F.when(

        F.col("baseline_avg") > 0,

        (
            F.col("daily_consumption") -
            F.col("baseline_avg")
        ) /
        F.col("baseline_avg")

    ).otherwise(0.0)
)


clean_df = clean_df.withColumn(
    "previous_consumption",

    F.lag(
        "daily_consumption"
    ).over(window_spec)
)


clean_df = clean_df.withColumn(
    "trend_change",

    F.when(

        F.col(
            "previous_consumption"
        ).isNotNull() &

        (
            F.col(
                "previous_consumption"
            ) > 0
        ),

        (
            F.col("daily_consumption") -
            F.col("previous_consumption")
        ) /
        F.col("previous_consumption")

    ).otherwise(0.0)
)


# ============================================================
# 12. CONVERT DEVELOPMENT DATA TO PANDAS
# ============================================================

print("\nPreparing ML datasets...")

train_pdf = clean_df.select(
    "historical_z",
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

    "historical_z",
    "relative_deviation",
    "median_deviation",

    "outside_percentile_range",

    "trend_change",

    "historical_signal",
    "percentile_signal",
    "relative_signal",
    "trend_signal",
    "persistence_signal",
    "persistence_count",

    "robust_score",
    "robust_prediction"
).toPandas()


print(
    f"Training rows : {len(train_pdf):,}"
)

print(
    f"Evaluation rows : {len(eval_pdf):,}"
)


# ============================================================
# 13. ISOLATION FOREST
# ============================================================

print("\nTraining Isolation Forest...")


feature_columns = [
    "historical_z",
    "relative_deviation",
    "trend_change"
]


X_train = train_pdf[
    feature_columns
].replace(
    [np.inf, -np.inf],
    np.nan
).fillna(0)


X_eval = eval_pdf[
    feature_columns
].replace(
    [np.inf, -np.inf],
    np.nan
).fillna(0)


model = IsolationForest(
    n_estimators=200,

    # Lower contamination than Step 6.
    # This is deliberately conservative.
    contamination=0.005,

    random_state=42,

    n_jobs=-1
)


model.fit(X_train)


if_prediction = model.predict(
    X_eval
)


eval_pdf["if_prediction"] = (
    if_prediction == -1
).astype(int)


# ============================================================
# 14. ISOLATION FOREST SCORE
# ============================================================

raw_score = model.decision_function(
    X_eval
)


eval_pdf["if_raw_score"] = raw_score


score_min = raw_score.min()
score_max = raw_score.max()


if score_max > score_min:

    eval_pdf["if_anomaly_score"] = (

        (score_max - raw_score) /
        (score_max - score_min)

    )

else:

    eval_pdf["if_anomaly_score"] = 0.0


# ============================================================
# 15. GRIDGUARD MULTI-SIGNAL ENSEMBLE
# ============================================================

print("\nBuilding GridGuard ensemble...")


# The ML model is treated as supporting evidence,
# rather than being allowed to generate an alert alone.

eval_pdf["ensemble_score"] = (

    eval_pdf["robust_score"] * 0.70 +

    eval_pdf["if_anomaly_score"] * 0.30

)


# Require reasonably strong combined evidence.

eval_pdf["ensemble_prediction"] = (

    eval_pdf["ensemble_score"] >= 0.60

).astype(int)


# ============================================================
# 16. EVALUATION FUNCTION
# ============================================================

def evaluate_model(
    name,
    prediction_column
):

    actual = (
        eval_pdf["ground_truth"]
        .astype(int)
    )

    predicted = (
        eval_pdf[prediction_column]
        .astype(int)
    )


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
    print("-" * 55)

    print(
        f"TP        : {tp}"
    )

    print(
        f"TN        : {tn}"
    )

    print(
        f"FP        : {fp}"
    )

    print(
        f"FN        : {fn}"
    )

    print(
        f"Precision : {precision:.4f}"
    )

    print(
        f"Recall    : {recall:.4f}"
    )

    print(
        f"F1 Score  : {f1:.4f}"
    )

    print(
        f"Accuracy  : {accuracy:.4f}"
    )


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
# 17. MODEL COMPARISON
# ============================================================

print("\n")
print("=" * 75)
print("MODEL EVALUATION")
print("=" * 75)


results = []


results.append(
    evaluate_model(
        "Robust Statistical Detector",
        "robust_prediction"
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
# 18. ANOMALY TYPE ANALYSIS
# ============================================================

print("\n")
print("=" * 75)
print("ANOMALY TYPE DETECTION")
print("=" * 75)


ground_truth_pdf = eval_pdf[
    eval_pdf["ground_truth"] == True
]


type_results = []


for anomaly_type, group in (
    ground_truth_pdf
    .groupby("anomaly_type")
):

    total = len(group)

    detected = int(
        group[
            "ensemble_prediction"
        ].sum()
    )

    detection_rate = (
        detected / total
        if total > 0
        else 0
    )


    type_results.append({

        "anomaly_type":
            anomaly_type,

        "total":
            total,

        "detected":
            detected,

        "detection_rate":
            detection_rate

    })


type_results_df = pd.DataFrame(
    type_results
)


print(
    type_results_df.to_string(
        index=False
    )
)


# ============================================================
# 19. FALSE POSITIVE ANALYSIS
# ============================================================

print("\n")
print("=" * 75)
print("FALSE POSITIVE ANALYSIS")
print("=" * 75)


false_positive_pdf = eval_pdf[
    (
        eval_pdf["ground_truth"] == False
    )
    &
    (
        eval_pdf["ensemble_prediction"] == 1
    )
]


print(
    f"False positives: "
    f"{len(false_positive_pdf):,}"
)


if len(false_positive_pdf) > 0:

    print(
        "\nTop false positives:"
    )


    print(

        false_positive_pdf[
            [
                "meter_id",
                "date",
                "daily_consumption",
                "historical_z",
                "relative_deviation",
                "trend_change",
                "robust_score",
                "if_anomaly_score",
                "ensemble_score"
            ]
        ]
        .sort_values(
            "ensemble_score",
            ascending=False
        )
        .head(20)
        .to_string(
            index=False
        )

    )


# ============================================================
# 20. DETECTED TRUE ANOMALIES
# ============================================================

print("\n")
print("=" * 75)
print("DETECTED GROUND-TRUTH ANOMALIES")
print("=" * 75)


true_detected = eval_pdf[
    (
        eval_pdf["ground_truth"] == True
    )
    &
    (
        eval_pdf["ensemble_prediction"] == 1
    )
]


if len(true_detected) > 0:

    print(

        true_detected[
            [
                "meter_id",
                "date",
                "daily_consumption",
                "original_daily_consumption",
                "anomaly_type",
                "historical_z",
                "relative_deviation",
                "trend_change",
                "persistence_count",
                "robust_score",
                "if_anomaly_score",
                "ensemble_score"
            ]
        ]
        .sort_values(
            ["meter_id", "date"]
        )
        .to_string(
            index=False
        )

    )

else:

    print(
        "No injected anomalies detected."
    )


# ============================================================
# 21. SAVE RESULTS
# ============================================================

print("\nSaving results...")


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

    "historical_z",
    "relative_deviation",
    "median_deviation",

    "outside_percentile_range",

    "trend_change",

    "historical_signal",
    "percentile_signal",
    "relative_signal",
    "trend_signal",

    "persistence_signal",
    "persistence_count",

    "robust_score",
    "robust_prediction",

    "if_prediction",
    "if_raw_score",
    "if_anomaly_score",

    "ensemble_score",
    "ensemble_prediction"

]


eval_pdf[
    result_columns
].to_parquet(

    OUTPUT_PATH +
    "/robust_detection_results.parquet",

    index=False

)


pd.DataFrame(
    results
).to_csv(

    OUTPUT_PATH +
    "/model_comparison.csv",

    index=False

)


type_results_df.to_csv(

    OUTPUT_PATH +
    "/anomaly_type_results.csv",

    index=False

)


# ============================================================
# 22. FINAL VERIFICATION
# ============================================================

saved = pd.read_parquet(

    OUTPUT_PATH +
    "/robust_detection_results.parquet"

)


print("\n")
print("=" * 75)
print("FINAL VERIFICATION")
print("=" * 75)


print(
    f"Input rows           : "
    f"{input_count:,}"
)


print(
    f"Output rows          : "
    f"{len(saved):,}"
)


print(
    f"Ground-truth anomalies : "
    f"{int(eval_pdf['ground_truth'].sum())}"
)


print(
    f"Predicted anomalies    : "
    f"{int(eval_pdf['ensemble_prediction'].sum())}"
)


if input_count == len(saved):

    print(
        "Row-count verification PASSED."
    )

else:

    print(
        "WARNING: Row-count mismatch!"
    )


print("\nOutput directory:")
print(
    OUTPUT_PATH
)


print("\nGenerated files:")

print(
    "  robust_detection_results.parquet"
)

print(
    "  model_comparison.csv"
)

print(
    "  anomaly_type_results.csv"
)


print("\n")
print("=" * 75)
print("STEP 7 COMPLETED")
print("=" * 75)


spark.stop()