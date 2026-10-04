from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.ensemble import IsolationForest
from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    accuracy_score
)


# =========================================================
# 1. Start Spark
# =========================================================

spark = (
    SparkSession.builder
    .appName("GridGuard-EnsembleDetection")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 2. Read anomaly-injected dataset
# =========================================================

input_path = "data/processed/anomaly_injected"

df = spark.read.parquet(input_path)

print("\n========== INPUT DATA ==========")
print(f"Total records: {df.count()}")


# =========================================================
# 3. Calculate meter-specific baseline
# =========================================================

meter_stats = (
    df.groupBy("meter_id")
    .agg(
        F.mean("consumption").alias("meter_mean"),
        F.stddev("consumption").alias("meter_std")
    )
)


# =========================================================
# 4. Join baseline
# =========================================================

df = df.join(
    meter_stats,
    on="meter_id",
    how="left"
)


# =========================================================
# 5. Feature Engineering
# =========================================================

df = (
    df
    .withColumn(
        "meter_z_score",
        F.when(
            F.col("meter_std") > 0,
            (F.col("consumption") - F.col("meter_mean"))
            / F.col("meter_std")
        ).otherwise(0)
    )
    .withColumn(
        "relative_consumption",
        F.when(
            F.col("meter_mean") > 0,
            F.col("consumption") / F.col("meter_mean")
        ).otherwise(1)
    )
    .withColumn(
        "hour",
        F.hour("timestamp")
    )
    .withColumn(
        "is_night",
        F.when(
            (F.hour("timestamp") >= 0) &
            (F.hour("timestamp") < 6),
            1
        ).otherwise(0)
    )
    .withColumn(
        "is_weekend",
        F.when(
            F.dayofweek("timestamp").isin([1, 7]),
            1
        ).otherwise(0)
    )
)


# =========================================================
# 6. Convert small development dataset to Pandas
# =========================================================

pdf = df.select(
    "meter_id",
    "timestamp",
    "consumption",
    "meter_z_score",
    "relative_consumption",
    "hour",
    "is_night",
    "is_weekend",
    "ground_truth",
    "anomaly_type"
).toPandas()


# =========================================================
# 7. Isolation Forest
# =========================================================

X = pdf[
    [
        "meter_z_score",
        "relative_consumption",
        "hour",
        "is_night",
        "is_weekend"
    ]
]

y_true = pdf["ground_truth"].astype(int)


model = IsolationForest(
    n_estimators=200,
    contamination=0.20,
    random_state=42
)

model.fit(X)


# Isolation Forest:
#  1  = normal
# -1  = anomaly

pdf["if_prediction"] = model.predict(X)

pdf["if_anomaly"] = (
    pdf["if_prediction"] == -1
).astype(int)


# =========================================================
# 8. Statistical Detection
# =========================================================

# Statistical anomaly:
# Absolute meter-specific Z-score >= 2

pdf["stat_anomaly"] = (
    pdf["meter_z_score"].abs() >= 2
).astype(int)


# =========================================================
# 9. Isolation Forest anomaly score
# =========================================================

pdf["if_score"] = -model.score_samples(X)


# =========================================================
# 10. Ensemble Detection
# =========================================================

# Weighted voting:
#
# Statistical detector = 40%
# Isolation Forest     = 60%
#
# Final score:
# 0.4 * statistical detection
# +
# 0.6 * Isolation Forest detection

pdf["ensemble_score"] = (
    0.4 * pdf["stat_anomaly"]
    +
    0.6 * pdf["if_anomaly"]
)


# Final anomaly:
# Score >= 0.6
#
# This means:
# Isolation Forest alone -> anomaly
#
# Both models -> anomaly
#
# Statistical only -> not final anomaly

pdf["ensemble_anomaly"] = (
    pdf["ensemble_score"] >= 0.6
).astype(int)


# =========================================================
# 11. Evaluation Function
# =========================================================

def evaluate_model(name, predictions):

    precision = precision_score(
        y_true,
        predictions,
        zero_division=0
    )

    recall = recall_score(
        y_true,
        predictions,
        zero_division=0
    )

    f1 = f1_score(
        y_true,
        predictions,
        zero_division=0
    )

    accuracy = accuracy_score(
        y_true,
        predictions
    )

    tp = ((y_true == 1) & (predictions == 1)).sum()
    tn = ((y_true == 0) & (predictions == 0)).sum()
    fp = ((y_true == 0) & (predictions == 1)).sum()
    fn = ((y_true == 1) & (predictions == 0)).sum()

    print(f"\n========== {name} ==========")

    print(f"Precision : {precision:.4f}")
    print(f"Recall    : {recall:.4f}")
    print(f"F1 Score  : {f1:.4f}")
    print(f"Accuracy  : {accuracy:.4f}")

    print("\nConfusion Matrix:")
    print(f"TP : {tp}")
    print(f"TN : {tn}")
    print(f"FP : {fp}")
    print(f"FN : {fn}")

    return {
        "model": name,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": accuracy
    }


# =========================================================
# 12. Evaluate Statistical Model
# =========================================================

stat_results = evaluate_model(
    "STATISTICAL Z-SCORE",
    pdf["stat_anomaly"]
)


# =========================================================
# 13. Evaluate Isolation Forest
# =========================================================

if_results = evaluate_model(
    "ISOLATION FOREST",
    pdf["if_anomaly"]
)


# =========================================================
# 14. Evaluate Ensemble
# =========================================================

ensemble_results = evaluate_model(
    "GRIDGUARD ENSEMBLE",
    pdf["ensemble_anomaly"]
)


# =========================================================
# 15. Display detected anomalies
# =========================================================

print("\n========== GRIDGUARD DETECTED ANOMALIES ==========")

detected = pdf[
    pdf["ensemble_anomaly"] == 1
].sort_values(
    "ensemble_score",
    ascending=False
)

print(
    detected[
        [
            "meter_id",
            "timestamp",
            "consumption",
            "meter_z_score",
            "relative_consumption",
            "stat_anomaly",
            "if_anomaly",
            "ensemble_score",
            "anomaly_type",
            "ground_truth"
        ]
    ].to_string(index=False)
)


# =========================================================
# 16. Save results
# =========================================================

output_pdf = pdf[
    [
        "meter_id",
        "timestamp",
        "consumption",
        "meter_z_score",
        "relative_consumption",
        "stat_anomaly",
        "if_anomaly",
        "if_score",
        "ensemble_score",
        "ensemble_anomaly",
        "anomaly_type",
        "ground_truth"
    ]
]


output_spark = spark.createDataFrame(output_pdf)

output_path = "data/processed/ensemble_results"

output_spark.write.mode(
    "overwrite"
).parquet(output_path)


# =========================================================
# 17. Save model comparison
# =========================================================

comparison = [
    stat_results,
    if_results,
    ensemble_results
]

comparison_df = spark.createDataFrame(comparison)

comparison_df.show(truncate=False)

comparison_df.write.mode(
    "overwrite"
).parquet(
    "data/processed/model_comparison"
)


# =========================================================
# 18. Finish
# =========================================================

print(
    f"\nEnsemble results saved to: {output_path}"
)

print(
    "\nModel comparison saved to: "
    "data/processed/model_comparison"
)


spark.stop()

print("\n========== ENSEMBLE DETECTION COMPLETE ==========")