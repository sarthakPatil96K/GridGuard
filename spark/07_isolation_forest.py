from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_score, recall_score, f1_score, accuracy_score
import pandas as pd


# ---------------------------------------------------------
# 1. Start Spark
# ---------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("GridGuard-IsolationForest")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# 2. Read anomaly-injected data
# ---------------------------------------------------------
input_path = "data/processed/anomaly_injected"

df = spark.read.parquet(input_path)

print("\n========== INPUT DATA ==========")
print(f"Total records: {df.count()}")

df.select(
    "meter_id",
    "timestamp",
    "consumption",
    "original_consumption",
    "anomaly_type",
    "ground_truth"
).show(15, truncate=False)


# ---------------------------------------------------------
# 3. Calculate per-meter baseline
# ---------------------------------------------------------
meter_stats = (
    df.groupBy("meter_id")
    .agg(
        F.mean("consumption").alias("meter_mean"),
        F.stddev("consumption").alias("meter_std")
    )
)


# ---------------------------------------------------------
# 4. Join baseline with readings
# ---------------------------------------------------------
df_features = df.join(
    meter_stats,
    on="meter_id",
    how="left"
)


# ---------------------------------------------------------
# 5. Create normalized features
# ---------------------------------------------------------

df_features = (
    df_features
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
)


print("\n========== FEATURE DATA ==========")

df_features.select(
    "meter_id",
    "timestamp",
    "consumption",
    "meter_mean",
    "meter_std",
    "meter_z_score",
    "relative_consumption",
    "hour",
    "ground_truth"
).show(20, truncate=False)


# ---------------------------------------------------------
# 6. Convert only the small development dataset to Pandas
# ---------------------------------------------------------
pdf = df_features.select(
    "meter_id",
    "timestamp",
    "consumption",
    "meter_z_score",
    "relative_consumption",
    "hour",
    "ground_truth",
    "anomaly_type"
).toPandas()


print("\n========== PANDAS DATA ==========")
print(f"Rows used for Isolation Forest: {len(pdf)}")


# ---------------------------------------------------------
# 7. Prepare ML features
# ---------------------------------------------------------
X = pdf[
    [
        "meter_z_score",
        "relative_consumption",
        "hour"
    ]
]

y_true = pdf["ground_truth"].astype(int)


# ---------------------------------------------------------
# 8. Train Isolation Forest
# ---------------------------------------------------------
model = IsolationForest(
    n_estimators=200,
    contamination=0.20,
    random_state=42
)

model.fit(X)


# ---------------------------------------------------------
# 9. Predict anomalies
# ---------------------------------------------------------
predictions = model.predict(X)

# Isolation Forest:
#   1  = normal
#  -1  = anomaly

pdf["prediction"] = predictions

pdf["predicted_anomaly"] = (
    pdf["prediction"] == -1
).astype(int)


# ---------------------------------------------------------
# 10. Calculate anomaly score
# ---------------------------------------------------------
pdf["anomaly_score"] = -model.score_samples(X)


# ---------------------------------------------------------
# 11. Evaluation
# ---------------------------------------------------------
y_pred = pdf["predicted_anomaly"]

precision = precision_score(
    y_true,
    y_pred,
    zero_division=0
)

recall = recall_score(
    y_true,
    y_pred,
    zero_division=0
)

f1 = f1_score(
    y_true,
    y_pred,
    zero_division=0
)

accuracy = accuracy_score(
    y_true,
    y_pred
)


print("\n========== ISOLATION FOREST RESULTS ==========")

print(f"Total records : {len(pdf)}")
print(f"Actual anomalies : {y_true.sum()}")
print(f"Predicted anomalies : {y_pred.sum()}")

print(f"\nPrecision : {precision:.4f}")
print(f"Recall    : {recall:.4f}")
print(f"F1 Score  : {f1:.4f}")
print(f"Accuracy  : {accuracy:.4f}")


# ---------------------------------------------------------
# 12. Confusion Matrix values
# ---------------------------------------------------------
tp = ((y_true == 1) & (y_pred == 1)).sum()
tn = ((y_true == 0) & (y_pred == 0)).sum()
fp = ((y_true == 0) & (y_pred == 1)).sum()
fn = ((y_true == 1) & (y_pred == 0)).sum()

print("\n========== CONFUSION MATRIX ==========")

print(f"True Positives  : {tp}")
print(f"True Negatives  : {tn}")
print(f"False Positives : {fp}")
print(f"False Negatives : {fn}")


# ---------------------------------------------------------
# 13. Show detected anomalies
# ---------------------------------------------------------
print("\n========== DETECTED ANOMALIES ==========")

detected = pdf[
    pdf["predicted_anomaly"] == 1
].sort_values(
    "anomaly_score",
    ascending=False
)

print(
    detected[
        [
            "meter_id",
            "timestamp",
            "consumption",
            "relative_consumption",
            "meter_z_score",
            "anomaly_type",
            "ground_truth",
            "anomaly_score"
        ]
    ].to_string(index=False)
)


# ---------------------------------------------------------
# 14. Save results
# ---------------------------------------------------------
output_pdf = pdf[
    [
        "meter_id",
        "timestamp",
        "consumption",
        "meter_z_score",
        "relative_consumption",
        "hour",
        "anomaly_type",
        "ground_truth",
        "predicted_anomaly",
        "anomaly_score"
    ]
]

output_spark = spark.createDataFrame(output_pdf)

output_path = "data/processed/isolation_forest_results"

output_spark.write.mode("overwrite").parquet(output_path)

print(
    f"\nResults saved to: {output_path}"
)


# ---------------------------------------------------------
# 15. Stop Spark
# ---------------------------------------------------------
spark.stop()

print("\n========== COMPLETE ==========")