from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    stddev,
    abs,
    col,
    lit,
    when,
    sum
)


spark = (
    SparkSession.builder
    .appName("GridGuard-Statistical-Evaluation")
    .master("local[2]")
    .getOrCreate()
)


input_path = "data/processed/anomaly_injected"


# --------------------------------------------------
# 1. Read anomaly-injected data
# --------------------------------------------------

df = spark.read.parquet(input_path)

print("\n===== INPUT DATA =====")

df.show(20, truncate=False)


# --------------------------------------------------
# 2. Calculate statistics
# --------------------------------------------------

stats = df.select(
    avg("consumption").alias("mean"),
    stddev("consumption").alias("std")
).collect()[0]


mean_value = stats["mean"]
std_value = stats["std"]


print("\nMean:", mean_value)
print("Std:", std_value)


# --------------------------------------------------
# 3. Z-score
# --------------------------------------------------

df = df.withColumn(
    "z_score",
    when(
        lit(std_value) != 0,
        (
            col("consumption") - lit(mean_value)
        ) / lit(std_value)
    ).otherwise(0)
)


df = df.withColumn(
    "predicted_anomaly",
    when(
        abs(col("z_score")) > 2,
        1
    ).otherwise(0)
)


# --------------------------------------------------
# 4. Ground truth
# --------------------------------------------------

df = df.withColumn(
    "actual_anomaly",
    when(
        col("ground_truth") == True,
        1
    ).otherwise(0)
)


# --------------------------------------------------
# 5. Confusion matrix
# --------------------------------------------------

tp = df.filter(
    (col("predicted_anomaly") == 1) &
    (col("actual_anomaly") == 1)
).count()


tn = df.filter(
    (col("predicted_anomaly") == 0) &
    (col("actual_anomaly") == 0)
).count()


fp = df.filter(
    (col("predicted_anomaly") == 1) &
    (col("actual_anomaly") == 0)
).count()


fn = df.filter(
    (col("predicted_anomaly") == 0) &
    (col("actual_anomaly") == 1)
).count()


# --------------------------------------------------
# 6. Metrics
# --------------------------------------------------

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


# --------------------------------------------------
# 7. Print results
# --------------------------------------------------

print("\n========================================")
print("STATISTICAL ANOMALY EVALUATION")
print("========================================")

print("True Positives :", tp)
print("True Negatives :", tn)
print("False Positives:", fp)
print("False Negatives:", fn)

print("----------------------------------------")

print(
    "Precision:",
    round(precision, 4)
)

print(
    "Recall:",
    round(recall, 4)
)

print(
    "F1 Score:",
    round(f1, 4)
)

print(
    "Accuracy:",
    round(accuracy, 4)
)

print("========================================")


# --------------------------------------------------
# 8. Show prediction results
# --------------------------------------------------

print("\n===== PREDICTIONS =====")

df.select(
    "meter_id",
    "timestamp",
    "consumption",
    "anomaly_type",
    "actual_anomaly",
    "z_score",
    "predicted_anomaly"
).show(
    60,
    truncate=False
)


spark.stop()