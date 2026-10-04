from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    stddev,
    abs,
    col,
    lit,
    when
)


spark = (
    SparkSession.builder
    .appName("GridGuard-Statistical-Anomaly")
    .master("local[2]")
    .getOrCreate()
)


input_path = "data/processed/meter_deviation"
output_path = "data/processed/statistical_anomalies"


# --------------------------------------------------
# 1. Read deviation data
# --------------------------------------------------

df = spark.read.parquet(input_path)

print("\n===== INPUT DATA =====")
df.show(truncate=False)


# --------------------------------------------------
# 2. Calculate population statistics
# --------------------------------------------------

stats = df.select(
    avg("avg_consumption").alias("mean_consumption"),
    stddev("avg_consumption").alias("std_consumption")
).collect()[0]


mean_consumption = stats["mean_consumption"]
std_consumption = stats["std_consumption"]


print("\nMean consumption:", mean_consumption)
print("Standard deviation:", std_consumption)


# --------------------------------------------------
# 3. Calculate Z-score
# --------------------------------------------------

df = df.withColumn(
    "z_score",
    when(
        lit(std_consumption) != 0,
        (
            col("avg_consumption")
            - lit(mean_consumption)
        ) / lit(std_consumption)
    ).otherwise(0)
)


df = df.withColumn(
    "abs_z_score",
    abs(col("z_score"))
)


# --------------------------------------------------
# 4. Z-score anomaly
# --------------------------------------------------

df = df.withColumn(
    "zscore_anomaly",
    when(
        col("abs_z_score") > 3,
        1
    ).otherwise(0)
)


# --------------------------------------------------
# 5. Calculate IQR
# --------------------------------------------------

quantiles = df.approxQuantile(
    "avg_consumption",
    [0.25, 0.75],
    0.01
)


q1 = quantiles[0]
q3 = quantiles[1]

iqr = q3 - q1

lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr


print("\nQ1:", q1)
print("Q3:", q3)
print("IQR:", iqr)
print("Lower bound:", lower_bound)
print("Upper bound:", upper_bound)


# --------------------------------------------------
# 6. IQR anomaly
# --------------------------------------------------

df = df.withColumn(
    "iqr_anomaly",
    when(
        (col("avg_consumption") < lit(lower_bound))
        |
        (col("avg_consumption") > lit(upper_bound)),
        1
    ).otherwise(0)
)


# --------------------------------------------------
# 7. Combined statistical anomaly
# --------------------------------------------------

df = df.withColumn(
    "statistical_anomaly",
    when(
        (col("zscore_anomaly") == 1)
        |
        (col("iqr_anomaly") == 1),
        1
    ).otherwise(0)
)


# --------------------------------------------------
# 8. Anomaly explanation
# --------------------------------------------------

df = df.withColumn(
    "anomaly_reason",
    when(
        (col("zscore_anomaly") == 1)
        &
        (col("iqr_anomaly") == 1),
        "Z-score and IQR anomaly"
    )
    .when(
        col("zscore_anomaly") == 1,
        "Z-score anomaly"
    )
    .when(
        col("iqr_anomaly") == 1,
        "IQR anomaly"
    )
    .otherwise(
        "Normal"
    )
)


# --------------------------------------------------
# 9. Display results
# --------------------------------------------------

result = df.select(
    "meter_id",
    "avg_consumption",
    "z_score",
    "abs_z_score",
    "zscore_anomaly",
    "iqr_anomaly",
    "statistical_anomaly",
    "anomaly_reason"
)


print("\n===== STATISTICAL ANOMALY RESULTS =====")

result.show(
    truncate=False
)


# --------------------------------------------------
# 10. Count anomalies
# --------------------------------------------------

anomaly_count = (
    df.filter(
        col("statistical_anomaly") == 1
    ).count()
)


print(
    "\nTotal statistical anomalies:",
    anomaly_count
)


# --------------------------------------------------
# 11. Save results
# --------------------------------------------------

result.write \
    .mode("overwrite") \
    .parquet(output_path)


print(
    "\nAnomaly results written to:",
    output_path
)


spark.stop()