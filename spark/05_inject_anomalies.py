from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit,
    row_number
)
from pyspark.sql.window import Window


spark = (
    SparkSession.builder
    .appName("GridGuard-Anomaly-Injection")
    .master("local[2]")
    .getOrCreate()
)


input_path = "data/parquet/smart_meter"
output_path = "data/processed/anomaly_injected"


# --------------------------------------------------
# 1. Read original data
# --------------------------------------------------

df = spark.read.parquet(input_path)

print("\n===== ORIGINAL DATA =====")
df.show(20, truncate=False)


# --------------------------------------------------
# 2. Create row number for each meter
# --------------------------------------------------

window = Window.partitionBy(
    "meter_id"
).orderBy(
    "timestamp"
)


df = df.withColumn(
    "reading_number",
    row_number().over(window)
)


# --------------------------------------------------
# 3. Preserve original consumption
# --------------------------------------------------

df = df.withColumn(
    "original_consumption",
    col("consumption")
)


# --------------------------------------------------
# 4. Create controlled anomalies
#
# M001 → sudden spike
# M002 → sudden drop
# M003 → persistent low consumption
# --------------------------------------------------

df = df.withColumn(
    "modified_consumption",
    when(
        (col("meter_id") == "M001") &
        (col("reading_number") == 10),
        col("consumption") * 4
    )
    .when(
        (col("meter_id") == "M002") &
        (col("reading_number") == 10),
        col("consumption") * 0.2
    )
    .when(
        (col("meter_id") == "M003") &
        (col("reading_number") >= 10),
        col("consumption") * 0.25
    )
    .otherwise(
        col("consumption")
    )
)


# --------------------------------------------------
# 5. Assign anomaly type
# --------------------------------------------------

df = df.withColumn(
    "anomaly_type",
    when(
        (col("meter_id") == "M001") &
        (col("reading_number") == 10),
        "sudden_spike"
    )
    .when(
        (col("meter_id") == "M002") &
        (col("reading_number") == 10),
        "sudden_drop"
    )
    .when(
        (col("meter_id") == "M003") &
        (col("reading_number") >= 10),
        "persistent_low"
    )
    .otherwise(
        "normal"
    )
)


# --------------------------------------------------
# 6. Ground truth
# --------------------------------------------------

df = df.withColumn(
    "ground_truth",
    when(
        col("anomaly_type") != "normal",
        lit(True)
    ).otherwise(
        lit(False)
    )
)


# --------------------------------------------------
# 7. Replace consumption with modified value
# --------------------------------------------------

df = df.drop("consumption")

df = df.withColumnRenamed(
    "modified_consumption",
    "consumption"
)


# --------------------------------------------------
# 8. Display results
# --------------------------------------------------

result = df.select(
    "meter_id",
    "timestamp",
    "original_consumption",
    "consumption",
    "anomaly_type",
    "ground_truth"
)


print("\n===== INJECTED ANOMALIES =====")

result.show(
    60,
    truncate=False
)


# --------------------------------------------------
# 9. Count ground-truth anomalies
# --------------------------------------------------

print("\n===== GROUND TRUTH COUNTS =====")

result.groupBy(
    "anomaly_type"
).count().show()


# --------------------------------------------------
# 10. Save
# --------------------------------------------------

result.write \
    .mode("overwrite") \
    .parquet(output_path)


print(
    "\nAnomaly-injected data written to:"
)

print(output_path)


spark.stop()