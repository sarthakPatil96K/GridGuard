from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# ============================================================
# GRIDGUARD - CONTROLLED ANOMALY INJECTION
# ============================================================

INPUT_PATH = "data/real/processed/daily_features_1m"
OUTPUT_PATH = "data/real/processed/anomaly_injected_1m"

spark = (
    SparkSession.builder
    .appName("GridGuard_Anomaly_Injection")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("GRIDGUARD - CONTROLLED ANOMALY INJECTION")
print("=" * 70)

# ------------------------------------------------------------
# 1. READ CLEAN DAILY DATA
# ------------------------------------------------------------

print("\nReading clean daily features...")

df = spark.read.parquet(INPUT_PATH)

df = (
    df
    .filter(F.col("meter_id").isNotNull())
    .filter(F.col("date").isNotNull())
    .filter(F.col("daily_consumption").isNotNull())
)

print(f"Input rows: {df.count()}")

# ------------------------------------------------------------
# 2. SELECT METERS WITH SUFFICIENT HISTORY
# ------------------------------------------------------------

meter_counts = (
    df.groupBy("meter_id")
    .agg(F.count("*").alias("days"))
    .filter(F.col("days") >= 100)
)

eligible_meters = (
    meter_counts
    .orderBy("meter_id")
    .limit(10)
    .select("meter_id")
)

selected_meters = [
    row["meter_id"]
    for row in eligible_meters.collect()
]

print("\nSelected meters:")
for meter in selected_meters:
    print(f"  {meter}")

if len(selected_meters) < 5:
    raise RuntimeError(
        "Not enough meters with at least 100 days of history."
    )

# ------------------------------------------------------------
# 3. ADD ROW NUMBER PER METER
# ------------------------------------------------------------

window = Window.partitionBy("meter_id").orderBy("date")

df_numbered = (
    df
    .withColumn(
        "row_number",
        F.row_number().over(window)
    )
)

# ------------------------------------------------------------
# 4. CHOOSE CONTROLLED INJECTION POINTS
# ------------------------------------------------------------

# We use different meters for different anomaly types.
#
# meter 1 -> sudden spike
# meter 2 -> sudden drop
# meter 3 -> persistent low
# meter 4 -> persistent high
# meter 5 -> night abnormality

m1 = selected_meters[0]
m2 = selected_meters[1]
m3 = selected_meters[2]
m4 = selected_meters[3]
m5 = selected_meters[4]

print("\nInjection assignment:")
print(f"  {m1} -> sudden_spike")
print(f"  {m2} -> sudden_drop")
print(f"  {m3} -> persistent_low")
print(f"  {m4} -> persistent_high")
print(f"  {m5} -> night_abnormality")

# ------------------------------------------------------------
# 5. CREATE ORIGINAL VALUE
# ------------------------------------------------------------

result = (
    df_numbered
    .withColumn(
        "original_daily_consumption",
        F.col("daily_consumption")
    )
    .withColumn(
        "modified_daily_consumption",
        F.col("daily_consumption")
    )
    .withColumn(
        "anomaly_type",
        F.lit("normal")
    )
    .withColumn(
        "ground_truth",
        F.lit(False)
    )
)

# ------------------------------------------------------------
# 6. SUDDEN SPIKE
# ------------------------------------------------------------

result = (
    result
    .withColumn(
        "modified_daily_consumption",
        F.when(
            (F.col("meter_id") == m1) &
            (F.col("row_number") == 150),
            F.col("original_daily_consumption") * 4.0
        ).otherwise(F.col("modified_daily_consumption"))
    )
    .withColumn(
        "anomaly_type",
        F.when(
            (F.col("meter_id") == m1) &
            (F.col("row_number") == 150),
            "sudden_spike"
        ).otherwise(F.col("anomaly_type"))
    )
    .withColumn(
        "ground_truth",
        F.when(
            (F.col("meter_id") == m1) &
            (F.col("row_number") == 150),
            True
        ).otherwise(F.col("ground_truth"))
    )
)

# ------------------------------------------------------------
# 7. SUDDEN DROP
# ------------------------------------------------------------

result = (
    result
    .withColumn(
        "modified_daily_consumption",
        F.when(
            (F.col("meter_id") == m2) &
            (F.col("row_number") == 150),
            F.col("original_daily_consumption") * 0.20
        ).otherwise(F.col("modified_daily_consumption"))
    )
    .withColumn(
        "anomaly_type",
        F.when(
            (F.col("meter_id") == m2) &
            (F.col("row_number") == 150),
            "sudden_drop"
        ).otherwise(F.col("anomaly_type"))
    )
    .withColumn(
        "ground_truth",
        F.when(
            (F.col("meter_id") == m2) &
            (F.col("row_number") == 150),
            True
        ).otherwise(F.col("ground_truth"))
    )
)

# ------------------------------------------------------------
# 8. PERSISTENT LOW
# ------------------------------------------------------------

result = (
    result
    .withColumn(
        "modified_daily_consumption",
        F.when(
            (F.col("meter_id") == m3) &
            (F.col("row_number") >= 150) &
            (F.col("row_number") <= 154),
            F.col("original_daily_consumption") * 0.25
        ).otherwise(F.col("modified_daily_consumption"))
    )
    .withColumn(
        "anomaly_type",
        F.when(
            (F.col("meter_id") == m3) &
            (F.col("row_number") >= 150) &
            (F.col("row_number") <= 154),
            "persistent_low"
        ).otherwise(F.col("anomaly_type"))
    )
    .withColumn(
        "ground_truth",
        F.when(
            (F.col("meter_id") == m3) &
            (F.col("row_number") >= 150) &
            (F.col("row_number") <= 154),
            True
        ).otherwise(F.col("ground_truth"))
    )
)

# ------------------------------------------------------------
# 9. PERSISTENT HIGH
# ------------------------------------------------------------

result = (
    result
    .withColumn(
        "modified_daily_consumption",
        F.when(
            (F.col("meter_id") == m4) &
            (F.col("row_number") >= 150) &
            (F.col("row_number") <= 154),
            F.col("original_daily_consumption") * 3.0
        ).otherwise(F.col("modified_daily_consumption"))
    )
    .withColumn(
        "anomaly_type",
        F.when(
            (F.col("meter_id") == m4) &
            (F.col("row_number") >= 150) &
            (F.col("row_number") <= 154),
            "persistent_high"
        ).otherwise(F.col("anomaly_type"))
    )
    .withColumn(
        "ground_truth",
        F.when(
            (F.col("meter_id") == m4) &
            (F.col("row_number") >= 150) &
            (F.col("row_number") <= 154),
            True
        ).otherwise(F.col("ground_truth"))
    )
)

# ------------------------------------------------------------
# 10. NIGHT-TIME ABNORMALITY
# ------------------------------------------------------------
#
# The daily feature dataset doesn't contain individual
# night-time readings. Therefore we create a daily-level
# night-consumption anomaly by modifying the daily total
# and labeling it separately.
#
# This is intentionally a daily-level anomaly, not a
# half-hourly night-reading anomaly.

result = (
    result
    .withColumn(
        "modified_daily_consumption",
        F.when(
            (F.col("meter_id") == m5) &
            (F.col("row_number") == 150),
            F.col("original_daily_consumption") * 2.5
        ).otherwise(F.col("modified_daily_consumption"))
    )
    .withColumn(
        "anomaly_type",
        F.when(
            (F.col("meter_id") == m5) &
            (F.col("row_number") == 150),
            "night_abnormality"
        ).otherwise(F.col("anomaly_type"))
    )
    .withColumn(
        "ground_truth",
        F.when(
            (F.col("meter_id") == m5) &
            (F.col("row_number") == 150),
            True
        ).otherwise(F.col("ground_truth"))
    )
)

# ------------------------------------------------------------
# 11. FINALIZE DATASET
# ------------------------------------------------------------

result = (
    result
    .drop("daily_consumption")
    .withColumnRenamed(
        "modified_daily_consumption",
        "daily_consumption"
    )
    .withColumn(
        "injection_ratio",
        F.when(
            F.col("original_daily_consumption") != 0,
            F.col("daily_consumption") /
            F.col("original_daily_consumption")
        ).otherwise(F.lit(1.0))
    )
    .drop("row_number")
)

# ------------------------------------------------------------
# 12. SAVE
# ------------------------------------------------------------

print("\nSaving injected evaluation dataset...")

(
    result
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

# ------------------------------------------------------------
# 13. VERIFICATION
# ------------------------------------------------------------

print("\n" + "=" * 70)
print("VERIFICATION")
print("=" * 70)

total_rows = result.count()

print(f"\nOutput rows: {total_rows}")

print("\nAnomaly distribution:")

(
    result
    .groupBy("anomaly_type")
    .count()
    .orderBy(F.desc("count"))
    .show(truncate=False)
)

print("\nGround-truth distribution:")

(
    result
    .groupBy("ground_truth")
    .count()
    .orderBy("ground_truth")
    .show()
)

print("\nInjected anomalies:")

(
    result
    .filter(F.col("ground_truth") == True)
    .select(
        "meter_id",
        "date",
        "original_daily_consumption",
        "daily_consumption",
        "injection_ratio",
        "anomaly_type",
        "ground_truth"
    )
    .orderBy("meter_id", "date")
    .show(50, truncate=False)
)

print("\nChecking clean records...")

clean_count = (
    result
    .filter(F.col("ground_truth") == False)
    .count()
)

anomaly_count = (
    result
    .filter(F.col("ground_truth") == True)
    .count()
)

print(f"Normal records:  {clean_count}")
print(f"Injected records: {anomaly_count}")
print(f"Total records:   {clean_count + anomaly_count}")

if clean_count + anomaly_count == total_rows:
    print("\nRow-count verification PASSED.")
else:
    print("\nWARNING: Row-count verification FAILED.")

print("\n" + "=" * 70)
print("STEP 5 COMPLETED")
print("=" * 70)

print(f"\nOutput:")
print(OUTPUT_PATH)

spark.stop()