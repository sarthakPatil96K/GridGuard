from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ============================================================
# GRIDGUARD - STEP 11B
# TEMPORAL TRAIN / TEST SPLIT + CONTROLLED ANOMALY INJECTION
# ============================================================

INPUT_PATH = "data/real/processed/final_daily_features_1m"

TRAIN_OUTPUT = "data/real/processed/temporal_train_1m"
TEST_CLEAN_OUTPUT = "data/real/processed/temporal_test_clean_1m"
TEST_INJECTED_OUTPUT = "data/real/processed/temporal_test_injected_1m"

spark = (
    SparkSession.builder
    .appName("GridGuard_Temporal_Split")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 75)
print("GRIDGUARD - STEP 11B")
print("TEMPORAL TRAIN / TEST SPLIT + CONTROLLED ANOMALY INJECTION")
print("=" * 75)


# ============================================================
# 1. READ DAILY FEATURES
# ============================================================

print("\nReading final daily features...")

df = spark.read.parquet(INPUT_PATH)

df = (
    df
    .filter(F.col("meter_id").isNotNull())
    .filter(F.col("date").isNotNull())
    .filter(F.col("daily_consumption").isNotNull())
    .filter(F.col("daily_consumption") >= 0)
)

input_count = df.count()

print(f"Valid daily rows: {input_count:,}")


# ============================================================
# 2. ADD CHRONOLOGICAL ROW NUMBER PER METER
# ============================================================

print("\nCreating chronological ordering per meter...")

order_window = Window.partitionBy("meter_id").orderBy("date")

df = (
    df
    .withColumn(
        "day_number",
        F.row_number().over(order_window)
    )
)

meter_counts = (
    df.groupBy("meter_id")
      .agg(F.max("day_number").alias("total_days"))
)

df = df.join(
    meter_counts,
    on="meter_id",
    how="left"
)


# ============================================================
# 3. 70/30 TEMPORAL SPLIT
# ============================================================

print("\nCreating 70/30 chronological split...")

# First 70% = training
# Last 30% = testing

df = df.withColumn(
    "split_point",
    F.greatest(
        F.lit(1),
        F.floor(F.col("total_days") * 0.70)
    )
)

df = df.withColumn(
    "dataset_split",
    F.when(
        F.col("day_number") <= F.col("split_point"),
        F.lit("TRAIN")
    ).otherwise(
        F.lit("TEST")
    )
)

train_df = (
    df
    .filter(F.col("dataset_split") == "TRAIN")
    .drop("split_point", "dataset_split")
)

test_clean_df = (
    df
    .filter(F.col("dataset_split") == "TEST")
    .drop("split_point", "dataset_split")
)


# ============================================================
# 4. DISPLAY SPLIT INFORMATION
# ============================================================

train_count = train_df.count()
test_count = test_clean_df.count()

print("\n" + "=" * 75)
print("TEMPORAL SPLIT RESULTS")
print("=" * 75)

print(f"Training rows : {train_count:,}")
print(f"Testing rows  : {test_count:,}")
print(f"Total rows    : {train_count + test_count:,}")

print("\nTraining date range:")
train_df.select(
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date")
).show(truncate=False)

print("Testing date range:")
test_clean_df.select(
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date")
).show(truncate=False)


# ============================================================
# 5. CHECK EACH METER'S SPLIT
# ============================================================

print("\nPer-meter split verification...")

split_check = (
    df.groupBy("meter_id")
      .agg(
          F.min(
              F.when(
                  F.col("dataset_split") == "TRAIN",
                  F.col("date")
              )
          ).alias("train_start"),

          F.max(
              F.when(
                  F.col("dataset_split") == "TRAIN",
                  F.col("date")
              )
          ).alias("train_end"),

          F.min(
              F.when(
                  F.col("dataset_split") == "TEST",
                  F.col("date")
              )
          ).alias("test_start"),

          F.max(
              F.when(
                  F.col("dataset_split") == "TEST",
                  F.col("date")
              )
          ).alias("test_end"),

          F.count("*").alias("total_days")
      )
      .orderBy("meter_id")
)

split_check.show(30, truncate=False)


# ============================================================
# 6. WRITE TRAINING DATA
# ============================================================

print("\nWriting temporal training data...")

(
    train_df
    .drop("day_number", "total_days")
    .write
    .mode("overwrite")
    .parquet(TRAIN_OUTPUT)
)


# ============================================================
# 7. WRITE CLEAN TEST DATA
# ============================================================

print("Writing clean temporal test data...")

(
    test_clean_df
    .drop("day_number", "total_days")
    .write
    .mode("overwrite")
    .parquet(TEST_CLEAN_OUTPUT)
)


# ============================================================
# 8. SELECT METERS FOR CONTROLLED ANOMALY INJECTION
# ============================================================

print("\nSelecting meters for controlled anomaly injection...")

# Only use meters with enough test observations.
eligible_meters = (
    test_clean_df
    .groupBy("meter_id")
    .agg(
        F.count("*").alias("test_days")
    )
    .filter(F.col("test_days") >= 100)
    .orderBy("meter_id")
)

eligible_list = [
    row["meter_id"]
    for row in eligible_meters.limit(10).collect()
]

print(f"Eligible meters: {len(eligible_list)}")

for meter in eligible_list:
    print(f"  {meter}")


# ============================================================
# 9. ADD TEST-DAY ROW NUMBER
# ============================================================

test_window = Window.partitionBy("meter_id").orderBy("date")

test_injection_df = (
    test_clean_df
    .withColumn(
        "test_day_number",
        F.row_number().over(test_window)
    )
)


# ============================================================
# 10. CONTROLLED ANOMALY PLAN
# ============================================================

# We intentionally use anomalies only in TEST data.
#
# Meter 1 -> sudden spike
# Meter 2 -> sudden drop
# Meter 3 -> persistent low
# Meter 4 -> persistent high
#
# We do NOT use "night_abnormality" here because this is
# daily-level data. A proper night anomaly requires modifying
# half-hourly readings before daily aggregation.

anomaly_plan = {}

if len(eligible_list) >= 4:

    anomaly_plan[eligible_list[0]] = {
        "type": "sudden_spike",
        "start_day": 30,
        "end_day": 30,
        "factor": 4.0
    }

    anomaly_plan[eligible_list[1]] = {
        "type": "sudden_drop",
        "start_day": 30,
        "end_day": 30,
        "factor": 0.20
    }

    anomaly_plan[eligible_list[2]] = {
        "type": "persistent_low",
        "start_day": 30,
        "end_day": 34,
        "factor": 0.25
    }

    anomaly_plan[eligible_list[3]] = {
        "type": "persistent_high",
        "start_day": 30,
        "end_day": 34,
        "factor": 3.0
    }


print("\n" + "=" * 75)
print("ANOMALY INJECTION PLAN")
print("=" * 75)

for meter_id, plan in anomaly_plan.items():
    print(
        f"{meter_id} -> "
        f"{plan['type']} | "
        f"test days {plan['start_day']}-{plan['end_day']} | "
        f"factor {plan['factor']}"
    )


# ============================================================
# 11. APPLY CONTROLLED ANOMALIES
# ============================================================

print("\nInjecting controlled anomalies into TEST period...")

# Default values
test_injection_df = (
    test_injection_df
    .withColumn(
        "original_daily_consumption",
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
    .withColumn(
        "injection_ratio",
        F.lit(1.0)
    )
)


# Apply each anomaly using when conditions.
for meter_id, plan in anomaly_plan.items():

    condition = (
        (F.col("meter_id") == meter_id)
        & (F.col("test_day_number") >= plan["start_day"])
        & (F.col("test_day_number") <= plan["end_day"])
    )

    modified_value = (
        F.col("original_daily_consumption")
        * F.lit(plan["factor"])
    )

    test_injection_df = (
        test_injection_df
        .withColumn(
            "daily_consumption",
            F.when(
                condition,
                modified_value
            ).otherwise(
                F.col("daily_consumption")
            )
        )
        .withColumn(
            "anomaly_type",
            F.when(
                condition,
                F.lit(plan["type"])
            ).otherwise(
                F.col("anomaly_type")
            )
        )
        .withColumn(
            "ground_truth",
            F.when(
                condition,
                F.lit(True)
            ).otherwise(
                F.col("ground_truth")
            )
        )
        .withColumn(
            "injection_ratio",
            F.when(
                condition,
                F.lit(plan["factor"])
            ).otherwise(
                F.col("injection_ratio")
            )
        )
    )


# ============================================================
# 12. WRITE INJECTED TEST DATA
# ============================================================

final_test_injected = (
    test_injection_df
    .drop("test_day_number")
)

print("\nWriting injected test dataset...")

(
    final_test_injected
    .write
    .mode("overwrite")
    .parquet(TEST_INJECTED_OUTPUT)
)


# ============================================================
# 13. VALIDATION
# ============================================================

final_count = final_test_injected.count()

ground_truth_count = (
    final_test_injected
    .filter(F.col("ground_truth") == True)
    .count()
)

print("\n" + "=" * 75)
print("ANOMALY INJECTION RESULTS")
print("=" * 75)

print(f"Test rows              : {final_count:,}")
print(f"Ground-truth anomalies : {ground_truth_count:,}")


print("\nAnomaly distribution:")

(
    final_test_injected
    .groupBy("anomaly_type")
    .agg(
        F.count("*").alias("records")
    )
    .orderBy("anomaly_type")
    .show(truncate=False)
)


# ============================================================
# 14. SHOW INJECTED RECORDS
# ============================================================

print("\nInjected anomaly records:")

(
    final_test_injected
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


# ============================================================
# 15. ROW COUNT INTEGRITY
# ============================================================

print("\n" + "=" * 75)
print("ROW COUNT VERIFICATION")
print("=" * 75)

if final_count == test_count:
    print("SUCCESS: Test row count preserved.")
else:
    print(
        f"ERROR: Expected {test_count:,}, "
        f"got {final_count:,}"
    )


# ============================================================
# 16. OUTPUT PATHS
# ============================================================

print("\n" + "=" * 75)
print("OUTPUT PATHS")
print("=" * 75)

print(f"Training data : {TRAIN_OUTPUT}")
print(f"Clean test    : {TEST_CLEAN_OUTPUT}")
print(f"Injected test : {TEST_INJECTED_OUTPUT}")


print("\n" + "=" * 75)
print("STEP 11B COMPLETED")
print("=" * 75)

spark.stop()