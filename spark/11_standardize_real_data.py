from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp,
    regexp_replace,
    count,
    when,
    min,
    max,
    avg
)
from pyspark.sql.types import DoubleType


# ============================================================
# GRIDGUARD
# REAL DATA STANDARDIZATION
# ============================================================

INPUT_PATH = "data/raw/LCL-June2015v2_0.csv"

OUTPUT_PATH = "data/real/processed/standardized_1m"


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("GridGuard-Real-Data-Standardization")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# HEADER
# ============================================================

print("\n" + "=" * 70)
print("GRIDGUARD — REAL DATA STANDARDIZATION")
print("=" * 70)

print("\nInput:")
print(INPUT_PATH)

print("\nOutput:")
print(OUTPUT_PATH)


# ============================================================
# 1. READ RAW CSV
# ============================================================

print("\n" + "=" * 70)
print("1. READING REAL DATA")
print("=" * 70)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("mode", "PERMISSIVE")
    .csv(INPUT_PATH)
)

print("\nCSV loaded successfully.")


# ============================================================
# 2. SHOW ORIGINAL COLUMNS
# ============================================================

print("\nOriginal columns:")

for c in df.columns:
    print(repr(c))


# ============================================================
# 3. CLEAN COLUMN NAMES
# ============================================================

df = df.toDF(
    *[c.strip() for c in df.columns]
)

print("\nCleaned column names:")

for c in df.columns:
    print(repr(c))


# ============================================================
# 4. VERIFY REQUIRED COLUMNS
# ============================================================

required_columns = [
    "LCLid",
    "stdorToU",
    "DateTime",
    "KWH/hh (per half hour)"
]

missing_columns = [
    c for c in required_columns
    if c not in df.columns
]

if missing_columns:

    print("\nERROR:")
    print("Missing columns:")

    for c in missing_columns:
        print(f" - {c}")

    spark.stop()

    raise ValueError(
        f"Missing required columns: {missing_columns}"
    )


print("\nRequired columns verified successfully.")


# ============================================================
# 5. RENAME COLUMNS
# ============================================================

df = (
    df
    .withColumnRenamed(
        "LCLid",
        "meter_id"
    )
    .withColumnRenamed(
        "stdorToU",
        "tariff_type"
    )
    .withColumnRenamed(
        "DateTime",
        "timestamp_raw"
    )
    .withColumnRenamed(
        "KWH/hh (per half hour)",
        "consumption_raw"
    )
)


# ============================================================
# 6. STANDARDIZE TIMESTAMP
# ============================================================

print("\n" + "=" * 70)
print("2. STANDARDIZING TIMESTAMP")
print("=" * 70)


# Actual data contains timestamps like:
#
# 2012-10-12 00:30:00.0000000
#
# Spark timestamp parser expects up to microsecond precision.
#
# Therefore:
#
# .0000000
#
# becomes:
#
# .000000
#
# We remove only the seventh fractional digit.


df = df.withColumn(
    "timestamp_clean",
    regexp_replace(
        trim(col("timestamp_raw")),
        r"(\.\d{6})\d$",
        r"$1"
    )
)


df = df.withColumn(
    "timestamp",
    to_timestamp(
        col("timestamp_clean"),
        "yyyy-MM-dd HH:mm:ss.SSSSSS"
    )
)


# ============================================================
# 7. STANDARDIZE CONSUMPTION
# ============================================================

print("\n" + "=" * 70)
print("3. STANDARDIZING CONSUMPTION")
print("=" * 70)


# The dataset contains literal strings such as:
#
# "Null"
#
# These must be converted to actual NULL values before
# converting the column to DOUBLE.


df = df.withColumn(
    "consumption_clean",
    when(
        trim(col("consumption_raw")).isin(
            "Null",
            "null",
            "",
            "NA",
            "N/A",
            "NULL"
        ),
        None
    ).otherwise(
        trim(col("consumption_raw"))
    )
)


# Convert valid numeric strings to DOUBLE.
#
# Because the known non-numeric values above were converted
# to NULL first, the cast will not fail on those values.


df = df.withColumn(
    "consumption",
    col("consumption_clean").cast(DoubleType())
)


# ============================================================
# 8. SELECT FINAL STANDARDIZED COLUMNS
# ============================================================

df_clean = df.select(
    "meter_id",
    "tariff_type",
    "timestamp",
    "consumption"
)


# ============================================================
# 9. DATA QUALITY CHECK
# ============================================================

print("\n" + "=" * 70)
print("4. DATA QUALITY CHECK")
print("=" * 70)


# ------------------------------------------------------------
# Total rows
# ------------------------------------------------------------

total_rows = df_clean.count()

print(
    f"\nTotal rows: {total_rows:,}"
)


# ------------------------------------------------------------
# NULL COUNTS
# ------------------------------------------------------------

null_summary = df_clean.select(

    count(
        when(
            col("meter_id").isNull(),
            True
        )
    ).alias("null_meter_id"),

    count(
        when(
            col("tariff_type").isNull(),
            True
        )
    ).alias("null_tariff_type"),

    count(
        when(
            col("timestamp").isNull(),
            True
        )
    ).alias("null_timestamp"),

    count(
        when(
            col("consumption").isNull(),
            True
        )
    ).alias("null_consumption")
)


print("\nNULL counts:")

null_summary.show(
    truncate=False
)


# ============================================================
# 10. MISSING CONSUMPTION
# ============================================================

missing_consumption = (
    df_clean
    .filter(
        col("consumption").isNull()
    )
    .count()
)

print(
    f"\nMissing consumption values: "
    f"{missing_consumption:,}"
)


# ============================================================
# 11. INVALID TIMESTAMPS
# ============================================================

invalid_timestamps = (
    df_clean
    .filter(
        col("timestamp").isNull()
    )
    .count()
)

print(
    f"Invalid timestamp values: "
    f"{invalid_timestamps:,}"
)


# ============================================================
# 12. NEGATIVE CONSUMPTION
# ============================================================

negative_consumption = (
    df_clean
    .filter(
        col("consumption") < 0
    )
    .count()
)

print(
    f"Negative consumption values: "
    f"{negative_consumption:,}"
)


# ============================================================
# 13. VALID CONSUMPTION RECORDS
# ============================================================

valid_consumption = (
    df_clean
    .filter(
        col("consumption").isNotNull()
    )
    .count()
)

print(
    f"Valid consumption records: "
    f"{valid_consumption:,}"
)


# ============================================================
# 14. CONSUMPTION STATISTICS
# ============================================================

print("\n" + "=" * 70)
print("5. CONSUMPTION STATISTICS")
print("=" * 70)


df_clean.select(

    min("consumption")
    .alias("min_consumption"),

    max("consumption")
    .alias("max_consumption"),

    avg("consumption")
    .alias("avg_consumption")

).show(
    truncate=False
)


# ============================================================
# 15. UNIQUE METERS
# ============================================================

print("\n" + "=" * 70)
print("6. METER INFORMATION")
print("=" * 70)


unique_meters = (
    df_clean
    .select("meter_id")
    .distinct()
    .count()
)

print(
    f"\nUnique meters: "
    f"{unique_meters:,}"
)


# ============================================================
# 16. TARIFF TYPES
# ============================================================

print("\nTariff types:")

(
    df_clean
    .groupBy("tariff_type")
    .count()
    .orderBy("tariff_type")
    .show(
        truncate=False
    )
)


# ============================================================
# 17. TIMESTAMP RANGE
# ============================================================

print("\n" + "=" * 70)
print("7. TIME RANGE")
print("=" * 70)


df_clean.select(

    min("timestamp")
    .alias("min_timestamp"),

    max("timestamp")
    .alias("max_timestamp")

).show(
    truncate=False
)


# ============================================================
# 18. SAMPLE DATA
# ============================================================

print("\n" + "=" * 70)
print("8. SAMPLE STANDARDIZED DATA")
print("=" * 70)


(
    df_clean
    .orderBy(
        "meter_id",
        "timestamp"
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# 19. FINAL SCHEMA
# ============================================================

print("\n" + "=" * 70)
print("9. FINAL SCHEMA")
print("=" * 70)

df_clean.printSchema()


# ============================================================
# 20. WRITE PARQUET
# ============================================================

print("\n" + "=" * 70)
print("10. WRITING PARQUET")
print("=" * 70)


(
    df_clean
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)


print("\nParquet written successfully.")


# ============================================================
# 21. VERIFY PARQUET
# ============================================================

print("\n" + "=" * 70)
print("11. VERIFYING PARQUET")
print("=" * 70)


verification_df = (
    spark.read
    .parquet(OUTPUT_PATH)
)


verification_count = (
    verification_df.count()
)


print(
    f"\nParquet row count: "
    f"{verification_count:,}"
)


print("\nParquet schema:")

verification_df.printSchema()


print("\nParquet sample:")

(
    verification_df
    .orderBy(
        "meter_id",
        "timestamp"
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# 22. VERIFY ROW COUNT
# ============================================================

if verification_count != total_rows:

    print("\nWARNING:")
    print(
        f"Input rows:  {total_rows:,}"
    )
    print(
        f"Output rows: {verification_count:,}"
    )

else:

    print("\nRow-count verification PASSED.")


# ============================================================
# 23. FINAL MESSAGE
# ============================================================

print("\n" + "=" * 70)
print("STANDARDIZATION COMPLETE")
print("=" * 70)

print(
    f"\nInput rows: "
    f"{total_rows:,}"
)

print(
    f"Output rows: "
    f"{verification_count:,}"
)

print(
    f"Missing consumption: "
    f"{missing_consumption:,}"
)

print(
    f"Invalid timestamps: "
    f"{invalid_timestamps:,}"
)

print(
    f"Negative consumption: "
    f"{negative_consumption:,}"
)

print(
    f"\nParquet location:"
)

print(
    OUTPUT_PATH
)

print(
    "\nGridGuard real-data standardization "
    "finished successfully."
)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()