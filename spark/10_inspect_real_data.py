from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    min as spark_min,
    max as spark_max
)
from pathlib import Path


# =========================================================
# GRIDGUARD
# STEP 1 — REAL DATA INSPECTION
# =========================================================
#
# Dataset:
# London Smart Meter Energy Consumption Dataset
#
# Location:
# C:\projects\GridGuard\data\raw\
#
# Expected files:
# LCL-June2015v2_0.csv
# LCL-June2015v2_1.csv
# ...
# LCL-June2015v2_166.csv
#
# IMPORTANT:
# This script DOES NOT process all 167 files.
# It only inspects the first CSV file to understand:
#   - schema
#   - columns
#   - number of rows
#   - sample records
#   - data types
#   - missing values
#   - timestamp range
#   - consumption columns
#
# =========================================================


# =========================================================
# 1. PROJECT PATHS
# =========================================================

BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "data" / "raw"


# =========================================================
# 2. START SPARK
# =========================================================

spark = (
    SparkSession.builder
    .appName("GridGuard-Real-Data-Inspection")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print()
print("=" * 70)
print("GRIDGUARD — REAL DATA INSPECTION")
print("=" * 70)

print(f"\nProject directory:")
print(BASE_DIR)

print(f"\nReal dataset directory:")
print(RAW_DIR)


# =========================================================
# 3. FIND ALL LONDON DATASET FILES
# =========================================================

csv_files = sorted(
    RAW_DIR.glob("LCL-June2015v2_*.csv")
)


print()
print("=" * 70)
print("DATASET FILE CHECK")
print("=" * 70)

print(f"\nCSV files found: {len(csv_files)}")


if not csv_files:

    print()
    print("ERROR: No London Smart Meter CSV files were found.")
    print()
    print("Expected location:")
    print(RAW_DIR)
    print()
    print("Expected filename pattern:")
    print("LCL-June2015v2_*.csv")
    print()

    spark.stop()
    raise SystemExit(1)


# =========================================================
# 4. DISPLAY FILE INFORMATION
# =========================================================

print("\nFirst 10 files:")

for file in csv_files[:10]:

    size_mb = file.stat().st_size / (1024 * 1024)

    print(
        f"{file.name:<40} "
        f"{size_mb:>10.2f} MB"
    )


# =========================================================
# 5. TOTAL DATASET SIZE
# =========================================================

total_size_bytes = sum(
    file.stat().st_size
    for file in csv_files
)

total_size_gb = (
    total_size_bytes /
    (1024 ** 3)
)


print()
print(f"Total dataset size: {total_size_gb:.2f} GB")


# =========================================================
# 6. SELECT FIRST FILE
# =========================================================
#
# We intentionally inspect only one file.
#
# This prevents your local machine from immediately
# processing the complete ~167M-row dataset.
#
# =========================================================

first_file = csv_files[0]

print()
print("=" * 70)
print("FIRST FILE SELECTED FOR INSPECTION")
print("=" * 70)

print(f"\nFile:")
print(first_file)

print(
    f"\nFile size: "
    f"{first_file.stat().st_size / (1024 * 1024):.2f} MB"
)


# =========================================================
# 7. READ FIRST FILE
# =========================================================

print()
print("Reading first CSV...")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("mode", "PERMISSIVE")
    .csv(str(first_file))
)


print("File loaded successfully.")


# =========================================================
# 8. DISPLAY SCHEMA
# =========================================================

print()
print("=" * 70)
print("SCHEMA")
print("=" * 70)

df.printSchema()


# =========================================================
# 9. DISPLAY COLUMN NAMES
# =========================================================

print()
print("=" * 70)
print("COLUMN NAMES")
print("=" * 70)

print()

for index, column_name in enumerate(
    df.columns,
    start=1
):

    print(
        f"{index:>3}. {column_name}"
    )


# =========================================================
# 10. ROW COUNT
# =========================================================

print()
print("=" * 70)
print("ROW COUNT")
print("=" * 70)

row_count = df.count()

print(
    f"\nRows in first file: "
    f"{row_count:,}"
)


# =========================================================
# 11. COLUMN COUNT
# =========================================================

print(
    f"Columns in first file: "
    f"{len(df.columns)}"
)


# =========================================================
# 12. SAMPLE DATA
# =========================================================

print()
print("=" * 70)
print("FIRST 10 RECORDS")
print("=" * 70)

df.show(
    10,
    truncate=False
)


# =========================================================
# 13. DATA TYPES
# =========================================================

print()
print("=" * 70)
print("DATA TYPES")
print("=" * 70)

print()

for field in df.schema.fields:

    print(
        f"{field.name:<40}"
        f"{str(field.dataType)}"
    )


# =========================================================
# 14. NULL VALUE ANALYSIS
# =========================================================

print()
print("=" * 70)
print("NULL VALUE ANALYSIS")
print("=" * 70)

null_expressions = []

for column_name in df.columns:

    null_expressions.append(
        spark_sum(
            col(column_name)
            .isNull()
            .cast("long")
        ).alias(column_name)
    )


null_counts = df.select(
    null_expressions
)


null_counts.show(
    truncate=False
)


# =========================================================
# 15. NON-NULL COUNTS
# =========================================================

print()
print("=" * 70)
print("NON-NULL COUNTS")
print("=" * 70)

non_null_expressions = []

for column_name in df.columns:

    non_null_expressions.append(
        count(
            col(column_name)
        ).alias(column_name)
    )


non_null_counts = df.select(
    non_null_expressions
)


non_null_counts.show(
    truncate=False
)


# =========================================================
# 16. DISTINCT VALUE COUNTS
# =========================================================

print()
print("=" * 70)
print("DISTINCT VALUE COUNTS")
print("=" * 70)

for column_name in df.columns:

    try:

        distinct_count = (
            df.select(column_name)
            .distinct()
            .count()
        )

        print(
            f"{column_name:<40}"
            f"{distinct_count:,}"
        )

    except Exception as error:

        print(
            f"{column_name:<40}"
            f"Could not calculate"
        )


# =========================================================
# 17. AUTOMATIC IDENTIFICATION OF POSSIBLE
#     TIMESTAMP / DATE COLUMNS
# =========================================================

print()
print("=" * 70)
print("POSSIBLE TIMESTAMP / DATE COLUMNS")
print("=" * 70)

possible_time_columns = []

for column_name in df.columns:

    column_lower = column_name.lower()

    if any(
        keyword in column_lower
        for keyword in [
            "date",
            "time",
            "datetime",
            "timestamp"
        ]
    ):

        possible_time_columns.append(
            column_name
        )


if possible_time_columns:

    for column_name in possible_time_columns:

        print(
            f"\nColumn: {column_name}"
        )

        try:

            df.select(
                spark_min(
                    col(column_name)
                ).alias("minimum"),
                spark_max(
                    col(column_name)
                ).alias("maximum")
            ).show(
                truncate=False
            )

        except Exception:

            print(
                "Could not calculate range."
            )

else:

    print(
        "\nNo obvious timestamp/date column "
        "was automatically identified."
    )


# =========================================================
# 18. AUTOMATIC IDENTIFICATION OF POSSIBLE
#     CONSUMPTION COLUMNS
# =========================================================

print()
print("=" * 70)
print("POSSIBLE CONSUMPTION / ENERGY COLUMNS")
print("=" * 70)

possible_consumption_columns = []

for column_name in df.columns:

    column_lower = column_name.lower()

    if any(
        keyword in column_lower
        for keyword in [
            "consumption",
            "energy",
            "kwh",
            "power",
            "load"
        ]
    ):

        possible_consumption_columns.append(
            column_name
        )


if possible_consumption_columns:

    for column_name in possible_consumption_columns:

        print(
            f"\nPossible consumption column: "
            f"{column_name}"
        )

        try:

            df.select(
                spark_min(
                    col(column_name)
                ).alias("minimum"),
                spark_max(
                    col(column_name)
                ).alias("maximum")
            ).show(
                truncate=False
            )

        except Exception:

            print(
                "Could not calculate numeric range."
            )

else:

    print(
        "\nNo obvious consumption/energy "
        "column was automatically identified."
    )


# =========================================================
# 19. FINAL SUMMARY
# =========================================================

print()
print("=" * 70)
print("INSPECTION SUMMARY")
print("=" * 70)

print(
    f"\nDataset files found : {len(csv_files):,}"
)

print(
    f"Total dataset size  : {total_size_gb:.2f} GB"
)

print(
    f"Inspected file      : {first_file.name}"
)

print(
    f"Rows inspected      : {row_count:,}"
)

print(
    f"Columns             : {len(df.columns):,}"
)

print(
    f"Possible timestamps  : "
    f"{possible_time_columns}"
)

print(
    f"Possible consumption : "
    f"{possible_consumption_columns}"
)


print()
print("=" * 70)
print("STEP 1 COMPLETE")
print("=" * 70)

print(
    "\nThe complete dataset was NOT processed."
)

print(
    "Only the first CSV file was inspected."
)

print(
    "\nNext step: standardize the real dataset "
    "into GridGuard format."
)

print()


# =========================================================
# 20. STOP SPARK
# =========================================================

spark.stop()