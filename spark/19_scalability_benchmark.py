"""
GridGuard - Step 10
Big Data Scalability Benchmark

Compares Pandas and PySpark processing performance
on progressively larger portions of the real smart-meter dataset.
"""

import os
import time
import glob
import gc
import csv

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURATION
# ============================================================

RAW_DIR = r"data/raw"
OUTPUT_DIR = r"data/real/processed/scalability_benchmark"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Benchmark sizes
BENCHMARK_SIZES = [
    100_000,
    500_000,
    1_000_000,
    5_000_000,
    10_000_000,
]

# Set True only if you want to continue toward very large datasets
RUN_LARGE_TESTS = False

if RUN_LARGE_TESTS:
    BENCHMARK_SIZES.extend([
        25_000_000,
        50_000_000,
        100_000_000,
    ])


# ============================================================
# FIND INPUT FILES
# ============================================================

csv_files = sorted(
    glob.glob(os.path.join(RAW_DIR, "*.csv"))
)

if not csv_files:
    raise FileNotFoundError(
        f"No CSV files found in {RAW_DIR}"
    )

print("=" * 75)
print("GRIDGUARD - BIG DATA SCALABILITY BENCHMARK")
print("=" * 75)

print(f"CSV files found: {len(csv_files)}")

# Total size
total_size_gb = (
    sum(os.path.getsize(f) for f in csv_files)
    / (1024 ** 3)
)

print(f"Total raw dataset size: {total_size_gb:.2f} GB")

print("\nBenchmark sizes:")
for size in BENCHMARK_SIZES:
    print(f"  {size:,} records")


# ============================================================
# HELPER - LOAD FIRST N ROWS USING PANDAS
# ============================================================

def load_pandas_n_rows(n_rows):
    """
    Reads enough CSV files to obtain approximately n_rows.
    """

    frames = []
    rows_remaining = n_rows

    for file_path in csv_files:

        if rows_remaining <= 0:
            break

        # Read only required number of rows.
        df = pd.read_csv(
            file_path,
            nrows=rows_remaining
        )

        frames.append(df)

        rows_remaining -= len(df)

        if len(df) == 0:
            break

    if not frames:
        return pd.DataFrame()

    return pd.concat(
        frames,
        ignore_index=True
    )


# ============================================================
# PANDAS BENCHMARK
# ============================================================

def benchmark_pandas(n_rows):

    print("\n" + "-" * 75)
    print(f"PANDAS - {n_rows:,} RECORDS")
    print("-" * 75)

    gc.collect()

    start = time.perf_counter()

    df = load_pandas_n_rows(n_rows)

    if df.empty:
        return {
            "framework": "Pandas",
            "records": 0,
            "execution_time_sec": 0,
            "throughput_records_sec": 0,
            "memory_mb": 0,
        }

    # Clean column names
    df.columns = [
        c.strip()
        for c in df.columns
    ]

    # Standardize timestamp
    if "DateTime" in df.columns:
        df["DateTime"] = pd.to_datetime(
            df["DateTime"],
            errors="coerce"
        )

    # Standardize consumption
    consumption_col = "KWH/hh (per half hour)"

    if consumption_col in df.columns:

        df["consumption"] = pd.to_numeric(
            df[consumption_col],
            errors="coerce"
        )

        # Simple analytical aggregation
        result = (
            df.groupby("LCLid")["consumption"]
            .agg(
                avg_consumption="mean",
                max_consumption="max",
                min_consumption="min",
                std_consumption="std",
                reading_count="count",
            )
            .reset_index()
        )

    else:

        result = (
            df.groupby("LCLid")
            .size()
            .reset_index(name="reading_count")
        )

    elapsed = time.perf_counter() - start

    records = len(df)

    throughput = (
        records / elapsed
        if elapsed > 0
        else 0
    )

    memory_mb = (
        df.memory_usage(deep=True)
        .sum()
        / (1024 ** 2)
    )

    print(f"Records processed : {records:,}")
    print(f"Execution time    : {elapsed:.2f} sec")
    print(f"Throughput        : {throughput:,.0f} records/sec")
    print(f"Memory usage      : {memory_mb:.2f} MB")
    print(f"Result meters     : {len(result):,}")

    del df
    del result

    gc.collect()

    return {
        "framework": "Pandas",
        "records": records,
        "execution_time_sec": round(elapsed, 4),
        "throughput_records_sec": round(throughput, 2),
        "memory_mb": round(memory_mb, 2),
    }


# ============================================================
# CREATE SPARK SESSION
# ============================================================

print("\nStarting Spark...")

spark = (
    SparkSession.builder
    .appName("GridGuard-Scalability-Benchmark")
    .master("local[2]")
    .config(
        "spark.sql.shuffle.partitions",
        "4"
    )
    .config(
        "spark.driver.memory",
        "4g"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("Spark started successfully.")


# ============================================================
# LOAD DATA WITH SPARK
# ============================================================

print("\nReading CSV files with Spark...")

spark_start = time.perf_counter()

spark_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(csv_files)
)

# Force Spark to actually read the data
total_spark_rows = spark_df.count()

spark_load_time = (
    time.perf_counter() - spark_start
)

print(
    f"Total records available to Spark: "
    f"{total_spark_rows:,}"
)

print(
    f"Initial Spark load/count time: "
    f"{spark_load_time:.2f} sec"
)


# ============================================================
# CLEAN SPARK DATA
# ============================================================

spark_df = spark_df.toDF(
    *[
        c.strip()
        for c in spark_df.columns
    ]
)

spark_df = (
    spark_df
    .withColumn(
        "timestamp",
        F.to_timestamp(
            "DateTime"
        )
    )
    .withColumn(
        "consumption",
        F.when(
            F.col(
                "KWH/hh (per half hour)"
            ).isin(
                "Null",
                "null",
                "",
                "NA",
                "N/A"
            ),
            None
        )
        .otherwise(
            F.col(
                "KWH/hh (per half hour)"
            ).cast("double")
        )
    )
    .select(
        "LCLid",
        "timestamp",
        "consumption"
    )
)


# ============================================================
# CACHE FOR REPEATED BENCHMARKS
# ============================================================

print("\nCaching cleaned Spark dataset...")

spark_df.cache()

cached_count = spark_df.count()

print(
    f"Cached Spark records: "
    f"{cached_count:,}"
)


# ============================================================
# SPARK BENCHMARK
# ============================================================

def benchmark_spark(n_rows):

    print("\n" + "-" * 75)
    print(f"PYSPARK - {n_rows:,} RECORDS")
    print("-" * 75)

    gc.collect()

    # Limit dataset for benchmark
    df = spark_df.limit(n_rows)

    start = time.perf_counter()

    result = (
        df.groupBy("LCLid")
        .agg(
            F.avg("consumption")
            .alias("avg_consumption"),

            F.max("consumption")
            .alias("max_consumption"),

            F.min("consumption")
            .alias("min_consumption"),

            F.stddev("consumption")
            .alias("std_consumption"),

            F.count("*")
            .alias("reading_count"),
        )
    )

    # Force execution
    result_count = result.count()

    elapsed = time.perf_counter() - start

    records = min(
        n_rows,
        total_spark_rows
    )

    throughput = (
        records / elapsed
        if elapsed > 0
        else 0
    )

    print(
        f"Records processed : {records:,}"
    )

    print(
        f"Execution time    : {elapsed:.2f} sec"
    )

    print(
        f"Throughput        : "
        f"{throughput:,.0f} records/sec"
    )

    print(
        f"Result meters     : "
        f"{result_count:,}"
    )

    return {
        "framework": "PySpark",
        "records": records,
        "execution_time_sec": round(
            elapsed,
            4
        ),
        "throughput_records_sec": round(
            throughput,
            2
        ),
        "memory_mb": None,
    }


# ============================================================
# RUN BENCHMARK
# ============================================================

results = []

for size in BENCHMARK_SIZES:

    # Don't request more records than available
    if size > total_spark_rows:
        print(
            f"\nSkipping {size:,}: "
            f"only {total_spark_rows:,} records available."
        )
        continue

    # Pandas
    pandas_result = benchmark_pandas(size)

    results.append(
        pandas_result
    )

    # Spark
    spark_result = benchmark_spark(size)

    results.append(
        spark_result
    )


# ============================================================
# SAVE RESULTS
# ============================================================

results_df = pd.DataFrame(results)

results_path = os.path.join(
    OUTPUT_DIR,
    "scalability_results.csv"
)

results_df.to_csv(
    results_path,
    index=False
)

print("\n" + "=" * 75)
print("BENCHMARK RESULTS")
print("=" * 75)

print(
    results_df.to_string(
        index=False
    )
)

print("\nResults saved to:")
print(results_path)


# ============================================================
# CREATE COMPARISON TABLE
# ============================================================

comparison = []

for size in BENCHMARK_SIZES:

    pandas_rows = results_df[
        (results_df["framework"] == "Pandas")
        &
        (results_df["records"] == size)
    ]

    spark_rows = results_df[
        (results_df["framework"] == "PySpark")
        &
        (results_df["records"] == size)
    ]

    if (
        pandas_rows.empty
        or spark_rows.empty
    ):
        continue

    pandas_time = pandas_rows.iloc[0][
        "execution_time_sec"
    ]

    spark_time = spark_rows.iloc[0][
        "execution_time_sec"
    ]

    speedup = (
        pandas_time / spark_time
        if spark_time > 0
        else None
    )

    comparison.append({
        "records": size,
        "pandas_time_sec": pandas_time,
        "spark_time_sec": spark_time,
        "spark_speedup_vs_pandas": (
            round(speedup, 3)
            if speedup is not None
            else None
        ),
    })


comparison_df = pd.DataFrame(
    comparison
)

comparison_path = os.path.join(
    OUTPUT_DIR,
    "pandas_vs_spark_comparison.csv"
)

comparison_df.to_csv(
    comparison_path,
    index=False
)


print("\n" + "=" * 75)
print("PANDAS VS PYSPARK")
print("=" * 75)

print(
    comparison_df.to_string(
        index=False
    )
)

print("\nComparison saved to:")
print(comparison_path)


# ============================================================
# CLEANUP
# ============================================================

spark_df.unpersist()

spark.stop()

print("\n" + "=" * 75)
print("STEP 10 COMPLETED")
print("=" * 75)