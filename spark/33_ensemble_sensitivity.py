from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    IntegerType,
    StringType
)


# ============================================================
# GRIDGUARD - ENSEMBLE SENSITIVITY ANALYSIS
# ============================================================

GOLD_PATH = r"C:\projects\GridGuard\data\parquet\gold"

# IMPORTANT:
# Isolation Forest output must be OUTSIDE the gold directory.
IF_PATH = r"C:\projects\GridGuard\data\parquet\isolation_forest"

OUTPUT_PATH = r"C:\projects\GridGuard\data\parquet\ensemble_sensitivity"


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

import os
import sys

PYTHON_EXECUTABLE = sys.executable

os.environ["PYSPARK_PYTHON"] = PYTHON_EXECUTABLE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXECUTABLE

spark = (
    SparkSession.builder
    .appName("GridGuard-Ensemble-Sensitivity")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.pyspark.python", PYTHON_EXECUTABLE)
    .config("spark.pyspark.driver.python", PYTHON_EXECUTABLE)
    .config("spark.python.worker.reuse", "true")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("GRIDGUARD - ENSEMBLE SENSITIVITY ANALYSIS")
print("=" * 70)


# ============================================================
# 2. LOAD ISOLATION FOREST RESULTS
# ============================================================

print("\n[1] Loading Isolation Forest results...")

df = spark.read.parquet(IF_PATH)

total_records = df.count()
total_meters = df.select("meter_id").distinct().count()

print(f"Records: {total_records:,}")
print(f"Meters: {total_meters:,}")


# ============================================================
# 3. CHECK REQUIRED COLUMNS
# ============================================================

required_columns = [
    "meter_id",
    "date",
    "contextual_score",
    "if_score",
    "signal_count",
    "daily_consumption"
]

missing_columns = [
    column
    for column in required_columns
    if column not in df.columns
]

if missing_columns:
    spark.stop()

    raise ValueError(
        "The following required columns are missing from "
        f"Isolation Forest output: {missing_columns}"
    )


print("\nRequired columns verified successfully.")


# ============================================================
# 4. CLEAN / STANDARDIZE NUMERIC COLUMNS
# ============================================================

df = (
    df
    .withColumn(
        "contextual_score",
        F.col("contextual_score").cast("double")
    )
    .withColumn(
        "if_score",
        F.col("if_score").cast("double")
    )
    .withColumn(
        "signal_count",
        F.col("signal_count").cast("int")
    )
    .withColumn(
        "daily_consumption",
        F.col("daily_consumption").cast("double")
    )
)


# Remove rows with invalid model scores

df = df.filter(
    F.col("contextual_score").isNotNull()
    &
    F.col("if_score").isNotNull()
)


# ============================================================
# 5. EXPERIMENT CONFIGURATION
# ============================================================

# Contextual model / Isolation Forest weights

weight_configs = [
    ("50_50", 0.50, 0.50),
    ("60_40", 0.60, 0.40),
    ("70_30", 0.70, 0.30),
    ("80_20", 0.80, 0.20),
    ("90_10", 0.90, 0.10),
]


# Isolation Forest operating thresholds

if_percentiles = [
    90.0,
    95.0,
    97.5,
    99.0
]


# ============================================================
# 6. CALCULATE IF THRESHOLDS
# ============================================================

print("\n[2] Calculating Isolation Forest thresholds...")

threshold_rows = []

for percentile in if_percentiles:

    quantile_value = df.approxQuantile(
        "if_score",
        [percentile / 100.0],
        0.001
    )[0]

    threshold_rows.append(
        (
            float(percentile),
            float(quantile_value)
        )
    )

    print(
        f"IF {percentile:g}th percentile threshold: "
        f"{quantile_value:.4f}"
    )


# ============================================================
# 7. STORAGE FOR EXPERIMENT RESULTS
# ============================================================

results = []


# ============================================================
# 8. RUN SENSITIVITY EXPERIMENT
# ============================================================

for threshold_percentile, if_threshold in threshold_rows:

    print("\n" + "-" * 70)

    print(
        f"IF THRESHOLD = "
        f"{threshold_percentile:g}th percentile "
        f"({if_threshold:.4f})"
    )

    print("-" * 70)


    # --------------------------------------------------------
    # Isolation Forest operating point
    # --------------------------------------------------------

    working_df = (
        df
        .withColumn(
            "if_anomaly",
            F.when(
                F.col("if_score") >= F.lit(if_threshold),
                1
            ).otherwise(0)
        )
    )


    # --------------------------------------------------------
    # Run every ensemble weight configuration
    # --------------------------------------------------------

    for (
        config_name,
        contextual_weight,
        if_weight
    ) in weight_configs:


        # ----------------------------------------------------
        # Ensemble score
        # ----------------------------------------------------

        ensemble_score = (
            F.col("contextual_score")
            * F.lit(contextual_weight)
            +
            (
                F.col("if_score") / F.lit(100.0)
            )
            * F.lit(if_weight)
        )


        # ----------------------------------------------------
        # Ensemble anomaly
        # ----------------------------------------------------

        result_df = (
            working_df
            .withColumn(
                "ensemble_score",
                ensemble_score
            )
            .withColumn(
                "ensemble_anomaly",
                F.when(
                    F.col("ensemble_score") >= F.lit(0.65),
                    1
                ).otherwise(0)
            )
            .withColumn(
                "contextual_anomaly",
                F.when(
                    F.col("contextual_score") >= F.lit(0.65),
                    1
                ).otherwise(0)
            )
            .withColumn(
                "both_models",
                F.when(
                    (
                        F.col("contextual_anomaly") == 1
                    )
                    &
                    (
                        F.col("if_anomaly") == 1
                    ),
                    1
                ).otherwise(0)
            )
        )


        # ----------------------------------------------------
        # Calculate metrics
        # ----------------------------------------------------

        total_events = total_records


        contextual_count = (
            result_df
            .filter(
                F.col("contextual_anomaly") == 1
            )
            .count()
        )


        if_count = (
            result_df
            .filter(
                F.col("if_anomaly") == 1
            )
            .count()
        )


        ensemble_count = (
            result_df
            .filter(
                F.col("ensemble_anomaly") == 1
            )
            .count()
        )


        both_count = (
            result_df
            .filter(
                (
                    F.col("ensemble_anomaly") == 1
                )
                &
                (
                    F.col("both_models") == 1
                )
            )
            .count()
        )


        ensemble_percentage = (
            ensemble_count
            / total_events
            * 100.0
        )


        corroborated_percentage = (
            both_count
            / ensemble_count
            * 100.0
            if ensemble_count > 0
            else 0.0
        )


        # ----------------------------------------------------
        # Save result
        # ----------------------------------------------------

        results.append(
            (
                float(threshold_percentile),
                float(if_threshold),
                str(config_name),
                float(contextual_weight),
                float(if_weight),
                int(contextual_count),
                int(if_count),
                int(ensemble_count),
                int(both_count),
                float(ensemble_percentage),
                float(corroborated_percentage)
            )
        )


        # ----------------------------------------------------
        # Print result
        # ----------------------------------------------------

        print(
            f"{config_name}: "
            f"Ensemble={ensemble_count:,} | "
            f"Both={both_count:,} | "
            f"Flagged={ensemble_percentage:.2f}% | "
            f"Corroborated={corroborated_percentage:.2f}%"
        )


# ============================================================
# 9. CREATE EXPLICIT SPARK SCHEMA
# ============================================================

result_schema = StructType([

    StructField(
        "if_percentile",
        DoubleType(),
        False
    ),

    StructField(
        "if_threshold",
        DoubleType(),
        False
    ),

    StructField(
        "weight_config",
        StringType(),
        False
    ),

    StructField(
        "contextual_weight",
        DoubleType(),
        False
    ),

    StructField(
        "if_weight",
        DoubleType(),
        False
    ),

    StructField(
        "contextual_anomalies",
        IntegerType(),
        False
    ),

    StructField(
        "if_anomalies",
        IntegerType(),
        False
    ),

    StructField(
        "ensemble_anomalies",
        IntegerType(),
        False
    ),

    StructField(
        "both_model_events",
        IntegerType(),
        False
    ),

    StructField(
        "ensemble_percentage",
        DoubleType(),
        False
    ),

    StructField(
        "corroborated_percentage",
        DoubleType(),
        False
    )
])


# ============================================================
# 10. CREATE RESULTS DATAFRAME
# ============================================================

results_df = spark.createDataFrame(
    results,
    schema=result_schema
)


# ============================================================
# 11. DISPLAY COMPLETE RESULTS
# ============================================================

print("\n" + "=" * 70)
print("SENSITIVITY RESULTS")
print("=" * 70)

(
    results_df
    .orderBy(
        "if_percentile",
        "weight_config"
    )
    .show(
        100,
        truncate=False
    )
)


# ============================================================
# 12. MOST CORROBORATED CONFIGURATIONS
# ============================================================

print("\n" + "=" * 70)
print("MOST CORROBORATED CONFIGURATIONS")
print("=" * 70)

(
    results_df
    .orderBy(
        F.desc("corroborated_percentage"),
        F.asc("ensemble_anomalies")
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# 13. MOST CONSERVATIVE CONFIGURATIONS
# ============================================================

print("\n" + "=" * 70)
print("MOST CONSERVATIVE CONFIGURATIONS")
print("=" * 70)

(
    results_df
    .orderBy(
        F.asc("ensemble_anomalies"),
        F.desc("corroborated_percentage")
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# 14. SAVE RESULTS
# ============================================================

print("\n[3] Saving sensitivity results...")

(
    results_df
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print(
    f"Results saved to:\n{OUTPUT_PATH}"
)


# ============================================================
# 15. CSV EXPORT FOR EASY REVIEW
# ============================================================

CSV_OUTPUT_PATH = (
    r"C:\projects\GridGuard\data\processed"
    r"\ensemble_sensitivity_csv"
)

(
    results_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(CSV_OUTPUT_PATH)
)

print(
    f"CSV result exported to:\n{CSV_OUTPUT_PATH}"
)


# ============================================================
# 16. FINAL SUMMARY
# ============================================================

print("\n" + "=" * 70)
print("STEP 33 COMPLETED SUCCESSFULLY")
print("=" * 70)

print(
    f"Total analytical events : {total_events:,}"
)

print(
    f"Configurations tested   : "
    f"{len(if_percentiles) * len(weight_configs)}"
)

print(
    f"IF thresholds tested    : "
    f"{len(if_percentiles)}"
)

print(
    f"Weight configurations   : "
    f"{len(weight_configs)}"
)

print(
    f"Parquet output          : "
    f"{OUTPUT_PATH}"
)

print(
    f"CSV output              : "
    f"{CSV_OUTPUT_PATH}"
)

print("=" * 70)


# ============================================================
# 17. STOP SPARK
# ============================================================

spark.stop()