# ============================================================
# GRIDGUARD - STEP 11F
# REFINED TEMPORAL ANOMALY DETECTOR
# ============================================================
#
# Purpose:
#   Improve Step 11D by:
#   - Using training-only baselines
#   - Stable standard-deviation floors
#   - Clipped z-scores
#   - Contextual + Isolation Forest detection
#   - Persistence-aware scoring
#   - Training-derived thresholds
#   - False-positive analysis
#
# Important:
#   "Anomaly" != "electricity theft"
#   The controlled anomalies are synthetic evaluation labels.
#
# ============================================================

import os
import shutil
import time
import json
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    accuracy_score,
    confusion_matrix
)

# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE_DIR = r"C:\projects\GridGuard"

TRAIN_PATH = os.path.join(
    BASE_DIR,
    "data",
    "real",
    "processed",
    "temporal_train_1m"
)

CLEAN_TEST_PATH = os.path.join(
    BASE_DIR,
    "data",
    "real",
    "processed",
    "temporal_test_clean_1m"
)

INJECTED_TEST_PATH = os.path.join(
    BASE_DIR,
    "data",
    "real",
    "processed",
    "temporal_test_injected_1m"
)

METER_BASELINE_PATH = os.path.join(
    BASE_DIR,
    "data",
    "real",
    "processed",
    "temporal_meter_baselines_1m"
)

OUTPUT_DIR = os.path.join(
    BASE_DIR,
    "data",
    "real",
    "processed",
    "refined_temporal_gridguard_1m"
)

# ------------------------------------------------------------
# Detection parameters
# ------------------------------------------------------------

MIN_STD = 0.10
STD_FLOOR_FACTOR = 0.25
Z_CLIP = 6.0

HISTORICAL_Z_THRESHOLD = 3.0
ROLLING_Z_THRESHOLD = 3.0
SEASONAL_Z_THRESHOLD = 4.0

RELATIVE_THRESHOLD = 1.0
PEER_Z_THRESHOLD = 3.0

LOW_ACTIVITY_QUANTILE = 0.10

# Isolation Forest
IF_ESTIMATORS = 250
IF_CONTAMINATION = 0.01
IF_RANDOM_STATE = 42

# Training-derived threshold
TRAIN_THRESHOLD_QUANTILE = 0.995

# Persistence
PERSISTENCE_WINDOW = 3

# ============================================================
# 2. SPARK SESSION
# ============================================================

print("=" * 70)
print("GRIDGUARD - STEP 11F")
print("REFINED TEMPORAL ANOMALY DETECTOR")
print("=" * 70)

start_time = time.time()

spark = (
    SparkSession.builder
    .appName("GridGuard-Step11F-RefinedTemporalDetector")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 3. CLEAN OUTPUT DIRECTORY
# ============================================================

if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("\nOutput directory:")
print(OUTPUT_DIR)

# ============================================================
# 4. READ TEMPORAL DATASETS
# ============================================================

print("\n[1/10] Reading temporal datasets...")

train = spark.read.parquet(TRAIN_PATH)
clean_test = spark.read.parquet(CLEAN_TEST_PATH)
injected_test = spark.read.parquet(INJECTED_TEST_PATH)

meter_baselines = spark.read.parquet(METER_BASELINE_PATH)

print(f"Training rows     : {train.count():,}")
print(f"Clean test rows   : {clean_test.count():,}")
print(f"Injected test rows: {injected_test.count():,}")
print(f"Meter baselines   : {meter_baselines.count():,}")

# ============================================================
# 5. STANDARDIZE DATE COLUMNS
# ============================================================

DATE_COLUMN = "date"

for df_name, df in [
    ("train", train),
    ("clean_test", clean_test),
    ("injected_test", injected_test)
]:

    if DATE_COLUMN not in df.columns:

        possible_dates = [
            c for c in df.columns
            if c.lower() in ["date", "aggregation_date", "day"]
        ]

        if possible_dates:
            DATE_COLUMN = possible_dates[0]
        else:
            raise ValueError(
                f"Could not identify date column in {df_name}"
            )

print(f"\nUsing date column: {DATE_COLUMN}")

train = train.withColumn(
    DATE_COLUMN,
    F.to_date(F.col(DATE_COLUMN))
)

clean_test = clean_test.withColumn(
    DATE_COLUMN,
    F.to_date(F.col(DATE_COLUMN))
)

injected_test = injected_test.withColumn(
    DATE_COLUMN,
    F.to_date(F.col(DATE_COLUMN))
)

# ============================================================
# 6. VERIFY REQUIRED COLUMNS
# ============================================================

required_base_columns = [
    "meter_id",
    DATE_COLUMN,
    "daily_consumption"
]

for df_name, df in [
    ("train", train),
    ("clean_test", clean_test),
    ("injected_test", injected_test)
]:

    missing = [
        c for c in required_base_columns
        if c not in df.columns
    ]

    if missing:
        raise ValueError(
            f"{df_name} is missing required columns: {missing}"
        )

# ============================================================
# 7. FIX / STANDARDIZE STEP 11C BASELINE SCHEMA
# ============================================================

print("\n[2/10] Preparing training-only meter baselines...")

print("\nOriginal baseline columns:")
print(meter_baselines.columns)

# ------------------------------------------------------------
# IMPORTANT:
#
# Step 11C actually produced:
#
# baseline_avg
# baseline_std
# baseline_min
# baseline_max
# baseline_median
# baseline_p05
# baseline_p95
#
# Step 11F internally expects:
#
# historical_avg_consumption
# historical_std_consumption
# historical_min_consumption
# historical_max_consumption
# historical_median_consumption
# historical_p05_consumption
# historical_p95_consumption
#
# Therefore rename the EXISTING artifact.
# Do NOT regenerate Step 11C.
# ------------------------------------------------------------

baseline_rename_map = {
    "baseline_avg": "historical_avg_consumption",
    "baseline_std": "historical_std_consumption",
    "baseline_min": "historical_min_consumption",
    "baseline_max": "historical_max_consumption",
    "baseline_median": "historical_median_consumption",
    "baseline_p05": "historical_p05_consumption",
    "baseline_p95": "historical_p95_consumption"
}

for old_name, new_name in baseline_rename_map.items():

    if old_name in meter_baselines.columns:

        meter_baselines = meter_baselines.withColumnRenamed(
            old_name,
            new_name
        )

required_baseline_columns = [
    "meter_id",
    "training_days",
    "historical_avg_consumption",
    "historical_std_consumption",
    "historical_min_consumption",
    "historical_max_consumption",
    "historical_median_consumption",
    "historical_p05_consumption",
    "historical_p95_consumption"
]

missing_baseline = [
    c for c in required_baseline_columns
    if c not in meter_baselines.columns
]

if missing_baseline:

    print("\nAvailable baseline columns:")
    print(meter_baselines.columns)

    raise ValueError(
        f"Missing baseline columns after standardization: "
        f"{missing_baseline}"
    )

meter_baselines = meter_baselines.select(
    *required_baseline_columns
)

print("\nStandardized baseline schema:")
meter_baselines.printSchema()

meter_baselines.show(
    5,
    truncate=False
)

# ============================================================
# 8. PREPARE METER BASELINES
# ============================================================

# Stable historical standard deviation.
#
# This prevents extremely small standard deviations from
# producing unrealistic z-scores.
#
# Project parameter:
#   MIN_STD = 0.10
#   STD_FLOOR_FACTOR = 0.25
#
# Effective:
#   max(original_std, 0.10, 25% of historical average)
#
# ------------------------------------------------------------

meter_baselines = (
    meter_baselines
    .withColumn(
        "historical_std_safe",
        F.greatest(
            F.coalesce(
                F.col("historical_std_consumption"),
                F.lit(0.0)
            ),
            F.lit(MIN_STD),
            F.lit(STD_FLOOR_FACTOR)
            * F.abs(
                F.coalesce(
                    F.col("historical_avg_consumption"),
                    F.lit(0.0)
                )
            )
        )
    )
)

# ============================================================
# 9. CREATE PEER GROUPS
# ============================================================

print("\n[3/10] Creating peer groups...")

# Peer groups are based on historical consumption level.
#
# IMPORTANT:
# These are analytical peer groups, not geographical
# or customer-type labels.
# ------------------------------------------------------------

peer_quantiles = meter_baselines.approxQuantile(
    "historical_avg_consumption",
    [0.20, 0.40, 0.60, 0.80],
    0.01
)

q20, q40, q60, q80 = peer_quantiles

print("Peer thresholds:")
print(f"20th percentile: {q20}")
print(f"40th percentile: {q40}")
print(f"60th percentile: {q60}")
print(f"80th percentile: {q80}")

meter_baselines = (
    meter_baselines
    .withColumn(
        "peer_group",
        F.when(
            F.col("historical_avg_consumption") <= q20,
            F.lit("P1_LOW")
        )
        .when(
            F.col("historical_avg_consumption") <= q40,
            F.lit("P2_LOW_MEDIUM")
        )
        .when(
            F.col("historical_avg_consumption") <= q60,
            F.lit("P3_MEDIUM")
        )
        .when(
            F.col("historical_avg_consumption") <= q80,
            F.lit("P4_MEDIUM_HIGH")
        )
        .otherwise(
            F.lit("P5_HIGH")
        )
    )
)

peer_baselines = (
    meter_baselines
    .groupBy("peer_group")
    .agg(
        F.avg("historical_avg_consumption")
        .alias("peer_avg_consumption"),

        F.stddev("historical_avg_consumption")
        .alias("peer_std_consumption")
    )
    .withColumn(
        "peer_std_safe",
        F.greatest(
            F.coalesce(
                F.col("peer_std_consumption"),
                F.lit(0.0)
            ),
            F.lit(MIN_STD)
        )
    )
)

print("\nPeer groups:")
peer_baselines.show(
    truncate=False
)

# ============================================================
# 10. PREPARE TRAINING SEASONAL BASELINE
# ============================================================

print("\n[4/10] Creating training-only seasonal baselines...")

train_seasonal = (
    train
    .withColumn(
        "month",
        F.month(F.col(DATE_COLUMN))
    )
    .withColumn(
        "day_of_week",
        F.dayofweek(F.col(DATE_COLUMN))
    )
)

seasonal_baseline = (
    train_seasonal
    .groupBy(
        "meter_id",
        "month",
        "day_of_week"
    )
    .agg(
        F.avg("daily_consumption")
        .alias("seasonal_avg_consumption"),

        F.stddev("daily_consumption")
        .alias("seasonal_std_consumption")
    )
)

# ============================================================
# 11. CREATE SAFE SEASONAL STD
# ============================================================

seasonal_baseline = (
    seasonal_baseline
    .withColumn(
        "seasonal_std_safe",
        F.greatest(
            F.coalesce(
                F.col("seasonal_std_consumption"),
                F.lit(0.0)
            ),
            F.lit(MIN_STD)
        )
    )
)

print(
    f"Seasonal baseline rows: "
    f"{seasonal_baseline.count():,}"
)

# ============================================================
# 12. PREPARE TRAINING DATA
# ============================================================

print("\n[5/10] Preparing contextual features...")

def prepare_base(df):

    result = (
        df
        .withColumn(
            "month",
            F.month(F.col(DATE_COLUMN))
        )
        .withColumn(
            "day_of_week",
            F.dayofweek(F.col(DATE_COLUMN))
        )
    )

    return result


train_base = prepare_base(train)
clean_test_base = prepare_base(clean_test)
injected_test_base = prepare_base(injected_test)

# ============================================================
# 13. LOW ACTIVITY THRESHOLD
# ============================================================

low_activity_threshold = train.approxQuantile(
    "daily_consumption",
    [LOW_ACTIVITY_QUANTILE],
    0.01
)[0]

print(
    f"Low-activity threshold "
    f"(10th percentile): {low_activity_threshold}"
)

# ============================================================
# 14. TRAINING ROLLING BASELINE
# ============================================================

print("\nCreating training rolling baseline...")

rolling_window_train = (
    Window
    .partitionBy("meter_id")
    .orderBy(F.col(DATE_COLUMN))
    .rowsBetween(-30, -1)
)

train_with_rolling = (
    train_base
    .withColumn(
        "rolling_avg_consumption",
        F.avg("daily_consumption")
        .over(rolling_window_train)
    )
    .withColumn(
        "rolling_std_consumption",
        F.stddev("daily_consumption")
        .over(rolling_window_train)
    )
)

# ============================================================
# 15. CLEAN TEST ROLLING BASELINE
# ============================================================

# Clean test rolling baseline is generated using:
#
# training + clean test
#
# This allows the detector to use historical observations
# before the current date.
# ------------------------------------------------------------

combined_clean = (
    train
    .select(
        "meter_id",
        DATE_COLUMN,
        "daily_consumption"
    )
    .unionByName(
        clean_test.select(
            "meter_id",
            DATE_COLUMN,
            "daily_consumption"
        )
    )
)

combined_clean = prepare_base(combined_clean)

rolling_window_clean = (
    Window
    .partitionBy("meter_id")
    .orderBy(F.col(DATE_COLUMN))
    .rowsBetween(-30, -1)
)

clean_rolling = (
    combined_clean
    .withColumn(
        "rolling_avg_consumption",
        F.avg("daily_consumption")
        .over(rolling_window_clean)
    )
    .withColumn(
        "rolling_std_consumption",
        F.stddev("daily_consumption")
        .over(rolling_window_clean)
    )
    .select(
        "meter_id",
        DATE_COLUMN,
        "rolling_avg_consumption",
        "rolling_std_consumption"
    )
)

# ============================================================
# 16. INJECTED TEST ROLLING BASELINE
# ============================================================

# IMPORTANT:
#
# We use CLEAN test observations to generate the rolling
# baseline for the injected test.
#
# This prevents synthetic anomalies from contaminating
# the rolling baseline.
# ------------------------------------------------------------

injected_rolling = clean_rolling

# ============================================================
# 17. CONTEXTUAL FEATURE FUNCTION
# ============================================================

def add_context_features(df, rolling_df=None):

    result = df

    # --------------------------------------------------------
    # Historical baseline
    # --------------------------------------------------------

    result = result.join(
        meter_baselines,
        "meter_id",
        "left"
    )

    # --------------------------------------------------------
    # Seasonal baseline
    # --------------------------------------------------------

    result = result.join(
        seasonal_baseline,
        [
            "meter_id",
            "month",
            "day_of_week"
        ],
        "left"
    )

    # --------------------------------------------------------
    # Peer baseline
    # --------------------------------------------------------

    result = result.join(
        peer_baselines,
        "peer_group",
        "left"
    )

    # --------------------------------------------------------
    # Rolling baseline
    #
    # IMPORTANT FIX:
    #
    # Training already contains rolling columns.
    # Therefore we only join rolling_df for test datasets.
    # --------------------------------------------------------

    if rolling_df is not None:

        result = result.join(
            rolling_df,
            [
                "meter_id",
                DATE_COLUMN
            ],
            "left"
        )

    # --------------------------------------------------------
    # Historical Z-score
    # --------------------------------------------------------

    result = result.withColumn(
        "historical_z_raw",
        (
            F.col("daily_consumption")
            - F.col("historical_avg_consumption")
        )
        /
        F.col("historical_std_safe")
    )

    result = result.withColumn(
        "historical_z",
        F.greatest(
            F.lit(-Z_CLIP),
            F.least(
                F.lit(Z_CLIP),
                F.col("historical_z_raw")
            )
        )
    )

    # --------------------------------------------------------
    # Relative historical deviation
    # --------------------------------------------------------

    result = result.withColumn(
        "relative_deviation",
        (
            F.col("daily_consumption")
            - F.col("historical_avg_consumption")
        )
        /
        F.greatest(
            F.abs(
                F.col("historical_avg_consumption")
            ),
            F.lit(MIN_STD)
        )
    )

    # --------------------------------------------------------
    # Seasonal fallback
    # --------------------------------------------------------

    result = result.withColumn(
        "seasonal_avg_final",
        F.coalesce(
            F.col("seasonal_avg_consumption"),
            F.col("historical_avg_consumption")
        )
    )

    result = result.withColumn(
        "seasonal_std_final",
        F.greatest(
            F.coalesce(
                F.col("seasonal_std_safe"),
                F.lit(MIN_STD)
            ),
            F.lit(MIN_STD)
        )
    )

    # --------------------------------------------------------
    # Seasonal Z-score
    # --------------------------------------------------------

    result = result.withColumn(
        "seasonal_z_raw",
        (
            F.col("daily_consumption")
            - F.col("seasonal_avg_final")
        )
        /
        F.col("seasonal_std_final")
    )

    result = result.withColumn(
        "seasonal_z",
        F.greatest(
            F.lit(-Z_CLIP),
            F.least(
                F.lit(Z_CLIP),
                F.col("seasonal_z_raw")
            )
        )
    )

    # --------------------------------------------------------
    # Rolling baseline fallback
    # --------------------------------------------------------

    result = result.withColumn(
        "rolling_avg_final",
        F.coalesce(
            F.col("rolling_avg_consumption"),
            F.col("historical_avg_consumption")
        )
    )

    result = result.withColumn(
        "rolling_std_final",
        F.greatest(
            F.coalesce(
                F.col("rolling_std_consumption"),
                F.col("historical_std_safe")
            ),
            F.lit(MIN_STD)
        )
    )

    # --------------------------------------------------------
    # Rolling Z-score
    # --------------------------------------------------------

    result = result.withColumn(
        "rolling_z_raw",
        (
            F.col("daily_consumption")
            - F.col("rolling_avg_final")
        )
        /
        F.col("rolling_std_final")
    )

    result = result.withColumn(
        "rolling_z",
        F.greatest(
            F.lit(-Z_CLIP),
            F.least(
                F.lit(Z_CLIP),
                F.col("rolling_z_raw")
            )
        )
    )

    # --------------------------------------------------------
    # Percentile signal
    # --------------------------------------------------------

    result = result.withColumn(
        "percentile_signal",
        F.when(
            F.col("daily_consumption")
            > F.col("historical_p95_consumption"),
            1.0
        )
        .when(
            F.col("daily_consumption")
            < F.col("historical_p05_consumption"),
            1.0
        )
        .otherwise(0.0)
    )

    # --------------------------------------------------------
    # Peer deviation
    # --------------------------------------------------------

    result = result.withColumn(
        "peer_deviation",
        (
            F.col("daily_consumption")
            - F.col("peer_avg_consumption")
        )
        /
        F.greatest(
            F.col("peer_std_safe"),
            F.lit(MIN_STD)
        )
    )

    result = result.withColumn(
        "peer_z",
        F.greatest(
            F.lit(-Z_CLIP),
            F.least(
                F.lit(Z_CLIP),
                F.col("peer_deviation")
            )
        )
    )

    # --------------------------------------------------------
    # Low activity flag
    #
    # This is NOT treated as theft.
    # It only helps analyze false positives.
    # --------------------------------------------------------

    result = result.withColumn(
        "low_activity_flag",
        F.when(
            F.col("daily_consumption")
            <= low_activity_threshold,
            1
        ).otherwise(0)
    )

    # --------------------------------------------------------
    # Detection signals
    # --------------------------------------------------------

    result = result.withColumn(
        "historical_signal",
        F.when(
            F.abs(F.col("historical_z"))
            >= HISTORICAL_Z_THRESHOLD,
            1
        ).otherwise(0)
    )

    result = result.withColumn(
        "seasonal_signal",
        F.when(
            F.abs(F.col("seasonal_z"))
            >= SEASONAL_Z_THRESHOLD,
            1
        ).otherwise(0)
    )

    result = result.withColumn(
        "rolling_signal",
        F.when(
            F.abs(F.col("rolling_z"))
            >= ROLLING_Z_THRESHOLD,
            1
        ).otherwise(0)
    )

    result = result.withColumn(
        "relative_signal",
        F.when(
            F.abs(F.col("relative_deviation"))
            >= RELATIVE_THRESHOLD,
            1
        ).otherwise(0)
    )

    result = result.withColumn(
        "peer_signal",
        F.when(
            F.abs(F.col("peer_z"))
            >= PEER_Z_THRESHOLD,
            1
        ).otherwise(0)
    )

    # --------------------------------------------------------
    # Signal count
    # --------------------------------------------------------

    result = result.withColumn(
        "signal_count",
        (
            F.col("historical_signal")
            + F.col("seasonal_signal")
            + F.col("rolling_signal")
            + F.col("percentile_signal").cast("int")
            + F.col("relative_signal")
            + F.col("peer_signal")
        )
    )

    # --------------------------------------------------------
    # Basic contextual prediction
    #
    # Require at least two independent signals.
    # --------------------------------------------------------

    result = result.withColumn(
        "contextual_prediction_basic",
        F.when(
            F.col("signal_count") >= 2,
            1
        ).otherwise(0)
    )

    return result


# ============================================================
# 18. CREATE CONTEXTUAL FEATURES
# ============================================================

print("\nCreating training contextual features...")

# IMPORTANT:
# Do NOT pass train_with_rolling as rolling_df.
# It already contains rolling columns.
#
# This fixes duplicate-column ambiguity.

train_features = add_context_features(
    train_with_rolling,
    None
)

print("Creating clean-test contextual features...")

clean_test_features = add_context_features(
    clean_test_base,
    clean_rolling
)

print("Creating injected-test contextual features...")

injected_test_features = add_context_features(
    injected_test_base,
    injected_rolling
)

# ============================================================
# 19. ADD PERSISTENCE
# ============================================================

print("\n[6/10] Adding persistence signals...")

def add_persistence(df):

    persistence_window = (
        Window
        .partitionBy("meter_id")
        .orderBy(F.col(DATE_COLUMN))
        .rowsBetween(
            -PERSISTENCE_WINDOW,
            0
        )
    )

    result = (
        df
        .withColumn(
            "historical_signal_sum",
            F.sum("historical_signal")
            .over(persistence_window)
        )
        .withColumn(
            "seasonal_signal_sum",
            F.sum("seasonal_signal")
            .over(persistence_window)
        )
        .withColumn(
            "rolling_signal_sum",
            F.sum("rolling_signal")
            .over(persistence_window)
        )
        .withColumn(
            "relative_signal_sum",
            F.sum("relative_signal")
            .over(persistence_window)
        )
    )

    result = result.withColumn(
        "persistence_score",
        (
            F.least(
                F.col("historical_signal_sum"),
                F.lit(4)
            )
            +
            F.least(
                F.col("seasonal_signal_sum"),
                F.lit(4)
            )
            +
            F.least(
                F.col("rolling_signal_sum"),
                F.lit(4)
            )
            +
            F.least(
                F.col("relative_signal_sum"),
                F.lit(4)
            )
        ) / 16.0
    )

    # Strong persistent evidence
    result = result.withColumn(
        "persistence_signal",
        F.when(
            (
                F.col("persistence_score") >= 0.50
            )
            &
            (
                F.col("historical_signal")
                + F.col("seasonal_signal")
                + F.col("relative_signal")
                >= 1
            ),
            1
        ).otherwise(0)
    )

    return result


train_features = add_persistence(train_features)
clean_test_features = add_persistence(clean_test_features)
injected_test_features = add_persistence(injected_test_features)

# ============================================================
# 20. CONTEXTUAL SCORE
# ============================================================

print("\nCalculating contextual scores...")

def add_contextual_score(df):

    result = df.withColumn(
        "historical_component",
        F.least(
            F.abs(F.col("historical_z")) / 6.0,
            F.lit(1.0)
        )
    )

    result = result.withColumn(
        "rolling_component",
        F.least(
            F.abs(F.col("rolling_z")) / 6.0,
            F.lit(1.0)
        )
    )

    result = result.withColumn(
        "seasonal_component",
        F.least(
            F.abs(F.col("seasonal_z")) / 6.0,
            F.lit(1.0)
        )
    )

    result = result.withColumn(
        "relative_component",
        F.least(
            F.abs(F.col("relative_deviation")) / 3.0,
            F.lit(1.0)
        )
    )

    result = result.withColumn(
        "peer_component",
        F.least(
            F.abs(F.col("peer_z")) / 6.0,
            F.lit(1.0)
        )
    )

    result = result.withColumn(
        "contextual_score",
        (
            0.25 * F.col("historical_component")
            +
            0.25 * F.col("rolling_component")
            +
            0.10 * F.col("seasonal_component")
            +
            0.15 * F.col("percentile_signal")
            +
            0.10 * F.col("relative_component")
            +
            0.05 * F.col("peer_component")
            +
            0.10 * F.col("persistence_score")
        )
    )

    return result


train_features = add_contextual_score(train_features)
clean_test_features = add_contextual_score(clean_test_features)
injected_test_features = add_contextual_score(injected_test_features)

# ============================================================
# 21. CONVERT TO PANDAS
# ============================================================

print("\n[7/10] Converting feature datasets to Pandas...")

FEATURE_COLUMNS = [
    "daily_consumption",
    "historical_z",
    "seasonal_z",
    "rolling_z",
    "relative_deviation",
    "peer_z"
]

ID_COLUMNS = [
    "meter_id",
    DATE_COLUMN
]

train_pd = (
    train_features
    .select(
        *(ID_COLUMNS + FEATURE_COLUMNS + [
            "contextual_score",
            "signal_count",
            "persistence_score"
        ])
    )
    .toPandas()
)

clean_test_pd = (
    clean_test_features
    .select(
        *(ID_COLUMNS + FEATURE_COLUMNS + [
            "contextual_score",
            "signal_count",
            "persistence_score"
        ])
    )
    .toPandas()
)

injected_select = ID_COLUMNS + FEATURE_COLUMNS + [
    "contextual_score",
    "signal_count",
    "persistence_score",
    "low_activity_flag"
]

# Add ground truth columns if available
for c in ["ground_truth", "anomaly_type"]:

    if c in injected_test_features.columns:
        injected_select.append(c)

injected_test_pd = (
    injected_test_features
    .select(*injected_select)
    .toPandas()
)

print(f"Train Pandas rows    : {len(train_pd):,}")
print(f"Clean test rows      : {len(clean_test_pd):,}")
print(f"Injected test rows   : {len(injected_test_pd):,}")

# ============================================================
# 22. TRAIN ISOLATION FOREST
# ============================================================

print("\nTraining Isolation Forest...")

X_train = train_pd[FEATURE_COLUMNS].replace(
    [np.inf, -np.inf],
    np.nan
)

X_clean = clean_test_pd[FEATURE_COLUMNS].replace(
    [np.inf, -np.inf],
    np.nan
)

X_injected = injected_test_pd[FEATURE_COLUMNS].replace(
    [np.inf, -np.inf],
    np.nan
)

imputer = SimpleImputer(
    strategy="median"
)

X_train_imp = imputer.fit_transform(X_train)

X_clean_imp = imputer.transform(X_clean)

X_injected_imp = imputer.transform(
    X_injected
)

iso_forest = IsolationForest(
    n_estimators=IF_ESTIMATORS,
    contamination=IF_CONTAMINATION,
    random_state=IF_RANDOM_STATE,
    n_jobs=-1
)

iso_forest.fit(X_train_imp)

# ============================================================
# 23. ISOLATION FOREST SCORE
# ============================================================

train_if_raw = -iso_forest.decision_function(
    X_train_imp
)

clean_if_raw = -iso_forest.decision_function(
    X_clean_imp
)

injected_if_raw = -iso_forest.decision_function(
    X_injected_imp
)

# ------------------------------------------------------------
# Normalize IF score using TRAINING distribution only.
# ------------------------------------------------------------

train_if_min = np.min(train_if_raw)
train_if_max = np.max(train_if_raw)

if train_if_max - train_if_min < 1e-12:

    train_if_score = np.zeros_like(
        train_if_raw
    )

    clean_if_score = np.zeros_like(
        clean_if_raw
    )

    injected_if_score = np.zeros_like(
        injected_if_raw
    )

else:

    train_if_score = (
        train_if_raw - train_if_min
    ) / (
        train_if_max - train_if_min
    )

    clean_if_score = (
        clean_if_raw - train_if_min
    ) / (
        train_if_max - train_if_min
    )

    injected_if_score = (
        injected_if_raw - train_if_min
    ) / (
        train_if_max - train_if_min
    )

train_if_score = np.clip(
    train_if_score,
    0,
    1
)

clean_if_score = np.clip(
    clean_if_score,
    0,
    1
)

injected_if_score = np.clip(
    injected_if_score,
    0,
    1
)

# ============================================================
# 24. ADD IF SCORES
# ============================================================

train_pd["if_score"] = train_if_score
clean_test_pd["if_score"] = clean_if_score
injected_test_pd["if_score"] = injected_if_score

# ============================================================
# 25. TEMPORAL ENSEMBLE
# ============================================================

# Contextual detector is weighted more heavily because it
# provides explainable evidence.
#
# IF contributes complementary unsupervised detection.
# ------------------------------------------------------------

train_pd["ensemble_score"] = (
    0.75 * train_pd["contextual_score"]
    +
    0.25 * train_pd["if_score"]
)

clean_test_pd["ensemble_score"] = (
    0.75 * clean_test_pd["contextual_score"]
    +
    0.25 * clean_test_pd["if_score"]
)

injected_test_pd["ensemble_score"] = (
    0.75 * injected_test_pd["contextual_score"]
    +
    0.25 * injected_test_pd["if_score"]
)

# ============================================================
# 26. TRAINING-DERIVED THRESHOLDS
# ============================================================

print("\n[8/10] Calculating training-derived thresholds...")

contextual_threshold = np.quantile(
    train_pd["contextual_score"],
    TRAIN_THRESHOLD_QUANTILE
)

if_threshold = np.quantile(
    train_pd["if_score"],
    TRAIN_THRESHOLD_QUANTILE
)

ensemble_threshold = np.quantile(
    train_pd["ensemble_score"],
    TRAIN_THRESHOLD_QUANTILE
)

print(
    f"Contextual threshold: "
    f"{contextual_threshold:.6f}"
)

print(
    f"IF threshold       : "
    f"{if_threshold:.6f}"
)

print(
    f"Ensemble threshold : "
    f"{ensemble_threshold:.6f}"
)

# ============================================================
# 27. FINAL PREDICTIONS
# ============================================================

def generate_predictions(df):

    df["contextual_prediction"] = (
        (
            df["contextual_score"]
            >= contextual_threshold
        )
        |
        (
            df["persistence_score"]
            >= 0.50
        )
        &
        (
            df["historical_z"].abs()
            >= 3
        )
    ).astype(int)

    df["if_prediction"] = (
        df["if_score"]
        >= if_threshold
    ).astype(int)

    df["ensemble_prediction"] = (
        df["ensemble_score"]
        >= ensemble_threshold
    ).astype(int)

    return df


train_pd = generate_predictions(
    train_pd
)

clean_test_pd = generate_predictions(
    clean_test_pd
)

injected_test_pd = generate_predictions(
    injected_test_pd
)

# ============================================================
# 28. EVALUATION FUNCTION
# ============================================================

def evaluate_model(
    y_true,
    y_pred,
    model_name
):

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

    tn, fp, fn, tp = confusion_matrix(
        y_true,
        y_pred,
        labels=[0, 1]
    ).ravel()

    return {
        "model": model_name,
        "TP": int(tp),
        "TN": int(tn),
        "FP": int(fp),
        "FN": int(fn),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": accuracy
    }


# ============================================================
# 29. GROUND TRUTH
# ============================================================

if "ground_truth" not in injected_test_pd.columns:

    raise ValueError(
        "ground_truth column not found in injected test dataset."
    )

injected_test_pd["ground_truth"] = (
    injected_test_pd["ground_truth"]
    .astype(int)
)

y_true = injected_test_pd[
    "ground_truth"
].values

# ============================================================
# 30. MODEL COMPARISON
# ============================================================

results = []

results.append(
    evaluate_model(
        y_true,
        injected_test_pd[
            "contextual_prediction"
        ].values,
        "Refined Contextual Detector"
    )
)

results.append(
    evaluate_model(
        y_true,
        injected_test_pd[
            "if_prediction"
        ].values,
        "Isolation Forest"
    )
)

results.append(
    evaluate_model(
        y_true,
        injected_test_pd[
            "ensemble_prediction"
        ].values,
        "GridGuard Refined Ensemble"
    )
)

model_comparison = pd.DataFrame(
    results
)

print("\n" + "=" * 70)
print("MODEL COMPARISON")
print("=" * 70)

print(
    model_comparison.to_string(
        index=False
    )
)

# ============================================================
# 31. CLEAN TEST FALSE POSITIVE CHECK
# ============================================================

clean_fp_count = int(
    clean_test_pd[
        "ensemble_prediction"
    ].sum()
)

clean_total = len(
    clean_test_pd
)

print("\nClean test false positives:")
print(
    f"{clean_fp_count:,} / {clean_total:,}"
)

print(
    f"Clean FP rate: "
    f"{clean_fp_count / clean_total:.4%}"
)

# ============================================================
# 32. ANOMALY TYPE ANALYSIS
# ============================================================

print("\n[9/10] Analyzing anomaly types...")

if "anomaly_type" in injected_test_pd.columns:

    anomaly_type_results = []

    for anomaly_type, group in (
        injected_test_pd[
            injected_test_pd["ground_truth"] == 1
        ]
        .groupby("anomaly_type")
    ):

        total = len(group)

        detected = int(
            group[
                "ensemble_prediction"
            ].sum()
        )

        recall = (
            detected / total
            if total > 0
            else 0
        )

        anomaly_type_results.append({
            "anomaly_type": anomaly_type,
            "ground_truth_count": total,
            "detected_count": detected,
            "recall": recall
        })

    anomaly_type_results = pd.DataFrame(
        anomaly_type_results
    )

else:

    anomaly_type_results = pd.DataFrame()

# ============================================================
# 33. GROUND TRUTH DETECTIONS
# ============================================================

ground_truth_detections = (
    injected_test_pd[
        injected_test_pd["ground_truth"] == 1
    ]
    .copy()
)

ground_truth_detections = (
    ground_truth_detections
    .sort_values(
        "ensemble_score",
        ascending=False
    )
)

# ============================================================
# 34. TOP DETECTED ANOMALIES
# ============================================================

detected_anomalies = (
    injected_test_pd[
        injected_test_pd[
            "ensemble_prediction"
        ] == 1
    ]
    .copy()
)

detected_anomalies = (
    detected_anomalies
    .sort_values(
        "ensemble_score",
        ascending=False
    )
)

# ============================================================
# 35. FALSE POSITIVE ANALYSIS
# ============================================================

print("\nAnalyzing false positives...")

false_positives = (
    injected_test_pd[
        (
            injected_test_pd["ground_truth"] == 0
        )
        &
        (
            injected_test_pd[
                "ensemble_prediction"
            ] == 1
        )
    ]
    .copy()
)

false_positives["abs_historical_z"] = (
    false_positives[
        "historical_z"
    ].abs()
)

false_positives["abs_seasonal_z"] = (
    false_positives[
        "seasonal_z"
    ].abs()
)

false_positives["abs_rolling_z"] = (
    false_positives[
        "rolling_z"
    ].abs()
)

false_positive_by_meter = (
    false_positives
    .groupby("meter_id")
    .agg(
        false_positive_count=(
            "meter_id",
            "count"
        ),
        avg_consumption=(
            "daily_consumption",
            "mean"
        ),
        avg_ensemble_score=(
            "ensemble_score",
            "mean"
        ),
        max_ensemble_score=(
            "ensemble_score",
            "max"
        ),
        avg_historical_z=(
            "historical_z",
            "mean"
        ),
        avg_seasonal_z=(
            "seasonal_z",
            "mean"
        ),
        avg_rolling_z=(
            "rolling_z",
            "mean"
        )
    )
    .reset_index()
    .sort_values(
        "false_positive_count",
        ascending=False
    )
)

# ============================================================
# 36. FALSE POSITIVE SCORE DISTRIBUTION
# ============================================================

if len(false_positives) > 0:

    fp_scores = (
        false_positives[
            "ensemble_score"
        ]
    )

    false_positive_score_distribution = pd.DataFrame({
        "metric": [
            "mean",
            "median",
            "p75",
            "p90",
            "p95",
            "p99",
            "max"
        ],
        "value": [
            fp_scores.mean(),
            fp_scores.median(),
            fp_scores.quantile(.75),
            fp_scores.quantile(.90),
            fp_scores.quantile(.95),
            fp_scores.quantile(.99),
            fp_scores.max()
        ]
    })

else:

    false_positive_score_distribution = pd.DataFrame(
        columns=[
            "metric",
            "value"
        ]
    )

# ============================================================
# 37. FALSE POSITIVE SIGNAL ANALYSIS
# ============================================================

if len(false_positives) > 0:

    signal_analysis = []

    for n in sorted(
        false_positives[
            "signal_count"
        ].unique()
    ):

        count = int(
            (
                false_positives[
                    "signal_count"
                ] == n
            ).sum()
        )

        signal_analysis.append({
            "signal_count": int(n),
            "false_positive_count": count
        })

    false_positive_signal_analysis = (
        pd.DataFrame(signal_analysis)
    )

else:

    false_positive_signal_analysis = (
        pd.DataFrame(
            columns=[
                "signal_count",
                "false_positive_count"
            ]
        )
    )

# ============================================================
# 38. TOP 100 FALSE POSITIVES
# ============================================================

top_100_false_positives = (
    false_positives
    .sort_values(
        "ensemble_score",
        ascending=False
    )
    .head(100)
)

# ============================================================
# 39. THRESHOLD SENSITIVITY
# ============================================================

print("\nThreshold sensitivity analysis...")

threshold_values = [
    0.50,
    0.55,
    0.60,
    0.65,
    0.70,
    0.75,
    0.80,
    0.85,
    0.90
]

threshold_results = []

for threshold in threshold_values:

    pred = (
        injected_test_pd[
            "ensemble_score"
        ]
        >= threshold
    ).astype(int)

    precision = precision_score(
        y_true,
        pred,
        zero_division=0
    )

    recall = recall_score(
        y_true,
        pred,
        zero_division=0
    )

    f1 = f1_score(
        y_true,
        pred,
        zero_division=0
    )

    accuracy = accuracy_score(
        y_true,
        pred
    )

    tn, fp, fn, tp = confusion_matrix(
        y_true,
        pred,
        labels=[0, 1]
    ).ravel()

    threshold_results.append({
        "threshold": threshold,
        "TP": int(tp),
        "TN": int(tn),
        "FP": int(fp),
        "FN": int(fn),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": accuracy
    })

threshold_analysis = pd.DataFrame(
    threshold_results
)

# ============================================================
# 40. SAVE FULL RESULT DATASET
# ============================================================

print("\n[10/10] Saving outputs...")

# Add labels
injected_test_pd["risk_level"] = pd.cut(
    injected_test_pd["ensemble_score"],
    bins=[
        -np.inf,
        0.30,
        0.60,
        0.80,
        np.inf
    ],
    labels=[
        "LOW",
        "MEDIUM",
        "HIGH",
        "CRITICAL"
    ]
)

# ============================================================
# 41. SAVE CSV OUTPUTS
# ============================================================

injected_test_pd.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "refined_gridguard_results.csv"
    ),
    index=False
)

model_comparison.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "model_comparison.csv"
    ),
    index=False
)

anomaly_type_results.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "anomaly_type_results.csv"
    ),
    index=False
)

ground_truth_detections.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "ground_truth_detections.csv"
    ),
    index=False
)

detected_anomalies.head(50).to_csv(
    os.path.join(
        OUTPUT_DIR,
        "detected_anomalies_top50.csv"
    ),
    index=False
)

false_positives.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "false_positives.csv"
    ),
    index=False
)

false_positive_by_meter.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "false_positive_by_meter.csv"
    ),
    index=False
)

false_positive_score_distribution.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "false_positive_score_distribution.csv"
    ),
    index=False
)

false_positive_signal_analysis.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "false_positive_signal_analysis.csv"
    ),
    index=False
)

top_100_false_positives.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "top_100_false_positives.csv"
    ),
    index=False
)

threshold_analysis.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "threshold_analysis.csv"
    ),
    index=False
)

# ============================================================
# 42. CONFIGURATION FILE
# ============================================================

config = {
    "step": "11F",
    "method": "Refined Temporal GridGuard Detector",

    "min_std": MIN_STD,
    "std_floor_factor": STD_FLOOR_FACTOR,
    "z_clip": Z_CLIP,

    "historical_z_threshold":
        HISTORICAL_Z_THRESHOLD,

    "rolling_z_threshold":
        ROLLING_Z_THRESHOLD,

    "seasonal_z_threshold":
        SEASONAL_Z_THRESHOLD,

    "relative_threshold":
        RELATIVE_THRESHOLD,

    "peer_z_threshold":
        PEER_Z_THRESHOLD,

    "low_activity_quantile":
        LOW_ACTIVITY_QUANTILE,

    "isolation_forest_estimators":
        IF_ESTIMATORS,

    "isolation_forest_contamination":
        IF_CONTAMINATION,

    "random_state":
        IF_RANDOM_STATE,

    "training_threshold_quantile":
        TRAIN_THRESHOLD_QUANTILE,

    "persistence_window":
        PERSISTENCE_WINDOW,

    "contextual_weight":
        0.75,

    "isolation_forest_weight":
        0.25,

    "contextual_threshold":
        float(contextual_threshold),

    "if_threshold":
        float(if_threshold),

    "ensemble_threshold":
        float(ensemble_threshold)
}

with open(
    os.path.join(
        OUTPUT_DIR,
        "configuration.json"
    ),
    "w"
) as f:

    json.dump(
        config,
        f,
        indent=4
    )

# ============================================================
# 43. SAVE SPARK PARQUET RESULT
# ============================================================

print("\nSaving Spark-compatible result...")

result_spark = spark.createDataFrame(
    injected_test_pd
)

result_spark.write.mode(
    "overwrite"
).parquet(
    os.path.join(
        OUTPUT_DIR,
        "refined_gridguard_results_parquet"
    )
)

# ============================================================
# 44. FINAL SUMMARY
# ============================================================

total_gt = int(
    injected_test_pd[
        "ground_truth"
    ].sum()
)

tp = int(
    (
        (
            injected_test_pd[
                "ground_truth"
            ] == 1
        )
        &
        (
            injected_test_pd[
                "ensemble_prediction"
            ] == 1
        )
    ).sum()
)

tn = int(
    (
        (
            injected_test_pd[
                "ground_truth"
            ] == 0
        )
        &
        (
            injected_test_pd[
                "ensemble_prediction"
            ] == 0
        )
    ).sum()
)

fp = int(
    (
        (
            injected_test_pd[
                "ground_truth"
            ] == 0
        )
        &
        (
            injected_test_pd[
                "ensemble_prediction"
            ] == 1
        )
    ).sum()
)

fn = int(
    (
        (
            injected_test_pd[
                "ground_truth"
            ] == 1
        )
        &
        (
            injected_test_pd[
                "ensemble_prediction"
            ] == 0
        )
    ).sum()
)

precision = precision_score(
    y_true,
    injected_test_pd[
        "ensemble_prediction"
    ],
    zero_division=0
)

recall = recall_score(
    y_true,
    injected_test_pd[
        "ensemble_prediction"
    ],
    zero_division=0
)

f1 = f1_score(
    y_true,
    injected_test_pd[
        "ensemble_prediction"
    ],
    zero_division=0
)

accuracy = accuracy_score(
    y_true,
    injected_test_pd[
        "ensemble_prediction"
    ]
)

print("\n")
print("=" * 70)
print("STEP 11F FINAL RESULTS")
print("=" * 70)

print(
    f"Training rows       : "
    f"{len(train_pd):,}"
)

print(
    f"Test rows           : "
    f"{len(injected_test_pd):,}"
)

print(
    f"Ground-truth anomalies: "
    f"{total_gt}"
)

print(
    f"TP                  : "
    f"{tp}"
)

print(
    f"TN                  : "
    f"{tn}"
)

print(
    f"FP                  : "
    f"{fp}"
)

print(
    f"FN                  : "
    f"{fn}"
)

print(
    f"Precision           : "
    f"{precision:.6f}"
)

print(
    f"Recall              : "
    f"{recall:.6f}"
)

print(
    f"F1                  : "
    f"{f1:.6f}"
)

print(
    f"Accuracy            : "
    f"{accuracy:.6f}"
)

print(
    f"Clean test FP       : "
    f"{clean_fp_count:,}"
)

print(
    f"Ensemble threshold  : "
    f"{ensemble_threshold:.6f}"
)

print(
    f"\nExecution time      : "
    f"{time.time() - start_time:.2f} seconds"
)

print("\nOutput files:")
print(OUTPUT_DIR)

print("\nStep 11F completed successfully.")

spark.stop()