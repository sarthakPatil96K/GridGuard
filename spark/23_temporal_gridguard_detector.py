"""
GridGuard - Step 11D
Final Temporal GridGuard Anomaly Detector

Purpose
-------
Train anomaly-detection baselines ONLY on the temporal training period,
then evaluate on a temporally later test period containing controlled
anomalies.

Architecture
------------
Training data
    |
    +--> Meter baseline
    +--> Seasonal baseline
    +--> Peer groups
    +--> Lag-based rolling baseline
    |
    v
Test data
    |
    +--> Historical deviation
    +--> Seasonal deviation
    +--> Rolling deviation
    +--> Peer deviation
    +--> Relative deviation
    +--> Persistence
    |
    +--> Contextual statistical detector
    |
    +--> Isolation Forest trained ONLY on training data
    |
    v
GridGuard Ensemble
    |
    +--> Detection
    +--> Evaluation
    +--> CSV / Parquet outputs

Important
---------
- Ground-truth anomalies exist ONLY in the injected test dataset.
- Training data contains no injected anomalies.
- Baselines are derived only from training data.
- Rolling features for test observations use CLEAN observations,
  preventing injected values from contaminating the rolling baseline.
- Isolation Forest is trained only on training-period features.
- IF anomaly score is a relative anomaly score, NOT a probability.
"""

import os
import shutil
import warnings

import numpy as np
import pandas as pd

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer


warnings.filterwarnings("ignore")


# ============================================================
# CONFIGURATION
# ============================================================

TRAIN_PATH = (
    "data/real/processed/temporal_train_1m"
)

TEST_CLEAN_PATH = (
    "data/real/processed/temporal_test_clean_1m"
)

TEST_INJECTED_PATH = (
    "data/real/processed/temporal_test_injected_1m"
)

METER_BASELINE_PATH = (
    "data/real/processed/temporal_meter_baselines_1m"
)

SEASONAL_BASELINE_PATH = (
    "data/real/processed/temporal_seasonal_baselines_1m"
)

PEER_BASELINE_PATH = (
    "data/real/processed/temporal_peer_baselines_1m"
)

OUTPUT_PATH = (
    "data/real/processed/temporal_gridguard_results_1m"
)


# ============================================================
# SPARK CONFIGURATION
# ============================================================

spark = (
    SparkSession.builder
    .appName("GridGuard_Temporal_Final_Detector")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.debug.maxToStringFields", "200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def remove_output(path):
    """
    Remove an existing Spark/Pandas output directory.
    """
    if os.path.exists(path):
        shutil.rmtree(path)


def calculate_metrics(y_true, y_pred):
    """
    Calculate confusion matrix and classification metrics.
    """

    y_true = np.asarray(y_true).astype(int)
    y_pred = np.asarray(y_pred).astype(int)

    tp = int(
        ((y_true == 1) & (y_pred == 1)).sum()
    )

    tn = int(
        ((y_true == 0) & (y_pred == 0)).sum()
    )

    fp = int(
        ((y_true == 0) & (y_pred == 1)).sum()
    )

    fn = int(
        ((y_true == 1) & (y_pred == 0)).sum()
    )

    precision = (
        tp / (tp + fp)
        if (tp + fp) > 0
        else 0.0
    )

    recall = (
        tp / (tp + fn)
        if (tp + fn) > 0
        else 0.0
    )

    if precision + recall > 0:
        f1 = (
            2 * precision * recall
            / (precision + recall)
        )
    else:
        f1 = 0.0

    accuracy = (
        (tp + tn) / len(y_true)
        if len(y_true) > 0
        else 0.0
    )

    return {
        "TP": tp,
        "TN": tn,
        "FP": fp,
        "FN": fn,
        "Precision": precision,
        "Recall": recall,
        "F1": f1,
        "Accuracy": accuracy,
    }


def normalize_score(series):
    """
    Min-max normalization to [0, 1].

    This is a relative anomaly score and NOT a probability.
    """

    series = pd.to_numeric(
        series,
        errors="coerce"
    ).fillna(0.0)

    min_value = series.min()
    max_value = series.max()

    if max_value - min_value <= 1e-12:
        return pd.Series(
            np.zeros(len(series)),
            index=series.index
        )

    return (
        (series - min_value)
        / (max_value - min_value)
    ).clip(0, 1)


def add_peer_group_spark(
    df,
    meter_baselines
):
    """
    Reconstruct training-derived peer groups dynamically
    from training meter baselines.

    Five groups are created using 20/40/60/80 percentiles.
    """

    thresholds = (
        meter_baselines
        .select(
            F.expr(
                "percentile_approx(baseline_avg, 0.20)"
            ).alias("p20"),

            F.expr(
                "percentile_approx(baseline_avg, 0.40)"
            ).alias("p40"),

            F.expr(
                "percentile_approx(baseline_avg, 0.60)"
            ).alias("p60"),

            F.expr(
                "percentile_approx(baseline_avg, 0.80)"
            ).alias("p80"),
        )
        .collect()[0]
    )

    p20 = float(thresholds["p20"])
    p40 = float(thresholds["p40"])
    p60 = float(thresholds["p60"])
    p80 = float(thresholds["p80"])

    print("\nTraining-derived peer thresholds:")
    print(f"20th percentile: {p20:.6f}")
    print(f"40th percentile: {p40:.6f}")
    print(f"60th percentile: {p60:.6f}")
    print(f"80th percentile: {p80:.6f}")

    peer_map = (
        meter_baselines
        .select(
            "meter_id",
            "baseline_avg"
        )
        .withColumn(
            "peer_group",
            F.when(
                F.col("baseline_avg") <= p20,
                F.lit("P1_LOW")
            )
            .when(
                F.col("baseline_avg") <= p40,
                F.lit("P2_LOW_MEDIUM")
            )
            .when(
                F.col("baseline_avg") <= p60,
                F.lit("P3_MEDIUM")
            )
            .when(
                F.col("baseline_avg") <= p80,
                F.lit("P4_MEDIUM_HIGH")
            )
            .otherwise(
                F.lit("P5_HIGH")
            )
        )
    )

    return peer_map


def prepare_static_features(
    df,
    meter_baselines,
    seasonal_baselines,
    peer_map,
    peer_baselines
):
    """
    Add all training-derived static features.

    Does NOT calculate rolling features.
    """

    df = (
        df
        .filter(
            F.col("meter_id").isNotNull()
        )
        .filter(
            F.col("date").isNotNull()
        )
        .filter(
            F.col("daily_consumption").isNotNull()
        )
        .filter(
            F.col("daily_consumption") >= 0
        )
    )

    # --------------------------------------------------------
    # Date features
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "month",
            F.month("date")
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("date")
        )
    )

    # --------------------------------------------------------
    # Meter historical baseline
    # --------------------------------------------------------

    meter_baseline_small = (
        meter_baselines
        .select(
            "meter_id",
            "baseline_avg",
            "baseline_std",
            "baseline_median",
            "baseline_p05",
            "baseline_p95"
        )
    )

    df = df.join(
        meter_baseline_small,
        on="meter_id",
        how="left"
    )

    # --------------------------------------------------------
    # Historical Z-score
    # --------------------------------------------------------

    df = df.withColumn(
        "historical_z",
        F.when(
            F.col("baseline_std") > 1e-9,
            (
                F.col("daily_consumption")
                - F.col("baseline_avg")
            )
            / F.col("baseline_std")
        )
        .otherwise(0.0)
    )

    # --------------------------------------------------------
    # Relative deviation
    # --------------------------------------------------------

    df = df.withColumn(
        "relative_deviation",
        F.when(
            F.col("baseline_avg") > 1e-9,
            (
                F.col("daily_consumption")
                - F.col("baseline_avg")
            )
            / F.col("baseline_avg")
        )
        .otherwise(0.0)
    )

    # --------------------------------------------------------
    # Median deviation
    # --------------------------------------------------------

    df = df.withColumn(
        "median_deviation",
        F.when(
            F.col("baseline_median") > 1e-9,
            (
                F.col("daily_consumption")
                - F.col("baseline_median")
            )
            / F.col("baseline_median")
        )
        .otherwise(0.0)
    )

    # --------------------------------------------------------
    # Percentile signal
    # --------------------------------------------------------

    df = df.withColumn(
        "outside_percentile_range",
        F.when(
            (
                F.col("daily_consumption")
                < F.col("baseline_p05")
            )
            |
            (
                F.col("daily_consumption")
                > F.col("baseline_p95")
            ),
            1
        )
        .otherwise(0)
    )

    # --------------------------------------------------------
    # Seasonal baseline
    # --------------------------------------------------------

    seasonal_small = (
        seasonal_baselines
        .select(
            "meter_id",
            "month",
            "day_of_week",
            "seasonal_avg",
            "seasonal_std",
            "seasonal_p05",
            "seasonal_p95"
        )
    )

    df = df.join(
        seasonal_small,
        on=[
            "meter_id",
            "month",
            "day_of_week"
        ],
        how="left"
    )

    # Fallback to meter baseline if no seasonal baseline exists.
    df = (
        df
        .withColumn(
            "seasonal_avg",
            F.coalesce(
                F.col("seasonal_avg"),
                F.col("baseline_avg")
            )
        )
        .withColumn(
            "seasonal_std",
            F.coalesce(
                F.col("seasonal_std"),
                F.col("baseline_std")
            )
        )
        .withColumn(
            "seasonal_p05",
            F.coalesce(
                F.col("seasonal_p05"),
                F.col("baseline_p05")
            )
        )
        .withColumn(
            "seasonal_p95",
            F.coalesce(
                F.col("seasonal_p95"),
                F.col("baseline_p95")
            )
        )
    )

    # --------------------------------------------------------
    # Seasonal Z-score
    # --------------------------------------------------------

    df = df.withColumn(
        "seasonal_z",
        F.when(
            F.col("seasonal_std") > 1e-9,
            (
                F.col("daily_consumption")
                - F.col("seasonal_avg")
            )
            / F.col("seasonal_std")
        )
        .otherwise(0.0)
    )

    df = df.withColumn(
        "seasonal_relative",
        F.when(
            F.col("seasonal_avg") > 1e-9,
            (
                F.col("daily_consumption")
                - F.col("seasonal_avg")
            )
            / F.col("seasonal_avg")
        )
        .otherwise(0.0)
    )

    # --------------------------------------------------------
    # Peer group
    # --------------------------------------------------------

    df = df.join(
        peer_map,
        on="meter_id",
        how="left"
    )

    # --------------------------------------------------------
    # Peer baseline
    # --------------------------------------------------------

    peer_small = (
        peer_baselines
        .select(
            "peer_group",
            "peer_average_consumption",
            "peer_median_consumption",
            "peer_std_consumption"
        )
    )

    df = df.join(
        peer_small,
        on="peer_group",
        how="left"
    )

    # --------------------------------------------------------
    # Peer deviation
    # --------------------------------------------------------

    df = df.withColumn(
        "peer_deviation",
        F.when(
            F.col("peer_median_consumption") > 1e-9,
            (
                F.col("daily_consumption")
                - F.col("peer_median_consumption")
            )
            / F.col("peer_median_consumption")
        )
        .otherwise(0.0)
    )

    # --------------------------------------------------------
    # Peer Z-score
    # --------------------------------------------------------

    df = df.withColumn(
        "peer_z",
        F.when(
            F.col("peer_std_consumption") > 1e-9,
            (
                F.col("daily_consumption")
                - F.col("peer_average_consumption")
            )
            / F.col("peer_std_consumption")
        )
        .otherwise(0.0)
    )

    return df


def add_lag_rolling_features(df):
    """
    Calculate a lag-only rolling baseline.

    Previous 30 observations are used.

    Current observation is NEVER included.

    This prevents the current anomaly from contaminating
    its own baseline.
    """

    rolling_window = (
        Window
        .partitionBy("meter_id")
        .orderBy("date")
        .rowsBetween(-30, -1)
    )

    df = df.withColumn(
        "rolling_avg",
        F.avg(
            "daily_consumption"
        ).over(rolling_window)
    )

    df = df.withColumn(
        "rolling_std",
        F.stddev_samp(
            "daily_consumption"
        ).over(rolling_window)
    )

    return df


def add_rolling_z(df):
    """
    Calculate rolling Z-score using the previously
    calculated lag-only rolling statistics.
    """

    df = df.withColumn(
        "rolling_z",
        F.when(
            F.col("rolling_std") > 1e-9,
            (
                F.col("daily_consumption")
                - F.col("rolling_avg")
            )
            / F.col("rolling_std")
        )
        .otherwise(0.0)
    )

    return df


# ============================================================
# START
# ============================================================

print("=" * 75)
print("GRIDGUARD - STEP 11D")
print("FINAL TEMPORAL GRIDGUARD ANOMALY DETECTOR")
print("=" * 75)


# ============================================================
# LOAD DATA
# ============================================================

print("\nReading TRAINING data...")

train_raw = spark.read.parquet(
    TRAIN_PATH
)

train_raw = (
    train_raw
    .filter(
        F.col("daily_consumption").isNotNull()
    )
    .filter(
        F.col("daily_consumption") >= 0
    )
)

train_count = train_raw.count()

print(
    f"Training records: {train_count:,}"
)


print("\nReading CLEAN temporal TEST data...")

test_clean_raw = spark.read.parquet(
    TEST_CLEAN_PATH
)

test_clean_raw = (
    test_clean_raw
    .filter(
        F.col("daily_consumption").isNotNull()
    )
    .filter(
        F.col("daily_consumption") >= 0
    )
)

test_clean_count = test_clean_raw.count()

print(
    f"Clean test records: "
    f"{test_clean_count:,}"
)


print("\nReading INJECTED temporal TEST data...")

test_injected_raw = spark.read.parquet(
    TEST_INJECTED_PATH
)

test_injected_raw = (
    test_injected_raw
    .filter(
        F.col("daily_consumption").isNotNull()
    )
    .filter(
        F.col("daily_consumption") >= 0
    )
)

test_injected_count = test_injected_raw.count()

print(
    f"Injected test records: "
    f"{test_injected_count:,}"
)


# ============================================================
# LOAD TRAINING BASELINES
# ============================================================

print("\nReading TRAINING-ONLY baselines...")

meter_baselines = spark.read.parquet(
    METER_BASELINE_PATH
)

seasonal_baselines = spark.read.parquet(
    SEASONAL_BASELINE_PATH
)

peer_baselines = spark.read.parquet(
    PEER_BASELINE_PATH
)

print(
    f"Meter baselines: "
    f"{meter_baselines.count():,}"
)

print(
    f"Seasonal baselines: "
    f"{seasonal_baselines.count():,}"
)

print(
    f"Peer groups: "
    f"{peer_baselines.count():,}"
)


# ============================================================
# RECONSTRUCT PEER GROUPS
# ============================================================

print(
    "\nReconstructing training-derived peer groups..."
)

peer_map = add_peer_group_spark(
    meter_baselines,
    meter_baselines
)

print(
    f"Peer assignments: "
    f"{peer_map.count():,}"
)


# ============================================================
# PREPARE TRAINING STATIC FEATURES
# ============================================================

print(
    "\nPreparing TRAINING static features..."
)

train_features = prepare_static_features(
    train_raw,
    meter_baselines,
    seasonal_baselines,
    peer_map,
    peer_baselines
)


# ============================================================
# TRAINING ROLLING BASELINE
# ============================================================

print(
    "Calculating TRAIN lag-based rolling deviation..."
)

train_features = add_lag_rolling_features(
    train_features
)

train_features = add_rolling_z(
    train_features
)


# ============================================================
# PREPARE CLEAN TEST STATIC FEATURES
# ============================================================

print(
    "\nPreparing CLEAN TEST static features..."
)

test_clean_features = prepare_static_features(
    test_clean_raw,
    meter_baselines,
    seasonal_baselines,
    peer_map,
    peer_baselines
)


# ============================================================
# CLEAN TEST ROLLING BASELINE
# ============================================================

print(
    "Calculating CLEAN TEST lag-based rolling baseline..."
)

"""
For test observations, rolling context must use:

TRAIN + CLEAN TEST

and must NEVER use injected test values.
"""

train_for_rolling = (
    train_raw
    .select(
        "meter_id",
        "date",
        "daily_consumption"
    )
)

clean_test_for_rolling = (
    test_clean_raw
    .select(
        "meter_id",
        "date",
        "daily_consumption"
    )
)

rolling_source = (
    train_for_rolling
    .unionByName(
        clean_test_for_rolling
    )
    .dropDuplicates(
        ["meter_id", "date"]
    )
)

rolling_source = (
    rolling_source
    .filter(
        F.col("daily_consumption").isNotNull()
    )
    .filter(
        F.col("daily_consumption") >= 0
    )
)

rolling_source = add_lag_rolling_features(
    rolling_source
)

rolling_source = add_rolling_z(
    rolling_source
)


# Keep only rolling information for test dates.

test_rolling_dates = (
    test_clean_raw
    .select(
        "meter_id",
        "date"
    )
    .dropDuplicates(
        ["meter_id", "date"]
    )
)

test_rolling_features = (
    rolling_source
    .select(
        "meter_id",
        "date",
        "rolling_avg",
        "rolling_std"
    )
    .join(
        test_rolling_dates,
        on=[
            "meter_id",
            "date"
        ],
        how="inner"
    )
)

print(
    f"Clean test rolling rows: "
    f"{test_rolling_features.count():,}"
)


# ============================================================
# PREPARE INJECTED TEST STATIC FEATURES
# ============================================================

print(
    "\nPreparing INJECTED TEST static features..."
)

test_features = prepare_static_features(
    test_injected_raw,
    meter_baselines,
    seasonal_baselines,
    peer_map,
    peer_baselines
)


# ============================================================
# JOIN CLEAN ROLLING FEATURES
# ============================================================

print(
    "Joining CLEAN rolling baseline into injected TEST..."
)

# The static feature function does not create rolling columns,
# but this drop makes the code safe if an earlier version did.

for rolling_column in [
    "rolling_avg",
    "rolling_std",
    "rolling_z"
]:

    if rolling_column in test_features.columns:
        test_features = test_features.drop(
            rolling_column
        )

test_features = test_features.join(
    test_rolling_features,
    on=[
        "meter_id",
        "date"
    ],
    how="left"
)

# Calculate rolling Z using the CLEAN rolling baseline.

test_features = add_rolling_z(
    test_features
)


# ============================================================
# FEATURE COLUMNS
# ============================================================

feature_columns = [
    "daily_consumption",
    "historical_z",
    "relative_deviation",
    "seasonal_z",
    "seasonal_relative",
    "rolling_z",
    "peer_deviation",
]


print(
    "\nVerifying TRAIN feature columns..."
)

missing_train = [
    column
    for column in feature_columns
    if column not in train_features.columns
]

if missing_train:
    raise RuntimeError(
        "Missing TRAIN feature columns: "
        + str(missing_train)
    )

print(
    "TRAIN feature verification PASSED."
)


print(
    "\nVerifying TEST feature columns..."
)

missing_test = [
    column
    for column in feature_columns
    if column not in test_features.columns
]

if missing_test:
    raise RuntimeError(
        "Missing TEST feature columns: "
        + str(missing_test)
    )

print(
    "TEST feature verification PASSED."
)


# ============================================================
# BUILD CONTEXTUAL SIGNALS IN SPARK
# ============================================================

print(
    "\nGenerating contextual detection signals..."
)

test_features = (
    test_features

    # --------------------------------------------------------
    # Historical signal
    # --------------------------------------------------------

    .withColumn(
        "historical_signal",
        F.when(
            F.abs(
                F.col("historical_z")
            ) >= 3.0,
            1
        )
        .otherwise(0)
    )

    # --------------------------------------------------------
    # Seasonal signal
    # --------------------------------------------------------

    .withColumn(
        "seasonal_signal",
        F.when(
            F.abs(
                F.col("seasonal_z")
            ) >= 3.0,
            1
        )
        .otherwise(0)
    )

    # --------------------------------------------------------
    # Rolling signal
    # --------------------------------------------------------

    .withColumn(
        "rolling_signal",
        F.when(
            F.abs(
                F.col("rolling_z")
            ) >= 3.0,
            1
        )
        .otherwise(0)
    )

    # --------------------------------------------------------
    # Percentile signal
    # --------------------------------------------------------

    .withColumn(
        "percentile_signal",
        F.when(
            (
                F.col("daily_consumption")
                < F.col("baseline_p05")
            )
            |
            (
                F.col("daily_consumption")
                > F.col("baseline_p95")
            ),
            1
        )
        .otherwise(0)
    )

    # --------------------------------------------------------
    # Relative signal
    # --------------------------------------------------------

    .withColumn(
        "relative_signal",
        F.when(
            F.abs(
                F.col("relative_deviation")
            ) >= 1.0,
            1
        )
        .otherwise(0)
    )

    # --------------------------------------------------------
    # Peer signal
    # --------------------------------------------------------

    .withColumn(
        "peer_signal",
        F.when(
            F.abs(
                F.col("peer_z")
            ) >= 3.0,
            1
        )
        .otherwise(0)
    )
)


# ============================================================
# CONVERT TO PANDAS
# ============================================================

print(
    "\nConverting temporal feature sets to Pandas..."
)


# ------------------------------------------------------------
# TRAIN DATA
# ------------------------------------------------------------

train_pdf = (
    train_features
    .select(
        "meter_id",
        "date",
        "daily_consumption",
        "historical_z",
        "relative_deviation",
        "seasonal_z",
        "seasonal_relative",
        "rolling_z",
        "peer_deviation",
    )
    .orderBy(
        "meter_id",
        "date"
    )
    .toPandas()
)


# ------------------------------------------------------------
# TEST DATA
#
# IMPORTANT:
# Do NOT use a conditional expression inside select().
# historical_signal must appear exactly once.
# contextual_score is NOT created yet.
# ------------------------------------------------------------

test_pdf = (
    test_features
    .select(
        "meter_id",
        "date",
        "daily_consumption",
        "ground_truth",
        "anomaly_type",

        # Numerical features
        "historical_z",
        "relative_deviation",
        "seasonal_z",
        "seasonal_relative",
        "rolling_z",
        "peer_deviation",

        # Detection signals
        "historical_signal",
        "seasonal_signal",
        "rolling_signal",
        "percentile_signal",
        "relative_signal",
        "peer_signal",
    )
    .orderBy(
        "meter_id",
        "date"
    )
    .toPandas()
)


# ============================================================
# PANDAS DATAFRAME VALIDATION
# ============================================================

train_pdf = (
    train_pdf
    .reset_index(drop=True)
)

test_pdf = (
    test_pdf
    .reset_index(drop=True)
)

train_pdf.index = pd.RangeIndex(
    len(train_pdf)
)

test_pdf.index = pd.RangeIndex(
    len(test_pdf)
)

print(
    "\nPandas DataFrame verification:"
)

print(
    f"TRAIN rows: "
    f"{len(train_pdf):,}"
)

print(
    f"TEST rows: "
    f"{len(test_pdf):,}"
)

print(
    f"TRAIN index unique: "
    f"{train_pdf.index.is_unique}"
)

print(
    f"TEST index unique: "
    f"{test_pdf.index.is_unique}"
)

print(
    f"TRAIN columns unique: "
    f"{train_pdf.columns.is_unique}"
)

print(
    f"TEST columns unique: "
    f"{test_pdf.columns.is_unique}"
)


train_duplicates = (
    train_pdf.columns[
        train_pdf.columns.duplicated()
    ]
    .tolist()
)

test_duplicates = (
    test_pdf.columns[
        test_pdf.columns.duplicated()
    ]
    .tolist()
)

if train_duplicates:
    raise RuntimeError(
        "Duplicate TRAIN Pandas columns: "
        + str(train_duplicates)
    )

if test_duplicates:
    raise RuntimeError(
        "Duplicate TEST Pandas columns: "
        + str(test_duplicates)
    )

print(
    "Pandas column verification PASSED."
)


# ============================================================
# NUMERIC CLEANUP
# ============================================================

for column in feature_columns:

    train_pdf[column] = pd.to_numeric(
        train_pdf[column],
        errors="coerce"
    )


for column in [
    "daily_consumption",
    "historical_z",
    "relative_deviation",
    "seasonal_z",
    "seasonal_relative",
    "rolling_z",
    "peer_deviation",
]:

    test_pdf[column] = pd.to_numeric(
        test_pdf[column],
        errors="coerce"
    )


# ============================================================
# IMPUTE MISSING TRAINING FEATURES
# ============================================================

print(
    "\nPreparing Isolation Forest training data..."
)

imputer = SimpleImputer(
    strategy="median"
)

X_train = imputer.fit_transform(
    train_pdf[feature_columns]
)

X_test = imputer.transform(
    test_pdf[feature_columns]
)


# ============================================================
# ISOLATION FOREST
# ============================================================

print(
    "\nTraining Isolation Forest..."
)

if_model = IsolationForest(
    n_estimators=250,
    contamination=0.01,
    random_state=42,
    n_jobs=-1
)

if_model.fit(
    X_train
)

print(
    "Isolation Forest training completed."
)


# ============================================================
# ISOLATION FOREST PREDICTION
# ============================================================

test_pdf["if_prediction"] = (
    if_model.predict(
        X_test
    ) == -1
).astype(int)


# decision_function:
# larger value = more normal
#
# Therefore multiply by -1 so larger value means
# more anomalous.

raw_if_score = (
    -if_model.decision_function(
        X_test
    )
)

test_pdf["if_raw_score"] = (
    raw_if_score
)

test_pdf["if_anomaly_score"] = (
    normalize_score(
        test_pdf["if_raw_score"]
    )
)


# ============================================================
# CONTEXTUAL SCORE
# ============================================================

print(
    "\nCalculating GridGuard contextual score..."
)


signal_columns = [
    "historical_signal",
    "seasonal_signal",
    "rolling_signal",
    "percentile_signal",
    "relative_signal",
    "peer_signal",
]


test_pdf["basic_signal_count"] = (
    test_pdf[
        signal_columns
    ]
    .sum(axis=1)
)


# ------------------------------------------------------------
# Individual contextual components
#
# Components are bounded to [0,1].
# ------------------------------------------------------------

historical_component = (
    test_pdf["historical_z"]
    .abs()
    .div(6.0)
    .clip(0, 1)
)


seasonal_component = (
    test_pdf["seasonal_z"]
    .abs()
    .div(6.0)
    .clip(0, 1)
)


rolling_component = (
    test_pdf["rolling_z"]
    .abs()
    .div(6.0)
    .clip(0, 1)
)


percentile_component = (
    test_pdf["percentile_signal"]
    .astype(float)
)


relative_component = (
    test_pdf["relative_deviation"]
    .abs()
    .div(3.0)
    .clip(0, 1)
)


peer_component = (
    test_pdf["peer_deviation"]
    .abs()
    .div(2.0)
    .clip(0, 1)
)


# ============================================================
# TEMPORAL PERSISTENCE
# ============================================================

print(
    "Calculating temporal persistence..."
)


# ------------------------------------------------------------
# Sort chronologically within each meter
# ------------------------------------------------------------

test_pdf = (
    test_pdf
    .sort_values(
        [
            "meter_id",
            "date"
        ],
        kind="mergesort"
    )
    .reset_index(drop=True)
)

test_pdf.index = pd.RangeIndex(
    len(test_pdf)
)


# ------------------------------------------------------------
# Candidate abnormal observation
#
# At least TWO independent contextual signals
# are required.
# ------------------------------------------------------------

test_pdf["candidate_signal"] = (
    test_pdf["basic_signal_count"]
    .ge(2)
    .astype(np.int8)
)


# ------------------------------------------------------------
# Validate Pandas structure BEFORE persistence
# ------------------------------------------------------------

print(
    "\nPandas index verification:"
)

print(
    f"Rows: {len(test_pdf):,}"
)

print(
    f"Index unique: "
    f"{test_pdf.index.is_unique}"
)

print(
    f"Columns unique: "
    f"{test_pdf.columns.is_unique}"
)

if not test_pdf.index.is_unique:

    raise RuntimeError(
        "Pandas index is still not unique."
    )

if not test_pdf.columns.is_unique:

    duplicate_columns = (
        test_pdf.columns[
            test_pdf.columns.duplicated()
        ]
        .tolist()
    )

    raise RuntimeError(
        "Pandas columns contain duplicates: "
        + str(duplicate_columns)
    )


# ------------------------------------------------------------
# Calculate persistence separately for each meter.
#
# Current observation + previous 4 observations.
#
# NumPy positional assignment is used intentionally to
# avoid Pandas index-alignment problems.
# ------------------------------------------------------------

persistence_values = np.zeros(
    len(test_pdf),
    dtype=np.float64
)


for meter_id, group_index in (
    test_pdf
    .groupby(
        "meter_id",
        sort=False
    )
    .groups
    .items()
):

    positions = np.asarray(
        list(group_index),
        dtype=np.int64
    )

    candidate_values = (
        test_pdf
        .iloc[
            positions
        ]["candidate_signal"]
        .to_numpy(
            dtype=np.float64
        )
    )

    rolling_values = (
        pd.Series(
            candidate_values
        )
        .rolling(
            window=5,
            min_periods=1
        )
        .sum()
        .to_numpy(
            dtype=np.float64
        )
    )

    persistence_values[
        positions
    ] = rolling_values


test_pdf["persistence_count"] = (
    persistence_values
)


# Persistence component normalized to [0,1].

persistence_component = (
    test_pdf["persistence_count"]
    .div(5.0)
    .clip(
        lower=0.0,
        upper=1.0
    )
)


# ============================================================
# CONTEXTUAL ENSEMBLE SCORE
# ============================================================

test_pdf["contextual_score"] = (
    0.20 * historical_component
    + 0.15 * seasonal_component
    + 0.15 * rolling_component
    + 0.15 * percentile_component
    + 0.15 * relative_component
    + 0.10 * peer_component
    + 0.10 * persistence_component
)

test_pdf["contextual_score"] = (
    test_pdf["contextual_score"]
    .clip(0, 1)
)


# ============================================================
# CONTEXTUAL STATISTICAL PREDICTION
# ============================================================

test_pdf["contextual_prediction"] = (
    (
        test_pdf["basic_signal_count"] >= 2
    )
    |
    (
        (test_pdf["persistence_count"] >= 3)
        &
        (
            (
                test_pdf["historical_signal"] == 1
            )
            |
            (
                test_pdf["seasonal_signal"] == 1
            )
            |
            (
                test_pdf["relative_signal"] == 1
            )
        )
    )
).astype(int)


# ============================================================
# FINAL GRIDGUARD ENSEMBLE
# ============================================================

print(
    "\nCalculating final GridGuard ensemble..."
)

test_pdf["ensemble_score"] = (
    0.70 * test_pdf["contextual_score"]
    + 0.30 * test_pdf["if_anomaly_score"]
)

test_pdf["ensemble_score"] = (
    test_pdf["ensemble_score"]
    .clip(0, 1)
)

test_pdf["ensemble_prediction"] = (
    test_pdf["ensemble_score"] >= 0.60
).astype(int)


# ============================================================
# GROUND TRUTH
# ============================================================

test_pdf["ground_truth"] = (
    pd.to_numeric(
        test_pdf["ground_truth"],
        errors="coerce"
    )
    .fillna(0)
    .astype(int)
)


# ============================================================
# FINAL PANDAS VALIDATION
# ============================================================

print(
    "\nFinal Pandas column verification:"
)

if not test_pdf.columns.is_unique:

    duplicate_columns = (
        test_pdf.columns[
            test_pdf.columns.duplicated()
        ]
        .tolist()
    )

    raise RuntimeError(
        "Duplicate columns remain: "
        + str(duplicate_columns)
    )

print(
    "Final Pandas column verification PASSED."
)


# ============================================================
# EVALUATION
# ============================================================

print("\n")
print("=" * 75)
print("MODEL EVALUATION")
print("=" * 75)


y_true = test_pdf["ground_truth"]


# ------------------------------------------------------------
# Contextual detector
# ------------------------------------------------------------

stat_metrics = calculate_metrics(
    y_true,
    test_pdf[
        "contextual_prediction"
    ]
)


# ------------------------------------------------------------
# Isolation Forest
# ------------------------------------------------------------

if_metrics = calculate_metrics(
    y_true,
    test_pdf[
        "if_prediction"
    ]
)


# ------------------------------------------------------------
# GridGuard Ensemble
# ------------------------------------------------------------

ensemble_metrics = calculate_metrics(
    y_true,
    test_pdf[
        "ensemble_prediction"
    ]
)


comparison = pd.DataFrame(
    [
        {
            "Model":
                "Contextual Statistical Detector",
            **stat_metrics
        },
        {
            "Model":
                "Isolation Forest",
            **if_metrics
        },
        {
            "Model":
                "GridGuard Temporal Ensemble",
            **ensemble_metrics
        }
    ]
)


print(
    comparison.to_string(
        index=False
    )
)


# ============================================================
# ANOMALY TYPE EVALUATION
# ============================================================

print("\n")
print("=" * 75)
print("ANOMALY TYPE RESULTS")
print("=" * 75)


anomaly_type_rows = []


ground_truth_groups = (
    test_pdf[
        test_pdf["ground_truth"] == 1
    ]
    .groupby(
        "anomaly_type"
    )
)


for anomaly_type, group in (
    ground_truth_groups
):

    total = len(group)

    detected = int(
        group[
            "ensemble_prediction"
        ].sum()
    )

    detection_rate = (
        detected / total
        if total > 0
        else 0.0
    )

    anomaly_type_rows.append(
        {
            "anomaly_type":
                anomaly_type,

            "ground_truth_count":
                total,

            "detected_count":
                detected,

            "detection_rate":
                detection_rate
        }
    )


anomaly_type_results = pd.DataFrame(
    anomaly_type_rows
)


if not anomaly_type_results.empty:

    print(
        anomaly_type_results.to_string(
            index=False
        )
    )


# ============================================================
# GROUND TRUTH DETECTIONS
# ============================================================

print("\n")
print("=" * 75)
print("GROUND-TRUTH ANOMALIES")
print("=" * 75)


ground_truth_columns = [
    "meter_id",
    "date",
    "daily_consumption",
    "anomaly_type",
    "historical_z",
    "seasonal_z",
    "rolling_z",
    "peer_deviation",
    "relative_deviation",
    "contextual_score",
    "if_anomaly_score",
    "ensemble_score",
    "contextual_prediction",
    "if_prediction",
    "ensemble_prediction",
]


ground_truth_detections = (
    test_pdf[
        test_pdf["ground_truth"] == 1
    ][
        ground_truth_columns
    ]
    .sort_values(
        [
            "meter_id",
            "date"
        ]
    )
)


if not ground_truth_detections.empty:

    print(
        ground_truth_detections.to_string(
            index=False
        )
    )


# ============================================================
# TOP DETECTED ANOMALIES
# ============================================================

print("\n")
print("=" * 75)
print("TOP 50 DETECTED ANOMALIES")
print("=" * 75)


top_detected = (
    test_pdf[
        test_pdf["ensemble_prediction"] == 1
    ]
    .sort_values(
        "ensemble_score",
        ascending=False
    )
    .head(50)
)


top_columns = [
    "meter_id",
    "date",
    "daily_consumption",
    "ground_truth",
    "anomaly_type",
    "historical_z",
    "seasonal_z",
    "rolling_z",
    "peer_deviation",
    "relative_deviation",
    "contextual_score",
    "if_anomaly_score",
    "ensemble_score",
]


print(
    top_detected[
        top_columns
    ].to_string(
        index=False
    )
)


# ============================================================
# SUMMARY
# ============================================================

print("\n")
print("=" * 75)
print("FINAL SUMMARY")
print("=" * 75)


print(
    f"Training records: "
    f"{len(train_pdf):,}"
)

print(
    f"Test records: "
    f"{len(test_pdf):,}"
)

print(
    f"Ground-truth anomalies: "
    f"{int(y_true.sum()):,}"
)

print(
    f"Contextual predictions: "
    f"{int(test_pdf['contextual_prediction'].sum()):,}"
)

print(
    f"Isolation Forest predictions: "
    f"{int(test_pdf['if_prediction'].sum()):,}"
)

print(
    f"GridGuard ensemble predictions: "
    f"{int(test_pdf['ensemble_prediction'].sum()):,}"
)


# ============================================================
# OUTPUT DIRECTORY
# ============================================================

remove_output(
    OUTPUT_PATH
)

os.makedirs(
    OUTPUT_PATH,
    exist_ok=True
)


# ============================================================
# SAVE COMPLETE RESULTS
# ============================================================

result_columns = [
    "meter_id",
    "date",
    "daily_consumption",
    "ground_truth",
    "anomaly_type",

    # Historical
    "historical_z",
    "relative_deviation",

    # Seasonal
    "seasonal_z",
    "seasonal_relative",

    # Rolling
    "rolling_z",

    # Peer
    "peer_deviation",

    # Signals
    "historical_signal",
    "seasonal_signal",
    "rolling_signal",
    "percentile_signal",
    "relative_signal",
    "peer_signal",

    # Persistence
    "basic_signal_count",
    "persistence_count",

    # Statistical model
    "contextual_score",
    "contextual_prediction",

    # ML
    "if_raw_score",
    "if_anomaly_score",
    "if_prediction",

    # Final
    "ensemble_score",
    "ensemble_prediction",
]


result_pdf = (
    test_pdf[
        result_columns
    ]
    .copy()
)


# ------------------------------------------------------------
# Save Parquet
# ------------------------------------------------------------

result_parquet = os.path.join(
    OUTPUT_PATH,
    "temporal_gridguard_results.parquet"
)

result_pdf.to_parquet(
    result_parquet,
    index=False
)


# ------------------------------------------------------------
# Save model comparison
# ------------------------------------------------------------

comparison_path = os.path.join(
    OUTPUT_PATH,
    "model_comparison.csv"
)

comparison.to_csv(
    comparison_path,
    index=False
)


# ------------------------------------------------------------
# Save anomaly type results
# ------------------------------------------------------------

anomaly_type_path = os.path.join(
    OUTPUT_PATH,
    "anomaly_type_results.csv"
)

anomaly_type_results.to_csv(
    anomaly_type_path,
    index=False
)


# ------------------------------------------------------------
# Save ground truth detections
# ------------------------------------------------------------

ground_truth_path = os.path.join(
    OUTPUT_PATH,
    "ground_truth_detections.csv"
)

ground_truth_detections.to_csv(
    ground_truth_path,
    index=False
)


# ------------------------------------------------------------
# Save top 50
# ------------------------------------------------------------

top50_path = os.path.join(
    OUTPUT_PATH,
    "detected_anomalies_top50.csv"
)

top_detected[
    top_columns
].to_csv(
    top50_path,
    index=False
)


# ============================================================
# FINAL ROW COUNT VERIFICATION
# ============================================================

saved_pdf = pd.read_parquet(
    result_parquet
)

saved_count = len(
    saved_pdf
)


print("\n")
print("=" * 75)
print("OUTPUT VALIDATION")
print("=" * 75)


print(
    f"Input TEST records: "
    f"{test_injected_count:,}"
)

print(
    f"Output records: "
    f"{saved_count:,}"
)


if saved_count == test_injected_count:

    print(
        "SUCCESS: Row count verification PASSED."
    )

else:

    print(
        "ERROR: Row count verification FAILED."
    )


# ============================================================
# OUTPUT FILES
# ============================================================

print("\n")
print("=" * 75)
print("OUTPUT FILES")
print("=" * 75)


print(
    f"1. {result_parquet}"
)

print(
    f"2. {comparison_path}"
)

print(
    f"3. {anomaly_type_path}"
)

print(
    f"4. {ground_truth_path}"
)

print(
    f"5. {top50_path}"
)


# ============================================================
# CLEANUP
# ============================================================

spark.stop()


print("\n")
print("=" * 75)
print("STEP 11D COMPLETED SUCCESSFULLY")
print("=" * 75)