import os
import shutil
import pandas as pd
import numpy as np


# ============================================================
# CONFIGURATION
# ============================================================

INPUT_PATH = (
    "data/real/processed/"
    "temporal_gridguard_results_1m/"
    "temporal_gridguard_results.parquet"
)

OUTPUT_DIR = (
    "data/real/processed/"
    "false_positive_analysis_1m"
)


# ============================================================
# OUTPUT DIRECTORY
# ============================================================

if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# READ RESULTS
# ============================================================

print("=" * 75)
print("GRIDGUARD - STEP 11E")
print("FALSE-POSITIVE ANALYSIS")
print("=" * 75)

print("\nReading temporal GridGuard results...")

df = pd.read_parquet(INPUT_PATH)

print(f"Total records: {len(df):,}")


# ============================================================
# BASIC VALIDATION
# ============================================================

required_columns = [
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
    "ensemble_prediction"
]

missing = [c for c in required_columns if c not in df.columns]

if missing:
    raise ValueError(f"Missing columns: {missing}")


# ============================================================
# CLASSIFY RESULTS
# ============================================================

df["result_type"] = np.select(
    [
        (df["ground_truth"] == 1) &
        (df["ensemble_prediction"] == 1),

        (df["ground_truth"] == 0) &
        (df["ensemble_prediction"] == 1),

        (df["ground_truth"] == 1) &
        (df["ensemble_prediction"] == 0),

        (df["ground_truth"] == 0) &
        (df["ensemble_prediction"] == 0)
    ],
    [
        "true_positive",
        "false_positive",
        "false_negative",
        "true_negative"
    ],
    default="unknown"
)


# ============================================================
# SUMMARY
# ============================================================

print("\n" + "=" * 75)
print("CLASSIFICATION SUMMARY")
print("=" * 75)

print(
    df["result_type"]
    .value_counts()
    .to_string()
)


# ============================================================
# FALSE POSITIVES
# ============================================================

fp = df[df["result_type"] == "false_positive"].copy()

print("\nFalse positives:", len(fp))


# ============================================================
# FALSE POSITIVE BY METER
# ============================================================

fp_by_meter = (
    fp.groupby("meter_id")
      .agg(
          false_positives=("meter_id", "size"),
          avg_consumption=("daily_consumption", "mean"),
          avg_ensemble_score=("ensemble_score", "mean"),
          max_ensemble_score=("ensemble_score", "max"),
          avg_historical_z=("historical_z", "mean"),
          avg_seasonal_z=("seasonal_z", "mean"),
          avg_rolling_z=("rolling_z", "mean"),
          avg_peer_deviation=("peer_deviation", "mean"),
          avg_relative_deviation=("relative_deviation", "mean")
      )
      .reset_index()
      .sort_values("false_positives", ascending=False)
)

print("\nTop meters producing false positives:")
print(fp_by_meter.head(20).to_string(index=False))


# ============================================================
# FALSE POSITIVE SCORE DISTRIBUTION
# ============================================================

fp_score_distribution = pd.DataFrame({
    "metric": [
        "count",
        "mean",
        "median",
        "p75",
        "p90",
        "p95",
        "p99",
        "max"
    ],
    "ensemble_score": [
        len(fp),
        fp["ensemble_score"].mean(),
        fp["ensemble_score"].median(),
        fp["ensemble_score"].quantile(0.75),
        fp["ensemble_score"].quantile(0.90),
        fp["ensemble_score"].quantile(0.95),
        fp["ensemble_score"].quantile(0.99),
        fp["ensemble_score"].max()
    ]
})

print("\nFalse-positive ensemble score distribution:")
print(fp_score_distribution.to_string(index=False))


# ============================================================
# THRESHOLD ANALYSIS
# ============================================================

print("\n" + "=" * 75)
print("ENSEMBLE THRESHOLD ANALYSIS")
print("=" * 75)

thresholds = [
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

threshold_rows = []

for threshold in thresholds:

    prediction = (
        df["ensemble_score"] >= threshold
    ).astype(int)

    actual = df["ground_truth"].astype(int)

    tp = int(((prediction == 1) & (actual == 1)).sum())
    tn = int(((prediction == 0) & (actual == 0)).sum())
    fp_count = int(((prediction == 1) & (actual == 0)).sum())
    fn = int(((prediction == 0) & (actual == 1)).sum())

    precision = (
        tp / (tp + fp_count)
        if (tp + fp_count) > 0
        else 0
    )

    recall = (
        tp / (tp + fn)
        if (tp + fn) > 0
        else 0
    )

    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall) > 0
        else 0
    )

    accuracy = (
        (tp + tn) / len(df)
    )

    threshold_rows.append({
        "threshold": threshold,
        "TP": tp,
        "TN": tn,
        "FP": fp_count,
        "FN": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": accuracy
    })


threshold_df = pd.DataFrame(threshold_rows)

print(
    threshold_df.to_string(index=False)
)


# ============================================================
# SIGNAL ANALYSIS
# ============================================================

print("\n" + "=" * 75)
print("FALSE-POSITIVE SIGNAL ANALYSIS")
print("=" * 75)

fp["historical_signal"] = (
    fp["historical_z"].abs() >= 3
).astype(int)

fp["seasonal_signal"] = (
    fp["seasonal_z"].abs() >= 3
).astype(int)

fp["rolling_signal"] = (
    fp["rolling_z"].abs() >= 3
).astype(int)

fp["relative_signal"] = (
    fp["relative_deviation"].abs() >= 1
).astype(int)

fp["peer_signal"] = (
    fp["peer_deviation"].abs() >= 1
).astype(int)

signal_columns = [
    "historical_signal",
    "seasonal_signal",
    "rolling_signal",
    "relative_signal",
    "peer_signal"
]

fp["signal_count"] = fp[signal_columns].sum(axis=1)

signal_summary = (
    fp.groupby("signal_count")
      .size()
      .reset_index(name="false_positive_count")
      .sort_values("signal_count")
)

print(
    signal_summary.to_string(index=False)
)


# ============================================================
# LOW-CONSUMPTION FALSE POSITIVES
# ============================================================

low_consumption_threshold = (
    df["daily_consumption"]
    .quantile(0.10)
)

fp["low_consumption"] = (
    fp["daily_consumption"] <= low_consumption_threshold
)

low_fp = fp[fp["low_consumption"]].copy()

print(
    f"\n10th percentile consumption threshold: "
    f"{low_consumption_threshold:.6f}"
)

print(
    f"Low-consumption false positives: "
    f"{len(low_fp):,}"
)


# ============================================================
# HIGH-CONSUMPTION FALSE POSITIVES
# ============================================================

high_consumption_threshold = (
    df["daily_consumption"]
    .quantile(0.90)
)

fp["high_consumption"] = (
    fp["daily_consumption"] >= high_consumption_threshold
)

high_fp = fp[fp["high_consumption"]].copy()

print(
    f"90th percentile consumption threshold: "
    f"{high_consumption_threshold:.6f}"
)

print(
    f"High-consumption false positives: "
    f"{len(high_fp):,}"
)


# ============================================================
# TOP FALSE POSITIVES
# ============================================================

top_fp = (
    fp.sort_values(
        "ensemble_score",
        ascending=False
    )
    .head(100)
)

print("\nTop 20 false positives:")
print(
    top_fp[
        [
            "meter_id",
            "date",
            "daily_consumption",
            "historical_z",
            "seasonal_z",
            "rolling_z",
            "peer_deviation",
            "relative_deviation",
            "contextual_score",
            "if_anomaly_score",
            "ensemble_score",
            "signal_count"
        ]
    ]
    .head(20)
    .to_string(index=False)
)


# ============================================================
# SAVE OUTPUTS
# ============================================================

fp.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "false_positives.csv"
    ),
    index=False
)

fp_by_meter.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "false_positive_by_meter.csv"
    ),
    index=False
)

fp_score_distribution.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "false_positive_score_distribution.csv"
    ),
    index=False
)

threshold_df.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "threshold_analysis.csv"
    ),
    index=False
)

signal_summary.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "false_positive_signal_analysis.csv"
    ),
    index=False
)

top_fp.to_csv(
    os.path.join(
        OUTPUT_DIR,
        "top_100_false_positives.csv"
    ),
    index=False
)


# ============================================================
# FINAL SUMMARY
# ============================================================

print("\n" + "=" * 75)
print("STEP 11E COMPLETED")
print("=" * 75)

print(f"Total records:       {len(df):,}")
print(f"False positives:     {len(fp):,}")
print(f"True positives:      {(df['result_type'] == 'true_positive').sum():,}")
print(f"False negatives:     {(df['result_type'] == 'false_negative').sum():,}")

print("\nOutput directory:")
print(OUTPUT_DIR)

print("\nGenerated files:")
print("1. false_positives.csv")
print("2. false_positive_by_meter.csv")
print("3. false_positive_score_distribution.csv")
print("4. threshold_analysis.csv")
print("5. false_positive_signal_analysis.csv")
print("6. top_100_false_positives.csv")