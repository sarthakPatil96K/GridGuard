from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.ensemble import IsolationForest
import pandas as pd


# =========================================================
# 1. Start Spark
# =========================================================

spark = (
    SparkSession.builder
    .appName("GridGuard-RiskEngine")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 2. Read anomaly-injected data
# =========================================================

input_path = "data/processed/anomaly_injected"

df = spark.read.parquet(input_path)

print("\n========== GRIDGUARD RISK ENGINE ==========")
print(f"Total readings: {df.count()}")


# =========================================================
# 3. Meter-level baseline
# =========================================================

meter_stats = (
    df.groupBy("meter_id")
    .agg(
        F.mean("consumption").alias("meter_mean"),
        F.stddev("consumption").alias("meter_std"),
        F.max("consumption").alias("meter_peak"),
        F.min("consumption").alias("meter_min")
    )
)


# =========================================================
# 4. Add baseline features
# =========================================================

df = df.join(
    meter_stats,
    on="meter_id",
    how="left"
)


df = (
    df
    .withColumn(
        "meter_z_score",
        F.when(
            F.col("meter_std") > 0,
            (
                F.col("consumption")
                - F.col("meter_mean")
            ) / F.col("meter_std")
        ).otherwise(0)
    )
    .withColumn(
        "relative_consumption",
        F.when(
            F.col("meter_mean") > 0,
            F.col("consumption")
            / F.col("meter_mean")
        ).otherwise(1)
    )
    .withColumn(
        "hour",
        F.hour("timestamp")
    )
    .withColumn(
        "is_night",
        F.when(
            (F.hour("timestamp") >= 0)
            & (F.hour("timestamp") < 6),
            1
        ).otherwise(0)
    )
)


# =========================================================
# 5. Convert development dataset to Pandas
# =========================================================

pdf = df.select(
    "meter_id",
    "timestamp",
    "consumption",
    "meter_mean",
    "meter_std",
    "meter_z_score",
    "relative_consumption",
    "hour",
    "is_night",
    "anomaly_type",
    "ground_truth"
).toPandas()


# =========================================================
# 6. Statistical anomaly evidence
# =========================================================

pdf["stat_anomaly"] = (
    pdf["meter_z_score"].abs() >= 2
).astype(int)


# =========================================================
# 7. Isolation Forest
# =========================================================

X = pdf[
    [
        "meter_z_score",
        "relative_consumption",
        "hour",
        "is_night"
    ]
]

if_model = IsolationForest(
    n_estimators=200,
    contamination=0.20,
    random_state=42
)

if_model.fit(X)


pdf["if_prediction"] = if_model.predict(X)

pdf["if_anomaly"] = (
    pdf["if_prediction"] == -1
).astype(int)


# =========================================================
# 8. Isolation Forest anomaly strength
# =========================================================

pdf["if_raw_score"] = -if_model.score_samples(X)


# Normalize IF score to 0–100
if_min = pdf["if_raw_score"].min()
if_max = pdf["if_raw_score"].max()

if if_max > if_min:

    pdf["if_score"] = (
        (pdf["if_raw_score"] - if_min)
        / (if_max - if_min)
        * 100
    )

else:

    pdf["if_score"] = 0


# =========================================================
# 9. Historical deviation score
# =========================================================

pdf["historical_deviation"] = (
    pdf["meter_z_score"].abs()
)


# Convert Z-score to 0–100
pdf["historical_score"] = (
    pdf["historical_deviation"]
    .clip(upper=4)
    / 4
    * 100
)


# =========================================================
# 10. Consumption deviation score
# =========================================================

pdf["relative_deviation"] = (
    (pdf["relative_consumption"] - 1)
    .abs()
)


pdf["relative_score"] = (
    pdf["relative_deviation"]
    .clip(upper=1)
    * 100
)


# =========================================================
# 11. Trend / anomaly evidence
# =========================================================

pdf = pdf.sort_values(
    ["meter_id", "timestamp"]
)


pdf["previous_consumption"] = (
    pdf.groupby("meter_id")["consumption"]
    .shift(1)
)


pdf["change_ratio"] = (
    (
        pdf["consumption"]
        - pdf["previous_consumption"]
    ).abs()
    /
    pdf["previous_consumption"].abs()
)


pdf["change_ratio"] = (
    pdf["change_ratio"]
    .fillna(0)
)


pdf["trend_score"] = (
    pdf["change_ratio"]
    .clip(upper=1)
    * 100
)


# =========================================================
# 12. Night anomaly
# =========================================================

pdf["night_anomaly"] = (
    (
        (pdf["is_night"] == 1)
        &
        (pdf["relative_consumption"] < 0.5)
    )
).astype(int)


pdf["night_score"] = (
    pdf["night_anomaly"] * 100
)


# =========================================================
# 13. Statistical + ML evidence
# =========================================================

pdf["stat_score"] = (
    pdf["historical_score"] * 0.6
    +
    pdf["relative_score"] * 0.4
)


pdf["ml_score"] = pdf["if_score"]


# =========================================================
# 14. Final GridGuard Risk Score
# =========================================================

pdf["risk_score"] = (
    0.30 * pdf["historical_score"]
    +
    0.20 * pdf["relative_score"]
    +
    0.20 * pdf["trend_score"]
    +
    0.10 * pdf["night_score"]
    +
    0.20 * pdf["ml_score"]
)


pdf["risk_score"] = (
    pdf["risk_score"]
    .clip(0, 100)
    .round(2)
)


# =========================================================
# 15. Risk Level
# =========================================================

def risk_level(score):

    if score < 30:
        return "LOW"

    elif score < 60:
        return "MEDIUM"

    elif score < 80:
        return "HIGH"

    else:
        return "CRITICAL"


pdf["risk_level"] = (
    pdf["risk_score"]
    .apply(risk_level)
)


# =========================================================
# 16. Explainable reason
# =========================================================

def generate_reason(row):

    reasons = []

    if row["historical_score"] >= 60:
        reasons.append("historical deviation")

    if row["relative_score"] >= 50:
        reasons.append("unusual consumption level")

    if row["trend_score"] >= 60:
        reasons.append("sudden consumption change")

    if row["night_score"] >= 50:
        reasons.append("unusual night consumption")

    if row["ml_score"] >= 60:
        reasons.append("ML anomaly detected")

    if not reasons:
        reasons.append("normal consumption pattern")

    return ", ".join(reasons)


pdf["risk_reason"] = pdf.apply(
    generate_reason,
    axis=1
)


# =========================================================
# 17. Display high-risk readings
# =========================================================

print("\n========== HIGH-RISK READINGS ==========")

high_risk = pdf[
    pdf["risk_score"] >= 60
].sort_values(
    "risk_score",
    ascending=False
)


print(
    high_risk[
        [
            "meter_id",
            "timestamp",
            "consumption",
            "risk_score",
            "risk_level",
            "risk_reason",
            "anomaly_type",
            "ground_truth"
        ]
    ].to_string(index=False)
)


# =========================================================
# 18. Risk distribution
# =========================================================

print("\n========== RISK DISTRIBUTION ==========")

print(
    pdf["risk_level"]
    .value_counts()
    .to_string()
)


# =========================================================
# 19. Meter-level risk summary
# =========================================================

meter_risk = (
    pdf.groupby("meter_id")
    .agg(
        avg_risk_score=(
            "risk_score",
            "mean"
        ),
        max_risk_score=(
            "risk_score",
            "max"
        ),
        anomaly_count=(
            "if_anomaly",
            "sum"
        ),
        avg_consumption=(
            "consumption",
            "mean"
        )
    )
    .reset_index()
)


meter_risk["risk_level"] = (
    meter_risk["max_risk_score"]
    .apply(risk_level)
)


print("\n========== METER RISK SUMMARY ==========")

print(
    meter_risk.to_string(index=False)
)


# =========================================================
# 20. Save results
# =========================================================

output_columns = [
    "meter_id",
    "timestamp",
    "consumption",
    "meter_z_score",
    "relative_consumption",
    "historical_score",
    "relative_score",
    "trend_score",
    "night_score",
    "ml_score",
    "risk_score",
    "risk_level",
    "risk_reason",
    "anomaly_type",
    "ground_truth"
]


result_pdf = pdf[output_columns]


# IMPORTANT:
# Write directly using Pandas for this tiny development dataset.
# This avoids another unnecessary Spark Python-worker conversion.

result_pdf.to_parquet(
    "data/processed/risk_results.parquet",
    index=False
)


meter_risk.to_parquet(
    "data/processed/meter_risk_summary.parquet",
    index=False
)


print(
    "\nResults saved:"
)

print(
    "data/processed/risk_results.parquet"
)

print(
    "data/processed/meter_risk_summary.parquet"
)


# =========================================================
# 21. Complete
# =========================================================

spark.stop()

print("\n========== RISK ENGINE COMPLETE ==========")