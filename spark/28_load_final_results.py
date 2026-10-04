import os
import pandas as pd
import psycopg
from dotenv import load_dotenv


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/gridguard"
)

INPUT_FILE = (
    "data/real/processed/"
    "refined_temporal_gridguard_1m/"
    "refined_gridguard_results.csv"
)


# ============================================================
# LOAD CSV
# ============================================================

print("=" * 70)
print("GRIDGUARD FINAL RESULTS → POSTGRESQL")
print("=" * 70)

print(f"\nReading:")
print(INPUT_FILE)

df = pd.read_csv(INPUT_FILE)

print(f"\nRows loaded: {len(df):,}")
print(f"Columns: {len(df.columns)}")

print("\nColumns:")
for col in df.columns:
    print(f"  - {col}")


# ============================================================
# VALIDATE REQUIRED COLUMNS
# ============================================================

required_columns = [
    "meter_id",
    "date",
    "daily_consumption",
    "historical_z",
    "seasonal_z",
    "rolling_z",
    "relative_deviation",
    "peer_z",
    "contextual_score",
    "signal_count",
    "low_activity_flag",
    "ground_truth",
    "anomaly_type",
    "if_score",
    "ensemble_score",
    "ensemble_prediction",
    "risk_level"
]

missing_columns = [
    col for col in required_columns
    if col not in df.columns
]

if missing_columns:

    print("\nERROR: Missing required columns:")

    for col in missing_columns:
        print(f"  - {col}")

    raise ValueError(
        "Input CSV does not contain all required columns."
    )


# ============================================================
# DATE
# ============================================================

# CSV uses "date".
# PostgreSQL uses "analysis_date".

df["analysis_date"] = pd.to_datetime(
    df["date"],
    errors="coerce"
).dt.date


if df["analysis_date"].isna().any():

    invalid_dates = df["analysis_date"].isna().sum()

    raise ValueError(
        f"{invalid_dates} invalid dates found."
    )


# ============================================================
# BOOLEAN COLUMNS
# ============================================================

boolean_columns = [
    "ground_truth",
    "low_activity_flag",
    "ensemble_prediction"
]

for col in boolean_columns:

    if col in df.columns:

        if df[col].dtype == object:

            df[col] = (
                df[col]
                .astype(str)
                .str.lower()
                .map({
                    "true": True,
                    "false": False,
                    "1": True,
                    "0": False
                })
                .fillna(False)
            )

        else:

            df[col] = (
                df[col]
                .fillna(False)
                .astype(bool)
            )


# ============================================================
# NUMERIC COLUMNS
# ============================================================

numeric_columns = [
    "daily_consumption",
    "historical_z",
    "seasonal_z",
    "rolling_z",
    "relative_deviation",
    "peer_z",
    "contextual_score",
    "if_score",
    "ensemble_score",
    "signal_count"
]

for col in numeric_columns:

    if col in df.columns:

        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        )


# ============================================================
# GRIDGUARD PREDICTION
# ============================================================

# The final CSV calls this "ensemble_prediction".
# PostgreSQL calls it "anomaly_prediction".

df["anomaly_prediction"] = (
    df["ensemble_prediction"]
    .fillna(False)
    .astype(bool)
)


# ============================================================
# RISK SCORE
# ============================================================

# ensemble_score is the final GridGuard anomaly score
# in the 0–1 range.
#
# PostgreSQL risk_score is stored on a 0–100 scale.

df["risk_score"] = (
    df["ensemble_score"] * 100
)


# ============================================================
# CONNECTION
# ============================================================

print("\nConnecting to PostgreSQL...")

conn = psycopg.connect(DATABASE_URL)

print("Connection successful.")


# ============================================================
# CLEAR PREVIOUS FINAL RESULTS
# ============================================================

with conn.cursor() as cur:

    print("\nClearing previous daily_analysis data...")

    cur.execute("""
        TRUNCATE TABLE daily_analysis
        RESTART IDENTITY
    """)

    conn.commit()

print("Previous data cleared.")


# ============================================================
# INSERT QUERY
# ============================================================

insert_query = """
INSERT INTO daily_analysis (
    meter_id,
    analysis_date,
    daily_consumption,

    historical_z,
    seasonal_z,
    rolling_z,

    relative_deviation,
    peer_z,

    contextual_score,
    if_score,
    ensemble_score,

    signal_count,
    low_activity_flag,

    anomaly_prediction,
    ground_truth,

    anomaly_type,

    risk_score,
    risk_level
)
VALUES (
    %s, %s, %s,

    %s, %s, %s,

    %s, %s,

    %s, %s, %s,

    %s, %s,

    %s, %s,

    %s,

    %s, %s
)
"""


# ============================================================
# PREPARE RECORDS
# ============================================================

records = []

for _, row in df.iterrows():

    def value(column):

        if column not in row.index:
            return None

        value = row[column]

        if pd.isna(value):
            return None

        return value


    records.append(
        (
            value("meter_id"),
            value("analysis_date"),
            value("daily_consumption"),

            value("historical_z"),
            value("seasonal_z"),
            value("rolling_z"),

            value("relative_deviation"),
            value("peer_z"),

            value("contextual_score"),
            value("if_score"),
            value("ensemble_score"),

            value("signal_count"),
            value("low_activity_flag"),

            value("anomaly_prediction"),
            value("ground_truth"),

            value("anomaly_type"),

            value("risk_score"),
            value("risk_level")
        )
    )


# ============================================================
# INSERT
# ============================================================

print("\nInserting records...")

with conn.cursor() as cur:

    cur.executemany(
        insert_query,
        records
    )

    conn.commit()


print(
    f"Inserted: {len(records):,} rows"
)


# ============================================================
# VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("DATABASE VALIDATION")
print("=" * 70)


with conn.cursor() as cur:

    # Total rows
    cur.execute("""
        SELECT COUNT(*)
        FROM daily_analysis
    """)

    total_rows = cur.fetchone()[0]


    # Unique meters
    cur.execute("""
        SELECT COUNT(DISTINCT meter_id)
        FROM daily_analysis
    """)

    unique_meters = cur.fetchone()[0]


    # Predictions
    cur.execute("""
        SELECT COUNT(*)
        FROM daily_analysis
        WHERE anomaly_prediction = TRUE
    """)

    predicted_anomalies = cur.fetchone()[0]


    # Ground truth
    cur.execute("""
        SELECT COUNT(*)
        FROM daily_analysis
        WHERE ground_truth = TRUE
    """)

    ground_truth = cur.fetchone()[0]


    # True positives
    cur.execute("""
        SELECT COUNT(*)
        FROM daily_analysis
        WHERE anomaly_prediction = TRUE
          AND ground_truth = TRUE
    """)

    true_positive = cur.fetchone()[0]


    # False positives
    cur.execute("""
        SELECT COUNT(*)
        FROM daily_analysis
        WHERE anomaly_prediction = TRUE
          AND ground_truth = FALSE
    """)

    false_positive = cur.fetchone()[0]


    # False negatives
    cur.execute("""
        SELECT COUNT(*)
        FROM daily_analysis
        WHERE anomaly_prediction = FALSE
          AND ground_truth = TRUE
    """)

    false_negative = cur.fetchone()[0]


    # True negatives
    cur.execute("""
        SELECT COUNT(*)
        FROM daily_analysis
        WHERE anomaly_prediction = FALSE
          AND ground_truth = FALSE
    """)

    true_negative = cur.fetchone()[0]


print(f"\nTotal rows          : {total_rows:,}")
print(f"Unique meters       : {unique_meters}")
print(f"Predicted anomalies : {predicted_anomalies}")
print(f"Ground truth        : {ground_truth}")
print(f"True positives      : {true_positive}")
print(f"False positives     : {false_positive}")
print(f"False negatives     : {false_negative}")
print(f"True negatives      : {true_negative}")


# ============================================================
# RISK DISTRIBUTION
# ============================================================

print("\nRisk distribution:")

with conn.cursor() as cur:

    cur.execute("""
        SELECT
            risk_level,
            COUNT(*)
        FROM daily_analysis
        GROUP BY risk_level
        ORDER BY
            CASE risk_level
                WHEN 'CRITICAL' THEN 1
                WHEN 'HIGH' THEN 2
                WHEN 'MEDIUM' THEN 3
                WHEN 'LOW' THEN 4
                ELSE 5
            END
    """)

    rows = cur.fetchall()


for risk_level, count in rows:

    print(
        f"  {risk_level:<10} : {count:,}"
    )


# ============================================================
# TOP ANOMALIES
# ============================================================

print("\nTop 10 anomaly predictions:")

with conn.cursor() as cur:

    cur.execute("""
        SELECT
            meter_id,
            analysis_date,
            daily_consumption,
            ensemble_score,
            risk_score,
            risk_level,
            anomaly_type,
            ground_truth
        FROM daily_analysis
        WHERE anomaly_prediction = TRUE
        ORDER BY ensemble_score DESC
        LIMIT 10
    """)

    rows = cur.fetchall()


for row in rows:

    print(
        f"{row[0]:<12} "
        f"{row[1]} "
        f"cons={row[2]:<10.3f} "
        f"score={row[3]:.3f} "
        f"risk={row[4]:.1f} "
        f"{row[5]:<10} "
        f"{str(row[6]):<20} "
        f"GT={row[7]}"
    )


# ============================================================
# CLOSE
# ============================================================

conn.close()


print("\n" + "=" * 70)
print("GRIDGUARD FINAL DATABASE LOAD COMPLETE")
print("=" * 70)