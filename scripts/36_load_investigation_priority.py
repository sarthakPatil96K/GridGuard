import os
import glob
import pandas as pd
import psycopg
from dotenv import load_dotenv


# ============================================================
# GRIDGUARD - STEP 36
# Load Meter Investigation Priority into PostgreSQL
# ============================================================

PROJECT_ROOT = r"C:\projects\GridGuard"

CSV_DIR = os.path.join(
    PROJECT_ROOT,
    "data",
    "processed",
    "meter_investigation_priority_csv"
)

# Load .env
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/gridguard"
)


# ============================================================
# 1. FIND CSV FILE
# ============================================================

csv_files = glob.glob(
    os.path.join(CSV_DIR, "part-*.csv")
)

if not csv_files:
    raise FileNotFoundError(
        f"No CSV files found in:\n{CSV_DIR}"
    )


print("=" * 70)
print("GRIDGUARD - STEP 36")
print("LOAD INVESTIGATION PRIORITY INTO POSTGRESQL")
print("=" * 70)

print(f"\nCSV directory:")
print(CSV_DIR)

print(f"\nCSV files found: {len(csv_files)}")


# ============================================================
# 2. READ CSV FILES
# ============================================================

frames = []

for file in csv_files:

    print(f"\nReading:")
    print(os.path.basename(file))

    df_part = pd.read_csv(file)

    print(f"Records: {len(df_part)}")

    frames.append(df_part)


df = pd.concat(
    frames,
    ignore_index=True
)


print("\n" + "-" * 70)
print(f"Total records loaded from CSV: {len(df)}")


# ============================================================
# 3. DISPLAY COLUMNS
# ============================================================

print("\nCSV columns:")

for column in df.columns:
    print(f"  - {column}")


# ============================================================
# 4. REQUIRED COLUMNS
# ============================================================

required_columns = [

    "meter_id",

    "analyzed_days",
    "anomaly_days",
    "high_risk_days",

    "avg_event_risk_score",
    "max_event_risk_score",
    "anomaly_rate",

    "persistence_score",
    "frequency_score",

    "model_agreement_days",
    "agreement_rate",
    "agreement_score",

    "recent_anomaly_days",
    "recent_activity_score",

    "severity_score",

    "investigation_priority_score",

    "priority_level"
]


missing_columns = [
    column
    for column in required_columns
    if column not in df.columns
]


if missing_columns:

    raise ValueError(
        "\nMissing required columns:\n"
        + "\n".join(missing_columns)
    )


print("\nAll required columns are present.")


# ============================================================
# 5. KEEP ONLY REQUIRED COLUMNS
# ============================================================

df = df[required_columns].copy()


# ============================================================
# 6. CLEAN DATA TYPES
# ============================================================

integer_columns = [

    "analyzed_days",
    "anomaly_days",
    "high_risk_days",
    "model_agreement_days",
    "recent_anomaly_days"

]


float_columns = [

    "avg_event_risk_score",
    "max_event_risk_score",
    "anomaly_rate",

    "persistence_score",
    "frequency_score",

    "agreement_rate",
    "agreement_score",

    "recent_activity_score",
    "severity_score",

    "investigation_priority_score"

]


for column in integer_columns:

    df[column] = (
        pd.to_numeric(
            df[column],
            errors="coerce"
        )
        .fillna(0)
        .astype(int)
    )


for column in float_columns:

    df[column] = (
        pd.to_numeric(
            df[column],
            errors="coerce"
        )
        .fillna(0.0)
    )


df["meter_id"] = (
    df["meter_id"]
    .astype(str)
    .str.strip()
)


df["priority_level"] = (
    df["priority_level"]
    .astype(str)
    .str.strip()
    .str.upper()
)


# ============================================================
# 7. BASIC VALIDATION
# ============================================================

print("\n" + "-" * 70)
print("DATA VALIDATION")
print("-" * 70)

print(f"Unique meters in CSV: {df['meter_id'].nunique()}")

print("\nPriority distribution:")

print(
    df["priority_level"]
    .value_counts()
    .to_string()
)


# Check score range

invalid_scores = df[
    (df["investigation_priority_score"] < 0)
    |
    (df["investigation_priority_score"] > 100)
]


if len(invalid_scores) > 0:

    raise ValueError(
        f"Found {len(invalid_scores)} records "
        "with priority scores outside 0-100."
    )


print("\nPriority score validation: PASSED")


# ============================================================
# 8. CONNECT TO POSTGRESQL
# ============================================================

print("\n" + "-" * 70)
print("CHECKING POSTGRESQL")
print("-" * 70)


with psycopg.connect(DATABASE_URL) as conn:

    with conn.cursor() as cur:

        # Check meters already populated by Step 35

        cur.execute("""
            SELECT meter_id
            FROM meters
            ORDER BY meter_id;
        """)

        db_meters = {
            row[0]
            for row in cur.fetchall()
        }


csv_meters = set(
    df["meter_id"]
)


missing_meters = (
    csv_meters
    - db_meters
)


if missing_meters:

    raise ValueError(
        "\nThese meters exist in the CSV "
        "but are missing from PostgreSQL meters table:\n"
        + "\n".join(
            sorted(missing_meters)
        )
    )


print(
    f"PostgreSQL meters verified: "
    f"{len(db_meters)}"
)


print(
    f"CSV meters verified: "
    f"{len(csv_meters)}"
)


# ============================================================
# 9. INSERT / UPDATE QUERY
# ============================================================

insert_query = """

INSERT INTO meter_investigation_priority (

    meter_id,

    analyzed_days,
    anomaly_days,
    high_risk_days,

    avg_event_risk_score,
    max_event_risk_score,
    anomaly_rate,

    persistence_score,
    frequency_score,

    model_agreement_days,
    agreement_rate,
    agreement_score,

    recent_anomaly_days,
    recent_activity_score,

    severity_score,

    investigation_priority_score,

    priority_level

)

VALUES (

    %s,

    %s,
    %s,
    %s,

    %s,
    %s,
    %s,

    %s,
    %s,

    %s,
    %s,
    %s,

    %s,
    %s,

    %s,

    %s,

    %s

)

ON CONFLICT (meter_id)

DO UPDATE SET

    calculated_at = CURRENT_TIMESTAMP,

    analyzed_days = EXCLUDED.analyzed_days,

    anomaly_days = EXCLUDED.anomaly_days,

    high_risk_days = EXCLUDED.high_risk_days,

    avg_event_risk_score =
        EXCLUDED.avg_event_risk_score,

    max_event_risk_score =
        EXCLUDED.max_event_risk_score,

    anomaly_rate =
        EXCLUDED.anomaly_rate,

    persistence_score =
        EXCLUDED.persistence_score,

    frequency_score =
        EXCLUDED.frequency_score,

    model_agreement_days =
        EXCLUDED.model_agreement_days,

    agreement_rate =
        EXCLUDED.agreement_rate,

    agreement_score =
        EXCLUDED.agreement_score,

    recent_anomaly_days =
        EXCLUDED.recent_anomaly_days,

    recent_activity_score =
        EXCLUDED.recent_activity_score,

    severity_score =
        EXCLUDED.severity_score,

    investigation_priority_score =
        EXCLUDED.investigation_priority_score,

    priority_level =
        EXCLUDED.priority_level;

"""


# ============================================================
# 10. INSERT DATA
# ============================================================

print("\n" + "-" * 70)
print("LOADING DATA INTO PostgreSQL")
print("-" * 70)


with psycopg.connect(DATABASE_URL) as conn:

    with conn.cursor() as cur:

        for _, row in df.iterrows():

            cur.execute(
                insert_query,
                (

                    row["meter_id"],

                    int(
                        row["analyzed_days"]
                    ),

                    int(
                        row["anomaly_days"]
                    ),

                    int(
                        row["high_risk_days"]
                    ),

                    float(
                        row["avg_event_risk_score"]
                    ),

                    float(
                        row["max_event_risk_score"]
                    ),

                    float(
                        row["anomaly_rate"]
                    ),

                    float(
                        row["persistence_score"]
                    ),

                    float(
                        row["frequency_score"]
                    ),

                    int(
                        row["model_agreement_days"]
                    ),

                    float(
                        row["agreement_rate"]
                    ),

                    float(
                        row["agreement_score"]
                    ),

                    int(
                        row["recent_anomaly_days"]
                    ),

                    float(
                        row["recent_activity_score"]
                    ),

                    float(
                        row["severity_score"]
                    ),

                    float(
                        row[
                            "investigation_priority_score"
                        ]
                    ),

                    row["priority_level"]

                )
            )

    conn.commit()


print("\nData successfully inserted/updated.")


# ============================================================
# 11. VERIFY DATABASE
# ============================================================

print("\n" + "=" * 70)
print("POSTGRESQL VERIFICATION")
print("=" * 70)


with psycopg.connect(DATABASE_URL) as conn:

    with conn.cursor() as cur:

        # Total records

        cur.execute("""
            SELECT COUNT(*)
            FROM meter_investigation_priority;
        """)

        total_records = cur.fetchone()[0]


        # Priority distribution

        cur.execute("""
            SELECT
                priority_level,
                COUNT(*)
            FROM meter_investigation_priority
            GROUP BY priority_level
            ORDER BY
                CASE priority_level
                    WHEN 'VERY HIGH' THEN 1
                    WHEN 'HIGH' THEN 2
                    WHEN 'MEDIUM' THEN 3
                    WHEN 'LOW' THEN 4
                    WHEN 'NORMAL' THEN 5
                    ELSE 6
                END;
        """)

        distribution = cur.fetchall()


        # Top meters

        cur.execute("""
            SELECT
                meter_id,
                investigation_priority_score,
                priority_level,
                anomaly_days,
                high_risk_days,
                agreement_rate
            FROM meter_investigation_priority

            ORDER BY
                investigation_priority_score DESC

            LIMIT 10;
        """)

        top_rows = cur.fetchall()


print(
    f"\nTotal priority records: "
    f"{total_records}"
)


print("\nPriority distribution:")
print("-" * 70)

for level, count in distribution:

    print(
        f"{level:<12} : {count}"
    )


print("\nTOP 10 INVESTIGATION PRIORITIES")
print("-" * 70)

print(
    f"{'Meter':<15}"
    f"{'Score':>10}"
    f"{'Level':>12}"
    f"{'Anomaly Days':>15}"
    f"{'High Risk':>12}"
    f"{'Agreement':>12}"
)


for row in top_rows:

    (
        meter_id,
        score,
        level,
        anomaly_days,
        high_risk_days,
        agreement_rate
    ) = row

    print(
        f"{meter_id:<15}"
        f"{score:>10.2f}"
        f"{level:>12}"
        f"{anomaly_days:>15}"
        f"{high_risk_days:>12}"
        f"{agreement_rate:>11.2f}%"
    )


# ============================================================
# 12. FINAL CHECK
# ============================================================

if total_records == len(df):

    print("\n" + "=" * 70)
    print("STEP 36 COMPLETED SUCCESSFULLY")
    print("=" * 70)

    print(
        f"\nLoaded {total_records} "
        "meter investigation-priority records."
    )

else:

    print("\nWARNING:")
    print(
        f"CSV records = {len(df)}"
    )

    print(
        f"Database records = {total_records}"
    )