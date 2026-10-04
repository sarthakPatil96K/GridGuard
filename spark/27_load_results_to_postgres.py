# ============================================================
# GRIDGUARD - STEP 12
# LOAD FINAL RESULTS INTO POSTGRESQL
# ============================================================

import os
import sys
from datetime import datetime

import pandas as pd
import psycopg


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = r"C:\projects\GridGuard"

RESULTS_PATH = os.path.join(
    BASE_DIR,
    "data",
    "real",
    "processed",
    "refined_temporal_gridguard_1m",
    "refined_gridguard_results.csv"
)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "gridguard",
    "user": "postgres",
    "password": "admin"
}


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_connection():

    return psycopg.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )


# ============================================================
# LOAD DATA
# ============================================================

print("=" * 70)
print("GRIDGUARD - STEP 12")
print("FINAL RESULTS → POSTGRESQL")
print("=" * 70)


if not os.path.exists(RESULTS_PATH):

    raise FileNotFoundError(
        f"Results file not found:\n{RESULTS_PATH}"
    )


print("\nReading final GridGuard results...")

df = pd.read_csv(
    RESULTS_PATH
)

print(
    f"Rows loaded from CSV: {len(df):,}"
)

print(
    f"Columns: {len(df.columns)}"
)


# ============================================================
# DATE CONVERSION
# ============================================================

df["date"] = pd.to_datetime(
    df["date"],
    errors="coerce"
)


# ============================================================
# BASIC VALIDATION
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
    "persistence_score",
    "low_activity_flag",
    "ground_truth",
    "anomaly_type",
    "if_score",
    "ensemble_score",
    "contextual_prediction",
    "if_prediction",
    "ensemble_prediction",
    "risk_level"
]


missing = [
    c
    for c in required_columns
    if c not in df.columns
]


if missing:

    raise ValueError(
        f"Missing columns: {missing}"
    )


# ============================================================
# DATA STATISTICS
# ============================================================

print("\nDataset statistics:")

print(
    f"Unique meters       : {df['meter_id'].nunique():,}"
)

print(
    f"Date range          : "
    f"{df['date'].min().date()} → "
    f"{df['date'].max().date()}"
)

print(
    f"Anomaly predictions  : "
    f"{df['ensemble_prediction'].sum():,}"
)

print(
    f"Ground-truth inject. : "
    f"{df['ground_truth'].sum():,}"
)

print(
    "\nRisk levels:"
)

print(
    df["risk_level"]
    .value_counts()
    .to_string()
)


# ============================================================
# CONNECT
# ============================================================

print("\nConnecting to PostgreSQL...")

try:

    conn = get_connection()

    print(
        "PostgreSQL connection successful."
    )

except Exception as e:

    print(
        "\nERROR: Could not connect to PostgreSQL."
    )

    print(e)

    print(
        "\nCheck DB_CONFIG in the script."
    )

    sys.exit(1)


# ============================================================
# STEP 12A
# CLEAR PREVIOUS DEMO DATA
# ============================================================

print("\nClearing previous GridGuard generated data...")

with conn.cursor() as cur:

    # Delete in dependency order.

    cur.execute(
        "DELETE FROM alerts"
    )

    cur.execute(
        "DELETE FROM anomalies"
    )

    cur.execute(
        "DELETE FROM risk_scores"
    )

    cur.execute(
        "DELETE FROM meter_aggregates"
    )

    cur.execute(
        "DELETE FROM meters"
    )

conn.commit()

print(
    "Previous generated data cleared."
)


# ============================================================
# STEP 12B
# INSERT METERS
# ============================================================

print("\nInserting meters...")

meters = (
    df[
        ["meter_id"]
    ]
    .drop_duplicates()
    .copy()
)

inserted_meters = 0

with conn.cursor() as cur:

    for meter_id in meters[
        "meter_id"
    ]:

        cur.execute(
            """
            INSERT INTO meters
            (
                meter_id,
                zone,
                consumer_type,
                status
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s
            )
            """,
            (
                meter_id,
                "London",
                "Smart Meter",
                "active"
            )
        )

        inserted_meters += 1


conn.commit()

print(
    f"Meters inserted: "
    f"{inserted_meters:,}"
)


# ============================================================
# STEP 12C
# INSERT METER AGGREGATES
# ============================================================

print("\nInserting meter aggregates...")

aggregate_rows = 0

with conn.cursor() as cur:

    for row in df.itertuples(
        index=False
    ):

        cur.execute(
            """
            INSERT INTO meter_aggregates
            (
                meter_id,
                aggregation_date,
                avg_consumption,
                peak_consumption,
                min_consumption,
                consumption_std,
                night_consumption_ratio,
                weekend_consumption_ratio,
                historical_deviation,
                peer_deviation
            )
            VALUES
            (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            """,
            (
                row.meter_id,
                row.date.date(),
                row.daily_consumption,
                row.daily_consumption,
                row.daily_consumption,
                None,
                None,
                None,
                row.historical_z,
                row.peer_z
            )
        )

        aggregate_rows += 1


conn.commit()

print(
    f"Aggregate rows inserted: "
    f"{aggregate_rows:,}"
)


# ============================================================
# STEP 12D
# INSERT RISK SCORES
# ============================================================

print("\nInserting risk scores...")

risk_score_ids = []

with conn.cursor() as cur:

    for row in df.itertuples(
        index=False
    ):

        # Convert ensemble score into 0-100
        # for the database's risk_score field.

        risk_score_100 = (
            float(row.ensemble_score)
            * 100.0
        )

        cur.execute(
            """
            INSERT INTO risk_scores
            (
                meter_id,
                calculated_at,
                risk_score,
                risk_level,
                historical_deviation,
                peer_deviation,
                trend_anomaly,
                night_anomaly,
                volatility_score,
                electrical_anomaly,
                persistence_score
            )
            VALUES
            (
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
            RETURNING id
            """,
            (
                row.meter_id,
                row.date,
                risk_score_100,
                row.risk_level,
                row.historical_z,
                row.peer_z,
                row.rolling_z,
                row.seasonal_z,
                row.contextual_score,
                row.if_score,
                row.persistence_score
            )
        )

        risk_id = cur.fetchone()[0]

        risk_score_ids.append(
            risk_id
        )


conn.commit()

print(
    f"Risk-score rows inserted: "
    f"{len(risk_score_ids):,}"
)


# ============================================================
# STEP 12E
# INSERT DETECTED ANOMALIES
# ============================================================

print("\nInserting detected anomalies...")

anomaly_count = 0

# Store risk-score ID against each
# meter/date combination.

risk_lookup = {}

for index, row in df.iterrows():

    risk_lookup[
        (
            row["meter_id"],
            row["date"]
        )
    ] = risk_score_ids[index]


with conn.cursor() as cur:

    for _, row in df[
        df[
            "ensemble_prediction"
        ] == 1
    ].iterrows():

        risk_id = risk_lookup[
            (
                row["meter_id"],
                row["date"]
            )
        ]

        description = (
            f"GridGuard detected an abnormal "
            f"consumption pattern. "
            f"Ensemble score: "
            f"{row['ensemble_score']:.4f}; "
            f"signals: "
            f"{int(row['signal_count'])}; "
            f"risk level: "
            f"{row['risk_level']}."
        )

        cur.execute(
            """
            INSERT INTO anomalies
            (
                meter_id,
                detected_at,
                anomaly_type,
                anomaly_score,
                description,
                ground_truth
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            """,
            (
                row["meter_id"],
                row["date"],
                row["anomaly_type"],
                float(
                    row["ensemble_score"]
                ),
                description,
                bool(
                    row["ground_truth"]
                )
            )
        )

        anomaly_count += 1


conn.commit()

print(
    f"Anomaly rows inserted: "
    f"{anomaly_count:,}"
)


# ============================================================
# STEP 12F
# INSERT ALERTS
# ============================================================

print("\nCreating alerts...")

alert_count = 0

with conn.cursor() as cur:

    # Create alerts only for predicted anomalies.

    for _, row in df[
        df[
            "ensemble_prediction"
        ] == 1
    ].iterrows():

        risk_id = risk_lookup[
            (
                row["meter_id"],
                row["date"]
            )
        ]

        message = (
            f"Abnormal electricity consumption "
            f"pattern detected for meter "
            f"{row['meter_id']} on "
            f"{row['date'].date()}. "
            f"Risk score: "
            f"{row['ensemble_score'] * 100:.2f}/100. "
            f"Risk level: "
            f"{row['risk_level']}."
        )

        cur.execute(
            """
            INSERT INTO alerts
            (
                meter_id,
                risk_score_id,
                alert_type,
                message,
                status
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s
            )
            """,
            (
                row["meter_id"],
                risk_id,
                "Consumption Anomaly",
                message,
                "pending"
            )
        )

        alert_count += 1


conn.commit()

print(
    f"Alerts created: "
    f"{alert_count:,}"
)


# ============================================================
# STEP 12G
# DATABASE VERIFICATION
# ============================================================

print("\n" + "=" * 70)
print("DATABASE VERIFICATION")
print("=" * 70)

with conn.cursor() as cur:

    queries = {

        "meters": """
            SELECT COUNT(*)
            FROM meters
        """,

        "meter_aggregates": """
            SELECT COUNT(*)
            FROM meter_aggregates
        """,

        "risk_scores": """
            SELECT COUNT(*)
            FROM risk_scores
        """,

        "anomalies": """
            SELECT COUNT(*)
            FROM anomalies
        """,

        "alerts": """
            SELECT COUNT(*)
            FROM alerts
        """
    }

    for table, query in queries.items():

        cur.execute(query)

        count = cur.fetchone()[0]

        print(
            f"{table:<22}: "
            f"{count:,}"
        )


# ============================================================
# RISK DISTRIBUTION
# ============================================================

print("\nRisk distribution:")

with conn.cursor() as cur:

    cur.execute(
        """
        SELECT
            risk_level,
            COUNT(*)
        FROM risk_scores
        GROUP BY risk_level
        ORDER BY
            CASE risk_level
                WHEN 'LOW' THEN 1
                WHEN 'MEDIUM' THEN 2
                WHEN 'HIGH' THEN 3
                WHEN 'CRITICAL' THEN 4
                ELSE 5
            END
        """
    )

    for risk_level, count in cur.fetchall():

        print(
            f"{risk_level:<12}: "
            f"{count:,}"
        )


# ============================================================
# TOP ALERTS
# ============================================================

print("\nTop 10 highest-risk alerts:")

with conn.cursor() as cur:

    cur.execute(
        """
        SELECT
            a.id,
            a.meter_id,
            rs.risk_score,
            rs.risk_level,
            a.alert_type,
            a.created_at
        FROM alerts a
        JOIN risk_scores rs
            ON a.risk_score_id = rs.id
        ORDER BY
            rs.risk_score DESC
        LIMIT 10
        """
    )

    rows = cur.fetchall()

    for row in rows:

        print(
            row
        )


# ============================================================
# CLOSE
# ============================================================

conn.close()

print("\n" + "=" * 70)
print("STEP 12 COMPLETED SUCCESSFULLY")
print("=" * 70)

print(
    "\nGridGuard final results are now available in PostgreSQL."
)

print(
    "\nNext pipeline:"
)

print(
    "PostgreSQL → FastAPI → React Dashboard"
)
