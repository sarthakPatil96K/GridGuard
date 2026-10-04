from fastapi import APIRouter
from backend.app.database import get_connection

router = APIRouter(
    prefix="/api/anomalies",
    tags=["Anomalies"]
)


@router.get("/")
def get_anomalies():

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    meter_id,
                    analysis_date,
                    daily_consumption,
                    historical_z,
                    seasonal_z,
                    rolling_z,
                    ensemble_score,
                    risk_score,
                    risk_level,
                    anomaly_type,
                    ground_truth
                FROM daily_analysis
                WHERE anomaly_prediction = TRUE
                ORDER BY risk_score DESC, analysis_date DESC;
            """)

            rows = cur.fetchall()

            return [
                {
                    "meter_id": row[0],
                    "date": row[1],
                    "consumption": (
                        float(row[2])
                        if row[2] is not None else 0
                    ),
                    "historical_z": (
                        float(row[3])
                        if row[3] is not None else 0
                    ),
                    "seasonal_z": (
                        float(row[4])
                        if row[4] is not None else 0
                    ),
                    "rolling_z": (
                        float(row[5])
                        if row[5] is not None else 0
                    ),
                    "ensemble_score": (
                        float(row[6])
                        if row[6] is not None else 0
                    ),
                    "risk_score": (
                        float(row[7])
                        if row[7] is not None else 0
                    ),
                    "risk_level": row[8],
                    "anomaly_type": row[9],
                    "ground_truth": bool(row[10])
                }
                for row in rows
            ]

    finally:
        conn.close()


@router.get("/high-risk")
def get_high_risk_anomalies():

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    meter_id,
                    analysis_date,
                    daily_consumption,
                    ensemble_score,
                    risk_score,
                    risk_level,
                    anomaly_type
                FROM daily_analysis
                WHERE anomaly_prediction = TRUE
                  AND risk_level IN ('HIGH', 'CRITICAL')
                ORDER BY risk_score DESC;
            """)

            rows = cur.fetchall()

            return [
                {
                    "meter_id": row[0],
                    "date": row[1],
                    "consumption": (
                        float(row[2])
                        if row[2] is not None else 0
                    ),
                    "ensemble_score": (
                        float(row[3])
                        if row[3] is not None else 0
                    ),
                    "risk_score": (
                        float(row[4])
                        if row[4] is not None else 0
                    ),
                    "risk_level": row[5],
                    "anomaly_type": row[6]
                }
                for row in rows
            ]

    finally:
        conn.close()


@router.get("/alerts")
def get_alerts():
    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    a.id,
                    a.meter_id,
                    a.alert_type,
                    a.message,
                    a.status,
                    a.created_at,
                    a.reviewed_at,
                    r.risk_score,
                    r.risk_level
                FROM alerts a
                LEFT JOIN risk_scores r
                    ON a.risk_score_id = r.id
                ORDER BY a.created_at DESC
            """)

            rows = cur.fetchall()

            columns = [
                "id",
                "meter_id",
                "alert_type",
                "message",
                "status",
                "created_at",
                "reviewed_at",
                "risk_score",
                "risk_level",
            ]

            return [
                dict(zip(columns, row))
                for row in rows
            ]