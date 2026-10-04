from fastapi import APIRouter
from backend.app.database import get_connection

router = APIRouter(
    prefix="/api/meters",
    tags=["Meters"]
)


@router.get("/")
def get_meters():

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    meter_id,
                    COUNT(*) AS analysis_records,
                    AVG(daily_consumption) AS avg_consumption,
                    MAX(ensemble_score) AS max_anomaly_score,
                    MAX(risk_score) AS max_risk_score
                FROM daily_analysis
                GROUP BY meter_id
                ORDER BY meter_id;
            """)

            rows = cur.fetchall()

            return [
                {
                    "meter_id": row[0],
                    "analysis_records": row[1],
                    "avg_consumption": float(row[2]) if row[2] is not None else 0,
                    "max_anomaly_score": float(row[3]) if row[3] is not None else 0,
                    "max_risk_score": float(row[4]) if row[4] is not None else 0,
                }
                for row in rows
            ]

    finally:
        conn.close()


@router.get("/{meter_id}")
def get_meter(meter_id: str):

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    meter_id,
                    COUNT(*) AS analysis_records,
                    AVG(daily_consumption) AS avg_consumption,
                    MAX(ensemble_score) AS max_anomaly_score,
                    MAX(risk_score) AS max_risk_score
                FROM daily_analysis
                WHERE meter_id = %s
                GROUP BY meter_id;
            """, (meter_id,))

            row = cur.fetchone()

            if row is None:
                return {
                    "error": "Meter not found",
                    "meter_id": meter_id
                }

            return {
                "meter_id": row[0],
                "analysis_records": row[1],
                "avg_consumption": float(row[2]) if row[2] is not None else 0,
                "max_anomaly_score": float(row[3]) if row[3] is not None else 0,
                "max_risk_score": float(row[4]) if row[4] is not None else 0,
            }

    finally:
        conn.close()


@router.get("/{meter_id}/consumption")
def get_meter_consumption(meter_id: str):

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    analysis_date,
                    daily_consumption,
                    historical_z,
                    seasonal_z,
                    rolling_z,
                    ensemble_score,
                    risk_score,
                    risk_level,
                    anomaly_prediction,
                    anomaly_type
                FROM daily_analysis
                WHERE meter_id = %s
                ORDER BY analysis_date;
            """, (meter_id,))

            rows = cur.fetchall()

            return [
                {
                    "date": row[0],
                    "consumption": float(row[1]) if row[1] is not None else 0,
                    "historical_z": float(row[2]) if row[2] is not None else 0,
                    "seasonal_z": float(row[3]) if row[3] is not None else 0,
                    "rolling_z": float(row[4]) if row[4] is not None else 0,
                    "ensemble_score": float(row[5]) if row[5] is not None else 0,
                    "risk_score": float(row[6]) if row[6] is not None else 0,
                    "risk_level": row[7],
                    "anomaly": bool(row[8]),
                    "anomaly_type": row[9]
                }
                for row in rows
            ]

    finally:
        conn.close()