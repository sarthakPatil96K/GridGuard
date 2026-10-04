from fastapi import APIRouter
from backend.app.database import get_connection

router = APIRouter(
    prefix="/api/analytics",
    tags=["Analytics"]
)


@router.get("/trends")
def get_trends():
    conn = get_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    analysis_date AS date,
                    ROUND(
                        AVG(daily_consumption)::numeric,
                        3
                    ) AS daily_consumption
                FROM daily_analysis
                WHERE
                    analysis_date IS NOT NULL
                    AND daily_consumption IS NOT NULL
                GROUP BY analysis_date
                ORDER BY analysis_date;
            """)

            rows = cur.fetchall()

            return [
                {
                    "date": row[0].isoformat()
                    if row[0] is not None
                    else None,

                    "daily_consumption": float(row[1])
                    if row[1] is not None
                    else 0.0,
                }
                for row in rows
            ]

    finally:
        conn.close()


@router.get("/risk-distribution")
def get_risk_distribution():

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    risk_level,
                    COUNT(*) AS count
                FROM daily_analysis
                GROUP BY risk_level
                ORDER BY
                    CASE risk_level
                        WHEN 'CRITICAL' THEN 1
                        WHEN 'HIGH' THEN 2
                        WHEN 'MEDIUM' THEN 3
                        WHEN 'LOW' THEN 4
                        ELSE 5
                    END;
            """)

            rows = cur.fetchall()

            return [
                {
                    "risk_level": row[0],
                    "count": row[1]
                }
                for row in rows
            ]

    finally:
        conn.close()


@router.get("/anomaly-types")
def get_anomaly_types():

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    COALESCE(anomaly_type, 'unknown') AS anomaly_type,
                    COUNT(*) AS count
                FROM daily_analysis
                WHERE anomaly_prediction = TRUE
                GROUP BY anomaly_type
                ORDER BY count DESC;
            """)

            rows = cur.fetchall()

            return [
                {
                    "anomaly_type": row[0],
                    "count": row[1]
                }
                for row in rows
            ]

    finally:
        conn.close()