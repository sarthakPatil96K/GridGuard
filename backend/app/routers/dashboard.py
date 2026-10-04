from fastapi import APIRouter
from backend.app.database import get_connection

router = APIRouter(
    prefix="/api/dashboard",
    tags=["Dashboard"]
)


@router.get("/summary")
def get_dashboard_summary():

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            # Overall statistics
            cur.execute("""
                SELECT
                    COUNT(*) AS total_records,
                    COUNT(DISTINCT meter_id) AS total_meters,
                    AVG(daily_consumption) AS average_consumption,
                    AVG(ensemble_score) AS average_anomaly_score
                FROM daily_analysis;
            """)

            overall = cur.fetchone()

            # Predicted anomalies
            cur.execute("""
                SELECT COUNT(*)
                FROM daily_analysis
                WHERE anomaly_prediction = TRUE;
            """)

            total_anomalies = cur.fetchone()[0]

            # Risk distribution
            cur.execute("""
                SELECT
                    risk_level,
                    COUNT(*)
                FROM daily_analysis
                GROUP BY risk_level;
            """)

            risk_rows = cur.fetchall()

            risk_distribution = {
                "LOW": 0,
                "MEDIUM": 0,
                "HIGH": 0,
                "CRITICAL": 0
            }

            for level, count in risk_rows:
                if level:
                    risk_distribution[level.upper()] = count

            return {
                "total_meters": overall[1],
                "total_analysis_records": overall[0],
                "total_anomalies": total_anomalies,

                "low": risk_distribution["LOW"],
                "medium": risk_distribution["MEDIUM"],
                "high": risk_distribution["HIGH"],
                "critical": risk_distribution["CRITICAL"],

                "average_consumption": (
                    float(overall[2])
                    if overall[2] is not None
                    else 0
                ),

                "average_anomaly_score": (
                    float(overall[3])
                    if overall[3] is not None
                    else 0
                )
            }

    finally:
        conn.close()