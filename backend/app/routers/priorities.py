from fastapi import APIRouter, HTTPException
from backend.app.database import get_connection


router = APIRouter(
    prefix="/api/priorities",
    tags=["Investigation Priorities"]
)


# ============================================================
# GET ALL METER PRIORITIES
# ============================================================

@router.get("/")
def get_priorities():

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
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
                    priority_level,
                    calculated_at
                FROM meter_investigation_priority
                ORDER BY investigation_priority_score DESC;
            """)

            rows = cur.fetchall()

            columns = [
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
                "priority_level",
                "calculated_at"
            ]

            return [
                dict(zip(columns, row))
                for row in rows
            ]

    finally:
        conn.close()


# ============================================================
# GET SINGLE METER PRIORITY
# ============================================================

@router.get("/{meter_id}")
def get_meter_priority(meter_id: str):

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
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
                    priority_level,
                    calculated_at
                FROM meter_investigation_priority
                WHERE meter_id = %s;
            """, (meter_id,))

            row = cur.fetchone()

            if not row:
                raise HTTPException(
                    status_code=404,
                    detail=f"No investigation priority found for meter {meter_id}"
                )

            columns = [
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
                "priority_level",
                "calculated_at"
            ]

            return dict(zip(columns, row))

    finally:
        conn.close()


# ============================================================
# PRIORITY DISTRIBUTION
# ============================================================

@router.get("/distribution/summary")
def get_priority_distribution():

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    priority_level,
                    COUNT(*) AS meter_count
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

            rows = cur.fetchall()

            return [
                {
                    "priority_level": row[0],
                    "meter_count": row[1]
                }
                for row in rows
            ]

    finally:
        conn.close()