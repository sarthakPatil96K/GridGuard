from fastapi import APIRouter, HTTPException
from backend.app.database import get_connection

router = APIRouter(
    prefix="/alerts",
    tags=["Alerts"]
)


@router.get("/")
def get_alerts():
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    a.id,
                    a.meter_id,
                    a.risk_score_id,
                    a.alert_type,
                    a.message,
                    a.status,
                    a.created_at,
                    a.reviewed_at
                FROM alerts a
                ORDER BY a.created_at DESC
            """)

            rows = cur.fetchall()

            columns = [
                "id",
                "meter_id",
                "risk_score_id",
                "alert_type",
                "message",
                "status",
                "created_at",
                "reviewed_at"
            ]

            return [
                dict(zip(columns, row))
                for row in rows
            ]

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch alerts: {str(e)}"
        )

    finally:
        conn.close()