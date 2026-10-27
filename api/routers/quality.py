from fastapi import APIRouter, Depends, Query
from api.auth.jwt_handler import require_viewer, require_admin
from api.schemas.pydantic_models import UserProfile, QuarantineEventItem
from api.db import get_db_cursor

router = APIRouter(prefix="/api", tags=["Quality & Pipeline Monitoring"])

@router.get("/pipeline/status")
def get_pipeline_status(user: UserProfile = Depends(require_viewer)):
    """
    Returns latest execution metrics, freshness, and quality scores.
    Accessible to viewer (summary) and admin (detail).
    """
    with get_db_cursor() as cur:
        # Latest quality run
        cur.execute("""
            SELECT run_id::text, evaluated_at, dag_id, total_records,
                   passed_records, auto_healed_records, quarantined_records,
                   quality_score, freshness_seconds, rules_evaluated
            FROM gold.fact_quality_and_healing_runs
            ORDER BY evaluated_at DESC
            LIMIT 5;
        """)
        runs = cur.fetchall()

        # Overall summary
        cur.execute("""
            SELECT 
                COUNT(*) AS total_clean_events,
                COUNT(*) FILTER (WHERE is_auto_healed = TRUE) AS total_healed_events
            FROM gold.fact_earthquake_events;
        """)
        summary_row = cur.fetchone()

        cur.execute("SELECT COUNT(*) AS total_quarantined FROM gold.fact_quarantine_events;")
        quarantine_count = cur.fetchone()["total_quarantined"]

    latest_run = runs[0] if runs else None

    return {
        "status": "HEALTHY",
        "quality_score": float(latest_run["quality_score"]) if latest_run else 100.0,
        "freshness_seconds": latest_run["freshness_seconds"] if latest_run else 0,
        "total_curated_events": summary_row["total_clean_events"] if summary_row else 0,
        "total_auto_healed_events": summary_row["total_healed_events"] if summary_row else 0,
        "total_quarantined_events": quarantine_count,
        "recent_runs": runs if user.role == "admin" else (runs[:1] if runs else [])
    }

@router.get("/quality/quarantine", response_model=list[QuarantineEventItem])
def get_quarantined_events(
    limit: int = Query(50, ge=1, le=200),
    user: UserProfile = Depends(require_admin)
):
    """
    Returns records rejected by validation rules.
    STRICTLY RESTRICTED TO ADMIN ROLE. Viewers receive 403 Forbidden.
    """
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT quarantine_id::text, raw_event_id, rejected_at,
                   rejection_reason, rule_violated, raw_payload
            FROM gold.fact_quarantine_events
            ORDER BY rejected_at DESC
            LIMIT %s;
        """, (limit,))
        rows = cur.fetchall()
        return [QuarantineEventItem(**r) for r in rows]
