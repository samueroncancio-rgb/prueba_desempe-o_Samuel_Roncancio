import io
import csv
import datetime
from typing import Optional
from fastapi import APIRouter, Depends, Query, Response
from fastapi.responses import StreamingResponse
from psycopg2 import sql

from api.auth.jwt_handler import require_viewer
from api.schemas.pydantic_models import (
    UserProfile, KPIResponse, PaginatedEventsResponse,
    EarthquakeEventItem, DailyReportItem, TimeseriesPoint
)
from api.db import get_db_cursor

router = APIRouter(prefix="/api", tags=["Dashboard & Analytics"])

@router.get("/kpis", response_model=KPIResponse)
def get_dashboard_kpis(user: UserProfile = Depends(require_viewer)):
    """Computes real-time KPIs strictly from the GOLD schema."""
    with get_db_cursor() as cur:
        # Aggregate 24h stats
        cur.execute("""
            SELECT 
                COUNT(*) AS total_24h,
                COALESCE(MAX(mag), 0.0) AS max_mag,
                COALESCE(ROUND(AVG(mag), 2), 0.0) AS avg_mag,
                COUNT(*) FILTER (WHERE tsunami_alert = TRUE) AS tsunamis,
                COALESCE(MAX(updated_time), NOW()) AS last_updated
            FROM gold.fact_earthquake_events
            WHERE event_time >= NOW() - INTERVAL '24 hours';
        """)
        row = cur.fetchone()
        
        # Max mag place
        cur.execute("""
            SELECT place_sanitized FROM gold.fact_earthquake_events
            WHERE event_time >= NOW() - INTERVAL '24 hours'
            ORDER BY mag DESC LIMIT 1;
        """)
        place_row = cur.fetchone()
        max_place = place_row["place_sanitized"] if place_row else "Sin eventos recientes"

        # Quality score from latest run
        cur.execute("""
            SELECT quality_score FROM gold.fact_quality_and_healing_runs
            ORDER BY evaluated_at DESC LIMIT 1;
        """)
        qual_row = cur.fetchone()
        quality_score = float(qual_row["quality_score"]) if qual_row else 100.0

        return KPIResponse(
            total_events_24h=int(row["total_24h"]),
            max_magnitude=float(row["max_mag"]),
            max_mag_place=max_place,
            avg_magnitude=float(row["avg_mag"]),
            tsunami_alerts=int(row["tsunamis"]),
            quality_score=quality_score,
            last_updated=row["last_updated"]
        )

@router.get("/events", response_model=PaginatedEventsResponse)
def get_events(
    min_mag: Optional[float] = Query(None, description="Magnitud mínima"),
    region: Optional[str] = Query(None, description="Filtro de búsqueda por región"),
    start_date: Optional[str] = Query(None, description="Fecha inicio YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="Fecha fin YYYY-MM-DD"),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    user: UserProfile = Depends(require_viewer)
):
    offset = (page - 1) * limit
    where_clauses = ["1=1"]
    params = []

    if min_mag is not None:
        where_clauses.append("mag >= %s")
        params.append(min_mag)

    if region:
        where_clauses.append("place_sanitized ILIKE %s")
        params.append(f"%{region}%")

    if start_date:
        where_clauses.append("event_time >= %s::date")
        params.append(start_date)

    if end_date:
        where_clauses.append("event_time <= (%s::date + INTERVAL '1 day')")
        params.append(end_date)

    clause_sqls = [sql.SQL(c) for c in where_clauses]
    combined_where = sql.SQL(" AND ").join(clause_sqls)

    with get_db_cursor() as cur:
        # Total count
        count_query = sql.SQL("SELECT COUNT(*) AS total FROM gold.fact_earthquake_events WHERE {}").format(combined_where)
        cur.execute(count_query, params)
        total = cur.fetchone()["total"]

        # Page items
        page_query = sql.SQL("""
            SELECT event_id, mag, mag_type, place_sanitized, event_time, updated_time,
                   longitude, latitude, depth_km, tsunami_alert, significance, status,
                   is_auto_healed, healing_action
            FROM gold.fact_earthquake_events
            WHERE {}
            ORDER BY event_time DESC
            LIMIT %s OFFSET %s;
        """).format(combined_where)
        cur.execute(page_query, params + [limit, offset])
        rows = cur.fetchall()

        items = [EarthquakeEventItem(**r) for r in rows]
        pages = (total + limit - 1) // limit if limit > 0 else 1

        return PaginatedEventsResponse(
            items=items,
            total=total,
            page=page,
            limit=limit,
            pages=pages
        )

@router.get("/stats/timeseries", response_model=list[TimeseriesPoint])
def get_timeseries_stats(
    days: int = Query(7, ge=1, le=30),
    user: UserProfile = Depends(require_viewer)
):
    """Hourly aggregated time series for dashboard charts."""
    with get_db_cursor() as cur:
        query = """
            SELECT 
                to_char(date_trunc('hour', event_time), 'YYYY-MM-DD"T"HH24:00:00"Z"') AS time_bucket,
                COUNT(*) AS event_count,
                COALESCE(ROUND(AVG(mag), 2), 0.0) AS avg_magnitude,
                COALESCE(MAX(mag), 0.0) AS max_magnitude
            FROM gold.fact_earthquake_events
            WHERE event_time >= NOW() - (%s || ' days')::interval
            GROUP BY date_trunc('hour', event_time)
            ORDER BY date_trunc('hour', event_time) ASC;
        """
        cur.execute(query, (days,))
        rows = cur.fetchall()
        return [TimeseriesPoint(**r) for r in rows]

@router.get("/reports", response_model=list[DailyReportItem])
def get_daily_reports(
    limit: int = Query(30, ge=1, le=100),
    user: UserProfile = Depends(require_viewer)
):
    """Retrieves AI-generated daily executive reports from gold."""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT report_id::text, report_date, title, summary_narrative,
                   calculated_metrics, llm_metrics_verified, is_fallback, created_at
            FROM gold.ai_daily_reports
            ORDER BY report_date DESC
            LIMIT %s;
        """, (limit,))
        rows = cur.fetchall()
        return [DailyReportItem(**r) for r in rows]

@router.get("/reports/export")
def export_reports_csv(
    format: str = Query("csv", pattern="^(csv|json)$"),
    user: UserProfile = Depends(require_viewer)
):
    """Exports events and reports history to downloadable CSV or JSON."""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT event_id, mag, mag_type, place_sanitized, event_time,
                   longitude, latitude, depth_km, tsunami_alert, status, is_auto_healed
            FROM gold.fact_earthquake_events
            ORDER BY event_time DESC
            LIMIT 1000;
        """)
        rows = cur.fetchall()

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "event_id", "mag", "mag_type", "place_sanitized", "event_time",
        "longitude", "latitude", "depth_km", "tsunami_alert", "status", "is_auto_healed"
    ])
    writer.writeheader()
    for row in rows:
        row["event_time"] = row["event_time"].isoformat() if row["event_time"] else ""
        writer.writerow(dict(row))

    output.seek(0)
    filename = f"sismowatch_curated_events_{datetime.date.today().strftime('%Y%m%d')}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
