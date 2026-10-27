import datetime
import logging
from fastapi import APIRouter, Depends, HTTPException
import requests

from api.auth.jwt_handler import require_admin
from api.schemas.pydantic_models import UserProfile, BackfillRequest, RegenerateReportRequest
from airflow.include.config import config
from airflow.include.minio_client import lakehouse_client
from airflow.include.spark_jobs.medallion_etl import MedallionETLJob
from airflow.include.ai.executive_reporter import ExecutiveReporter

logger = logging.getLogger("sismowatch.admin_ops")
router = APIRouter(prefix="/api/admin", tags=["Admin Operations"])

@router.post("/backfill")
def trigger_backfill(payload: BackfillRequest, user: UserProfile = Depends(require_admin)):
    """
    Triggers historical backfill from USGS API (up to 30 days).
    STRICTLY RESTRICTED TO ADMIN ROLE. Viewers receive 403 Forbidden.
    """
    days = payload.days
    min_magnitude = payload.min_magnitude
    logger.info(f"Admin {user.username} initiated backfill for {days} days, min_magnitude {min_magnitude}.")

    # Direct extraction & ingestion execution
    now = datetime.datetime.now(datetime.timezone.utc)
    start_date = (now - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")
    url = f"{config.usgs_query_url}?format=geojson&starttime={start_date}&endtime={end_date}&minmagnitude={min_magnitude}"

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        raw_payload = resp.text

        timestamp_str = now.strftime("%Y%m%d_%H%M%S")
        bronze_object_name = f"backfill/{now.year}/{now.month:02d}/usgs_backfill_{days}d_{timestamp_str}.geojson"
        lakehouse_client.put_object(config.minio_bucket_bronze, bronze_object_name, raw_payload)

        # Trigger medallion ETL job
        etl_job = MedallionETLJob()
        etl_result = etl_job.run_pipeline(bronze_object_name=bronze_object_name, dag_id="admin_backfill_trigger")

        return {
            "status": "COMPLETED",
            "message": f"Backfill histórico de {days} días completado exitosamente sin duplicar registros.",
            "bronze_object": bronze_object_name,
            "etl_result": etl_result
        }
    except Exception as e:
        logger.error(f"Backfill execution failed: {e}")
        raise HTTPException(status_code=500, detail=f"Fallo durante el backfill: {str(e)}")

@router.post("/reports/regenerate")
def regenerate_ai_report(payload: RegenerateReportRequest, user: UserProfile = Depends(require_admin)):
    """
    Regenerates AI daily report for a specific date (or today).
    STRICTLY RESTRICTED TO ADMIN ROLE. Viewers receive 403 Forbidden.
    """
    target_date = None
    if payload.target_date:
        try:
            target_date = datetime.datetime.strptime(payload.target_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Formato de fecha inválido. Utilice YYYY-MM-DD.")
    else:
        target_date = datetime.date.today()

    logger.info(f"Admin {user.username} triggered report regeneration for date {target_date}.")
    reporter = ExecutiveReporter()
    report = reporter.generate_daily_report(target_date=target_date)

    return {
        "status": "SUCCESS",
        "message": f"Reporte ejecutivo para {target_date} regenerado exitosamente.",
        "report": {
            "title": report["title"],
            "report_date": str(report["report_date"]),
            "is_fallback": report["is_fallback"],
            "metrics_verified": report["llm_metrics_verified"]
        }
    }
