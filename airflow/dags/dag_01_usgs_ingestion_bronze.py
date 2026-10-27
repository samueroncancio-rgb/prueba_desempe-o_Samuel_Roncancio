"""
# DAG 01: Ingesta Cruda Inmutable hacia MinIO Bronze (Lakehouse)

Este DAG consulta el feed en tiempo real de la API de USGS cada 5 minutos
(o ejecuta un backfill parametrizado de hasta 30 días) y persiste el payload GeoJSON
crudo exactamente como llega en el Data Lake MinIO (Capa Bronze).

REGLA ARQUITECTÓNICA:
No se escriben tablas de Bronze en PostgreSQL. El almacenamiento crudo se delega
al Object Storage (MinIO S3) para preservar inmutabilidad y bajo costo.
"""
from datetime import datetime, timedelta
import json
import logging
import requests
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.models.param import Param

from airflow.include.config import config
from airflow.include.minio_client import lakehouse_client
from airflow.include.error_handler import on_failure_alert

logger = logging.getLogger("airflow.dag_01_usgs_ingestion_bronze")

BRONZE_DATASET = Dataset(f"s3a://{config.minio_bucket_bronze}/latest")

default_args = {
    "owner": "sismowatch_data_architect",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=15),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert
}

@dag(
    dag_id="dag_01_usgs_ingestion_bronze",
    default_args=default_args,
    description="Ingesta continua desde USGS API hacia MinIO Bronze (GeoJSON inmutable)",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "bronze", "minio", "sismowatch"],
    params={
        "is_backfill": Param(False, type="boolean", description="Activar para ejecutar extracción histórica"),
        "days": Param(30, type="integer", minimum=1, maximum=30, description="Días históricos a extraer"),
        "min_magnitude": Param(2.5, type="number", description="Magnitud mínima para backfill")
    },
    doc_md=__doc__
)
def usgs_ingestion_bronze():

    @task()
    def check_usgs_api_health() -> bool:
        """Verifica disponibilidad y latencia del endpoint de USGS."""
        logger.info(f"Verificando disponibilidad de USGS: {config.usgs_realtime_url}")
        try:
            resp = requests.head(config.usgs_realtime_url, timeout=config.request_timeout_seconds)
            if resp.status_code in [200, 304]:
                logger.info("USGS API responde OK (HTTP 200).")
                return True
            else:
                logger.warning(f"USGS API respondió con código no estándar: {resp.status_code}")
                return False
        except requests.RequestException as e:
            logger.error(f"Fallo de conexión contra USGS API: {e}")
            raise e

    @task(outlets=[BRONZE_DATASET])
    def extract_and_load_to_minio_bronze(is_healthy: bool, **context) -> str:
        """
        Extrae el feed GeoJSON y lo escribe en el bucket de MinIO Bronze.
        Devuelve el nombre del objeto para evitar paso de datos pesados por XCom.
        """
        params = context.get("params", {})
        is_backfill = params.get("is_backfill", False)
        
        now = datetime.utcnow()
        timestamp_str = now.strftime("%Y%m%d_%H%M%S")

        if is_backfill:
            days = params.get("days", 30)
            min_mag = params.get("min_magnitude", 2.5)
            start_date = (now - timedelta(days=days)).strftime("%Y-%m-%d")
            end_date = now.strftime("%Y-%m-%d")
            url = f"{config.usgs_query_url}?format=geojson&starttime={start_date}&endtime={end_date}&minmagnitude={min_mag}"
            logger.info(f"Modo Backfill activado: {days} días desde {start_date} hasta {end_date}")
            object_name = f"backfill/{now.year}/{now.month:02d}/usgs_backfill_{days}d_{timestamp_str}.geojson"
        else:
            url = config.usgs_realtime_url
            object_name = f"{now.year}/{now.month:02d}/{now.day:02d}/usgs_realtime_{timestamp_str}.geojson"

        resp = requests.get(url, timeout=config.request_timeout_seconds)
        resp.raise_for_status()
        raw_payload = resp.text

        # Validación estructural mínima antes de persistir
        data = json.loads(raw_payload)
        if "features" not in data:
            raise ValueError("Payload de USGS inválido: falta la llave 'features'")

        count = len(data.get("features", []))
        logger.info(f"Descargados {count} eventos de USGS. Guardando en MinIO Bronze: {object_name}")

        result = lakehouse_client.put_object(
            bucket_name=config.minio_bucket_bronze,
            object_name=object_name,
            data=raw_payload
        )
        logger.info(f"Guardado exitoso en Bronze: SHA256={result['sha256']}, bytes={result['bytes_length']}")
        return object_name

    healthy = check_usgs_api_health()
    extract_and_load_to_minio_bronze(healthy)

dag_instance = usgs_ingestion_bronze()
