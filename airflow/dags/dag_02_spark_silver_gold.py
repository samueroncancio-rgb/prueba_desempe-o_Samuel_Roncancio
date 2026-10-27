"""
# DAG 02: Procesamiento Medallion Silver y Gold (Spark & Auto-Curación)

Este DAG es disparado automáticamente cuando el DAG 01 actualiza el Dataset
de MinIO Bronze.
1. Lee el archivo GeoJSON crudo desde MinIO.
2. Aplica el motor de Auto-Remediación Determinista en vuelo (Cero Cuarentena).
3. Persiste el dataset limpio y particionado en formato Parquet en MinIO Silver.
4. Ejecuta un UPSERT idempotente en la Serving Layer de PostgreSQL (esquema GOLD).
5. Registra auditoría de calidad y acciones de reparación en caliente.

REGLA ARQUITECTÓNICA:
PostgreSQL NO contiene tablas de Bronze ni Silver. Todo el almacenamiento intermedio
se realiza en el Data Lake MinIO, y solo los hechos finales curados se cargan en GOLD.
"""
from datetime import datetime, timedelta
import logging
from airflow.decorators import dag, task
from airflow.datasets import Dataset

from airflow.include.config import config
from airflow.include.minio_client import lakehouse_client
from airflow.include.spark_jobs.medallion_etl import MedallionETLJob
from airflow.include.error_handler import on_failure_alert

logger = logging.getLogger("airflow.dag_02_spark_silver_gold")

BRONZE_DATASET = Dataset(f"s3a://{config.minio_bucket_bronze}/latest")
GOLD_DATASET = Dataset(f"postgres://{config.postgres_host}:{config.postgres_port}/{config.postgres_db}/gold/fact_earthquake_events")

default_args = {
    "owner": "sismowatch_data_architect",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": on_failure_alert
}

@dag(
    dag_id="dag_02_spark_silver_gold",
    default_args=default_args,
    description="Procesa datos crudos desde MinIO Bronze hacia MinIO Silver (Parquet) y PostgreSQL Gold",
    schedule=[BRONZE_DATASET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "silver", "gold", "self_healing", "sismowatch"],
    doc_md=__doc__
)
def spark_silver_gold():

    @task(outlets=[GOLD_DATASET])
    def execute_medallion_etl(**context) -> dict:
        """
        Ejecuta el job de transformación.
        Si viene disparado por DAG 01, lee el último objeto Bronze;
        de lo contrario busca el más reciente en el almacenamiento.
        """
        ti = context.get("ti")
        bronze_obj = None
        if ti:
            bronze_obj = ti.xcom_pull(dag_id="dag_01_usgs_ingestion_bronze", task_ids="extract_and_load_to_minio_bronze", include_prior_dates=True)

        if not bronze_obj:
            latest_in_lake = lakehouse_client.get_latest_object(config.minio_bucket_bronze)
            if latest_in_lake:
                bronze_obj = latest_in_lake
            else:
                now = datetime.utcnow()
                bronze_obj = f"{now.year}/{now.month:02d}/{now.day:02d}/latest.geojson"

        logger.info(f"Iniciando procesamiento Medallion para objeto Bronze: {bronze_obj}")
        etl_job = MedallionETLJob()
        result = etl_job.run_pipeline(bronze_object_name=bronze_obj, dag_id="dag_02_spark_silver_gold")
        logger.info(f"Resultado Medallion ETL: {result}")
        return result

    execute_medallion_etl()

dag_instance = spark_silver_gold()
