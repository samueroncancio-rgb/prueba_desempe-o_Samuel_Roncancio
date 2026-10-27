"""
# DAG 02: Procesamiento PySpark Bronze hacia MinIO Silver (Parquet & Auto-Curación)

Este DAG es disparado automáticamente cuando el DAG 01 actualiza el Dataset de MinIO Bronze.
1. Lee el archivo GeoJSON crudo desde MinIO Bronze.
2. Aplica el motor de Auto-Remediación Determinista en vuelo (Cero Cuarentena).
3. Transforma y calcula energía sísmica Gutenberg-Richter con Apache PySpark en el clúster distribuido.
4. Persiste el dataset limpio y particionado en formato Parquet/JSON en MinIO Silver.
5. Emite el Dataset de MinIO Silver que dispara automáticamente el DAG 03.
"""
from datetime import datetime, timedelta
import logging
from airflow.decorators import dag, task
from airflow.datasets import Dataset

from airflow.include.config import config
from airflow.include.minio_client import lakehouse_client
from airflow.include.spark_jobs.medallion_etl import MedallionETLJob
from airflow.include.error_handler import on_failure_alert

logger = logging.getLogger("airflow.dag_02_spark_bronze_to_silver")

BRONZE_DATASET = Dataset(f"s3a://{config.minio_bucket_bronze}/latest")
SILVER_DATASET = Dataset(f"s3a://{config.minio_bucket_silver}/latest")

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
    dag_id="dag_02_spark_bronze_to_silver",
    default_args=default_args,
    description="Procesa datos crudos desde MinIO Bronze hacia MinIO Silver con PySpark distribuido",
    schedule=[BRONZE_DATASET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "pyspark", "bronze", "silver", "medallion", "sismowatch"],
    doc_md=__doc__
)
def spark_bronze_to_silver():

    @task(outlets=[SILVER_DATASET])
    def execute_bronze_to_silver_etl(**context) -> dict:
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

        logger.info(f"Iniciando PySpark Bronze -> Silver para: {bronze_obj}")
        etl_job = MedallionETLJob()
        result = etl_job.run_bronze_to_silver(bronze_object_name=bronze_obj, dag_id="dag_02_spark_bronze_to_silver")
        logger.info(f"Resultado PySpark Bronze -> Silver: {result}")
        return result

    execute_bronze_to_silver_etl()

dag_instance = spark_bronze_to_silver()
