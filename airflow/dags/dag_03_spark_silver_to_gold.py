"""
# DAG 03: Curación y Agregación PySpark Silver hacia MinIO Gold y PostgreSQL Gold

Este DAG es disparado automáticamente cuando el DAG 02 actualiza el Dataset de MinIO Silver.
1. Lee los eventos limpios en formato Parquet/JSON desde MinIO Silver.
2. Ejecuta PySpark para calcular métricas de negocio, magnitudes máximas y agregados diarios.
3. Persiste los datasets analíticos de capa Gold en MinIO (`sismowatch-gold`).
4. Realiza el UPSERT idempotente de los hechos en PostgreSQL (`gold.fact_earthquake_events`).
5. Registra auditoría de calidad y acciones de remediación en `gold.fact_quality_and_healing_runs`.
6. Emite el Dataset de Gold que actualiza la Serving Layer.
"""
from datetime import datetime, timedelta
import logging
from airflow.decorators import dag, task
from airflow.datasets import Dataset

from airflow.include.config import config
from airflow.include.minio_client import lakehouse_client
from airflow.include.spark_jobs.medallion_etl import MedallionETLJob
from airflow.include.error_handler import on_failure_alert

logger = logging.getLogger("airflow.dag_03_spark_silver_to_gold")

SILVER_DATASET = Dataset(f"s3a://{config.minio_bucket_silver}/latest")
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
    dag_id="dag_03_spark_silver_to_gold",
    default_args=default_args,
    description="Curación y carga desde MinIO Silver hacia MinIO Gold y PostgreSQL Gold con PySpark",
    schedule=[SILVER_DATASET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "pyspark", "silver", "gold", "serving", "sismowatch"],
    doc_md=__doc__
)
def spark_silver_to_gold():

    @task(outlets=[GOLD_DATASET])
    def execute_silver_to_gold_etl(**context) -> dict:
        ti = context.get("ti")
        silver_obj = None
        if ti:
            res_silver = ti.xcom_pull(dag_id="dag_02_spark_bronze_to_silver", task_ids="execute_bronze_to_silver_etl", include_prior_dates=True)
            if isinstance(res_silver, dict):
                silver_obj = res_silver.get("silver_destination")

        if not silver_obj:
            latest_in_silver = lakehouse_client.get_latest_object(config.minio_bucket_silver)
            if latest_in_silver:
                silver_obj = latest_in_silver
            else:
                silver_obj = "events_parquet/latest_silver.json"

        logger.info(f"Iniciando PySpark Silver -> Gold para: {silver_obj}")
        etl_job = MedallionETLJob()
        result = etl_job.run_silver_to_gold(silver_object_name=silver_obj, dag_id="dag_03_spark_silver_to_gold")
        logger.info(f"Resultado PySpark Silver -> Gold: {result}")
        return result

    execute_silver_to_gold_etl()

dag_instance = spark_silver_to_gold()
