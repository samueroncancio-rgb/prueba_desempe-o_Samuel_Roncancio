"""
# DAG 04: Inteligencia Artificial, Detección de Anomalías y Actualización RAG

Este DAG corre de manera diaria (y bajo demanda desde la API de administración):
1. Ejecuta analítica de Machine Learning (DBSCAN + Isolation Forest) para detectar
   enjambres sísmicos espaciales y picos de energía anómalos.
2. Calcula cifras agregadas 100% deterministas en SQL/Python y genera el Reporte
   Ejecutivo Diario en lenguaje natural mediante un LLM gobernado por guardrails
   numéricos y un plan de contingencia (fallback).
3. Genera representaciones vectoriales densas (embeddings) de los eventos y reportes
   actuales para alimentar la base de conocimiento RAG en `gold.rag_knowledge_base` (`pgvector`).
"""
from datetime import datetime, timedelta
import logging
from airflow.decorators import dag, task
from airflow.models.param import Param

from airflow.include.ai.anomaly_detector import AnomalyDetector
from airflow.include.ai.executive_reporter import ExecutiveReporter
from airflow.include.ai.rag_indexer import RAGIndexer
from airflow.include.error_handler import on_failure_alert

logger = logging.getLogger("airflow.dag_04_ai_reports_and_rag")

default_args = {
    "owner": "sismowatch_ai_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": on_failure_alert
}

@dag(
    dag_id="dag_04_ai_reports_and_rag",
    default_args=default_args,
    description="Analítica de anomalías sísmicas (DBSCAN), reporte ejecutivo LLM y vectorización RAG",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ai", "ml", "dbscan", "llm", "rag", "pgvector", "sismowatch"],
    params={
        "target_date": Param("", type="string", description="Fecha YYYY-MM-DD para regeneración de reporte (vacío para hoy)")
    },
    doc_md=__doc__
)
def ai_reports_and_rag():

    @task()
    def detect_seismic_anomalies() -> int:
        """Aplica DBSCAN y análisis de energía para identificar enjambres anómalos."""
        logger.info("Iniciando detección de anomalías espaciales y energéticas...")
        detector = AnomalyDetector()
        anomalies = detector.run_detection()
        logger.info(f"Detección finalizada: {len(anomalies)} anomalías identificadas y persistidas en gold.ai_anomalies.")
        return len(anomalies)

    @task()
    def generate_ai_executive_report(**context) -> dict:
        """Calcula métricas deterministas y genera informe narrativo controlado."""
        params = context.get("params", {})
        date_str = params.get("target_date")
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else datetime.utcnow().date()
        
        logger.info(f"Generando informe ejecutivo para fecha: {target_date}")
        reporter = ExecutiveReporter()
        report = reporter.generate_daily_report(target_date=target_date)
        logger.info(f"Reporte generado exitosamente. Fallback={report.get('is_fallback')}, Verificado={report.get('llm_metrics_verified')}")
        return {
            "report_date": str(report["report_date"]),
            "title": report["title"],
            "is_fallback": report["is_fallback"]
        }

    @task()
    def index_vectors_for_rag() -> int:
        """Genera embeddings e indexa los últimos eventos y reportes en pgvector."""
        logger.info("Indexando eventos y reportes recientes en gold.rag_knowledge_base...")
        indexer = RAGIndexer()
        indexed_count = indexer.index_gold_events(limit=50)
        logger.info(f"Indexación vectorial completada: {indexed_count} chunks almacenados con índice HNSW.")
        return indexed_count

    # Flujo de ejecución
    anomalies_count = detect_seismic_anomalies()
    report_info = generate_ai_executive_report()
    indexed_vectors = index_vectors_for_rag()

    [anomalies_count, report_info] >> indexed_vectors

dag_instance = ai_reports_and_rag()
