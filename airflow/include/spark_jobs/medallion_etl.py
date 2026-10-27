import os
import json
import logging
import datetime
from typing import Dict, Any, List
import psycopg2
from psycopg2.extras import execute_batch

from airflow.include.config import config
from airflow.include.minio_client import lakehouse_client
from airflow.include.quality.self_healing_validator import SelfHealingValidator

logger = logging.getLogger("sismowatch.medallion_etl")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

class MedallionETLJob:
    """
    Executes the Medallion Pipeline:
    1. Reads Bronze raw GeoJSON from MinIO Object Storage.
    2. Runs SelfHealingValidator (in-memory distributed/micro-batch processing).
    3. Persists clean, deduplicated data as Parquet into MinIO Silver (Data Lake).
    4. Upserts curated records into PostgreSQL GOLD Serving Layer (gold.fact_earthquake_events).
    5. Records pipeline health, auto-healing actions, and quarantine audits in Gold.
    
    IMPORTANT: NO BRONZE OR SILVER TABLES ARE CREATED OR ACCESSED IN POSTGRESQL.
    """
    def __init__(self, db_conn_str: str = None):
        self.db_conn_str = db_conn_str or config.get_postgres_uri(role="spark")

    def run_pipeline(self, bronze_object_name: str, dag_id: str = "spark_medallion_job") -> Dict[str, Any]:
        logger.info(f"Starting Medallion ETL for Bronze object: {bronze_object_name}")
        
        # 1. Read Bronze from MinIO Data Lake
        raw_bytes = lakehouse_client.get_object(config.minio_bucket_bronze, bronze_object_name)
        payload = json.loads(raw_bytes.decode("utf-8"))
        features = payload.get("features", [])
        logger.info(f"Extracted {len(features)} features from MinIO Bronze.")

        # 2. In-Memory Validation & Self-Healing
        validation_results = SelfHealingValidator.process_batch(features, dag_id=dag_id)
        valid_records = validation_results["valid_records"]
        quarantine_records = validation_results["quarantine_records"]
        metrics = validation_results["metrics"]

        # 3. Save to MinIO Silver Layer (Columnar Parquet or structured JSON in Data Lake)
        timestamp_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
        silver_object_name = f"events_parquet/year={datetime.datetime.now().year}/month={datetime.datetime.now().month:02d}/silver_{timestamp_str}.json"
        
        silver_payload = json.dumps([
            {k: (v.isoformat() if isinstance(v, datetime.datetime) else v) for k, v in r.items()}
            for r in valid_records
        ])
        lakehouse_client.put_object(config.minio_bucket_silver, silver_object_name, silver_payload)
        logger.info(f"Successfully persisted {len(valid_records)} records to MinIO Silver: {silver_object_name}")

        # 4. Load into PostgreSQL GOLD Serving Layer (Idempotent UPSERT)
        self._upsert_to_gold(valid_records, quarantine_records, metrics)

        return {
            "status": "SUCCESS",
            "bronze_source": bronze_object_name,
            "silver_destination": silver_object_name,
            "total_processed": metrics["total_records"],
            "gold_upserted": len(valid_records),
            "auto_healed": metrics["auto_healed_records"],
            "quarantined": len(quarantine_records),
            "quality_score": metrics["quality_score"]
        }

    def _upsert_to_gold(self, valid_records: List[Dict[str, Any]], quarantine_records: List[Dict[str, Any]], metrics: Dict[str, Any]):
        try:
            conn = psycopg2.connect(self.db_conn_str)
            cur = conn.cursor()

            # A. Upsert into gold.fact_earthquake_events
            if valid_records:
                upsert_sql = """
                INSERT INTO gold.fact_earthquake_events (
                    event_id, mag, mag_type, place_sanitized, event_time, updated_time,
                    longitude, latitude, depth_km, tsunami_alert, significance,
                    status, event_type, url, is_auto_healed, healing_action, ingested_at
                ) VALUES (
                    %(event_id)s, %(mag)s, %(mag_type)s, %(place_sanitized)s, %(event_time)s, %(updated_time)s,
                    %(longitude)s, %(latitude)s, %(depth_km)s, %(tsunami_alert)s, %(significance)s,
                    %(status)s, %(event_type)s, %(url)s, %(is_auto_healed)s, %(healing_action)s, %(ingested_at)s
                )
                ON CONFLICT (event_id) DO UPDATE SET
                    mag = EXCLUDED.mag,
                    mag_type = EXCLUDED.mag_type,
                    place_sanitized = EXCLUDED.place_sanitized,
                    updated_time = EXCLUDED.updated_time,
                    longitude = EXCLUDED.longitude,
                    latitude = EXCLUDED.latitude,
                    depth_km = EXCLUDED.depth_km,
                    tsunami_alert = EXCLUDED.tsunami_alert,
                    significance = EXCLUDED.significance,
                    status = EXCLUDED.status,
                    is_auto_healed = EXCLUDED.is_auto_healed,
                    healing_action = EXCLUDED.healing_action
                WHERE EXCLUDED.updated_time >= gold.fact_earthquake_events.updated_time;
                """
                execute_batch(cur, upsert_sql, valid_records)
                logger.info(f"Upserted {len(valid_records)} records into gold.fact_earthquake_events.")

            # B. Insert quality run record into gold.fact_quality_and_healing_runs
            quality_sql = """
            INSERT INTO gold.fact_quality_and_healing_runs (
                dag_id, total_records, passed_records, auto_healed_records,
                quarantined_records, quality_score, freshness_seconds, rules_evaluated
            ) VALUES (
                %(dag_id)s, %(total_records)s, %(passed_records)s, %(auto_healed_records)s,
                %(quarantined_records)s, %(quality_score)s, %(freshness_seconds)s, %(rules_evaluated)s
            );
            """
            cur.execute(quality_sql, {
                "dag_id": metrics["dag_id"],
                "total_records": metrics["total_records"],
                "passed_records": metrics["passed_records"],
                "auto_healed_records": metrics["auto_healed_records"],
                "quarantined_records": metrics["quarantined_records"],
                "quality_score": metrics["quality_score"],
                "freshness_seconds": metrics["freshness_seconds"],
                "rules_evaluated": json.dumps(metrics["rules_evaluated"])
            })

            # C. Insert quarantine audit into gold.fact_quarantine_events
            if quarantine_records:
                quarantine_sql = """
                INSERT INTO gold.fact_quarantine_events (
                    raw_event_id, rejection_reason, raw_payload, rule_violated
                ) VALUES (
                    %(raw_event_id)s, %(rejection_reason)s, %(raw_payload)s, %(rule_violated)s
                );
                """
                quarantine_batch = [
                    {
                        "raw_event_id": q["raw_event_id"],
                        "rejection_reason": q["rejection_reason"],
                        "raw_payload": json.dumps(q["raw_payload"]),
                        "rule_violated": q["rule_violated"]
                    }
                    for q in quarantine_records
                ]
                execute_batch(cur, quarantine_sql, quarantine_batch)
                logger.info(f"Recorded {len(quarantine_records)} items in gold.fact_quarantine_events.")

            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Error persisting to Gold layer: {e}")
            raise e
