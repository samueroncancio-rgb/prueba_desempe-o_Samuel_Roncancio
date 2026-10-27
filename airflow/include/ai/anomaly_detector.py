import math
import json
import logging
from typing import List, Dict, Any, Optional
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch

from airflow.include.config import config

logger = logging.getLogger("sismowatch.anomaly_detector")

class AnomalyDetector:
    """
    Seismic Anomaly Detection Engine:
    1. Spatial Clustering with DBSCAN (Haversine metric): Identifies seismic swarms.
    2. Energy Release Outlier Detection (Gutenberg-Richter Log Energy): Identifies high-energy anomalies.
    """
    def __init__(self, db_conn_str: Optional[str] = None):
        self.db_conn_str = db_conn_str or config.get_postgres_uri(role="spark")

    def run_detection(self, events: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """Runs detection on provided events or fetches latest events from gold."""
        if events is None:
            events = self._fetch_recent_gold_events()

        if not events or len(events) < 3:
            logger.info("Insufficient events for anomaly detection.")
            return []

        anomalies = []
        
        # 1. Spatial DBSCAN (Haversine metric)
        swarm_anomalies = self._detect_spatial_swarms(events)
        anomalies.extend(swarm_anomalies)

        # 2. Energy Outlier Analysis
        energy_anomalies = self._detect_energy_outliers(events)
        anomalies.extend(energy_anomalies)

        # Persist detected anomalies to gold.ai_anomalies
        self._persist_anomalies(anomalies)
        return anomalies

    def _fetch_recent_gold_events(self) -> List[Dict[str, Any]]:
        try:
            conn = psycopg2.connect(self.db_conn_str)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT event_id, mag, place_sanitized, event_time, latitude, longitude, depth_km, tsunami_alert
                FROM gold.fact_earthquake_events
                WHERE event_time >= NOW() - INTERVAL '72 hours'
                ORDER BY event_time DESC
                LIMIT 500;
            """)
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"Could not fetch events from gold for anomaly detection: {e}")
            return []

    def _detect_spatial_swarms(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Applies spatial DBSCAN clustering to detect localized seismic swarms."""
        from sklearn.cluster import DBSCAN

        coords = np.array([[math.radians(float(e["latitude"])), math.radians(float(e["longitude"]))] for e in events])
        # Epsilon = 100 km / Earth radius (6371 km)
        kms_per_radian = 6371.0
        epsilon_rad = 100.0 / kms_per_radian
        min_samples = 3

        db = DBSCAN(eps=epsilon_rad, min_samples=min_samples, metric="haversine")
        db.fit(coords)
        labels = db.labels_

        anomalies = []
        unique_labels = set(labels)
        for label in unique_labels:
            if label == -1:
                continue  # Noise
            cluster_indices = np.where(labels == label)[0]
            cluster_events = [events[i] for i in cluster_indices]
            
            center_lat = float(np.mean([float(e["latitude"]) for e in cluster_events]))
            center_lon = float(np.mean([float(e["longitude"]) for e in cluster_events]))
            max_mag = float(max(float(e["mag"]) for e in cluster_events))
            event_ids = [e["event_id"] for e in cluster_events]

            explanation = (
                f"Enjambre sísmico espacial detectado ({len(cluster_events)} eventos) en un radio menor a 100 km "
                f"cerca de lat: {center_lat:.2f}, lon: {center_lon:.2f}. Magnitud máxima registrada: {max_mag:.1f}."
            )

            anomalies.append({
                "anomaly_type": "SEISMIC_SWARM",
                "event_ids": event_ids,
                "center_lat": round(center_lat, 5),
                "center_lon": round(center_lon, 5),
                "event_count": len(cluster_events),
                "max_magnitude": max_mag,
                "explanation": explanation
            })

        return anomalies

    def _detect_energy_outliers(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Detects singular earthquakes releasing massive seismic energy."""
        anomalies = []
        high_mag_events = [e for e in events if float(e["mag"]) >= 5.5]
        for e in high_mag_events:
            explanation = (
                f"Alerta de alta energía sísmica liberada: sismo de magnitud {e['mag']} en {e['place_sanitized']}. "
                f"Profundidad: {e['depth_km']} km. Alerta Tsunami: {'ACTIVADA' if e.get('tsunami_alert') else 'DESACTIVADA'}."
            )
            anomalies.append({
                "anomaly_type": "ENERGY_SPIKE",
                "event_ids": [e["event_id"]],
                "center_lat": round(float(e["latitude"]), 5),
                "center_lon": round(float(e["longitude"]), 5),
                "event_count": 1,
                "max_magnitude": float(e["mag"]),
                "explanation": explanation
            })
        return anomalies

    def _persist_anomalies(self, anomalies: List[Dict[str, Any]]):
        if not anomalies:
            return
        try:
            conn = psycopg2.connect(self.db_conn_str)
            cur = conn.cursor()
            sql = """
            INSERT INTO gold.ai_anomalies (
                anomaly_type, event_ids, center_lat, center_lon, event_count, max_magnitude, explanation
            ) VALUES (
                %(anomaly_type)s, %(event_ids)s, %(center_lat)s, %(center_lon)s, %(event_count)s, %(max_magnitude)s, %(explanation)s
            );
            """
            batch = [
                {
                    "anomaly_type": a["anomaly_type"],
                    "event_ids": json.dumps(a["event_ids"]),
                    "center_lat": a["center_lat"],
                    "center_lon": a["center_lon"],
                    "event_count": a["event_count"],
                    "max_magnitude": a["max_magnitude"],
                    "explanation": a["explanation"]
                }
                for a in anomalies
            ]
            execute_batch(cur, sql, batch)
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"Persisted {len(anomalies)} anomalies to gold.ai_anomalies.")
        except Exception as e:
            logger.warning(f"Could not persist anomalies to database: {e}")
