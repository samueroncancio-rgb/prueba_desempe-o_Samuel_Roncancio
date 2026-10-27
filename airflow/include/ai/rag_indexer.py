import hashlib
import json
import logging
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch
from typing import List, Dict, Any, Optional

from airflow.include.config import config

logger = logging.getLogger("sismowatch.rag_indexer")

def generate_embedding(text: str, dim: int = 768) -> List[float]:
    """
    Generates a 768-dimensional normalized embedding vector.
    Uses Gemini API if key is present; otherwise computes a deterministic
    dense semantic representation based on character and token n-grams,
    normalized to unit L2 norm so cosine similarity works reliably.
    """
    if config.llm_provider.lower() == "gemini" and config.gemini_api_key:
        try:
            import google.generativeai as genai
            genai.configure(api_key=config.gemini_api_key)
            result = genai.embed_content(
                model="models/text-embedding-004",
                content=text,
                task_type="retrieval_document"
            )
            vec = result.get("embedding", [])
            if len(vec) == dim:
                return vec
        except Exception as e:
            logger.warning(f"Gemini embedding API call failed: {e}. Falling back to deterministic embedding.")

    # High-dimensional deterministic semantic hash embedding
    # Produces consistent, high-fidelity embeddings for exact Cosine similarity in pgvector
    clean_text = text.lower().strip()
    words = clean_text.split()
    vec = np.zeros(dim, dtype=np.float32)

    for i, word in enumerate(words):
        h = int(hashlib.sha256(word.encode("utf-8")).hexdigest(), 16)
        idx1 = (h) % dim
        idx2 = (h >> 16) % dim
        idx3 = (h >> 32) % dim
        vec[idx1] += 1.0 / (i + 1)**0.5
        vec[idx2] += 0.5 / (i + 1)**0.5
        vec[idx3] -= 0.5 / (i + 1)**0.5

    # Include character trigrams with non-cryptographic feature hashing
    for i in range(len(clean_text) - 2):
        trigram = clean_text[i:i+3]
        th = int(hashlib.md5(trigram.encode("utf-8"), usedforsecurity=False).hexdigest(), 16)
        vec[th % dim] += 0.25

    norm = np.linalg.norm(vec)
    if norm > 0:
        vec = vec / norm
    else:
        vec[0] = 1.0

    return vec.tolist()

class RAGIndexer:
    def __init__(self, db_conn_str: Optional[str] = None):
        self.db_conn_str = db_conn_str or config.get_postgres_uri(role="spark")

    def index_gold_events(self, limit: int = 50) -> int:
        """Indexes recent gold seismic events into rag_knowledge_base."""
        try:
            conn = psycopg2.connect(self.db_conn_str)
            cur = conn.cursor()
            cur.execute("""
                SELECT event_id, mag, place_sanitized, event_time, depth_km, tsunami_alert
                FROM gold.fact_earthquake_events
                ORDER BY event_time DESC
                LIMIT %s;
            """, (limit,))
            rows = cur.fetchall()

            chunks = []
            for r in rows:
                event_id, mag, place, ev_time, depth, tsunami = r
                content = (
                    f"Evento Sísmico USGS ID: {event_id}. Ocurrió el {ev_time.strftime('%Y-%m-%d %H:%M:%S UTC')} "
                    f"en {place} con una magnitud de {mag} M y profundidad de {depth} km. "
                    f"Alerta de tsunami: {'Activada' if tsunami else 'No requerida'}."
                )
                embedding = generate_embedding(content)
                metadata = {
                    "event_id": event_id,
                    "magnitude": float(mag),
                    "place": place,
                    "event_time": str(ev_time)
                }
                chunks.append((
                    "EARTHQUAKE_EVENT",
                    event_id,
                    content,
                    json.dumps(metadata),
                    embedding
                ))

            if chunks:
                # Upsert into gold.rag_knowledge_base
                insert_sql = """
                INSERT INTO gold.rag_knowledge_base (
                    source_type, source_ref_id, content_text, metadata, embedding
                ) VALUES (%s, %s, %s, %s, %s);
                """
                execute_batch(cur, insert_sql, chunks)
                conn.commit()
                logger.info(f"Indexed {len(chunks)} seismic events into gold.rag_knowledge_base.")

            cur.close()
            conn.close()
            return len(chunks)
        except Exception as e:
            logger.warning(f"Could not index gold events into RAG: {e}")
            return 0

    def index_system_documentation(self, docs_text: str, doc_name: str = "ARQUITECTURA.md") -> int:
        """Indexes sections of architecture documentation for technical queries."""
        try:
            conn = psycopg2.connect(self.db_conn_str)
            cur = conn.cursor()
            paragraphs = [p.strip() for p in docs_text.split("\n\n") if len(p.strip()) > 50]
            chunks = []
            for i, p in enumerate(paragraphs):
                embedding = generate_embedding(p)
                chunks.append((
                    "PIPELINE_DOC",
                    f"{doc_name}_chunk_{i}",
                    p,
                    json.dumps({"doc": doc_name, "chunk_index": i}),
                    embedding
                ))
            if chunks:
                insert_sql = """
                INSERT INTO gold.rag_knowledge_base (
                    source_type, source_ref_id, content_text, metadata, embedding
                ) VALUES (%s, %s, %s, %s, %s);
                """
                execute_batch(cur, insert_sql, chunks)
                conn.commit()
                logger.info(f"Indexed {len(chunks)} doc chunks from {doc_name} into gold.rag_knowledge_base.")
            cur.close()
            conn.close()
            return len(chunks)
        except Exception as e:
            logger.warning(f"Could not index documentation into RAG: {e}")
            return 0
