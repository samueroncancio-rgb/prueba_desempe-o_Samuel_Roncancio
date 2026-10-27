# Arquitectura de Datos e Inteligencia Artificial: SismoWatch Enterprise

## 1. Propósito de la Arquitectura y Visión General

**SismoWatch** es la plataforma corporativa de monitoreo sísmico global en tiempo casi real desarrollada para **SuperApp**. Las áreas de operaciones críticas y gestión del riesgo requieren visibilidad inmediata de eventos telúricos a nivel mundial (magnitud, localización, profundidad, alertas de tsunami y enjambres anómalos) para priorizar protocolos de contingencia y asignación de recursos.

### Problema que resuelve
1. **Pérdida de Historial e Inmutabilidad:** La fuente pública de USGS actualiza su feed cada minuto sin persistencia histórica propia. La arquitectura Lakehouse implementada retiene el 100% de los datos crudos en formato inmutable.
2. **Eliminación de la Cuarentena Pasiva (Self-Healing & Cero Cuarentena):** Las arquitecturas tradicionales acumulan registros con errores menores en tablas de cuarentena donde mueren sin ser revisados. SismoWatch aplica un motor determinista de auto-remediación en vuelo (inversión de ejes lat/lon, imputación de atributos estadísticos y sanitización) registrando métricas de auditoría.
3. **Restricción Estricta de Almacenamiento en Base de Datos (Regla de Oro):** Para garantizar un rendimiento óptimo de consulta analítica y evitar sobrecargar la base de datos relacional con payloads crudos, **la base de datos PostgreSQL no contiene tablas de Bronze ni de Silver**. El Data Lake (MinIO) aloja las capas Bronze (GeoJSON) y Silver (Parquet columnar), mientras que PostgreSQL actúa como Serving Layer conteniendo **exclusivamente las tablas de hechos, dimensiones, auditoría y vectores de la capa GOLD**.
4. **Inteligencia Artificial Segura y Gobernada:** Generación de reportes ejecutivos con validación numérica determinista anti-alucinación, detección de enjambres sísmicos mediante `DBSCAN`, y un Agente conversacional con RAG estricto (`pgvector`) limitado por guardrails semánticos al dominio exclusivo de sismología y operaciones de la plataforma.

---

## 2. Diagrama de Componentes y Capas

```mermaid
flowchart TD
    subgraph INGESTA["1. Ingesta Externa"]
        USGS["USGS Earthquake API\n(all_hour.geojson / Backfill 30d)"]
    end

    subgraph ORQUESTACION["2. Orquestación (Apache Airflow 2.9)"]
        DAG1["DAG 01: Ingesta Bronze (Cada 5 min)"]
        DAG2["DAG 02: Spark Silver & Gold (Dataset Trigger)"]
        DAG3["DAG 03: IA Reports, ML & RAG (Diario / On-demand)"]
    end

    subgraph DATALAKE["3. Data Lake Inmutable (MinIO S3)"]
        BRONZE["s3a://sismowatch/bronze/\n(GeoJSON Crudo Inmutable)"]
        SILVER["s3a://sismowatch/silver/\n(Parquet Limpio y Particionado)"]
    end

    subgraph COMPUTO["4. Cómputo Distribuido & Validación"]
        SPARK["Motor PySpark / SelfHealingValidator\n(Auto-remediación en memoria)"]
    end

    subgraph SERVING_DB["5. Serving Database (PostgreSQL 16 + pgvector) - SOLO GOLD"]
        FACT_EVENTS["gold.fact_earthquake_events\n(Hechos de sismos limpios)"]
        FACT_QUALITY["gold.fact_quality_and_healing_runs\n(Métricas de calidad y corridas)"]
        FACT_QUARANTINE["gold.fact_quarantine_events\n(Auditoría de rechazos)"]
        AI_REPORTS["gold.ai_daily_reports\n(Reportes ejecutivos validados)"]
        AI_ANOMALIES["gold.ai_anomalies\n(Enjambres DBSCAN)"]
        AI_RAG["gold.rag_knowledge_base\n(pgvector HNSW 768d)"]
        CURATED_VIEWS["curated.* (Vistas espejo para compatibilidad)"]
    end

    subgraph CONSUMO["6. Plataforma de Consumo"]
        FASTAPI["FastAPI Backend\n(RBAC JWT: viewer / admin, SSE Stream)"]
        FRONTEND["Frontend React + TypeScript + Leaflet\n(Dashboard, Reportes, Chat RAG)"]
    end

    USGS -->|HTTPS + Tenacity Retry| DAG1
    DAG1 -->|Escribe GeoJSON| BRONZE
    BRONZE -->|Lectura S3A| DAG2
    DAG2 --> SPARK
    SPARK -->|Escribe Parquet Limpio| SILVER
    SPARK -->|UPSERT Idempotente| FACT_EVENTS
    SPARK -->|Registra Calidad| FACT_QUALITY
    SPARK -->|Rechazos Críticos| FACT_QUARANTINE
    FACT_EVENTS --> DAG3
    DAG3 -->|ML Anomalías| AI_ANOMALIES
    DAG3 -->|LLM + Cifras Exactas| AI_REPORTS
    DAG3 -->|Embeddings| AI_RAG
    SERVING_DB -->|Solo Lectura (role_api_reader)| FASTAPI
    FASTAPI --> FRONTEND
```

---

## 3. Matriz de Almacenamiento Lakehouse Medallion

| Capa | Almacenamiento Físico | Esquema en Base de Datos | Formato y Contenido | Justificación Arquitectónica |
| :--- | :--- | :--- | :--- | :--- |
| **Bronze** | MinIO (`s3a://sismowatch-bronze/`) | **NINGUNO (Sin tablas en BD)** | Archivos GeoJSON crudos inmutables | Aislamiento del dato original. Permite re-procesamiento completo histórico sin costo de almacenamiento ni bloqueo en la base de datos relacional. |
| **Silver** | MinIO (`s3a://sismowatch-silver/`) | **NINGUNO (Sin tablas en BD)** | Archivos Apache Parquet particionados por `year/month/day` | Formato columnar comprimido de alto rendimiento para analítica pesada. Las transformaciones y deduplicación se realizan en memoria antes de persistir. |
| **Gold** | PostgreSQL 16 + pgvector | **`gold.*` (con vistas espejo en `curated.*`)** | Tablas relacionales indexadas + Vectores HNSW | **Única capa con tablas en PostgreSQL.** Optimizado para consultas de sub-segundo desde FastAPI, KPIs agregados, persistencia de embeddings vectoriales y auditoría de remediación. |

---

## 4. Modelo de Datos en PostgreSQL (Capa Gold Exclusiva)

```
+-------------------------------------------------------------+
|               gold.fact_earthquake_events                  |
+-------------------------------------------------------------+
| * event_id : VARCHAR(100) [PK]                              |
|   mag : NUMERIC(4,2)                                        |
|   mag_type : VARCHAR(20)                                    |
|   place_sanitized : TEXT (Anti-Prompt Injection)            |
|   event_time : TIMESTAMPTZ                                  |
|   updated_time : TIMESTAMPTZ                                |
|   longitude : NUMERIC(9,5)                                  |
|   latitude : NUMERIC(9,5)                                   |
|   depth_km : NUMERIC(8,3)                                   |
|   tsunami_alert : BOOLEAN                                   |
|   significance : INTEGER                                    |
|   status : VARCHAR(50)                                      |
|   event_type : VARCHAR(50)                                  |
|   url : TEXT                                                |
|   is_auto_healed : BOOLEAN                                  |
|   healing_action : TEXT                                     |
|   ingested_at : TIMESTAMPTZ                                 |
+-------------------------------------------------------------+
                              |
                              | 1:N (Contexto y Embeddings)
                              v
+-------------------------------------------------------------+
|                 gold.rag_knowledge_base                     |
+-------------------------------------------------------------+
| * chunk_id : UUID [PK]                                      |
|   source_type : VARCHAR(50)                                 |
|   source_ref_id : VARCHAR(100)                              |
|   content_text : TEXT                                       |
|   metadata : JSONB                                          |
|   created_at : TIMESTAMPTZ                                  |
|   embedding : vector(768) [HNSW Cosine Index]               |
+-------------------------------------------------------------+

+-------------------------------------------------------------+
|            gold.fact_quality_and_healing_runs               |
+-------------------------------------------------------------+
| * run_id : UUID [PK]                                        |
|   evaluated_at : TIMESTAMPTZ                                |
|   dag_id : VARCHAR(100)                                     |
|   total_records : INTEGER                                   |
|   passed_records : INTEGER                                  |
|   auto_healed_records : INTEGER                             |
|   quarantined_records : INTEGER                             |
|   quality_score : NUMERIC(5,2)                              |
|   freshness_seconds : INTEGER                               |
|   rules_evaluated : JSONB                                   |
+-------------------------------------------------------------+

+-------------------------------------------------------------+
|                  gold.ai_daily_reports                      |
+-------------------------------------------------------------+
| * report_id : UUID [PK]                                     |
|   report_date : DATE [UNIQUE]                               |
|   title : TEXT                                              |
|   summary_narrative : TEXT                                  |
|   calculated_metrics : JSONB (Cifras deterministas)         |
|   llm_metrics_verified : BOOLEAN                            |
|   is_fallback : BOOLEAN                                     |
|   created_at : TIMESTAMPTZ                                  |
+-------------------------------------------------------------+

+-------------------------------------------------------------+
|                     gold.ai_anomalies                       |
+-------------------------------------------------------------+
| * anomaly_id : UUID [PK]                                    |
|   detected_at : TIMESTAMPTZ                                 |
|   anomaly_type : VARCHAR(50)                                |
|   event_ids : JSONB                                         |
|   center_lat : NUMERIC(9,5)                                 |
|   center_lon : NUMERIC(9,5)                                 |
|   event_count : INTEGER                                     |
|   max_magnitude : NUMERIC(4,2)                              |
|   explanation : TEXT                                        |
+-------------------------------------------------------------+

+-------------------------------------------------------------+
|                 gold.ai_execution_logs                      |
+-------------------------------------------------------------+
| * log_id : UUID [PK]                                        |
|   executed_at : TIMESTAMPTZ                                 |
|   task_name : VARCHAR(100)                                  |
|   model_name : VARCHAR(100)                                 |
|   prompt_version : VARCHAR(50)                              |
|   prompt_tokens : INTEGER                                   |
|   completion_tokens : INTEGER                               |
|   total_tokens : INTEGER                                    |
|   latency_ms : INTEGER                                      |
|   estimated_cost_usd : NUMERIC(8,6)                         |
|   status : VARCHAR(20)                                      |
+-------------------------------------------------------------+
```

---

## 5. Decisiones Técnicas y Justificación (Trade-offs)

### 5.1. ¿Por qué Apache Airflow y no un simple Cron?
* **Gestión de dependencias y Datasets:** Airflow 2.9 permite desencadenar DAGs por actualización de `Dataset("s3a://sismowatch-bronze/latest")`, desacoplando la ingesta del procesamiento.
* **Control de concurrencia y Backfill:** Permite lanzar un backfill histórico de 30 días con ventanas temporales controladas y reintentos automáticos con retroceso exponencial (`exponential_backoff`), algo inviable de gestionar con confiabilidad en un script crontab.
* **Trazabilidad operativa:** Alertas inmediatas mediante `on_failure_callback`, métricas de SLA y visibilidad de cada tarea.

### 5.2. ¿Por qué NO almacenar tablas Bronze y Silver en PostgreSQL?
* **Escalabilidad y Costo:** Los datos crudos en GeoJSON contienen estructuras anidadas voluminosas. Mantenerlos en tablas de base de datos causa bloat de tablas, fragmentación de índices y consumo excesivo de memoria transaccional.
* **Lakehouse Pattern Moderno:** Siguiendo la arquitectura de Databricks / Snowflake, el almacenamiento de bajo costo (MinIO S3) maneja las fases crudas (Bronze) y columnar limpias (Silver Parquet). La base de datos relacional se reserva exclusivamente como **Capa de Servicio (Gold)** para servir tráfico de usuario concurrente con baja latencia.

### 5.3. ¿Por qué `pgvector` frente a una base de datos vectorial externa (Pinecone / Qdrant)?
* **Consistencia Transaccional y Simplicidad:** Almacenar vectores en la misma base de datos `gold` permite ejecutar consultas híbridas (`Hybrid Search`: filtrar por magnitud $> 5.0$ y rango de fechas en SQL mientras se calcula la distancia coseno HNSW).
* **Eficiencia en Recursos:** Evita levantar otro servicio pesado en Docker, reduce la latencia de red entre servicios y aprovecha el mismo esquema de seguridad por roles de PostgreSQL.

### 5.4. ¿Por qué Auto-Remediación Determinista en vuelo?
* En alertas de riesgo sísmico, un evento con coordenadas invertidas o un campo sucio no debe ser descartado silenciosamente en una cuarentena donde nadie lo leerá a tiempo. El motor `SelfHealingValidator` intercambia ejes (`swap lat/lon`), normaliza profundidades, sanitiza texto malicioso y audita la corrección en `is_auto_healed = TRUE`, manteniendo al operador informado sin interrupciones operativas.

---

## 6. Seguridad y Mínimo Privilegio

1. **Gestión de Secretos:** Ninguna credencial en código duro. Todo se inyecta vía variables de entorno cargadas en `.env` (ignorado en Git).
2. **Roles de Base de Datos:**
   * `role_spark_etl`: Escritura y actualización en `gold`.
   * `role_api_reader`: Permiso exclusivo de `SELECT` sobre `gold` y vistas `curated`.
3. **Autenticación y RBAC en FastAPI:**
   * Tokens JWT con algoritmo HS256 y expiración controlada.
   * `viewer`: Acceso a Dashboard, métricas, mapas, reportes y RAG.
   * `admin`: Acceso privilegiado a auditoría de cuarentena, disparo de backfill y regeneración forzada de IA.
4. **Sanitización Anti-Prompt Injection:** Limpieza de todo campo de texto libre proveniente de fuentes externas mediante expresiones regulares antes de persistirlo o suministrarlo al LLM.
