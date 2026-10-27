# Módulo de Orquestación y Pipelines de Datos (Apache Airflow 2.9)

> **SismoWatch Enterprise • Data Orchestration & Pipeline Engine**  
> *Ruta: [`airflow/`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow)*

---

## 1. Visión General y Propósito

El módulo de **Apache Airflow 2.9** orquesta el ciclo de vida completo de los datos en **SismoWatch Enterprise**, coordinando desde la extracción de feeds sísmicos en tiempo real hasta la ejecución del cómputo distribuido en Apache Spark y la generación de inteligencia artificial con guardrails deterministas.

### Principios Arquitectónicos
* **Data-Aware Scheduling (Airflow Datasets):** Los DAGs no dependen de cronogramas rígidos acoplados en el tiempo. La escritura en la capa Bronze emite un `Dataset` lógico que dispara de inmediato la ejecución del cómputo Spark.
* **Aislamiento Lakehouse (Regla de Oro):** Ningún operador de Airflow escribe directamente datos de Bronze o Silver en PostgreSQL. Toda la persistencia intermedia se realiza en **MinIO S3**, reservando PostgreSQL exclusivamente para la capa **GOLD**.
* **Zero-Quarantine & Self-Healing:** Integración nativa del motor [`SelfHealingValidator`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/quality/self_healing_validator.py) para reparar registros recuperables en vuelo (`swap(lat, lon)`, sanitización de texto, imputación de nulos) antes de persistir.
* **Resiliencia y Reintentos:** Manejo robusto de fallos de red con la librería `tenacity`, reintentos exponenciales y callbacks unificados de alerta (`on_failure_alert`).

---

## 2. Estructura del Módulo

```
airflow/
├── dags/                                 # Definición de DAGs de Airflow
│   ├── dag_01_usgs_ingestion_bronze.py   # Ingesta cruda continua USGS -> MinIO Bronze
│   ├── dag_02_spark_bronze_to_silver.py  # Spark Medallion Bronze -> Silver Parquet + Auto-Curación
│   ├── dag_03_spark_silver_to_gold.py    # Spark Medallion Silver -> MinIO Gold & Postgres Gold
│   └── dag_04_ai_reports_and_rag.py      # ML DBSCAN, Reportes LLM y pgvector RAG
├── include/                              # Lógica de negocio reutilizable y utilidades
│   ├── ai/                               # Motores de IA, clustering y embeddings
│   │   ├── anomaly_detector.py           # Detección de enjambres sísmicos (DBSCAN + Energía)
│   │   ├── executive_reporter.py         # Reporte diario ejecutivo determinista + LLM
│   │   └── rag_indexer.py                # Generación de embeddings 768d e indexación pgvector
│   ├── quality/                          # Motores de validación y auto-curación
│   │   └── self_healing_validator.py     # Cero cuarentena, swap de coordenadas y sanitización
│   ├── spark_jobs/                       # Scripts de cómputo distribuido PySpark
│   │   └── medallion_etl.py              # Job Medallion Silver/Gold con JDBC Upsert
│   ├── config.py                         # Configuración centralizada Pydantic y URIs
│   ├── error_handler.py                  # Callbacks on_failure y manejo de excepciones
│   └── minio_client.py                   # Wrapper S3/MinIO para operaciones Lakehouse
├── Dockerfile                            # Imagen personalizada Airflow 2.9 + Java + PySpark + JARs
├── requirements.txt                      # Dependencias Python específicas del orquestador
├── datasets.py                           # Definición de Datasets lógicos para Data-Aware Scheduling
└── decorators.py                         # Helpers y decoradores TaskFlow API
```

---

## 3. Desglose Detallado de DAGs

### DAG 01: Ingesta Inmutable USGS a Bronze
* **Archivo:** [`airflow/dags/dag_01_usgs_ingestion_bronze.py`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/dags/dag_01_usgs_ingestion_bronze.py)
* **Frecuencia:** `*/5 * * * *` (Cada 5 minutos) y soporte de ejecución manual / backfill parametrizado.
* **Salida de Datos:** Archivos GeoJSON crudos inmutables en `s3a://sismowatch-bronze/raw/YYYY/MM/DD/usgs_events_*.geojson`.
* **Emisión de Dataset:** `Dataset("s3a://sismowatch-bronze/latest")`.
* **Características:**
  * Consulta el feed oficial de USGS (`https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson`).
  * Reintentos automáticos con backoff exponencial mediante políticas de reintento de Airflow (3 reintentos, delay de 15s) y timeouts estrictos.
  * Preservación del payload exacto sin mutaciones para garantizar la inmutabilidad histórica.

### DAG 02: Procesamiento PySpark Bronze hacia MinIO Silver (Auto-Curación)
* **Archivo:** [`airflow/dags/dag_02_spark_bronze_to_silver.py`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/dags/dag_02_spark_bronze_to_silver.py)
* **Disparador:** Disparado reactivamente por el dataset `Dataset("s3a://sismowatch-bronze/latest")`.
* **Emisión de Dataset:** `Dataset("s3a://sismowatch-silver/latest")`.
* **Flujo Operativo:**
  1. Lee el último GeoJSON depositado en MinIO Bronze.
  2. Ejecuta el [`SelfHealingValidator`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/quality/self_healing_validator.py):
     * Detecta e intercambia ejes invertidos si $|lat| > 90$ y $|lon| \le 90$.
     * Sanitiza el campo `place` eliminando patrones de Prompt Injection y etiquetas peligrosas.
     * Imputa profundidad y magnitud mediana cuando sea necesario.
     * Marca `is_auto_healed = TRUE` y documenta la acción.
  3. Ejecuta PySpark para desanidar, normalizar y calcular energía Gutenberg-Richter.
  4. Escribe el dataset columnar limpio y particionado en formato **Parquet** en `s3a://sismowatch-silver/`.

### DAG 03: Curación y Agregación PySpark Silver hacia PostgreSQL Gold
* **Archivo:** [`airflow/dags/dag_03_spark_silver_to_gold.py`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/dags/dag_03_spark_silver_to_gold.py)
* **Disparador:** Disparado reactivamente por el dataset `Dataset("s3a://sismowatch-silver/latest")`.
* **Emisión de Dataset:** `Dataset("postgres://.../gold/fact_earthquake_events")`.
* **Flujo Operativo:**
  1. Lee los eventos limpios en formato Parquet/JSON desde MinIO Silver.
  2. Computa métricas agregadas y analíticas Gold en MinIO (`sismowatch-gold`).
  3. Realiza un **UPSERT idempotente** masivo en la Serving Layer de PostgreSQL ([`gold.fact_earthquake_events`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/db/init/03_tables_and_indexes.sql#L7)).
  4. Registra la auditoría de calidad y salud del pipeline en `gold.fact_quality_and_healing_runs`.
  5. Enruta registros con corrupción irrecuperable a `gold.fact_quarantine_events`.

### DAG 04: Inteligencia Artificial, Detección de Anomalías y RAG
* **Archivo:** [`airflow/dags/dag_04_ai_reports_and_rag.py`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/dags/dag_04_ai_reports_and_rag.py)
* **Frecuencia:** `0 6 * * *` (Diario a las 06:00 UTC) y bajo demanda desde la Serving API.
* **Componentes:**
  1. **Detección de Anomalías ([`AnomalyDetector`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/ai/anomaly_detector.py)):**
     * Agrupamiento espacial DBSCAN con métrica `haversine` ($\epsilon = 100\text{ km}$, $\text{min\_samples} = 3$) para detectar enjambres sísmicos.
     * Detección de anomalías energéticas de Gutenberg-Richter y magnitudes extremas ($\ge 5.5\text{ M}$).
     * Persistencia en `gold.ai_anomalies`.
  2. **Reporte Ejecutivo Diario Anti-Alucinación ([`ExecutiveReporter`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/ai/executive_reporter.py)):**
     * Cálculo de métricas agregadas exactas en SQL/Python (`total_events`, `max_mag`, `tsunami_alerts`).
     * Invocación controlada del LLM para narrar el resumen sin alterar métricas numéricas.
     * Validación determinista post-generación; en caso de discrepancia o fallo de red, se activa el fallback por plantilla determinista Jinja2.
     * Persistencia en `gold.ai_daily_reports`.
  3. **Indexación Vectorial RAG ([`RAGIndexer`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/ai/rag_indexer.py)):**
     * Generación de embeddings densos de 768 dimensiones normalizados a norma $L_2$.
     * Indexación en `gold.rag_knowledge_base` con índice HNSW (`vector_cosine_ops`) en `pgvector`.

---

## 4. Componentes Compartidos (`airflow/include/`)

| Archivo / Módulo | Propósito y Capacidades |
| :--- | :--- |
| [`config.py`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/config.py) | Configuración tipada con Pydantic. Provee `get_postgres_uri(role)` dinámico con credenciales específicas (`role_spark_etl`, `role_api_reader`, admin). |
| [`minio_client.py`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/minio_client.py) | Cliente S3/MinIO con auto-creación de buckets (`sismowatch-bronze`, `sismowatch-silver`, `sismowatch-gold`) y subida con retry. |
| [`error_handler.py`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/error_handler.py) | Manejador de fallos de tareas (`on_failure_alert`) con extracción de contexto de Airflow y logging seguro. |
| [`quality/self_healing_validator.py`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/quality/self_healing_validator.py) | Motor Zero-Quarantine. Contiene `swap_coordinates`, `sanitize_text`, validación de rangos sísmicos y scoring de calidad. |
| [`spark_jobs/medallion_etl.py`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/spark_jobs/medallion_etl.py) | Orquestador de PySpark que transforma GeoJSON en DataFrames tipados, genera particiones Parquet e inserta en PostgreSQL. |
| [`ai/anomaly_detector.py`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/ai/anomaly_detector.py) | Motor matemático con `scikit-learn` y `numpy` para clusterización geodésica y outliers energéticos. |
| [`ai/executive_reporter.py`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/ai/executive_reporter.py) | Generador de reportes en Markdown con doble red de seguridad determinista. |
| [`ai/rag_indexer.py`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/ai/rag_indexer.py) | Generador de representaciones semánticas densas compatibles con el modelo HNSW de PostgreSQL. |

---

## 5. Imagen Docker y Dependencias

El contenedor de Airflow se compila desde [`airflow/Dockerfile`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/Dockerfile) sobre la base `apache/airflow:2.9.1-python3.11`:
* **Java Runtime:** Se instala `default-jre-headless` para permitir la ejecución de `pyspark`.
* **Librerías Python:** Instaladas desde [`airflow/requirements.txt`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/requirements.txt) (`pyspark==3.5.1`, `boto3`, `scikit-learn`, `psycopg2-binary`, `tenacity`).
* **JARs de Conectividad:** Se pre-descargan e inyectan los conectores Maven requeridos:
  * `hadoop-aws-3.3.4.jar`
  * `aws-java-sdk-bundle-1.12.262.jar`
  * `postgresql-42.6.0.jar`

---

## 6. Variables de Entorno Clave

| Variable | Descripción | Valor por Defecto |
| :--- | :--- | :--- |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Conexión de metadatos de Airflow a PostgreSQL | `postgresql+psycopg2://...` |
| `AIRFLOW__CORE__FERNET_KEY` | Llave Fernet para encriptación de conexiones | Generada de 32 bytes base64 |
| `SPARK_MASTER_URL` | URL del Master de Spark en la red Docker | `spark://spark-master:7077` |
| `MINIO_ENDPOINT` | Dirección del Object Storage S3 | `minio:9000` |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | Credenciales de acceso al Data Lake | Configurables en `.env` |
| `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_DB` | Conexión a la Serving Database | `postgres:5432/sismowatch_db` |

---

## 7. Comandos Frecuentes y Operación CLI

Para interactuar con Airflow desde la terminal dentro de los contenedores:

```bash
# Ver lista de DAGs registrados
docker exec -it sismowatch_airflow_scheduler airflow dags list

# Probar la ejecución de una tarea individual del DAG 01
docker exec -it sismowatch_airflow_scheduler airflow tasks test dag_01_usgs_ingestion_bronze ingest_usgs_realtime 2026-10-27

# Disparar manualmente el DAG de IA y Reportes
docker exec -it sismowatch_airflow_scheduler airflow dags trigger dag_03_ai_reports_and_rag

# Validar integridad sintáctica de todos los DAGs
.venv/bin/pytest tests/test_dag_integrity.py -v
```
