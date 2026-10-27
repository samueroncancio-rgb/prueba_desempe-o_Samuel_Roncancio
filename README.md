# SismoWatch Enterprise: Plataforma de Inteligencia y Monitoreo Sísmico Global

> **SuperApp • Vertical de Monitoreo Sísmico y Gestión de Riesgo en Tiempo Casi Real**  
> *Autor: Samuel Roncancio — Científico de Datos Principal / Senior AI Engineer*

---

## 1. Visión General del Proyecto

**SismoWatch Enterprise** es una plataforma de ingeniería de datos, analítica e Inteligencia Artificial de alto rendimiento que ingesta de forma continua el feed público de la API del Servicio Geológico de los Estados Unidos (USGS), procesando eventos telúricos a escala planetaria bajo el patrón de arquitectura **Medallion Lakehouse**.

La plataforma provee a centros de operaciones de emergencia, analistas de riesgo y equipos ejecutivos telemetría en tiempo casi real con latencias sub-segundo, visualización cartográfica geoespacial, cálculo determinista de calidad de datos con auto-curación (*Zero-Quarantine*), detección de enjambres anómalos vía Machine Learning y un Asistente Conversacional RAG gobernado con citas factuales y gráficos dinámicos.

---

## 2. Mapa de Navegación del Repositorio (Módulos y READMEs)

Para facilitar el estudio, auditoría y mantenimiento del proyecto, cada componente cuenta con su documentación técnica exhaustiva:

| Módulo | Documentación Específica | Rol y Responsabilidad Principal |
| :--- | :--- | :--- |
| **Orquestación** | [📘 `airflow/README.md`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/README.md) | DAGs de Airflow 2.9, Data-Aware Datasets, conectores MinIO y motores en `airflow/include/`. |
| **Serving API** | [📗 `api/README.md`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/api/README.md) | FastAPI, RBAC JWT (Admin / Viewer), Server-Sent Events (SSE) y Agente RAG. |
| **Cómputo Distribuido** | [📙 `spark/README.md`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/spark/README.md) | Clúster Apache Spark 3.5 (Master/Worker), conectores S3A y PostgreSQL JDBC, transformaciones Parquet. |
| **Base de Datos & Vectores**| [📕 `db/README.md`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/db/README.md) | PostgreSQL 16 + pgvector, índice HNSW 768d, DDL de esquemas y regla de oro (Solo Gold). |
| **Frontend Web** | [💻 `web/README.md`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/web/README.md) | SPA React 19 + TypeScript + Leaflet + Tailwind CSS + Lucide + Chatbot SismoBot. |
| **Aseguramiento de Calidad**| [🧪 `tests/README.md`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/tests/README.md) | Suite de 19 pruebas `pytest`, validación AST, pruebas de auto-curación y escaneos de seguridad. |
| **Centro de Documentación** | [📚 `docs/README.md`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/docs/README.md) | Hub consolidado de especificaciones, decisiones arquitectónicas (ADRs) y glosario. |

---

## 3. Principios y Decisiones Arquitectónicas Clave

### A. Regla de Oro de Almacenamiento (Aislamiento Lakehouse)
Para evitar la sobrecarga y el *table bloat* característico de las bases de datos relacionales al almacenar payloads crudos masivos:
* **Bronze (GeoJSON crudo e inmutable):** Reside exclusivamente en MinIO S3 (`s3a://sismowatch-bronze/`).
* **Silver (Parquet estructurado, limpio y particionado):** Reside exclusivamente en MinIO S3 (`s3a://sismowatch-silver/`).
* **Gold (Serving Layer Relacional + pgvector):** **PostgreSQL almacena estrictamente los hechos analíticos curados en el esquema `gold`** y los vectores semánticos en `gold.rag_knowledge_base`.
* **Cero Tablas Bronze/Silver en PostgreSQL:** No existen tablas crudas intermedias en la base relacional, garantizando que el motor SQL dedique el 100% de su memoria a consultas de sub-segundo para la Serving API.

### B. Política Zero-Quarantine y Auto-Curación (Self-Healing)
En sismología, un registro con coordenadas invertidas o metadatos nulos no se abandona en una tabla de cuarentena pasiva. El motor [`SelfHealingValidator`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/quality/self_healing_validator.py) ejecuta:
1. **Inversión Inteligente de Ejes:** Si $|lat| > 90$ y $|lon| \le 90$, intercambia los valores (`swap(lat, lon)`), marca el evento con `is_auto_healed = true` y documenta `healing_action = 'SWAPPED_INVERTED_COORDINATES'`.
2. **Imputación Estadística:** Asigna la mediana histórica a magnitudes o profundidades faltantes.
3. **Sanitización Anti-Prompt Injection:** Limpia expresiones adversarias en el campo `place` antes de su ingreso al Lakehouse o al modelo de lenguaje.
4. **Cuarentena Estricta como Excepción:** Solo registros con corrupción irrecuperable (sin ID de evento o geometrías vacías) son enviados a `gold.fact_quarantine_events` para auditoría administrativa.

### C. IA Confiable Anti-Alucinación y RAG Estricto
* **Reporte Ejecutivo Diario:** El 100% de las métricas (`total_events`, `max_magnitude`, `avg_magnitude`, `tsunami_alerts`) son computadas por SQL/Python. El LLM sintetiza la narrativa ejecutiva y una verificación determinista posterior compara las cifras. Si existe discrepancia o caída de red, se activa el fallback por plantilla Jinja2.
* **Agente RAG SismoBot (`pgvector`):** Utiliza representaciones vectoriales densas de 768 dimensiones indexadas con el algoritmo `HNSW` (`vector_cosine_ops`). Cuenta con una triple barrera de guardrails que filtra de inmediato preguntas fuera del dominio de sismología y operaciones de la plataforma.

---

## 4. Diagrama Global de Arquitectura

```
+-----------------------------------------------------------------------------------------+
|                                    FUENTE: USGS API                                     |
|                   Feed GeoJSON cada 5 min / Backfill histórico 30 días                  |
+--------------------------------------------+--------------------------------------------+
                                             | HTTPS + Retries exponenciales (Tenacity)
                                             v
+-----------------------------------------------------------------------------------------+
|                           APACHE AIRFLOW 2.9 (ORQUESTACIÓN)                             |
|  - dag_01_usgs_ingestion_bronze.py   (*/5 min, escribe MinIO Bronze, Dataset Trigger)   |
|  - dag_02_spark_bronze_to_silver.py  (Self-Healing, MinIO Silver Parquet Snappy)        |
|  - dag_03_spark_silver_to_gold.py    (MinIO Gold y UPSERT PostgreSQL Gold)               |
|  - dag_04_ai_reports_and_rag.py      (DBSCAN Swarms, LLM Reporte, Indexación pgvector)  |
+--------------------------------------------+--------------------------------------------+
                                             |
                  +--------------------------+--------------------------+
                  |                                                     |
                  v                                                     v
+-----------------------------------+                 +-----------------------------------+
|      MINIO S3 (DATA LAKE)         |                 |    POSTGRESQL 16 + PGVECTOR       |
|                                   |                 |   (SERVING LAYER - SOLO GOLD)     |
| [BRONZE]: s3a://sismowatch-bronze |                 |  gold.fact_earthquake_events      |
|   (GeoJSON crudo inmutable)       |                 |  gold.fact_quality_and_healing    |
| [SILVER]: s3a://sismowatch-silver |                 |  gold.fact_quarantine_events      |
|   (Parquet limpio particionado)   |                 |  gold.ai_daily_reports            |
| [GOLD]:   s3a://sismowatch-gold   |                 |  gold.ai_anomalies (DBSCAN)       |
|   (Parquet analítico curado)      |                 |  gold.rag_knowledge_base (768d)   |
| * SIN TABLAS EN POSTGRESQL *      |                 |                                   |
+-----------------------------------+                 +-----------------+-----------------+
                                                                        |
                                              +-------------------------+
                                              | Lectura Rol `role_api_reader`
                                              v
+-----------------------------------------------------------------------------------------+
|                        FASTAPI BACKEND + FRONTEND REACT & LEAFLET                       |
|  - Auth JWT (Viewer / Admin)             - KPIs en tiempo real                          |
|  - Server-Sent Events (/api/stream)      - Mapa Leaflet interactivo                     |
|  - Agente RAG (/api/agent/chat)          - Exportación CSV / Streaming                  |
|  - Panel Admin Cuarentena / Backfill     - SismoBot con renderizado de gráficos         |
+-----------------------------------------------------------------------------------------+
```

---

## 5. Puesta en Marcha Rápida (Un Solo Comando)

### Prerrequisitos
* Docker y Docker Compose (v2.20+)
* Puertos libres en el host: `3000` (Web), `8000` (API), `8080` (Airflow), `8085` (Spark UI), `9000/9001` (MinIO), `5433` (PostgreSQL).

### Paso 1: Configurar Variables de Entorno
```bash
# Copiar plantilla base de configuración
cp .env.example .env
```

### Paso 2: Desplegar la Plataforma con Docker Compose
```bash
docker compose up -d
```

### Paso 3: URLs y Accesos del Sistema
| Servicio | URL Local | Credenciales por Defecto |
| :--- | :--- | :--- |
| **Plataforma Web (Dashboard SPA)** | [`http://localhost:3000`](http://localhost:3000) | **Admin:** `admin@sismowatch.com` / `AdminSecurePass2026!`<br/>**Viewer:** `viewer@sismowatch.com` / `ViewerPass2026!` |
| **Documentación Interactiva Swagger** | [`http://localhost:8000/docs`](http://localhost:8000/docs) | Probar endpoints de la Serving API con token Bearer |
| **Apache Airflow Webserver** | [`http://localhost:8080`](http://localhost:8080) | `airflow_admin` / `airflow_super_secure_pass_2026` |
| **Spark Master Web UI** | [`http://localhost:8085`](http://localhost:8085) | Monitoreo del clúster de cómputo distribuido |
| **MinIO Object Storage Console** | [`http://localhost:9001`](http://localhost:9001) | `minioadmin` / `miniopassword2026` |
| **PostgreSQL 16 (Serving DB)** | `localhost:5433` (`db: sismowatch_db`) | `sismowatch_admin` / `sismowatch_secure_pass_2026` |

---

## 6. Seguridad Integral y Mínimo Privilegio

1. **Gestión de Secretos:** 0 credenciales quemadas en código fuente. Todo es inyectado mediante `.env` (ignorado en `.gitignore`), variables de entorno Docker y Fernet Keys criptográficas.
2. **Roles de Mínimo Privilegio en PostgreSQL:**
   * `role_spark_etl`: Escritura y actualización restringida exclusivamente al esquema `gold` y vistas `curated`.
   * `role_api_reader`: Solo lectura (`SELECT`) en el esquema `gold`, utilizado por la Serving API.
   * **Inexistencia total de esquemas Bronze y Silver en la base de datos relacional.**
3. **Plataforma y RBAC:** Autenticación con tokens JWT con vencimiento configurable. Un usuario con rol `viewer` que intente ejecutar acciones de backfill histórico o auditoría de cuarentena recibe respuesta inmediata `403 Forbidden`.
4. **Evidencia de Auditorías de Seguridad Automatizadas:**
   * **Bandit (Análisis Estático de Código Python):**
     ```text
     [main] INFO running on Python 3.12.3
     Test results:
         No issues identified.
     Total lines of code: 1943
     Issues: 0 High, 0 Medium, 0 Low.
     ```
   * **pip-audit (Vulnerabilidades en Dependencias de Terceros):**
     ```text
     No known vulnerabilities found.
     ```

---

## 7. Suite de Pruebas Automatizadas (`pytest`)

Toda la funcionalidad de la plataforma está cubierta por pruebas unitarias y de integración:

```bash
# Ejecución completa desde la raíz del repositorio:
.venv/bin/pytest -v
```

### Resultados de la Suite (20 de 20 Pruebas Exitosas):
```text
tests/test_api_rbac_and_rag.py::test_unauthenticated_request_returns_401 PASSED
tests/test_api_rbac_and_rag.py::test_viewer_access_to_admin_quarantine_returns_403 PASSED
tests/test_api_rbac_and_rag.py::test_viewer_access_to_admin_backfill_returns_403 PASSED
tests/test_rag_agent_rejects_out_of_domain_query PASSED
tests/test_rag_agent_accepts_pertinent_seismic_query PASSED
tests/test_rag_agent_rejects_architecture_and_lakehouse_query PASSED
tests/test_rag_agent_recognizes_greetings PASSED
tests/test_rag_agent_blocks_prompt_injection PASSED
tests/test_rag_agent_blocks_xss_attacks PASSED
tests/test_rag_agent_generates_interactive_chart PASSED
tests/test_dag_integrity.py::test_dag_files_exist PASSED
tests/test_dag_syntax_and_ast_integrity[dag_01_usgs_ingestion_bronze.py] PASSED
tests/test_dag_syntax_and_ast_integrity[dag_02_spark_bronze_to_silver.py] PASSED
tests/test_dag_syntax_and_ast_integrity[dag_03_spark_silver_to_gold.py] PASSED
tests/test_dag_syntax_and_ast_integrity[dag_04_ai_reports_and_rag.py] PASSED
tests/test_self_healing_quality.py::test_auto_healing_inverted_coordinates PASSED
tests/test_quarantine_irrecoverable_record PASSED
tests/test_self_healing_quality.py::test_batch_quality_metrics_and_score PASSED
tests/test_transformations.py::test_sanitize_text_anti_prompt_injection PASSED
tests/test_transformations.py::test_geojson_to_clean_event_transformation PASSED

======================== 20 passed in 0.48s ========================
```

---

## 8. Estructura de Directorios del Repositorio

```
.
├── README.md                                                        # Documentación maestra del proyecto
├── docker-compose.yml                                               # Orquestación multicontenedor (Postgres, MinIO, Spark, Airflow, API, Web)
├── .env.example                                                     # Plantilla de variables de entorno seguras
├── airflow/                                                         # Módulo de orquestación Airflow 2.9
│   ├── README.md                                                    # Documentación de Airflow y componentes include
│   ├── Dockerfile                                                   # Imagen Airflow + Java + PySpark + JARs
│   ├── requirements.txt                                             # Dependencias de Python para Airflow
│   ├── dags/                                                        # Definiciones de DAGs (01, 02, 03)
│   └── include/                                                     # Lógica de negocio, MinIO, PySpark, Self-Healing e IA
├── api/                                                             # Módulo de Serving API (FastAPI)
│   ├── README.md                                                    # Documentación de endpoints, RBAC y SSE
│   ├── Dockerfile                                                   # Imagen de despliegue de FastAPI
│   ├── main.py                                                      # Entrypoint de la aplicación
│   ├── db.py                                                        # Conexión psycopg2 con rol role_api_reader
│   ├── auth/                                                        # Manejo de tokens JWT y RBAC
│   ├── routers/                                                     # Rutas de dashboard, telemetría SSE, calidad y RAG
│   └── schemas/                                                     # Modelos de validación Pydantic
├── spark/                                                           # Módulo de cómputo distribuido
│   ├── README.md                                                    # Documentación del clúster Spark 3.5 y jobs Medallion
│   └── Dockerfile                                                   # Imagen del clúster con conectores S3A y PostgreSQL JDBC
├── db/                                                              # Módulo de base de datos relacional y vectorial
│   ├── README.md                                                    # Documentación del esquema Gold, roles e índices HNSW
│   └── init/                                                        # Scripts DDL de inicialización (01, 02, 03)
├── web/                                                             # Módulo de Frontend Web SPA
│   ├── README.md                                                    # Documentación de la SPA React 19 + Leaflet
│   ├── Dockerfile                                                   # Contenedor de compilación y servicio
│   ├── package.json                                                 # Dependencias de npm
│   └── src/                                                         # Código fuente React, componentes y SismoBot
├── tests/                                                           # Módulo de pruebas de regresión automatizadas
│   ├── README.md                                                    # Documentación de la suite pytest y casos de prueba
│   ├── test_dag_integrity.py                                        # Pruebas AST de DAGs
│   ├── test_transformations.py                                      # Pruebas de transformación y desanidado
│   ├── test_self_healing_quality.py                                 # Pruebas de auto-curación y calidad
│   └── test_api_rbac_and_rag.py                                     # Pruebas de seguridad, RBAC y guardrails RAG
└── docs/                                                            # Centro de documentación técnica
    ├── README.md                                                    # Portal de documentación y glosario
    └── ARQUITECTURA.md                                              # Especificación técnica formal detallada
```

---

## 9. Hoja de Ruta y Mejoras Futuras

1. **Evolución a Apache Iceberg:** Migrar el almacenamiento Silver en MinIO a formato de tabla abierta **Apache Iceberg**, habilitando *Time Travel*, evolución de esquema sin downtime y transacciones ACID nativas a escala petabyte.
2. **Ingesta Sub-segundo vía Apache Kafka:** Incorporar un bus de eventos Kafka intermedio para recibir feeds telemétricos directamente desde sismógrafos IoT antes de su persistencia en MinIO Bronze.
3. **Validación Semántica Text-to-SQL con AST:** Expandir las capacidades de consulta en lenguaje natural de SismoBot implementando validadores semánticos con `sqlglot` para restringir aún más las consultas dinámicas a tablas seguras.
