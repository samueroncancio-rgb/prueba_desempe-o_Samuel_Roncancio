# Módulo de Base de Datos y Almacenamiento Vectorial (PostgreSQL 16 + pgvector)

> **SismoWatch Enterprise • Curated Serving Layer & Vector Store**  
> *Ruta: [`db/`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/db)*

---

## 1. Visión General y Regla de Oro Arquitectónica

El módulo **`db/`** gestiona la base de datos relacional de **PostgreSQL 16** con la extensión vectorial **`pgvector`**. En la arquitectura Lakehouse de SismoWatch, esta base de datos funciona estrictamente como la **Serving Layer** (Capa de Consulta Rápida) de la plataforma.

### ⚠️ Regla de Oro Inquebrantable
> **La base de datos PostgreSQL NO aloja tablas de Bronze ni de Silver.**
* Las capas Bronze (GeoJSON crudo) y Silver (Parquet limpio particionado) residen **exclusivamente en el Data Lake MinIO S3**.
* PostgreSQL almacena **únicamente el esquema curado `gold`** (con vistas espejo en `curated` para compatibilidad) y la base de conocimiento vectorial para el Agente RAG.
* Esta decisión previene el sobrecalentamiento, bloat y fragmentación de índices en el motor relacional, asegurando latencias sub-segundo en el dashboard y en las consultas del asistente inteligente.

---

## 2. Estructura del Módulo

```
db/
├── init/
│   ├── 01_extensions_and_schemas.sql    # Habilitación de pgvector, pgcrypto y esquemas gold/curated
│   ├── 02_roles_and_permissions.sql      # Principio de mínimo privilegio (role_spark_etl, role_api_reader)
│   └── 03_tables_and_indexes.sql         # DDL de tablas Gold, índices B-Tree, índice HNSW y vistas
└── README.md                             # Esta documentación
```

Los scripts dentro de `db/init/` son ejecutados automáticamente por el contenedor oficial de PostgreSQL al arrancar por primera vez (`/docker-entrypoint-initdb.d`).

---

## 3. Scripts de Inicialización

### A. Extensiones y Esquemas ([`01_extensions_and_schemas.sql`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/db/init/01_extensions_and_schemas.sql))
1. **`vector`:** Permite el tipo de datos nativo `vector(768)` y operadores de distancia angular y coseno (`<=>`, `<#>`).
2. **`pgcrypto`:** Provee generación criptográfica de identificadores UUID (`gen_random_uuid()`).
3. **Esquema `gold`:** Esquema principal donde residen las tablas físicas curadas.
4. **Esquema `curated`:** Esquema de vistas espejo para compatibilidad con la rúbrica de evaluación.

### B. Mínimo Privilegio y Seguridad de Roles ([`02_roles_and_permissions.sql`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/db/init/02_roles_and_permissions.sql))
Se configuran dos roles técnicos desacoplados:

| Rol | Contraseña por Defecto | Permisos Otorgados | Uso |
| :--- | :--- | :--- | :--- |
| **`role_spark_etl`** | `spark_etl_secure_pass_2026` | `USAGE`, `CREATE`, `SELECT`, `INSERT`, `UPDATE`, `DELETE` en esquemas `gold` y `curated` | Utilizado exclusivamente por el pipeline de Spark y Airflow para realizar UPSERTs y registrar auditorías. |
| **`role_api_reader`** | `api_reader_secure_pass_2026` | `USAGE`, `SELECT` en esquemas `gold` y `curated` | Utilizado por la Serving API (FastAPI). No puede modificar ni eliminar registros bajo ninguna circunstancia. |

### C. Tablas, Índices y Vistas ([`03_tables_and_indexes.sql`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/db/init/03_tables_and_indexes.sql))

#### 1. `gold.fact_earthquake_events`
Tabla principal de hechos sísmicos curados:
* `event_id` (VARCHAR(100), PRIMARY KEY): Identificador oficial de USGS (ej. `us7000test`).
* `mag` (NUMERIC(4,2)): Magnitud normalizada.
* `place_sanitized` (TEXT): Nombre geográfico sanitizado contra Prompt Injection.
* `event_time` (TIMESTAMPTZ): Marca temporal del sismo en UTC.
* `longitude`, `latitude`, `depth_km` (NUMERIC): Coordenadas corregidas en vuelo.
* `tsunami_alert` (BOOLEAN): Indicador de alerta de tsunami.
* `is_auto_healed` (BOOLEAN): Marca si el registro fue remediado por `SelfHealingValidator`.
* `healing_action` (TEXT): Detalle de la corrección realizada (`SWAPPED_INVERTED_COORDINATES`, `IMPUTED_MEDIAN_DEPTH`, etc.).
* **Índices:**
  * `idx_gold_events_time` sobre `event_time DESC` (optimiza feed en vivo y filtros temporales).
  * `idx_gold_events_mag` sobre `mag DESC` (optimiza rankings de sismos mayores).
  * `idx_gold_events_geo` compuesto sobre `(latitude, longitude)` (optimiza queries geoespaciales).

#### 2. `gold.fact_quality_and_healing_runs`
Auditoría y telemetría de cada corrida del pipeline:
* `run_id` (UUID, PRIMARY KEY).
* `evaluated_at` (TIMESTAMPTZ): Fecha y hora de la evaluación.
* `total_records`, `passed_records`, `auto_healed_records`, `quarantined_records`.
* `quality_score` (NUMERIC(5,2)): Porcentaje de calidad del lote ($0.00$ a $100.00\%$).
* `freshness_seconds` (INTEGER): Latencia del lote contra el tiempo real.
* `rules_evaluated` (JSONB): Detalle de reglas evaluadas.

#### 3. `gold.fact_quarantine_events`
Almacenamiento de auditoría de registros irrecuperables (por ejemplo, registros sin identificador o coordenadas irrecuperables):
* `quarantine_id` (UUID, PRIMARY KEY).
* `raw_payload` (JSONB): Contenido crudo recibido.
* `rejection_reason` (TEXT): Motivo del descarte.
* `quarantined_at` (TIMESTAMPTZ).

#### 4. `gold.ai_daily_reports`
Reportes ejecutivos diarios generados por IA y validados deterministamente:
* `report_id` (UUID, PRIMARY KEY).
* `report_date` (DATE, UNIQUE).
* `title`, `summary_md`, `full_report_md` (TEXT).
* `total_events`, `max_magnitude`, `avg_magnitude`, `tsunami_alerts` (Métricas exactas verificadas).
* `is_validated_against_hallucination` (BOOLEAN).

#### 5. `gold.ai_anomalies`
Registro de enjambres sísmicos y picos anómalos detectados por `DBSCAN`:
* `anomaly_id` (UUID, PRIMARY KEY).
* `detected_at` (TIMESTAMPTZ), `anomaly_type` (VARCHAR(50)).
* `affected_area` (TEXT), `cluster_event_count` (INTEGER), `max_cluster_mag` (NUMERIC(4,2)).
* `metadata_json` (JSONB).

#### 6. `gold.rag_knowledge_base` (Vector Store)
Base de conocimiento vectorial para el Asistente Inteligente SismoBot:
* `doc_id` (UUID, PRIMARY KEY).
* `content` (TEXT): Fragmento textual descriptivo del evento o reporte.
* `metadata_json` (JSONB): Metadatos del evento (IDs, magnitud, fecha).
* `embedding` (`vector(768)`): Vector semántico denso de 768 dimensiones normalizado a norma $L_2$.
* **Índice HNSW:**
  ```sql
  CREATE INDEX idx_gold_rag_hnsw ON gold.rag_knowledge_base 
  USING hnsw (embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 64);
  ```

---

## 4. Conexión y Operaciones Comunes

### Parámetros de Conexión
* **Host:** `localhost` (externo) o `postgres` (dentro de Docker).
* **Puerto Host:** `5433` (mapeado para evitar colisiones con instancias locales preinstaladas) o `5432` en red interna.
* **Base de Datos:** `sismowatch_db`.
* **Usuario Administrador:** `sismowatch_admin` / `sismowatch_secure_pass_2026`.

### Comandos de Utilidad

```bash
# Conectarse interactivamente vía psql con el usuario administrador
docker exec -it sismowatch_postgres psql -U sismowatch_admin -d sismowatch_db

# Verificar que la extensión vector esté instalada
SELECT extname, extversion FROM pg_extension WHERE extname = 'vector';

# Verificar el estado del índice vectorial HNSW
SELECT indexrelid::regclass, indisvalid FROM pg_index WHERE indexrelid = 'gold.idx_gold_rag_hnsw'::regclass;

# Consultar los últimos sismos remediados automáticamente
SELECT event_id, mag, place_sanitized, is_auto_healed, healing_action 
FROM gold.fact_earthquake_events 
WHERE is_auto_healed = TRUE 
ORDER BY event_time DESC LIMIT 5;

# Comprobar que NO existen tablas en Bronze ni Silver
SELECT table_schema, table_name FROM information_schema.tables 
WHERE table_schema IN ('bronze', 'silver');
-- (Debe retornar 0 filas)
```
