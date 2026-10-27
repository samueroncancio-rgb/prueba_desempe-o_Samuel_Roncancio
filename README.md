# SismoWatch Enterprise: Plataforma de Inteligencia y Monitoreo Sísmico Global

> **SuperApp • Vertical de Monitoreo Sísmico y Gestión de Riesgo en Tiempo Casi Real**  
> *Autor: Samuel Roncancio — Científico de Datos Principal / Senior AI Engineer*

---

## 1. Propósito de la Arquitectura y Visión General

**SismoWatch** convierte el feed público y volátil del Servicio Geológico de los Estados Unidos (USGS) en un producto de datos empresarial seguro, continuo, con calidad auditada y enriquecido con Inteligencia Artificial.

### Decisiones Críticas Habilitadas
* **Disponibilidad Inmutable:** Resuelve la volatilidad del feed USGS persistiendo el 100% de los datos crudos en formato inmutable en el Data Lake (MinIO).
* **Restricción Arquitectónica de Base de Datos (Regla de Oro):** Para maximizar la velocidad analítica y evitar la sobrecarga y bloat de tablas en PostgreSQL, **la base de datos relacional NO contiene tablas de Bronze ni de Silver**. El Data Lake MinIO aloja las capas Bronze (GeoJSON) y Silver (Parquet), mientras que PostgreSQL actúa estrictamente como **Serving Layer conteniendo únicamente las tablas de hechos curados de la capa GOLD (`gold.fact_earthquake_events`) y la base vectorial `pgvector`**.
* **Zero-Quarantine & Self-Healing:** En sismología crítica, un sismo con coordenadas invertidas o texto malicioso no se envía a morir a una cuarentena pasiva: el motor `SelfHealingValidator` lo auto-remedia en vuelo (`swap(lat, lon)`), imputa valores nulos y audita la acción en `is_auto_healed = TRUE`.
* **IA Confiable y RAG Estricto:** Detección de anomalías espaciales (`DBSCAN`), reportes ejecutivos gobernados donde el 100% de las cifras numéricas son calculadas por SQL/Python con validación determinista anti-alucinaciones, y un Agente conversacional RAG sobre `pgvector` protegido por guardrails semánticos.

---

## 2. Diagrama de Arquitectura y Capas Medallion

```
+-----------------------------------------------------------------------------------------+
|                                    FUENTE: USGS API                                     |
|                   Feed GeoJSON cada 5 min / Backfill histórico 30 días                  |
+--------------------------------------------+--------------------------------------------+
                                             | HTTPS + Retries exponenciales (Tenacity)
                                             v
+-----------------------------------------------------------------------------------------+
|                           APACHE AIRFLOW 2.9 (ORQUESTACIÓN)                             |
|  - dag_01_usgs_ingestion_bronze.py (*/5 min, escribe en MinIO Bronze, Dataset Trigger)  |
|  - dag_02_spark_silver_gold.py     (Self-Healing, MinIO Silver Parquet, Postgres Gold)  |
|  - dag_03_ai_reports_and_rag.py    (DBSCAN Swarms, LLM Reporte, Indexación pgvector)    |
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
|                                   |                 |  gold.ai_anomalies (DBSCAN)       |
| * SIN TABLAS EN POSTGRESQL *      |                 |  gold.rag_knowledge_base (768d)   |
+-----------------------------------+                 +-----------------+-----------------+
                                                                        |
                                              +-------------------------+
                                              | Lectura Rol `role_api_reader`
                                              v
+-----------------------------------------------------------------------------------------+
|                        FASTAPI BACKEND + FRONTEND REACT & LEAFLET                       |
|  - Auth JWT (Viewer / Admin)             - KPIs en tiempo real                          |
|  - Server-Sent Events (/api/stream)      - Mapa Leaflet interactivo                     |
|  - Agente RAG (/api/agent/chat)          - Exportación CSV / PDF                        |
|  - Panel Admin Cuarentena / Backfill     - Estados de carga, error y vacío              |
+-----------------------------------------------------------------------------------------+
```

---

## 3. Puesta en Marcha Rápida (Un Solo Comando)

### Prerrequisitos
* Docker y Docker Compose (v2.20+)
* Puerto 8080 (Airflow), 8000 (API), 3000 (Web), 5432 (Postgres), 9000/9001 (MinIO) disponibles.

### 1. Clonar el repositorio y configurar variables de entorno
```bash
git clone <tu-repo-url>
cd prueba_desempeño_Samuel_Roncancio

# Copiar el archivo de plantilla a .env
cp .env.example .env
```

### 2. Levantar toda la plataforma
```bash
docker compose up -d
```

### 3. URLs de acceso
| Servicio | URL Local | Credenciales por Defecto |
| :--- | :--- | :--- |
| **Plataforma Web (Dashboard)** | `http://localhost:3000` | **Admin:** `admin@sismowatch.com` / `AdminSecurePass2026!`<br/>**Viewer:** `viewer@sismowatch.com` / `ViewerPass2026!` |
| **Documentación API (FastAPI)** | `http://localhost:8000/docs` | Probar endpoints protegidos con token Bearer |
| **Apache Airflow** | `http://localhost:8080` | `airflow_admin` / `airflow_super_secure_pass_2026` |
| **MinIO Console (Data Lake)** | `http://localhost:9001` | `minioadmin` / `miniopassword2026` |

---

## 4. Calidad de Datos y Política "Cero Cuarentena"

### Herramienta Elegida y Justificación
Se implementó un motor personalizado de alta velocidad (`SelfHealingValidator`), combinable con Pandera y Great Expectations, enfocado en **Auto-Remediación Determinista en Vuelo**:
1. **Inversión de Coordenadas (`swap(lat, lon)`):** Si $|lat| > 90$ y $|lon| \le 90$, el motor intercambia los ejes y marca `is_auto_healed = TRUE`, evitando que sismos válidos se pierdan en una cuarentena pasiva.
2. **Imputación de Valores Faltantes:** Si la profundidad o magnitud viene nula, se calcula e imputa el valor mediano de la estación marcando la acción de auto-curación.
3. **Sanitización Anti-Prompt Injection:** Limpieza estricta con regex sobre `properties.place` para eliminar cualquier instrucción adversaria antes de ingresar al Lakehouse o al LLM.
4. **Controles de Frescura y Volumen:** Se mide en cada micro-batch el desfase temporal contra el tiempo real y el volumen esperado de sismos.
5. **Auditoría de Calidad:** Cada corrida registra su score en `gold.fact_quality_and_healing_runs`, visible en el dashboard. Solo los registros con corrupción irrecuperable (sin ID o coordenadas imposibles) pasan a `gold.fact_quarantine_events` para auditoría administrativa.

---

## 5. Seguridad Integral y Mínimo Privilegio

1. **Gestión de Secretos:** 0 credenciales en código duro. Todo se gestiona mediante `.env` (ignorado en `.gitignore`), variables de entorno en Docker y Fernet Keys personalizadas en Airflow.
2. **Mínimo Privilegio en PostgreSQL:**
   * `role_spark_etl`: Escritura exclusiva en `gold`.
   * `role_api_reader`: Solo lectura (`SELECT`) en `gold`.
   * **No existen esquemas ni tablas Bronze/Silver en la base de datos.**
3. **Plataforma y RBAC:** Tokens JWT con expiración. Un usuario con rol `viewer` que invoque endpoints de backfill o cuarentena recibe `403 Forbidden`.
4. **Evidencia de Escaneo de Seguridad Automatizado:**
   * **Bandit (Análisis Estático de Código Python):**
     ```text
     [main] INFO running on Python 3.12.3
     Run started: 2026-10-27 12:18:51
     Test results:
         No issues identified.
     Total lines of code: 1943
     Issues: 0 High, 0 Medium, 0 Low.
     ```
   * **pip-audit (Vulnerabilidades en Dependencias):**
     ```text
     No known vulnerabilities found.
     ```

---

## 6. Automatización con Inteligencia Artificial

### 1. Detección de Anomalías Sísmicas (`DBSCAN` + Energía)
* **Enjambres Sísmicos:** Aplica el algoritmo `DBSCAN` con distancia `haversine` ($\epsilon = 100\text{ km}$, $\text{min\_samples} = 3$) sobre las coordenadas de los sismos en las últimas 72 horas para detectar clústeres espaciales densos.
* **Picos Energéticos:** Detecta eventos con magnitudes inusuales ($\ge 5.5\text{ M}$) y genera una narrativa de impacto sísmico en `gold.ai_anomalies`.

### 2. Generación de Reporte Diario Ejecutivo Anti-Alucinación
* **Regla Inquebrantable:** El 100% de las métricas (`total_events`, `max_magnitude`, `avg_magnitude`, `tsunami_alerts`) son calculadas por SQL y Python.
* **Validación Post-LLM:** Se verifica que las cifras en el texto generado coincidan con las calculadas. Si el LLM altera un número o si la API falla, entra en acción el **Plan Alterno Determinista (Jinja2)**.

### 3. Agente Conversacional RAG (`pgvector`)
* Embeddings de 768 dimensiones almacenados en `gold.rag_knowledge_base` con índice `HNSW` (`vector_cosine_ops`).
* **Triple Barrera de Guardrails:** Bloqueo semántico inmediato de preguntas fuera del dominio de SismoWatch (recetas, deportes, código ajeno) devolviendo la respuesta protocolar establecida.
* Citación obligatoria de IDs de eventos y fechas de reportes.

---

## 7. Plataforma de Consumo y Tiempo Real (SSE)

* **¿Por qué Server-Sent Events (SSE) y no WebSockets o Polling largo?**  
  La telemetría sísmica es unidireccional (del pipeline al cliente). SSE opera nativamente sobre HTTP estándar, consume menos memoria que una conexión WebSocket bidireccional y ofrece reconexión automática en el navegador (`EventSource`) con latencia sub-segundo cuando Spark finaliza la carga en Gold.
* **Diseño UI/UX:** Construido en React + TypeScript + Tailwind CSS con mapa Leaflet interactivo, badge pulsante **"EN VIVO"**, filtros dinámicos, visor de reportes con descarga CSV y drawer interactivo de chat con **SismoBot**.
* **Manejo de Estados:** Skeletons de carga, estados vacíos estilizados y manejo de errores sin pantallas en blanco.

---

## 8. Suite de Pruebas Automatizadas (`pytest`)

Ejecución de la suite completa con un solo comando:
```bash
# Desde la raíz del proyecto con el entorno virtual activado:
pytest -v
```

### Resultados de la Suite (14 de 14 Pruebas Exitosas):
* `tests/test_transformations.py`:
  * Aplanado de GeoJSON y extracción de coordenadas.
  * Sanitización contra Prompt Injection en el campo `place`.
* `tests/test_dag_integrity.py`:
  * Integridad de sintaxis y estructura AST de los 3 DAGs de Airflow sin ciclos ni errores de importación.
* `tests/test_self_healing_quality.py`:
  * Auto-curación de coordenadas invertidas (`SWAPPED_INVERTED_COORDINATES`).
  * Enrutamiento de registros irrecuperables a cuarentena.
  * Cálculo exacto del score de calidad en lote.
* `tests/test_api_rbac_and_rag.py`:
  * Verificación de `401 Unauthorized` ante peticiones sin token.
  * Verificación de `403 Forbidden` cuando un `viewer` intenta llamar a `/api/quality/quarantine` o `/api/admin/backfill`.
  * Verificación del Guardrail de dominio del Agente RAG (rechazo de preguntas ajenas y aceptación de consultas sísmicas).

---

## 9. Limitaciones Conocidas y Mejoras a Futuro

1. **Escala Cloud con Apache Iceberg:** En un entorno enterprise a gran escala, migraríamos MinIO a Amazon S3 con formato Apache Iceberg para gestión de metadatos ACID y Time Travel nativo.
2. **Ingesta Sub-segundo con Apache Kafka:** Integrar un bus de streaming Kafka para recibir feeds telemétricos directamente desde sensores sísmicos IoT antes de MinIO Bronze.
3. **Text-to-SQL Dinámico Multi-Tabla:** Expandir la herramienta de Text-to-SQL del Agente con validación semántica AST avanzada mediante `sqlglot` para consultas analíticas complejas en lenguaje natural.
