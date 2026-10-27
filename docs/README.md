# Centro de Documentación Técnica (SismoWatch Enterprise)

> **SismoWatch Enterprise • Technical Documentation & Architectural Hub**  
> *Ruta: [`docs/`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/docs)*

---

## 1. Visión General del Hub de Documentación

Bienvenido al centro de documentación técnica y arquitectónica de **SismoWatch Enterprise**. Este espacio consolida las especificaciones de diseño, diagramas de flujo, decisiones de ingeniería y guías operativas que fundamentan la plataforma de monitoreo sísmico global para **SuperApp**.

---

## 2. Índice de Documentos Arquitectónicos y Guías

| Documento | Ubicación | Descripción y Alcance |
| :--- | :--- | :--- |
| **Especificación de Arquitectura Formal** | [`docs/ARQUITECTURA.md`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/docs/ARQUITECTURA.md) | Documento principal de arquitectura. Incluye diagramas Mermaid de extremo a extremo, justificación de la arquitectura Medallion Lakehouse, el modelo de datos en PostgreSQL Gold, algoritmos de detección de anomalías y la estrategia de RAG sobre `pgvector`. |
| **Diseño Lakehouse y Gobernanza** | [`arquitectura_sismowatch_medallion_spark_minio_openmetadata_rag.md`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/arquitectura_sismowatch_medallion_spark_minio_openmetadata_rag.md) | Documento técnico de diseño enfocado en la interacción entre MinIO S3, Apache Spark 3.5, políticas Zero-Quarantine y gobernanza de metadatos. |

---

## 3. Navegación hacia los READMEs de Módulos

Cada subsistema de la plataforma cuenta con su propio `README.md` detallado con instrucciones de despliegue, configuración de variables, modelos y operaciones:

```
[Raíz del Proyecto] ───────────────────────────> README.md
       │
       ├── Orquestación de Pipelines ──────────> airflow/README.md
       ├── Serving Layer & Inferencia API ────> api/README.md
       ├── Cómputo Distribuido Lakehouse ────> spark/README.md
       ├── Base de Datos & Vector Store ───────> db/README.md
       ├── Visualizador & Consumo Web ─────────> web/README.md
       ├── Suite de Pruebas Automatizadas ─────> tests/README.md
       └── Centro de Documentación Técnica ───> docs/README.md
```

* **[Orquestación (Apache Airflow 2.9)](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/README.md):** DAGs de ingesta, disparadores por Dataset, llamadas a Spark y componentes en `airflow/include/`.
* **[Serving API (FastAPI)](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/api/README.md):** Catálogo de endpoints REST, Server-Sent Events (SSE), control RBAC y agente RAG.
* **[Cómputo Distribuido (Apache Spark 3.5)](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/spark/README.md):** Clúster Master/Worker, conectores S3A y JDBC, particionamiento en Parquet y UPSERT idempotente.
* **[Base de Datos (PostgreSQL 16 + pgvector)](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/db/README.md):** Esquemas `gold`/`curated`, roles de mínimo privilegio, índice HNSW de 768d y regla de oro de almacenamiento.
* **[Frontend Web (React 19 + Leaflet)](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/web/README.md):** Visualizador de mapas interactivo, KPIs en tiempo real, catálogo de sismos y chatbot con gráficos.
* **[Aseguramiento de Calidad (pytest)](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/tests/README.md):** Desglose de los 19 casos de prueba, cobertura de auto-curación y auditorías de seguridad (Bandit / pip-audit).

---

## 4. Decisiones de Arquitectura Clave (ADRs Resumidos)

### ADR-01: Almacenamiento Lakehouse vs Base de Datos Relacional (La Regla de Oro)
* **Contexto:** Almacenar payloads GeoJSON crudos y datos intermedios en una base de datos relacional genera saturación de disco, bloat en tablas y degradación severa en tiempos de consulta.
* **Decisión:** La base de datos PostgreSQL **no contiene tablas de Bronze ni de Silver**. El Data Lake MinIO aloja Bronze (GeoJSON) y Silver (Parquet). PostgreSQL funge exclusivamente como Serving Layer conteniendo las tablas analíticas de la capa **GOLD**.
* **Impacto:** Consultas en tiempo real por debajo de 50 milisegundos en la Serving API y retención histórica de bajo costo en almacenamiento de objetos.

### ADR-02: Zero-Quarantine y Auto-Curación Determinista (Self-Healing)
* **Contexto:** En sismología y gestión de desastres, descartar un evento sísmico por un error de formato común (ej. coordenadas invertidas por un sensor) puede comprometer la vida de las personas.
* **Decisión:** Implementar el motor determinista `SelfHealingValidator`. Los registros con coordenadas invertidas se corrigen al vuelo (`swap(lat, lon)`), se imputan estadísticos faltantes y se audita la reparación con `is_auto_healed = true`. Solo fallos estructurales irrecuperables van a la tabla de auditoría `gold.fact_quarantine_events`.

### ADR-03: Streaming Server-Sent Events (SSE) en lugar de WebSockets
* **Contexto:** La telemetría sísmica es eminentemente unidireccional (del pipeline al usuario final). Los WebSockets imponen sobrecarga de memoria, manejo de conexiones bidireccionales y complejidad con proxies.
* **Decisión:** Emplear Server-Sent Events (`/api/stream`) sobre HTTP estándar, aprovechando la reconexión automática nativa del navegador (`EventSource`) y menor consumo de recursos.

### ADR-04: IA Confiable Anti-Alucinación y RAG Semántico Estricto
* **Contexto:** Los modelos LLM son propensos a alucinar cifras numéricas críticas (magnitudes, conteos de víctimas o alertas).
* **Decisión:** El 100% de las cifras numéricas son calculadas por SQL y Python con validación determinista post-LLM. El agente conversacional utiliza búsqueda por similitud coseno en `pgvector` y está protegido por una triple barrera de guardrails que descarta de forma terminante preguntas ajenas al dominio sísmico.

---

## 5. Glosario de Términos

* **Medallion Architecture:** Patrón de diseño de datos que organiza la información en capas progresivas de calidad: Bronze (crudo e inmutable), Silver (limpio y enriquecido) y Gold (agregado y listo para el negocio).
* **Self-Healing Data Pipeline:** Tubería de datos con capacidades de auto-recuperación que identifica y remedia inconsistencias en vuelo sin detener el procesamiento ni perder eventos.
* **pgvector:** Extensión de código abierto para PostgreSQL que permite almacenar vectores densos y realizar búsquedas de vecinos más cercanos (ANN) mediante algoritmos como HNSW o IVFFlat.
* **HNSW (Hierarchical Navigable Small World):** Grafo multicapa de alta velocidad para indexación y búsqueda por similitud vectorial aproximada con complejidad logarítmica.
* **DBSCAN (Density-Based Spatial Clustering of Applications with Noise):** Algoritmo de agrupamiento basado en densidad que permite identificar enjambres sísmicos espaciales sin asumir de antemano el número de clústeres.
* **Haversine Distance:** Fórmula trigonométrica que calcula la distancia de círculo máximo entre dos puntos en una esfera a partir de sus longitudes y latitudes.
