# Módulo de Serving API e Inferencia (FastAPI)

> **SismoWatch Enterprise • High-Performance Serving Layer & AI Inference API**  
> *Ruta: [`api/`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/api)*

---

## 1. Visión General y Arquitectura

El módulo **API** es el corazón de la capa de consumo de **SismoWatch Enterprise**. Construido con **FastAPI** y **Python 3.12**, proporciona acceso de alto rendimiento, seguro y en tiempo real a las capas curadas de datos sísmicos (`gold.*`) y al motor de inferencia de Inteligencia Artificial.

### Principios Fundamentales
* **Cumplimiento Estricto de la Regla de Oro:** La API **NO** lee datos de Bronze ni de Silver; se conecta directamente al esquema curado `gold` de PostgreSQL y a la base vectorial `pgvector`.
* **Mínimo Privilegio de Base de Datos:** Por diseño de seguridad, la API se conecta utilizando el rol PostgreSQL [`role_api_reader`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/db/init/02_roles_and_permissions.sql#L20), restringido exclusivamente a permisos `SELECT` sobre tablas curadas, impidiendo cualquier modificación o borrado accidental.
* **Control de Acceso Basado en Roles (RBAC):** Autenticación mediante tokens JWT firmados criptográficamente. Se diferencian estrictamente los roles `viewer` y `admin`.
* **Streaming en Tiempo Real (SSE):** Implementa Server-Sent Events nativos sobre HTTP para alimentar la UI sin el overhead de WebSockets ni la latencia de polling continuo.
* **Agente RAG con Triple Barrera de Guardrails:** El chatbot sísmico integra verificación semántica de pertinencia de dominio, sanitización estricta anti-Prompt Injection y anti-XSS, y búsqueda vectorial por similitud coseno sobre 768 dimensiones.

---

## 2. Estructura del Módulo

```
api/
├── auth/
│   ├── __init__.py
│   └── jwt_handler.py           # Creación y validación de JWT, dependencias RBAC (require_viewer, require_admin)
├── routers/
│   ├── __init__.py
│   ├── auth.py                  # Autenticación: /auth/login
│   ├── dashboard.py             # KPIs, listado de sismos paginado, series temporales, exportación CSV
│   ├── realtime_sse.py          # Telemetría en tiempo real: /api/stream (Server-Sent Events)
│   ├── quality.py               # Monitoreo de pipeline: /api/pipeline/status y /api/quality/quarantine
│   ├── admin_ops.py             # Operaciones privilegiadas: backfill 30 días y regeneración de reportes
│   └── ai_agent_rag.py          # Chatbot RAG SismoBot con pgvector, guardrails y generación de gráficos
├── schemas/
│   ├── __init__.py
│   └── pydantic_models.py       # Modelos de validación de entrada/salida Pydantic v2
├── db.py                        # Context manager y connection pool con psycopg2 y RealDictCursor
├── Dockerfile                   # Contenedor optimizado Python 3.11-slim con Uvicorn
├── requirements.txt             # Dependencias de producción (FastAPI, PyJWT, Pydantic, etc.)
└── main.py                      # Punto de entrada de la aplicación, configuración CORS y middleware
```

---

## 3. Catálogo de Endpoints y Routers

### A. Autenticación (`/auth`)
* **`POST /auth/login`**
  * **Acceso:** Público.
  * **Payload:** `{"username": "...", "password": "..."}`
  * **Respuesta:** `{"access_token": "...", "token_type": "bearer", "role": "admin|viewer", "username": "..."}`
  * **Credenciales por defecto:**
    * Administrador: `admin@sismowatch.com` / `AdminSecurePass2026!`
    * Observador: `viewer@sismowatch.com` / `ViewerPass2026!`

### B. Dashboard y Analítica (`/api`)
* **`GET /api/kpis`**
  * **Acceso:** Reversible con rol `viewer` o `admin`.
  * **Descripción:** Calcula métricas agregadas de las últimas 24 horas: total de sismos, magnitud máxima con localidad, magnitud promedio, alertas de tsunami activas y score de calidad del pipeline.
* **`GET /api/events`**
  * **Acceso:** `viewer` / `admin`.
  * **Parámetros:** `page`, `limit`, `min_mag`, `search`, `days`.
  * **Descripción:** Listado paginado de eventos sísmicos limpios ordenados cronológicamente, indicando el estado de auto-curación (`is_auto_healed`) y la acción ejecutada (`healing_action`).
* **`GET /api/events/timeseries`**
  * **Acceso:** `viewer` / `admin`.
  * **Descripción:** Histograma agrupado por franjas horarias con recuento de eventos, magnitud media y magnitud máxima para gráficos temporales.
* **`GET /api/events/export/csv`**
  * **Acceso:** `viewer` / `admin`.
  * **Descripción:** Descarga en streaming de los datos curados en formato CSV para auditoría externa o analítica en hojas de cálculo.
* **`GET /api/reports/daily`**
  * **Acceso:** `viewer` / `admin`.
  * **Descripción:** Devuelve los reportes ejecutivos diarios gobernados con métricas analíticas validadas.

### C. Telemetría en Tiempo Real (`/api`)
* **`GET /api/stream`**
  * **Acceso:** `viewer` / `admin` (mediante token Bearer o query param).
  * **Protocolo:** **Server-Sent Events (SSE)** (`text/event-stream`).
  * **Eventos transmitidos:**
    * `event: connect` — Confirmación de enlace y metadatos de sesión.
    * `event: seismic_pulse` — Notificación inmediata ante la llegada de nuevos sismos curados en Gold.
    * `event: heartbeat` — Señal de vida periódica cada 15 segundos para mantener activo el túnel HTTP.

### D. Calidad y Auditoría del Pipeline (`/api`)
* **`GET /api/pipeline/status`**
  * **Acceso:** `viewer` / `admin`.
  * **Descripción:** Devuelve el historial de las últimas 5 corridas de calidad, incluyendo registros evaluados, registros curados en caliente, puntuación de calidad (0 a 100%) y tiempo de frescura.
* **`GET /api/quality/quarantine`**
  * **Acceso:** **Exclusivo `admin`** (retorna `403 Forbidden` a usuarios `viewer`).
  * **Descripción:** Permite a los administradores auditar los registros irrecuperables en `gold.fact_quarantine_events`, mostrando el motivo de rechazo original.

### E. Operaciones de Administración (`/api/admin`)
* **`POST /api/admin/backfill`**
  * **Acceso:** **Exclusivo `admin`**.
  * **Payload:** `{"days": 30, "min_magnitude": 4.0}`
  * **Descripción:** Solicita a la API de USGS los datos históricos de hasta 30 días atrás y dispara el pipeline completo hacia Bronze, Silver y Gold.
* **`POST /api/admin/reports/regenerate`**
  * **Acceso:** **Exclusivo `admin`**.
  * **Descripción:** Fuerza la re-evaluación analítica y re-generación del reporte ejecutivo del día.

### F. Agente Conversacional RAG (`/api/agent`)
* **`POST /api/agent/chat`**
  * **Acceso:** `viewer` / `admin`.
  * **Payload:** `{"message": "¿Cuáles han sido los sismos más fuertes de hoy?", "history": []}`
  * **Respuesta:**
    ```json
    {
      "reply": "Respuesta contextualizada con citas de eventos...",
      "sources": ["us7000abcd", "us7000efgh"],
      "chart": {
        "type": "bar",
        "labels": ["Sismo A", "Sismo B"],
        "datasets": [{ "label": "Magnitud", "data": [6.2, 5.8] }]
      }
    }
    ```
  * **Mecanismos de Protección:**
    1. **Filtro de Dominio Estricto:** Si la pregunta no se refiere a sismología, tsunamis, calidad de datos o la plataforma SismoWatch, se devuelve una respuesta fija sin consumir LLM.
    2. **Defensa Anti-Prompt Injection:** Detecta y bloquea comandos maliciosos (`"ignore previous instructions"`, `"system prompt"`, `"reveal api key"`).
    3. **Defensa Anti-XSS:** Sanitiza etiquetas HTML (`<script>`, `<iframe>`, `javascript:`) en las entradas y salidas.
    4. **Síntesis de Gráficos:** Genera estructuras JSON interpretables automáticamente por `Chart.js` en el frontend.

---

## 4. Matriz de Permisos RBAC

| Endpoint | Rol `viewer` | Rol `admin` | Sin Token |
| :--- | :---: | :---: | :---: |
| `POST /auth/login` | Permitido | Permitido | Permitido |
| `GET /api/kpis` | 200 OK | 200 OK | 401 Unauthorized |
| `GET /api/events` | 200 OK | 200 OK | 401 Unauthorized |
| `GET /api/stream` (SSE) | 200 OK | 200 OK | 401 Unauthorized |
| `POST /api/agent/chat` | 200 OK | 200 OK | 401 Unauthorized |
| `GET /api/pipeline/status` | 200 OK | 200 OK | 401 Unauthorized |
| `GET /api/quality/quarantine` | **403 Forbidden** | **200 OK** | 401 Unauthorized |
| `POST /api/admin/backfill` | **403 Forbidden** | **200 OK** | 401 Unauthorized |
| `POST /api/admin/reports/regenerate` | **403 Forbidden** | **200 OK** | 401 Unauthorized |

---

## 5. Ejecución Local y Desarrollo

### Prerrequisitos
Tener el entorno virtual configurado y variables de entorno disponibles:

```bash
# Activar entorno virtual
source .venv/bin/activate

# Iniciar servidor FastAPI con recarga en caliente
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

### Exploración Interactiva de la Documentación
* **Swagger UI:** [`http://localhost:8000/docs`](http://localhost:8000/docs)
* **ReDoc:** [`http://localhost:8000/redoc`](http://localhost:8000/redoc)

### Pruebas Unitarias del Módulo API
```bash
# Ejecutar pruebas específicas de RBAC, autenticación y Agente RAG
.venv/bin/pytest tests/test_api_rbac_and_rag.py -v
```
