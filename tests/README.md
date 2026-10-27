# Suite de Pruebas Automatizadas y Aseguramiento de Calidad (pytest)

> **SismoWatch Enterprise • Quality Assurance & Automated Test Suite**  
> *Ruta: [`tests/`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/tests)*

---

## 1. Visión General y Filosofía de Calidad

La confiabilidad operacional y la seguridad de datos en **SismoWatch Enterprise** están garantizadas mediante una suite integral de pruebas automatizadas ejecutadas con **`pytest`**. 

El enfoque de pruebas abarca cuatro pilares críticos:
1. **Integridad de Orquestación:** Verificación estática y de sintaxis AST para garantizar que ningún DAG contenga ciclos, errores de importación o dependencias rotas que puedan degradar el Scheduler de Airflow.
2. **Transformaciones y Sanitización:** Validación rigurosa del aplanado de GeoJSON y comprobación exhaustiva del motor de sanitización contra ataques de **Prompt Injection** y **Cross-Site Scripting (XSS)**.
3. **Resiliencia y Auto-Curación (Zero-Quarantine):** Verificación de que el algoritmo de intercambio de coordenadas invertidas (`swap(lat, lon)`) rescate sismos reales en vuelo y derive a cuarentena únicamente datos irrecuperables.
4. **Seguridad RBAC y Guardrails del Agente RAG:** Comprobación estricta de códigos HTTP `401 Unauthorized` y `403 Forbidden`, y auditoría de la triple barrera de guardrails que restringe las respuestas del LLM al dominio exclusivo de sismología.

---

## 2. Estructura de la Suite de Pruebas

```
tests/
├── __init__.py
├── test_dag_integrity.py              # Pruebas de sintaxis y estructura AST de los DAGs de Airflow
├── test_transformations.py            # Pruebas de transformación GeoJSON y sanitización de texto
├── test_self_healing_quality.py       # Pruebas del motor Self-Healing, cálculo de métricas y cuarentena
├── test_api_rbac_and_rag.py           # Pruebas de RBAC JWT, protección de endpoints y guardrails del Agente RAG
└── README.md                          # Esta documentación
```

---

## 3. Desglose de los 20 Casos de Prueba

Actualmente la suite cuenta con **20 pruebas automatizadas** que se ejecutan con **100% de éxito**:

### A. Integridad de DAGs (`test_dag_integrity.py`)
| Caso de Prueba | Descripción |
| :--- | :--- |
| `test_dag_files_exist` | Valida que existan los DAGs principales en `airflow/dags`. |
| `test_dag_syntax_and_ast_integrity[dag_01]` | Parsea el Abstract Syntax Tree (AST) de Python para certificar ausencia de errores de sintaxis en la ingesta Bronze. |
| `test_dag_syntax_and_ast_integrity[dag_02]` | Parsea el AST del procesamiento Spark Bronze -> Silver Parquet verificando decoradores y dependencias. |
| `test_dag_syntax_and_ast_integrity[dag_03]` | Parsea el AST del procesamiento Spark Silver -> Gold Serving y UPSERT idempotente. |
| `test_dag_syntax_and_ast_integrity[dag_04]` | Parsea el AST del pipeline de IA, detección de anomalías y RAG. |

### B. Transformaciones y Sanitización (`test_transformations.py`)
| Caso de Prueba | Descripción |
| :--- | :--- |
| `test_sanitize_text_anti_prompt_injection` | Inyecta textos maliciosos con patrones adversarios (`"IGNORE PREVIOUS INSTRUCTIONS"`, `"DROP TABLE"`, `<script>`) y verifica su neutralización inmediata preservando el texto geográfico legítimo. |
| `test_geojson_to_clean_event_transformation` | Valida el desanidado completo de un feature GeoJSON oficial de USGS, normalizando timestamps en milisegundos y extrayendo longitud, latitud y profundidad. |

### C. Auto-Curación y Calidad de Datos (`test_self_healing_quality.py`)
| Caso de Prueba | Descripción |
| :--- | :--- |
| `test_auto_healing_inverted_coordinates` | Simula un sismo con latitud y longitud intercambiadas ($|lat| > 90$, $|lon| \le 90$). Verifica que `SelfHealingValidator` aplique `swap(lat, lon)`, asigne `is_auto_healed = True` y documente la acción sin descartar el registro. |
| `test_quarantine_irrecoverable_record` | Envía un registro corrupto sin identificador `id`. Verifica que sea catalogado como inválido y enrutado a la tabla de cuarentena con su causal técnica. |
| `test_batch_quality_metrics_and_score` | Evalúa una colección heterogénea de sismos válidos, auto-curados y descartados, comprobando la exactitud del cálculo porcentual de calidad. |

### D. Seguridad RBAC y Guardrails del Agente RAG (`test_api_rbac_and_rag.py`)
| Caso de Prueba | Descripción |
| :--- | :--- |
| `test_unauthenticated_request_returns_401` | Comprueba que cualquier invocación a endpoints privados sin token Bearer devuelva `401 Unauthorized`. |
| `test_viewer_access_to_admin_quarantine_returns_403` | Verifica que un token con rol `viewer` sea rechazado con `403 Forbidden` al consultar `/api/quality/quarantine`. |
| `test_viewer_access_to_admin_backfill_returns_403` | Verifica que un token `viewer` no pueda disparar el backfill histórico en `/api/admin/backfill`. |
| `test_rag_agent_rejects_out_of_domain_query` | Envía una consulta fuera de dominio (ej. recetas de cocina, deportes) y valida que el guardrail la bloquee educadamente sin gastar cuota de LLM. |
| `test_rag_agent_accepts_pertinent_seismic_query` | Envía una consulta sismológica legítima y comprueba que sea procesada y respondida. |
| `test_rag_agent_rejects_architecture_and_lakehouse_query` | Comprueba que consultas impertinentes sean filtradas correctamente. |
| `test_rag_agent_recognizes_greetings` | Valida que saludos de cortesía iniciales reciban una respuesta de bienvenida orientadora. |
| `test_rag_agent_blocks_prompt_injection` | Somete el endpoint conversacional a ataques de fuga de directivas (*system prompt jailbreak*) comprobando la contención. |
| `test_rag_agent_blocks_xss_attacks` | Comprueba la neutralización de scripts inyectados en el chat. |
| `test_rag_agent_generates_interactive_chart` | Verifica que una solicitud de comparación geográfica devuelva la estructura JSON adecuada para su visualización interactiva en `Chart.js`. |

---

## 4. Cómo Ejecutar las Pruebas

### Ejecución Rápida de la Suite Completa
Desde la raíz del proyecto, utilizando el intérprete del entorno virtual:

```bash
# Ejecutar todas las pruebas con salida detallada
.venv/bin/pytest -v
```

### Ejecutar un Archivo Específico de Pruebas
```bash
# Solo pruebas de RBAC y Agente RAG
.venv/bin/pytest tests/test_api_rbac_and_rag.py -v

# Solo pruebas de Auto-Curación y Calidad
.venv/bin/pytest tests/test_self_healing_quality.py -v

# Solo pruebas de Integridad de DAGs de Airflow
.venv/bin/pytest tests/test_dag_integrity.py -v
```

### Opciones Útiles de Pytest
```bash
# Detenerse en el primer fallo (-x)
.venv/bin/pytest -x

# Filtrar por nombre de prueba (-k)
.venv/bin/pytest -k "prompt_injection" -v

# Mostrar trazas cortas sin mensajes verbosos (--tb=short)
.venv/bin/pytest --tb=short
```

---

## 5. Auditorías de Seguridad Automatizadas

Además de las pruebas de regresión funcional, el proyecto supera los dos estándares principales de seguridad estática en Python:

### 1. Bandit (Análisis Estático de Código Seguro)
Herramienta que analiza el código fuente en busca de malas prácticas de seguridad (inyecciones SQL, credenciales quemadas, funciones inseguras):
```bash
.venv/bin/bandit -r api/ airflow/ -ll
# Resultado: 0 High, 0 Medium, 0 Low (Sin vulnerabilidades)
```

### 2. pip-audit (Escaneo de Dependencias Vulnerables)
Escaneo de las dependencias instaladas contra la base de datos oficial de CVEs de PyPI y OSV:
```bash
.venv/bin/pip-audit
# Resultado: No known vulnerabilities found
```
