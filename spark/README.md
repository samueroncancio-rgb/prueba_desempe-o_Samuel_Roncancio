# Módulo de Cómputo Distribuido (Apache Spark 3.5)

> **SismoWatch Enterprise • Distributed Compute Engine & Medallion Lakehouse**  
> *Ruta: [`spark/`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/spark)*

---

## 1. Visión General y Propósito

El módulo **Spark** proporciona la infraestructura y los jobs de procesamiento distribuido para transformar los flujos de datos sísmicos crudos (Bronze) en formatos columnares limpios (Silver) y publicar las tablas de hechos curadas de sub-segundo en la capa de consumo (Gold).

### Aspectos Arquitectónicos Destacados
* **Topología en Contenedores:** Opera como un clúster desacoplado compuesto por un nodo **Spark Master** y un nodo **Spark Worker** ejecutándose en la red Docker interna `sismowatch_net`.
* **Conectividad Lakehouse S3A:** Configurado con los conectores Maven `hadoop-aws` y `aws-java-sdk-bundle` para leer y escribir directamente en **MinIO** mediante el protocolo `s3a://`.
* **Conector JDBC PostgreSQL:** Equipado con el driver JDBC oficial de PostgreSQL 42.6.0 para ejecutar operaciones de carga optimizadas y UPSERTs masivos.
* **Procesamiento de Regla de Oro:** Garantiza que las capas intermedias masivas se mantengan en el almacenamiento de objetos S3 columnar (Parquet) y que únicamente los hechos analíticos curados se escriban en PostgreSQL.

---

## 2. Estructura y Archivos del Módulo

```
spark/
├── Dockerfile                  # Definición de la imagen del clúster (Java 11, PySpark 3.5.1, JARs Hadoop/S3/PostgreSQL)
└── README.md                   # Esta documentación

# Archivo de trabajo asociado en airflow/include/:
airflow/include/spark_jobs/
└── medallion_etl.py            # Clase MedallionETLJob con lógica completa de transformación y persistencia
```

---

## 3. Topología del Clúster y Puertos

En el entorno de `docker-compose.yml`, los servicios de Spark se configuran de la siguiente manera:

| Contenedor | Rol | Puerto Interno | Puerto Host Mapeado | Propósito |
| :--- | :--- | :--- | :--- | :--- |
| `sismowatch_spark_master` | Spark Master | `7077` | `7077` | Coordinador del clúster y receptor de spark-submit |
| `sismowatch_spark_master` | Web UI Master | `8080` | `8085` | Panel de control de trabajos, recursos y ejecutores |
| `sismowatch_spark_worker` | Spark Worker | `8081` | `8086` | Panel de monitoreo de threads, memoria y tareas del worker |

### Configuración de Recursos por Defecto
* **Worker Cores:** 2 núcleos virtuales asignados.
* **Worker Memory:** 1 GB de memoria RAM.
* **Modo de Despliegue:** Standalone Cluster Manager.

---

## 4. Conectores y Dependencias Maven Pre-instaladas

El archivo [`spark/Dockerfile`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/spark/Dockerfile) descarga y compila directamente en el directorio `$SPARK_HOME/jars/`:

1. **`hadoop-aws-3.3.4.jar`:** Proveedor del sistema de archivos `org.apache.hadoop.fs.s3a.S3AFileSystem`.
2. **`aws-java-sdk-bundle-1.12.262.jar`:** SDK oficial para autenticación y llamadas REST hacia endpoints compatibles con Amazon S3 / MinIO.
3. **`postgresql-42.6.0.jar`:** Conector JDBC para streaming y persistencia relacional en PostgreSQL 16.

---

## 5. El Job de Procesamiento Medallion (`medallion_etl.py`)

Ubicado en [`airflow/include/spark_jobs/medallion_etl.py`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/spark_jobs/medallion_etl.py), el trabajo implementa las siguientes fases:

```
[MinIO Bronze: GeoJSON]
         │
         ▼
[Self-Healing & PySpark In-Memory Transformation]
         ├──────────────────────────────────────────┐
         ▼                                          ▼
[MinIO Silver: Parquet Particionado]     [PostgreSQL Gold Serving Layer]
(s3a://sismowatch-silver/year=.../)       (gold.fact_earthquake_events)
```

### 1. Ingesta desde MinIO Bronze
Lee los payloads GeoJSON recién depositados mediante la sesión configurada con `fs.s3a.path.style.access = true` y credenciales de MinIO.

### 2. Auto-Curación y Limpieza en Vuelo
Integra el motor [`SelfHealingValidator`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/quality/self_healing_validator.py):
* Desanida la estructura jerárquica de `features.properties` y `features.geometry.coordinates`.
* Si detecta coordenadas invertidas ($|lat| > 90$ y $|lon| \le 90$), intercambia automáticamente los ejes (`swap(lat, lon)`), marca el evento con `is_auto_healed = true` y registra `healing_action = 'SWAPPED_INVERTED_COORDINATES'`.
* Sanitiza el texto de `place` contra ataques de Prompt Injection.
* Filtra eventos irrecuperables (por ejemplo, sin ID) enviándolos a la tabla de auditoría `gold.fact_quarantine_events`.

### 3. Escritura en Capa Silver (Parquet Particionado)
Persiste el DataFrame estructurado con esquema [`SEISMIC_EVENT_SCHEMA`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/airflow/include/spark_jobs/medallion_etl.py#L24) en el bucket `sismowatch-silver`, particionado dinámicamente por año, mes y día:
```
s3a://sismowatch-silver/year=2026/month=10/day=27/part-00000.snappy.parquet
```

### 4. UPSERT Idempotente en Capa Gold (PostgreSQL)
A través de transacciones optimizadas (`psycopg2.extras.execute_batch` o JDBC), ejecuta la inserción con cláusula `ON CONFLICT (event_id) DO UPDATE SET ...` para garantizar que actualizaciones de magnitudes o revisiones de USGS no dupliquen registros.

### 5. Registro de Salud del Lote
Calcula y escribe métricas de calidad en `gold.fact_quality_and_healing_runs`:
$$\text{Quality Score} = \left(\frac{\text{Passed Records} + \text{Auto-Healed Records}}{\text{Total Records}}\right) \times 100$$

---

## 6. Monitoreo y Verificación

### Acceso a la Interfaz Web de Spark
* **Spark Master UI:** [`http://localhost:8085`](http://localhost:8085)
* **Spark Worker UI:** [`http://localhost:8086`](http://localhost:8086)

### Comandos de Inspección
```bash
# Ver los logs del Master de Spark
docker logs sismowatch_spark_master

# Ver los logs del Worker de Spark
docker logs sismowatch_spark_worker

# Probar la conectividad entre Spark y MinIO
docker exec -it sismowatch_spark_master python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').appName('TestMinIO').getOrCreate()
print('PySpark Session iniciada exitosamente.')
"
```
