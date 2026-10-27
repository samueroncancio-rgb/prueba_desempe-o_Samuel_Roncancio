# Módulo de Frontend y Consumo Web (React 19 + TypeScript + Leaflet)

> **SismoWatch Enterprise • Operational Dashboard & Real-Time Geospatial Visualizer**  
> *Ruta: [`web/`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/web)*

---

## 1. Visión General y Propósito

El módulo **Frontend Web** de **SismoWatch Enterprise** es una Single Page Application (SPA) moderna, responsiva y de grado empresarial construida con **React 19**, **TypeScript**, **Vite**, **Tailwind CSS** y **Leaflet**. Proporciona a los equipos de operaciones críticas y científicos de datos una interfaz unificada para supervisar la actividad telúrica global, auditar la calidad del dato y dialogar con el asistente inteligente **SismoBot**.

### Principios de Experiencia de Usuario (UX/UI)
* **Visualización en Tiempo Real sin Recarga:** Integración nativa de **Server-Sent Events (SSE)** que actualiza los mapas y métricas al instante en que Spark persiste nuevos sismos curados en PostgreSQL Gold.
* **Mapa Geoespacial Interactivo:** Representación cartográfica mundial con Leaflet, donde los círculos sísmicos se escalan proporcionalmente a la magnitud del evento y cambian dinámicamente de color.
* **Control de Acceso Basado en Roles (RBAC):** La interfaz adapta sus vistas y botones según el rol del usuario autenticado (`admin` vs `viewer`), bloqueando u ocultando acciones privilegiadas (Backfill y Cuarentena).
* **IA Interactiva Multimodal:** Cajón de chat conversacional ("SismoBot") que no solo responde preguntas sísmicas fundamentadas en RAG, sino que renderiza dinámicamente gráficos interactivos (`Chart.js`) dentro de las burbujas de diálogo.
* **Resiliencia de Estados:** Pantallas diseñadas con esqueletos de carga (*skeletons*), estados vacíos estilizados y gestión de desconexión/reconexión automática del stream SSE.

---

## 2. Estructura del Módulo

```
web/
├── src/
│   ├── App.tsx                  # Aplicación SPA principal (Dashboard, Mapa, Tablas, Chatbot, Modales)
│   └── main.tsx                 # Montaje de la aplicación React en el DOM
├── dist/                        # Compilación optimizada para producción generada por Vite
├── node_modules/                # Dependencias instaladas
├── Dockerfile                   # Contenedor para construcción y despliegue del frontend
├── index.html                   # HTML base con fuentes y metadatos de la aplicación
├── package.json                 # Definición de scripts y dependencias (React 19, Leaflet, Chart.js, etc.)
├── tsconfig.json                # Configuración del compilador TypeScript
├── vite.config.ts               # Configuración de empaquetado de Vite y plugins
└── README.md                    # Esta documentación
```

---

## 3. Características Principales de la Plataforma

### A. Dashboard de KPIs en Vivo
Cinco tarjetas analíticas superiores con cálculo automático desde `gold.fact_earthquake_events`:
1. **Total de Sismos (24h):** Volumen de eventos procesados en el último ciclo diario.
2. **Sismo de Mayor Magnitud:** Magnitud máxima registrada junto con su ubicación geográfica exacta.
3. **Magnitud Promedio:** Nivel medio de energía telúrica global.
4. **Alertas de Tsunami:** Indicador de eventos críticos con riesgo marítimo activo.
5. **Score de Calidad y Salud:** Puntuación de calidad del pipeline ($0 - 100\%$) y frescura en segundos.

### B. Visualizador Cartográfico (Leaflet)
* **Marcadores Sísmicos Escala-Magnitud:**
  * Verde ($\text{Mag} < 3.0$): Micro-sismos no perceptibles.
  * Amarillo ($3.0 \le \text{Mag} < 4.5$): Sismos menores.
  * Naranja ($4.5 \le \text{Mag} < 6.0$): Sismos moderados con potencial de percepción.
  * Rojo / Carmesí ($\text{Mag} \ge 6.0$): Sismos mayores con alerta prioritaria.
* **Efecto de Pulso:** Indicadores luminosos pulsantes en los epicentros más recientes.
* **Popups Informativos:** Muestran hora UTC, profundidad, alerta de tsunami, coordenadas corregidas y el badge de auto-curación si el motor aplicó `swap(lat, lon)`.

### C. Conexión de Streaming (Server-Sent Events)
* Escucha permanentemente el canal `/api/stream` de FastAPI.
* Distintivo visual superior con la etiqueta **"EN VIVO"** y animación de pulso verde.
* En caso de interrupción del backend, implementa reintento silencioso cada 5 segundos sin congelar la interfaz ni perder el estado local.

### D. Catálogo de Eventos y Exportación
* Tabla paginada de eventos curados con filtros en caliente:
  * Filtro por magnitud mínima ($\ge 2.5, \ge 4.5, \ge 6.0$).
  * Búsqueda por texto en tiempo real sobre la ubicación sanitizada.
  * Filtro por ventana de tiempo (24h, 7d, 30d).
* **Descarga CSV:** Botón de un solo clic que invoca `/api/events/export/csv` para exportar el dataset curado.

### E. Pestaña de Auditoría de Calidad y Cuarentena (Exclusivo Administrador)
* Visualización de las últimas ejecuciones de calidad del motor `SelfHealingValidator`.
* Tabla de eventos en cuarentena (`gold.fact_quarantine_events`) que muestra el payload crudo descartado y la causa técnica del rechazo.
* Si un usuario con rol `viewer` intenta invocar esta vista, el sistema previene la llamada o muestra un mensaje explicativo de restricción RBAC.

### F. Operaciones de Administración
* **Backfill Histórico de 30 Días:** Modal administrativo para forzar la ingesta masiva de hasta 30 días de historial de la API de USGS definiendo la magnitud mínima de corte.
* **Regeneración de Reportes:** Permite relanzar la síntesis del reporte ejecutivo diario.

### G. SismoBot: Asistente Conversacional RAG con Gráficos Integrados
* Panel lateral deslizable (Drawer) accesible desde el botón flotante inferior derecho.
* **Respuestas con Citas Verificables:** Muestra badges interactivos con los IDs de los sismos USGS utilizados como evidencia factual.
* **Renderizado de Gráficos en el Chat:** Cuando la consulta solicita distribuciones o comparaciones (ej. *"Compara los sismos de Chile y Japón"*), el backend retorna un esquema de gráfico que el componente [`ChatBubbleChart`](file:///home/cohorte5/Música/prueba_desempeño_Samuel_Roncancio/web/src/App.tsx#L24) renderiza como un gráfico de barras o dona interactivo mediante `Chart.js`.
* **Protección Anti-Inyección:** Las advertencias de seguridad del backend son presentadas con tarjetas de alerta contextuales.

---

## 4. Usuarios de Acceso Predeterminados

| Rol | Usuario | Contraseña | Capacidades en UI |
| :--- | :--- | :--- | :--- |
| **Administrador** | `admin@sismowatch.com` | `AdminSecurePass2026!` | Acceso total: Dashboard, Mapa, Exportación, SismoBot, Panel de Cuarentena, Backfill Histórico 30d y Regeneración de Reportes. |
| **Observador** | `viewer@sismowatch.com` | `ViewerPass2026!` | Acceso operacional: Dashboard, Mapa, Exportación, SismoBot, Reportes diarios. Botones administrativos ocultos o protegidos. |

---

## 5. Desarrollo y Compilación Local

### Prerrequisitos
* Node.js v20+ o v22+
* npm v10+

### Instalación de Dependencias
```bash
cd web
npm install
```

### Ejecución en Servidor de Desarrollo
```bash
npm run dev
```
La aplicación estará accesible en: [`http://localhost:3000`](http://localhost:3000) (o el puerto asignado por Vite).

### Compilación para Producción
```bash
npm run build
```
Genera los artefactos minificados y optimizados en la carpeta `dist/`.

### Previsualización de la Versión Compilada
```bash
npm run preview -- --port 3000 --host 0.0.0.0
```

---

## 6. Variables de Entorno del Frontend

Por defecto, la aplicación se comunica con el backend en `http://localhost:8000`. Puede configurarse modificando la constante `API_BASE` o mediante variables de entorno Vite (`VITE_API_BASE_URL`).
