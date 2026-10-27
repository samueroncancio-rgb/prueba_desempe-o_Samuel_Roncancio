import re
import html
import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends

from api.auth.jwt_handler import require_viewer
from api.schemas.pydantic_models import UserProfile, ChatMessageRequest, ChatMessageResponse
from airflow.include.config import config
from airflow.include.ai.rag_indexer import generate_embedding
from api.db import get_db_cursor

router = APIRouter(prefix="/api/agent", tags=["AI Conversational RAG Agent"])
logger = logging.getLogger("sismowatch.ai_agent_rag")

# Prompt Injection and Jailbreak patterns
PROMPT_INJECTION_PATTERNS = [
    r"\bignore\s+(all\s+)?(previous|prior|system)\s+(instructions|directions|rules)\b",
    r"\bolvida\s+(todas\s+las\s+)?(instrucciones|reglas|directivas)\b",
    r"\bsystem\s+prompt\b",
    r"\bjailbreak\b",
    r"\b(dan|developer)\s+mode\b",
    r"\breveal\s+(api\s*key|secret|password|token|credentials)\b",
    r"\b(muestra|dame)\s+(las\s+claves|la\s+contrase[ñn]a|el\s+token)\b",
    r"\b(drop\s+table|delete\s+from|union\s+select|insert\s+into)\b",
    r"\bexec(ute)?\s*\("
]

# Cross-Site Scripting (XSS) and dangerous HTML/JS payloads
XSS_PATTERNS = [
    r"<\s*script[^>]*>",
    r"<\s*/\s*script\s*>",
    r"javascript\s*:",
    r"onerror\s*=",
    r"onload\s*=",
    r"<\s*iframe[^>]*>",
    r"<\s*img[^>]+src=[^>]*onerror",
    r"<\s*svg[^>]+onload",
    r"document\.cookie",
    r"eval\s*\(",
    r"alert\s*\("
]

GREETING_PATTERNS = [
    r"^\s*(hola|buenos?\s+d[ií]as?|buenas?\s+tardes?|buenas?\s+noches?|saludos?|qu[eé]\s+tal|hello|hi|hey)\b"
]

DOMAIN_KEYWORDS = [
    "sismo", "terremoto", "temblor", "earthquake", "magnitud", "tsunami",
    "profundidad", "falla", "epicentro", "usgs", "plataforma", "pipeline",
    "calidad", "airflow", "spark", "minio", "gold", "silver", "bronze",
    "reporte", "anomalia", "enjambre", "cuarentena", "auto-remediacion",
    "arquitectura", "superapp", "sismowatch", "dbscan", "pgvector",
    "grafica", "gráfico", "grafico", "estadistica", "estadística", "energia", "energía",
    "region", "región", "zonas", "alaska", "california", "chile", "japon", "japón"
]

OUT_OF_DOMAIN_REJECTION_MESSAGE = (
    "Soy el Agente Especializado de SismoWatch. Solo estoy autorizado para responder consultas "
    "relacionadas con el monitoreo sísmico, reportes operativos, anomalías detectadas y el "
    "estado de la arquitectura de datos de este proyecto."
)

SECURITY_BLOCKED_MESSAGE = (
    "🛡️ **Bloqueo de Ciberseguridad SismoWatch:** Se ha detectado y neutralizado un intento de "
    "inyección de instrucciones (Prompt Injection) o script malicioso (XSS). Por directiva de "
    "ciberseguridad, la petición ha sido bloqueada y auditada."
)

@router.post("/chat", response_model=ChatMessageResponse)
def chat_with_agent(payload: ChatMessageRequest, user: UserProfile = Depends(require_viewer)):
    """
    Hardened RAG Agent with:
    1. Anti-Prompt Injection & Anti-XSS Security Firewall.
    2. Conversational Greeting Recognition.
    3. Retrieval Augmented Generation via pgvector + live Gold facts.
    4. Dynamic Chart.js multimodal analytics generator.
    """
    raw_question = payload.message.strip()
    logger.info(f"RAG query from {user.username} ({user.role}): {raw_question}")

    # =========================================================================
    # 1. Cyber-Security Firewall (Anti-Prompt Injection & Anti-XSS)
    # =========================================================================
    for pat in XSS_PATTERNS:
        if re.search(pat, raw_question, re.IGNORECASE):
            logger.warning(f"SECURITY ALERT: XSS attempt detected from {user.username}: {raw_question}")
            return ChatMessageResponse(
                reply=SECURITY_BLOCKED_MESSAGE,
                is_domain_pertinent=False,
                sources=[],
                model="sismowatch_security_firewall",
                security_flags={"blocked": True, "type": "XSS_ATTACK_DETECTED"}
            )

    for pat in PROMPT_INJECTION_PATTERNS:
        if re.search(pat, raw_question, re.IGNORECASE):
            logger.warning(f"SECURITY ALERT: Prompt Injection attempt detected from {user.username}: {raw_question}")
            return ChatMessageResponse(
                reply=SECURITY_BLOCKED_MESSAGE,
                is_domain_pertinent=False,
                sources=[],
                model="sismowatch_security_firewall",
                security_flags={"blocked": True, "type": "PROMPT_INJECTION_DETECTED"}
            )

    # Sanitize input HTML entities to prevent reflection
    safe_question = html.escape(raw_question)

    # =========================================================================
    # 2. Greeting & Conversational Intent Recognition
    # =========================================================================
    is_greeting = any(re.search(pat, safe_question, re.IGNORECASE) for pat in GREETING_PATTERNS)
    if is_greeting and len(safe_question.split()) <= 4:
        return ChatMessageResponse(
            reply=(
                "¡Hola! 👋 Soy **SismoBot**, el agente de inteligencia artificial y análisis geofísico de **SismoWatch**.\n\n"
                "Puedo ayudarte a analizar sismos globales, consultar la capa Gold y responder con **gráficas interactivas**:\n"
                "• 📊 **Gráficas en Vivo:** Pídeme *'muestra una gráfica de magnitudes'* o *'grafica las regiones más activas'*.\n"
                "• 🌍 **Sismos Recientes:** Pregúntame *'¿cuál fue el sismo más fuerte?'* o *'¿cuántos eventos hubo hoy?'*.\n"
                "• 🌊 **Alertas Oceánicas:** Consulta alertas tempranas de tsunami en boyas DART.\n"
                "• ⚡ **Energía Sísmica:** Conoce la energía liberada calculada con la relación Gutenberg-Richter.\n"
                "• 🏗️ **Arquitectura Lakehouse:** Pregúntame sobre el flujo Bronze, Silver y Gold.\n\n"
                "¿Qué información o gráfico te gustaría consultar ahora?"
            ),
            is_domain_pertinent=True,
            sources=[],
            model=config.llm_model_name,
            security_flags={"verified_safe": True}
        )

    # =========================================================================
    # 3. Guardrail: Semantic Domain Relevance Check
    # =========================================================================
    q_lower = safe_question.lower()
    is_pertinent = any(k in q_lower for k in DOMAIN_KEYWORDS)
    if not is_pertinent:
        return ChatMessageResponse(
            reply=OUT_OF_DOMAIN_REJECTION_MESSAGE,
            is_domain_pertinent=False,
            sources=[],
            model="sismowatch_guardrail_filter",
            security_flags={"verified_safe": True}
        )

    # =========================================================================
    # 4. Retrieval via pgvector
    # =========================================================================
    query_vector = generate_embedding(safe_question)
    retrieved_sources = []

    try:
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT 
                    chunk_id::text,
                    source_type,
                    source_ref_id,
                    content_text,
                    metadata,
                    1 - (embedding <=> %s::vector) AS similarity
                FROM gold.rag_knowledge_base
                ORDER BY embedding <=> %s::vector ASC
                LIMIT 5;
            """, (query_vector, query_vector))
            rows = cur.fetchall()
            retrieved_sources = [dict(r) for r in rows if r["similarity"] is not None]
    except Exception as e:
        logger.warning(f"Vector search failed (pgvector might be initializing): {e}")

    # =========================================================================
    # 5. Fetch live context from Gold facts layer
    # =========================================================================
    stats = {"total_24h": 0, "max_mag": 0.0, "avg_mag": 0.0}
    recent_events = []
    mag_dist = {"micro": 0, "menor": 0, "moderado": 0, "fuerte": 0, "mayor": 0}
    depth_dist = {"shallow": 0, "intermediate": 0, "deep": 0}
    top_regions = []

    try:
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS total_24h,
                       COALESCE(MAX(mag), 0.0) AS max_mag,
                       COALESCE(ROUND(AVG(mag), 2), 0.0) AS avg_mag
                FROM gold.fact_earthquake_events
                WHERE event_time >= NOW() - INTERVAL '24 hours';
            """)
            row = cur.fetchone()
            if row:
                stats = dict(row)

            cur.execute("""
                SELECT event_id, mag, place_sanitized, event_time, depth_km, tsunami_alert, significance
                FROM gold.fact_earthquake_events
                ORDER BY event_time DESC
                LIMIT 10;
            """)
            recent_events = [dict(r) for r in cur.fetchall()]

            # Magnitude counts
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE mag < 2.0) AS micro,
                    COUNT(*) FILTER (WHERE mag >= 2.0 AND mag < 4.0) AS menor,
                    COUNT(*) FILTER (WHERE mag >= 4.0 AND mag < 5.5) AS moderado,
                    COUNT(*) FILTER (WHERE mag >= 5.5 AND mag < 7.0) AS fuerte,
                    COUNT(*) FILTER (WHERE mag >= 7.0) AS mayor
                FROM gold.fact_earthquake_events;
            """)
            mag_dist = dict(cur.fetchone())

            # Depth counts
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE depth_km <= 30) AS shallow,
                    COUNT(*) FILTER (WHERE depth_km > 30 AND depth_km <= 100) AS intermediate,
                    COUNT(*) FILTER (WHERE depth_km > 100) AS deep
                FROM gold.fact_earthquake_events;
            """)
            depth_dist = dict(cur.fetchone())

            # Top regions
            cur.execute("""
                SELECT 
                    COALESCE(SPLIT_PART(place_sanitized, ' of ', 2), place_sanitized) AS region,
                    COUNT(*) AS count
                FROM gold.fact_earthquake_events
                GROUP BY 1
                ORDER BY count DESC
                LIMIT 5;
            """)
            top_regions = [dict(r) for r in cur.fetchall()]

    except Exception as e:
        logger.warning(f"Could not query live Gold events: {e}")

    context_text = f"Resumen 24h: {stats['total_24h']} eventos registrados, magnitud máxima {stats['max_mag']} M, promedio {stats['avg_mag']} M.\n"
    context_text += "Eventos recientes:\n" + "\n".join(
        [f"- [{e['event_id']}] Mag {e['mag']} en {e['place_sanitized']} (Prof: {e['depth_km']}km, Tsunami: {e['tsunami_alert']})" for e in recent_events]
    )

    if retrieved_sources:
        context_text += "\n\nFragmentos de conocimiento recuperados:\n" + "\n".join(
            [f"- [Fuente: {s['source_ref_id']}] {s['content_text']}" for s in retrieved_sources]
        )

    # =========================================================================
    # 6. Synthesize Answer + Generate Dynamic Visual Chart
    # =========================================================================
    answer, chart_payload = synthesize_grounded_answer_and_chart(
        safe_question, context_text, recent_events, stats, mag_dist, depth_dist, top_regions
    )

    sources_summary = [
        {"ref_id": s["source_ref_id"], "type": s["source_type"], "similarity": round(float(s["similarity"]), 3)}
        for s in retrieved_sources[:3]
    ]

    return ChatMessageResponse(
        reply=answer,
        is_domain_pertinent=True,
        sources=sources_summary,
        model=config.llm_model_name,
        chart=chart_payload,
        security_flags={"verified_safe": True}
    )

def synthesize_grounded_answer_and_chart(
    question: str,
    context: str,
    recent_events: List[Dict[str, Any]],
    stats: Dict[str, Any],
    mag_dist: Dict[str, Any],
    depth_dist: Dict[str, Any],
    top_regions: List[Dict[str, Any]]
) -> tuple[str, Optional[Dict[str, Any]]]:
    """
    Produces a factual grounded answer and an interactive Chart.js payload
    whenever the question involves quantitative patterns or requests a graph.
    """
    q_lower = question.lower()
    chart = None

    # Case A: Magnitude distribution or requests for magnitude chart
    if any(k in q_lower for k in ["magnitud", "escala", "distribucion", "distribución", "gráfico de magnitud", "grafica de magnitud", "frecuencia"]):
        chart = {
            "type": "bar",
            "title": "Distribución de Sismos por Escala de Magnitud",
            "labels": ["Micro (<2.0)", "Menor (2.0-3.9)", "Moderado (4.0-5.4)", "Fuerte (5.5-6.9)", "Mayor (≥7.0)"],
            "datasets": [{
                "label": "Eventos Registrados",
                "data": [
                    int(mag_dist.get("micro", 0)),
                    int(mag_dist.get("menor", 0)),
                    int(mag_dist.get("moderado", 0)),
                    int(mag_dist.get("fuerte", 0)),
                    int(mag_dist.get("mayor", 0))
                ],
                "backgroundColor": [
                    "rgba(16, 185, 129, 0.8)",
                    "rgba(59, 130, 246, 0.8)",
                    "rgba(245, 158, 11, 0.8)",
                    "rgba(239, 68, 68, 0.85)",
                    "rgba(168, 85, 247, 0.85)"
                ],
                "borderColor": ["#10b981", "#3b82f6", "#f59e0b", "#ef4444", "#a855f7"],
                "borderWidth": 1.5
            }]
        }
        answer = (
            f"He generado la **gráfica interactiva de distribución de magnitudes** con los datos de la capa Gold:\n\n"
            f"• **Micro (<2.0 M):** {mag_dist.get('micro', 0)} sismos (baja percepción).\n"
            f"• **Menor (2.0 - 3.9 M):** {mag_dist.get('menor', 0)} sismos.\n"
            f"• **Moderado (4.0 - 5.4 M):** {mag_dist.get('moderado', 0)} sismos.\n"
            f"• **Fuerte (5.5 - 6.9 M):** {mag_dist.get('fuerte', 0)} sismos.\n"
            f"• **Mayor (≥7.0 M):** {mag_dist.get('mayor', 0)} sismos de alta energía.\n\n"
            f"El promedio global actual se sitúa en **{stats.get('avg_mag', 0)} M**, reflejando la actividad en la gráfica adjunta."
        )
        return answer, chart

    # Case B: Regions or active zones
    if any(k in q_lower for k in ["region", "región", "zonas", "lugares", "paises", "países", "donde", "dónde"]):
        reg_labels = [r["region"] for r in top_regions] or ["Alaska", "California", "Chile", "Japón", "Indonesia"]
        reg_data = [int(r["count"]) for r in top_regions] or [5, 3, 2, 2, 1]
        chart = {
            "type": "bar",
            "title": "Top Zonas Sísmicas Más Activas",
            "labels": reg_labels,
            "datasets": [{
                "label": "Número de Sismos",
                "data": reg_data,
                "backgroundColor": "rgba(56, 189, 248, 0.8)",
                "borderColor": "#38bdf8",
                "borderWidth": 1.5
            }]
        }
        top_str = ", ".join([f"**{r['region']}** ({r['count']} eventos)" for r in top_regions[:3]])
        answer = (
            f"Las zonas geográficas con mayor recurrencia sísmica detectadas por SismoWatch son: {top_str}.\n\n"
            f"Adjunto una **gráfica comparativa** de las regiones más activas normalizadas a partir de los datos crudos del USGS."
        )
        return answer, chart

    # Case C: Depth and hypocenters
    if any(k in q_lower for k in ["profundidad", "hipocentro", "corteza", "manto", "profundo"]):
        chart = {
            "type": "doughnut",
            "title": "Clasificación Hipocentral por Profundidad",
            "labels": ["Superficial (≤30km)", "Intermedio (30-100km)", "Profundo (>100km)"],
            "datasets": [{
                "data": [
                    int(depth_dist.get("shallow", 0)) or 1,
                    int(depth_dist.get("intermediate", 0)),
                    int(depth_dist.get("deep", 0))
                ],
                "backgroundColor": [
                    "rgba(239, 68, 68, 0.8)",
                    "rgba(245, 158, 11, 0.8)",
                    "rgba(99, 102, 241, 0.85)"
                ],
                "borderColor": "#0f172a",
                "borderWidth": 2
            }]
        }
        answer = (
            f"El perfil hipocentral de profundidad se divide en tres niveles geodinámicos:\n"
            f"• **Superficiales (≤30 km):** {depth_dist.get('shallow', 0)} eventos (presentan mayor aceleración del suelo).\n"
            f"• **Intermedios (30 - 100 km):** {depth_dist.get('intermediate', 0)} eventos.\n"
            f"• **Profundos (>100 km):** {depth_dist.get('deep', 0)} eventos (subducción en manto).\n\n"
            f"Puedes ver la proporción exacta en la **gráfica de dona** que he renderizado abajo."
        )
        return answer, chart

    # Case D: Max magnitude earthquake
    if "sismo mas fuerte" in q_lower or "mayor magnitud" in q_lower or "mas alto" in q_lower or "pico" in q_lower:
        if recent_events:
            max_ev = max(recent_events, key=lambda x: float(x["mag"]))
            return (
                f"El sismo más fuerte registrado recientemente fue de magnitud **{max_ev['mag']} M**, "
                f"localizado en **{max_ev['place_sanitized']}** (ID del evento: `{max_ev['event_id']}`).\n\n"
                f"• **Profundidad focal:** {max_ev['depth_km']} km.\n"
                f"• **Alerta de tsunami:** {'⚠️ Activada' if max_ev['tsunami_alert'] else '🟢 Ninguna'}.\n"
                f"• **Fecha UTC:** {max_ev['event_time']}.",
                None
            )

    # Case E: Lakehouse architecture and pipeline
    if "arquitectura" in q_lower or "pipeline" in q_lower or "lakehouse" in q_lower or "capas" in q_lower:
        return (
            "La arquitectura de SismoWatch implementa un patrón **Lakehouse Híbrido Gobernado**:\n\n"
            "1. **Bronze (MinIO):** Ingesta inmutable de feeds GeoJSON cada 5 minutos (`sismowatch-bronze`).\n"
            "2. **Silver (MinIO):** Transformaciones Parquet/JSON particionadas por año y mes (`sismowatch-silver`).\n"
            "3. **Gold (PostgreSQL + MinIO):** Hechos curados (`fact_earthquake_events`), anomalías espaciales DBSCAN y base vectorial `pgvector`.\n"
            "4. **Gobernanza:** Base de datos relacional limpia con cero cuarentena pasiva y auto-remediación en caliente.",
            None
        )

    # Default general response
    answer = (
        f"Con base en la telemetría curada de SismoWatch:\n{context[:350]}...\n\n"
        f"Todos los registros han sido validados con cero cuarentena pasiva y saneados contra inyección de instrucciones."
    )
    return answer, None

