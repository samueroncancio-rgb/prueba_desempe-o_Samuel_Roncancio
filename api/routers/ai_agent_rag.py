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

FORBIDDEN_TECH_PATTERNS = [
    r"\barquitectura\b",
    r"\blakehouse\b",
    r"\bcapas?\b",
    r"\bbronze\b",
    r"\bsilver\b",
    r"\bgold\b",
    r"\bpipeline\b",
    r"\bairflow\b",
    r"\bspark\b",
    r"\bminio\b",
    r"\bpostgres(ql)?\b",
    r"\bpgvector\b",
    r"\bcuarentena\b",
    r"\bauto-?remediaci[oó]n\b",
    r"\binfraestructura\b"
]

DOMAIN_KEYWORDS = [
    "sismo", "sismos", "terremoto", "terremotos", "temblor", "temblores",
    "earthquake", "earthquakes", "magnitud", "magnitudes", "tsunami", "tsunamis",
    "profundidad", "profundidades", "falla", "hipocentro", "epicentro", "usgs",
    "grafica", "graficas", "gráfica", "gráficas", "gráfico", "gráficos", "grafico", "graficos",
    "estadistica", "estadísticas", "estadística", "estadisticas", "energia", "energía",
    "region", "región", "regiones", "zonas", "alaska", "california", "chile", "japon", "japón",
    "evento", "eventos", "actividad", "telurico", "telúrico", "placas", "tectonica", "tectónica"
]

ARCHITECTURE_REJECTION_MESSAGE = (
    "Como SismoBot, estoy especializado exclusivamente en el análisis de eventos sísmicos "
    "y la generación de gráficas interactivas de actividad telúrica. La arquitectura técnica del proyecto, "
    "sus capas y el Lakehouse no son temas pertinentes para este asistente. "
    "¿Deseas consultar una gráfica de magnitudes, regiones activas o sismos recientes?"
)

OUT_OF_DOMAIN_REJECTION_MESSAGE = (
    "Soy SismoBot, el asistente de análisis sísmico. Únicamente respondo preguntas y genero "
    "gráficas interactivas sobre eventos sísmicos, magnitudes, ubicaciones geográficas y alertas "
    "de tsunami. No respondo sobre otros temas o sobre la arquitectura técnica interna."
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
                "¡Hola! 👋 Soy **SismoBot**, el asistente de análisis geofísico y monitoreo sísmico de **SismoWatch**.\n\n"
                "Estoy especializado exclusivamente en responder consultas sobre eventos sísmicos y generar **gráficas interactivas** en tiempo real:\n"
                "• 📊 **Gráfica de Magnitudes:** Pídeme *'muestra una gráfica de magnitudes'* para ver la distribución de eventos.\n"
                "• 🗺️ **Gráfica de Regiones:** Pídeme *'grafica las regiones con más sismos'* para comparar las zonas más activas.\n"
                "• 🌋 **Gráfica de Profundidad:** Pídeme *'gráfica de profundidad de los sismos'* para ver niveles superficiales o profundos.\n"
                "• 🌍 **Sismos Más Fuertes:** Pregúntame *'¿cuál fue el sismo más fuerte?'* o detalles de los eventos más recientes.\n"
                "• 🌊 **Alertas de Tsunami:** Consulta sismos con potencial de afectación oceánica.\n\n"
                "¿Qué evento sísmico o gráfica te gustaría consultar?"
            ),
            is_domain_pertinent=True,
            sources=[],
            model=config.llm_model_name,
            security_flags={"verified_safe": True}
        )

    # =========================================================================
    # 3. Guardrail: Prohibition of Internal Architecture / Lakehouse / Capas
    # =========================================================================
    for pat in FORBIDDEN_TECH_PATTERNS:
        if re.search(pat, safe_question, re.IGNORECASE):
            logger.info(f"Rejected technical/architecture query from {user.username}: {safe_question}")
            return ChatMessageResponse(
                reply=ARCHITECTURE_REJECTION_MESSAGE,
                is_domain_pertinent=False,
                sources=[],
                model="sismowatch_domain_filter",
                security_flags={"verified_safe": True}
            )

    # =========================================================================
    # 4. Guardrail: Semantic Domain Relevance Check (Seismic & Charts only)
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
            f"He generado la **gráfica interactiva de distribución de magnitudes** con los eventos sísmicos registrados:\n\n"
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
            f"Adjunto una **gráfica comparativa** de las zonas con mayor actividad sísmica."
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

    # Default general response
    answer = (
        f"Resumen de actividad sísmica registrada:\n{context[:350]}...\n\n"
        f"Puedes solicitar una gráfica interactiva de magnitudes, regiones o profundidad para visualizar estos datos."
    )
    return answer, None

