import logging
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.routers import auth, dashboard, realtime_sse, quality, admin_ops, ai_agent_rag

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sismowatch.api")

app = FastAPI(
    title="SismoWatch Enterprise Platform API",
    description=(
        "Plataforma de monitoreo sísmico global en tiempo casi real con arquitectura Lakehouse Medallion. "
        "PostgreSQL aloja estrictamente los hechos limpios en GOLD y embeddings pgvector para el agente RAG."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Restricted to frontend domains in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount Routers
app.include_router(auth.router)
app.include_router(dashboard.router)
app.include_router(realtime_sse.router)
app.include_router(quality.router)
app.include_router(admin_ops.router)
app.include_router(ai_agent_rag.router)

@app.get("/", tags=["Health"])
def root():
    return {
        "service": "SismoWatch Enterprise Serving API",
        "status": "ONLINE",
        "version": "2.0.0",
        "architecture": "Lakehouse Medallion (MinIO Bronze/Silver -> PostgreSQL Gold Serving)"
    }

@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "HEALTHY"}

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception at {request.url.path}: {exc}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Error interno del servidor. La telemetría fue registrada de forma segura."}
    )
