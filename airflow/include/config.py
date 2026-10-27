import os
from pydantic import BaseModel

class SismoWatchConfig(BaseModel):
    # PostgreSQL Serving DB (GOLD only)
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "sismowatch_db")
    postgres_user: str = os.getenv("POSTGRES_USER", "sismowatch_admin")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "sismowatch_secure_pass_2026")
    
    # Specific least-privilege DB roles
    db_spark_pass: str = os.getenv("DB_SPARK_PASS", "spark_etl_secure_pass_2026")
    db_api_reader_pass: str = os.getenv("DB_API_READER_PASS", "api_reader_secure_pass_2026")

    # MinIO Data Lake (Bronze & Silver)
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_root_user: str = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_root_password: str = os.getenv("MINIO_ROOT_PASSWORD", "miniopassword2026")
    minio_bucket_bronze: str = os.getenv("MINIO_BUCKET_BRONZE", "sismowatch-bronze")
    minio_bucket_silver: str = os.getenv("MINIO_BUCKET_SILVER", "sismowatch-silver")
    minio_bucket_gold: str = os.getenv("MINIO_BUCKET_GOLD", "sismowatch-gold")
    
    # USGS API
    usgs_realtime_url: str = os.getenv("USGS_REALTIME_URL", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson")
    usgs_query_url: str = os.getenv("USGS_QUERY_URL", "https://earthquake.usgs.gov/fdsnws/event/1/query")
    request_timeout_seconds: int = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
    max_retries: int = int(os.getenv("MAX_RETRIES", "5"))

    # Security & JWT
    jwt_secret_key: str = os.getenv("JWT_SECRET_KEY", "sismowatch_enterprise_jwt_ultra_secret_key_2026_xyz")
    jwt_algorithm: str = os.getenv("JWT_ALGORITHM", "HS256")
    jwt_access_token_expire_minutes: int = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))
    admin_user: str = os.getenv("ADMIN_USER", "admin@sismowatch.com")
    admin_password: str = os.getenv("ADMIN_PASSWORD", "AdminSecurePass2026!")
    viewer_user: str = os.getenv("VIEWER_USER", "viewer@sismowatch.com")
    viewer_password: str = os.getenv("VIEWER_PASSWORD", "ViewerPass2026!")

    # AI & LLM
    llm_provider: str = os.getenv("LLM_PROVIDER", "mock_fallback")
    gemini_api_key: str = os.getenv("GEMINI_API_KEY", "")
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    ollama_base_url: str = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    llm_model_name: str = os.getenv("LLM_MODEL_NAME", "gemini-1.5-flash")

    def get_postgres_uri(self, role: str = "admin") -> str:
        if role == "spark":
            user = "role_spark_etl"
            pwd = self.db_spark_pass
        elif role == "api_reader":
            user = "role_api_reader"
            pwd = self.db_api_reader_pass
        else:
            user = self.postgres_user
            pwd = self.postgres_password
        return f"postgresql://{user}:{pwd}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

config = SismoWatchConfig()
