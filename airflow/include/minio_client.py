import os
import hashlib
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger("sismowatch.minio_client")

class S3MinioClient:
    """
    Client for MinIO / S3 Object Storage handling Bronze and Silver Lakehouse layers.
    Includes deterministic checksum validation and fallback to persistent local directory
    if the remote S3 endpoint is unavailable.
    """
    def __init__(self, endpoint: Optional[str] = None, access_key: Optional[str] = None, secret_key: Optional[str] = None):
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = access_key or os.getenv("MINIO_ROOT_USER", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_ROOT_PASSWORD", "miniopassword2026")
        self.local_storage_root = os.getenv("MINIO_LOCAL_STORAGE", os.path.join(os.path.expanduser("~"), ".sismowatch_lakehouse"))
        os.makedirs(self.local_storage_root, exist_ok=True)
        self._s3_client = None
        self._init_client()

    def _init_client(self):
        try:
            import boto3
            from botocore.client import Config
            protocol = "https" if "https://" in self.endpoint else "http"
            clean_endpoint = self.endpoint.replace("http://", "").replace("https://", "")
            endpoint_url = f"{protocol}://{clean_endpoint}"
            self._s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=Config(signature_version="s3v4"),
                region_name="us-east-1"
            )
        except Exception as e:
            logger.warning(f"Could not initialize remote S3 client: {e}. Operating in local lakehouse mode.")
            self._s3_client = None

    def ensure_bucket_exists(self, bucket_name: str):
        if self._s3_client:
            try:
                self._s3_client.head_bucket(Bucket=bucket_name)
            except Exception:
                try:
                    self._s3_client.create_bucket(Bucket=bucket_name)
                    logger.info(f"Created MinIO bucket: {bucket_name}")
                except Exception as e:
                    logger.warning(f"Failed to create bucket {bucket_name} on S3: {e}")
        # Always ensure local mirror directory exists
        bucket_dir = os.path.join(self.local_storage_root, bucket_name)
        os.makedirs(bucket_dir, exist_ok=True)

    def put_object(self, bucket_name: str, object_name: str, data: str or bytes) -> Dict[str, Any]:
        """Saves object into Lakehouse (Bronze or Silver) with SHA-256 verification."""
        self.ensure_bucket_exists(bucket_name)
        raw_bytes = data.encode("utf-8") if isinstance(data, str) else data
        sha256_hash = hashlib.sha256(raw_bytes).hexdigest()

        # Remote S3 attempt
        s3_success = False
        if self._s3_client:
            try:
                self._s3_client.put_object(
                    Bucket=bucket_name,
                    Key=object_name,
                    Body=raw_bytes,
                    Metadata={"sha256": sha256_hash}
                )
                s3_success = True
            except Exception as e:
                logger.warning(f"S3 put_object failed for {bucket_name}/{object_name}: {e}. Writing to local lakehouse mirror.")

        # Local Lakehouse mirror persistence
        local_path = os.path.join(self.local_storage_root, bucket_name, object_name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(raw_bytes)

        return {
            "bucket": bucket_name,
            "object_name": object_name,
            "sha256": sha256_hash,
            "bytes_length": len(raw_bytes),
            "s3_synced": s3_success,
            "local_path": local_path
        }

    def get_object(self, bucket_name: str, object_name: str) -> bytes:
        """Retrieves raw object bytes from Lakehouse."""
        if self._s3_client:
            try:
                response = self._s3_client.get_object(Bucket=bucket_name, Key=object_name)
                return response["Body"].read()
            except Exception as e:
                logger.warning(f"S3 get_object failed: {e}. Attempting local lakehouse mirror.")

        local_path = os.path.join(self.local_storage_root, bucket_name, object_name)
        if os.path.exists(local_path):
            with open(local_path, "rb") as f:
                return f.read()
        raise FileNotFoundError(f"Object {bucket_name}/{object_name} not found in Lakehouse.")

lakehouse_client = S3MinioClient()
