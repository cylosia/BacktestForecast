from __future__ import annotations

import time
from typing import Protocol
from uuid import UUID

import structlog

from backtestforecast.config import Settings
from backtestforecast.errors import ConfigurationError

logger = structlog.get_logger("exports.storage")


class ExportStorage(Protocol):
    """Interface for persisting and retrieving export file content."""

    def put(self, export_job_id: UUID, content: bytes, file_name: str) -> str:
        """Store *content* and return a storage key (opaque string).

        For DB storage the key is simply the stringified job id.
        For S3 storage the key is the object key.
        """
        ...

    def get(self, storage_key: str) -> bytes:
        """Retrieve previously-stored content by its key."""
        ...

    def delete(self, storage_key: str) -> None:
        """Remove stored content (best-effort)."""
        ...

    def exists(self, storage_key: str) -> bool:
        """Check if content exists at the given key."""
        ...


class DatabaseStorage:
    """Stores export content in the ExportJob.content_bytes DB column (default)."""

    def put(self, export_job_id: UUID, content: bytes, file_name: str) -> str:
        return str(export_job_id)

    def get(self, storage_key: str) -> bytes:
        raise RuntimeError(
            "DatabaseStorage.get() must not be called directly. "
            "Content is accessed via the ExportJob.content_bytes ORM column."
        )

    def delete(self, storage_key: str) -> None:
        pass

    def exists(self, storage_key: str) -> bool:
        """Content is in the DB row; check cannot verify without a session.

        Callers should verify ``ExportJob.content_bytes is not None`` before
        streaming.  Returns ``bool(storage_key)`` as a basic sanity check.
        """
        return bool(storage_key)


def _retry(fn, max_attempts=3, base_delay=0.5):
    from botocore.exceptions import ClientError, EndpointConnectionError, ConnectionClosedError
    for attempt in range(max_attempts):
        try:
            return fn()
        except (EndpointConnectionError, ConnectionClosedError, OSError):
            if attempt == max_attempts - 1:
                raise
            time.sleep(base_delay * (2 ** attempt))
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("RequestTimeout", "RequestTimeoutException", "SlowDown", "InternalError", "ServiceUnavailable"):
                if attempt == max_attempts - 1:
                    raise
                time.sleep(base_delay * (2 ** attempt))
            else:
                raise


class S3Storage:
    """Stores export content in an S3-compatible object store."""

    def __init__(self, settings: Settings) -> None:
        import boto3  # type: ignore[import-untyped]

        self._bucket = settings.s3_bucket or ""
        if not self._bucket:
            raise ConfigurationError("S3_BUCKET must be set when using S3 storage.")
        self._prefix = "exports/"
        from botocore.config import Config as BotoConfig
        self._client = boto3.client(
            "s3",
            region_name=settings.s3_region,
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            config=BotoConfig(
                connect_timeout=10,
                read_timeout=30,
                retries={"max_attempts": 2, "mode": "standard"},
            ),
        )

    @staticmethod
    def _sanitize_file_name(file_name: str) -> str:
        import posixpath
        name = posixpath.basename(file_name.replace("\\", "/"))
        name = name.lstrip(".")
        return name or "export"

    def _object_key(self, export_job_id: UUID, file_name: str) -> str:
        safe_name = self._sanitize_file_name(file_name)
        return f"{self._prefix}{export_job_id}/{safe_name}"

    def _guess_content_type(self, file_name: str) -> str:
        ext = file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
        return {
            "csv": "text/csv",
            "json": "application/json",
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "pdf": "application/pdf",
        }.get(ext, "application/octet-stream")

    def put(self, export_job_id: UUID, content: bytes, file_name: str) -> str:
        key = self._object_key(export_job_id, file_name)
        content_type = self._guess_content_type(file_name)
        _retry(lambda: self._client.put_object(Bucket=self._bucket, Key=key, Body=content, ContentType=content_type))
        logger.info("s3.put", bucket=self._bucket, key=key, size=len(content))
        return key

    def get(self, storage_key: str) -> bytes:
        resp = _retry(lambda: self._client.get_object(Bucket=self._bucket, Key=storage_key))
        body = resp["Body"]
        try:
            data: bytes = body.read()
        finally:
            body.close()
        logger.info("s3.get", bucket=self._bucket, key=storage_key, size=len(data))
        return data

    def delete(self, storage_key: str) -> None:
        try:
            _retry(lambda: self._client.delete_object(Bucket=self._bucket, Key=storage_key))
            logger.info("s3.delete", bucket=self._bucket, key=storage_key)
        except Exception:
            logger.warning("s3.delete_failed", bucket=self._bucket, key=storage_key, exc_info=True)
            raise

    def exists(self, storage_key: str) -> bool:
        def _head():
            self._client.head_object(Bucket=self._bucket, Key=storage_key)
        try:
            _retry(_head)
            return True
        except Exception as e:
            from botocore.exceptions import ClientError
            if isinstance(e, ClientError) and e.response["Error"]["Code"] == "404":
                return False
            raise


def get_export_storage(settings: Settings) -> ExportStorage:
    """Return S3Storage when an S3 bucket is configured, otherwise DatabaseStorage."""
    if settings.s3_bucket:
        logger.info("export_storage.using_s3", bucket=settings.s3_bucket)
        return S3Storage(settings)
    logger.info("export_storage.using_database")
    return DatabaseStorage()
