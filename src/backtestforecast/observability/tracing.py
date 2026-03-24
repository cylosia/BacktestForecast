"""Optional OpenTelemetry tracing integration.

Initialises the OTel SDK when the ``OTEL_EXPORTER_OTLP_ENDPOINT`` environment
variable is set.  When the SDK is not installed or the env var is absent,
all public functions are safe no-ops - no application code needs to check
for availability.

Usage in application startup (e.g. ``main.py`` lifespan)::

    from backtestforecast.observability.tracing import init_tracing
    init_tracing(service_name="backtestforecast-api")

Usage in code that wants to create spans::

    from backtestforecast.observability.tracing import get_tracer
    tracer = get_tracer(__name__)

    with tracer.start_as_current_span("my_operation") as span:
        span.set_attribute("user.id", str(user_id))
        ...

If the OTel SDK is not installed, ``get_tracer`` returns a no-op tracer
that silently discards spans - no ``ImportError`` or runtime cost.

Dependencies (add to pyproject.toml ``[project.optional-dependencies]``)::

    otel = [
        "opentelemetry-api>=1.20",
        "opentelemetry-sdk>=1.20",
        "opentelemetry-exporter-otlp-proto-grpc>=1.20",
        "opentelemetry-instrumentation-fastapi>=0.44b0",
        "opentelemetry-instrumentation-sqlalchemy>=0.44b0",
        "opentelemetry-instrumentation-redis>=0.44b0",
        "opentelemetry-instrumentation-httpx>=0.44b0",
        "opentelemetry-instrumentation-celery>=0.44b0",
    ]
"""
from __future__ import annotations

import os
from typing import Any

import structlog

logger = structlog.get_logger("observability.tracing")

_TRACER_PROVIDER: Any = None


def init_tracing(
    *,
    service_name: str = "backtestforecast",
    service_version: str | None = None,
) -> bool:
    """Initialise OpenTelemetry tracing if the SDK is available and configured.

    Returns True if tracing was successfully initialised, False otherwise.
    Safe to call multiple times - subsequent calls are no-ops.
    """
    global _TRACER_PROVIDER

    if _TRACER_PROVIDER is not None:
        return True

    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        logger.debug("tracing.skipped", reason="OTEL_EXPORTER_OTLP_ENDPOINT not set")
        return False

    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        logger.info("tracing.skipped", reason="opentelemetry SDK not installed")
        return False

    if service_version is None:
        try:
            from backtestforecast.version import get_public_version

            service_version = get_public_version()
        except Exception:
            service_version = "unknown"

    resource = Resource.create({
        "service.name": service_name,
        "service.version": service_version,
        "deployment.environment": os.environ.get("APP_ENV", "development"),
    })

    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=endpoint, insecure="localhost" in endpoint)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _TRACER_PROVIDER = provider

    _auto_instrument()

    logger.info(
        "tracing.initialized",
        endpoint=endpoint,
        service_name=service_name,
        service_version=service_version,
    )
    return True


def _auto_instrument() -> None:
    """Apply auto-instrumentation for supported libraries."""
    _instrument_fastapi()
    _instrument_sqlalchemy()
    _instrument_redis()
    _instrument_httpx()
    _instrument_celery()


def _instrument_fastapi() -> None:
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        FastAPIInstrumentor.instrument()
        logger.debug("tracing.instrumented", library="fastapi")
    except ImportError:
        pass
    except Exception:
        logger.debug("tracing.instrument_failed", library="fastapi", exc_info=True)


def _instrument_sqlalchemy() -> None:
    try:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
        SQLAlchemyInstrumentor().instrument()
        logger.debug("tracing.instrumented", library="sqlalchemy")
    except ImportError:
        pass
    except Exception:
        logger.debug("tracing.instrument_failed", library="sqlalchemy", exc_info=True)


def _instrument_redis() -> None:
    try:
        from opentelemetry.instrumentation.redis import RedisInstrumentor
        RedisInstrumentor().instrument()
        logger.debug("tracing.instrumented", library="redis")
    except ImportError:
        pass
    except Exception:
        logger.debug("tracing.instrument_failed", library="redis", exc_info=True)


def _instrument_httpx() -> None:
    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        HTTPXClientInstrumentor().instrument()
        logger.debug("tracing.instrumented", library="httpx")
    except ImportError:
        pass
    except Exception:
        logger.debug("tracing.instrument_failed", library="httpx", exc_info=True)


def _instrument_celery() -> None:
    try:
        from opentelemetry.instrumentation.celery import CeleryInstrumentor
        CeleryInstrumentor().instrument()
        logger.debug("tracing.instrumented", library="celery")
    except ImportError:
        pass
    except Exception:
        logger.debug("tracing.instrument_failed", library="celery", exc_info=True)


def get_tracer(name: str = __name__) -> Any:
    """Return an OTel tracer, or a no-op tracer if the SDK is not available."""
    try:
        from opentelemetry import trace
        return trace.get_tracer(name)
    except ImportError:
        return _NoOpTracer()


class _NoOpSpan:
    """Drop-in span replacement when OTel is not installed."""
    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def set_status(self, *args: Any, **kwargs: Any) -> None:
        pass

    def record_exception(self, exc: BaseException, **kwargs: Any) -> None:
        pass

    def __enter__(self) -> _NoOpSpan:
        return self

    def __exit__(self, *args: Any) -> None:
        pass


class _NoOpTracer:
    """Drop-in tracer replacement when OTel is not installed."""
    def start_as_current_span(self, name: str, **kwargs: Any) -> _NoOpSpan:
        return _NoOpSpan()

    def start_span(self, name: str, **kwargs: Any) -> _NoOpSpan:
        return _NoOpSpan()
