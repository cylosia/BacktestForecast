FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

COPY pyproject.toml README.md /build/
COPY apps/__init__.py /build/apps/
COPY apps/api /build/apps/api
COPY apps/worker /build/apps/worker
COPY src /build/src
COPY alembic /build/alembic
COPY alembic.ini /build/alembic.ini

# For reproducible builds, use: pip install --constraint constraints.txt ".[s3]"
RUN pip install --upgrade pip && pip install ".[s3]"

FROM python:3.12-slim AS runner

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /build /app

RUN addgroup --system app && adduser --system --ingroup app app \
    && mkdir -p /tmp/prometheus_multiproc && chown app:app /tmp/prometheus_multiproc
USER app

EXPOSE 8000

STOPSIGNAL SIGTERM

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; r = urllib.request.urlopen('http://localhost:8000/health/live'); exit(0 if r.getcode() == 200 else 1)"

CMD ["uvicorn", "apps.api.app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4", "--proxy-headers", "--timeout-graceful-shutdown", "30"]
