# TODO: Pin base images to SHA digests for reproducible builds in production.
# Example: FROM python:3.12-slim@sha256:<digest> AS builder
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

COPY pyproject.toml README.md /build/
COPY apps/worker /build/apps/worker
COPY src /build/src
COPY alembic /build/alembic
COPY alembic.ini /build/alembic.ini

COPY constraints.txt /build/constraints.txt
RUN pip install --upgrade pip && pip install --constraint constraints.txt ".[s3,sentry]"

FROM python:3.12-slim AS runner

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /build /app

RUN addgroup --system app && adduser --system --ingroup app app
USER app

STOPSIGNAL SIGTERM

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD celery -A apps.worker.app.celery_app.celery_app status --timeout 5 || exit 1

CMD ["sh", "-c", "exec celery -A apps.worker.app.celery_app.celery_app worker --loglevel=INFO --queues=research,exports,maintenance,pipeline --concurrency=${CELERY_CONCURRENCY:-4}"]
