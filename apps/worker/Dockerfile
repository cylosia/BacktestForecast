FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

COPY pyproject.toml README.md /build/
COPY apps/worker /build/apps/worker
COPY src /build/src

# For reproducible builds, use: pip install --constraint constraints.txt ".[s3]"
RUN pip install --upgrade pip && pip install ".[s3]"

FROM python:3.12-slim AS runner

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /build /app

RUN addgroup --system app && adduser --system --ingroup app app
USER app

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD celery -A apps.worker.app.celery_app.celery_app status --timeout 5 || exit 1

CMD ["sh", "-c", "exec celery -A apps.worker.app.celery_app.celery_app worker --loglevel=INFO --queues=research,exports,maintenance,pipeline --concurrency=${CELERY_CONCURRENCY:-4}"]
