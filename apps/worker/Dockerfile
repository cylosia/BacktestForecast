FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY apps/worker /app/apps/worker
COPY src /app/src

RUN pip install --upgrade pip && pip install ".[s3]"

RUN addgroup --system app && adduser --system --ingroup app app
USER app

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD celery -A apps.worker.app.celery_app.celery_app inspect ping --timeout=5 || exit 1

CMD ["celery", "-A", "apps.worker.app.celery_app.celery_app", "worker", "--loglevel=INFO", "--queues=research,exports,maintenance,pipeline", "--concurrency=4"]
