# Pin base images to SHA digests for reproducible builds.
# To update: docker pull python:3.12.8-slim && docker inspect --format='{{index .RepoDigests 0}}' python:3.12.8-slim
ARG PYTHON_BASE_DIGEST=sha256:2578148b0dca1066e91384ed13dee56cea3ccca234513f3af907bd7d92ce554a
FROM python:3.12.8-slim@${PYTHON_BASE_DIGEST} AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

COPY pyproject.toml README.md /build/
COPY apps/__init__.py /build/apps/
COPY apps/worker /build/apps/worker
COPY src /build/src
COPY alembic /build/alembic
COPY alembic.ini /build/alembic.ini

COPY constraints.txt /build/constraints.txt
RUN pip install --upgrade "pip>=24.0,<25.0" && pip install --constraint constraints.txt ".[s3,sentry]"

FROM python:3.12.8-slim@${PYTHON_BASE_DIGEST} AS runner

# PROMETHEUS_MULTIPROC_DIR is not set because the worker runs its own
# single-process HTTP metrics server, not the multiprocess Prometheus
# client used by the API.
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
  CMD celery -A apps.worker.app.celery_app.celery_app inspect ping --timeout 5 || exit 1

EXPOSE 9090

CMD ["sh", "-c", "exec celery -A apps.worker.app.celery_app.celery_app worker --loglevel=INFO --queues=research,exports,maintenance,pipeline --concurrency=${CELERY_CONCURRENCY:-4}"]
