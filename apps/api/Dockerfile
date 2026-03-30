# Pin base images to SHA digests for reproducible builds.
# To update: docker pull python:3.12.8-slim && docker inspect --format='{{index .RepoDigests 0}}' python:3.12.8-slim
ARG PYTHON_BASE_DIGEST=sha256:2578148b0dca1066e91384ed13dee56cea3ccca234513f3af907bd7d92ce554a
FROM python:3.12.8-slim@${PYTHON_BASE_DIGEST} AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

# NOTE: Ideally we'd COPY only dependency metadata first, install deps, then
# COPY source code for better layer caching.  However, setuptools needs the
# full source tree to discover packages, so we must copy everything before
# pip install.  Changing to a build backend that supports PEP 660 editable
# installs with --no-build-isolation would allow splitting these steps.
COPY pyproject.toml README.md /build/
COPY apps/__init__.py /build/apps/
COPY apps/api /build/apps/api
# Required: dispatch.py imports celery_app for task routing
COPY apps/worker /build/apps/worker
COPY src /build/src
COPY alembic /build/alembic
COPY alembic.ini /build/alembic.ini

COPY constraints.txt /build/constraints.txt
RUN pip install --upgrade "pip>=24.0,<25.0" && pip install --constraint constraints.txt ".[s3,sentry]"

FROM python:3.12.8-slim@${PYTHON_BASE_DIGEST} AS runner

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
  CMD python -c "import urllib.request; r = urllib.request.urlopen('http://localhost:8000/health/ready'); exit(0 if r.getcode() == 200 else 1)"

CMD ["sh", "-c", "exec uvicorn apps.api.app.main:app --host 0.0.0.0 --port 8000 --workers ${UVICORN_WORKERS:-4} --proxy-headers --timeout-graceful-shutdown 30 --limit-max-requests ${UVICORN_MAX_REQUESTS:-10000}"]
