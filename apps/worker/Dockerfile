FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY apps /app/apps
COPY src /app/src
COPY alembic /app/alembic
COPY alembic.ini /app/alembic.ini

RUN pip install --upgrade pip && pip install -e .

RUN addgroup --system app && adduser --system --ingroup app app
USER app

CMD ["celery", "-A", "apps.worker.app.celery_app.celery_app", "worker", "--loglevel=INFO", "--queues=research,market_data,exports,maintenance,pipeline"]
