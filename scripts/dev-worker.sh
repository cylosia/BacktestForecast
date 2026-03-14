#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f "apps/api/.env" ]]; then
  echo "Missing apps/api/.env"
  echo "Copy apps/api/.env.example to apps/api/.env first."
  exit 1
fi

set -a
source apps/api/.env
set +a

celery -A apps.worker.app.celery_app.celery_app worker \
  --loglevel=INFO \
  --queues=research,exports,maintenance,pipeline \
  --max-tasks-per-child=200
