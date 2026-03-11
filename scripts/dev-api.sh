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

uvicorn apps.api.app.main:app --reload --host 0.0.0.0 --port "${API_PORT:-8000}"
