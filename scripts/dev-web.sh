#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f "apps/web/.env.local" ]]; then
  echo "Missing apps/web/.env.local"
  echo "Copy apps/web/.env.example to apps/web/.env.local first."
  exit 1
fi

set -a
source apps/web/.env.local
set +a

pnpm --filter @backtestforecast/web exec next dev --hostname 0.0.0.0 --port "${WEB_PORT:-3000}"
