#!/usr/bin/env bash
# Verifies that the OpenAPI TypeScript types are in sync with the snapshot.
# Run: bash scripts/check_openapi_sync.sh
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Generating OpenAPI types from snapshot..."
pnpm --filter @backtestforecast/api-client generate

if git diff --quiet packages/api-client/src/schema.d.ts 2>/dev/null; then
  echo "✓ OpenAPI types are in sync."
  exit 0
else
  echo "✗ OpenAPI types are out of sync with openapi.snapshot.json"
  echo "  Run: pnpm --filter @backtestforecast/api-client generate"
  echo "  Then commit the updated schema.d.ts"
  git diff --stat packages/api-client/src/schema.d.ts
  exit 1
fi
