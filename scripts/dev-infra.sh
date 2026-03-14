#!/usr/bin/env bash
set -euo pipefail

if ! command -v docker &>/dev/null; then
  echo "ERROR: docker is not installed or not in PATH."
  exit 1
fi

docker compose up -d postgres redis
echo ""
docker compose ps
