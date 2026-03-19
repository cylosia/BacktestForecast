#!/usr/bin/env bash
# Verifies that Alembic has exactly one migration head (no branches).
# Run: bash scripts/check_migration_heads.sh
set -euo pipefail

cd "$(dirname "$0")/.."

HEAD_COUNT=$(python -c "
import os, re
versions_dir = 'alembic/versions'
revisions = {}
for f in os.listdir(versions_dir):
    if not f.endswith('.py') or f.startswith('__'):
        continue
    path = os.path.join(versions_dir, f)
    content = open(path).read()
    rev_match = re.search(r'revision\s*=\s*[\"\\']([^\"\\']*)[\"\\'']', content)
    down_match = re.search(r'down_revision\s*=\s*[\"\\']([^\"\\']*)[\"\\'']', content)
    if rev_match:
        rev = rev_match.group(1)
        down = down_match.group(1) if down_match else None
        revisions[rev] = down

all_downs = set(v for v in revisions.values() if v)
heads = [r for r in revisions if r not in all_downs]
print(len(heads))
")

if [ "$HEAD_COUNT" -eq 1 ]; then
  echo "✓ Alembic has exactly 1 migration head."
  exit 0
elif [ "$HEAD_COUNT" -eq 0 ]; then
  echo "✗ No migration heads found. Check alembic/versions/."
  exit 1
else
  echo "✗ Alembic has $HEAD_COUNT migration heads (expected 1)."
  echo "  This means there are branching migrations that need to be resolved."
  echo "  Run: alembic heads"
  exit 1
fi
