# @backtestforecast/api-client

Generated TypeScript types from the FastAPI OpenAPI schema.

## How it works

1. **Export** the OpenAPI spec from the running FastAPI app:

   ```bash
   python scripts/export_openapi.py > packages/api-client/openapi.json
   ```

2. **Generate** TypeScript types using `openapi-typescript`:

   ```bash
   pnpm --filter @backtestforecast/api-client generate
   ```

3. **Import** the generated types in the web app:

   ```typescript
   import type { paths, components } from "@backtestforecast/api-client";
   ```

## CI

The `backend-and-web` CI job runs `python scripts/export_openapi.py` as a smoke
check to ensure the OpenAPI schema can be exported without errors.  Once types
are actively consumed, add a CI step that regenerates and diffs to prevent drift.
