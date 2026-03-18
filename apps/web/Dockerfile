# TODO: Pin base images to SHA digests for reproducible builds in production.
# Example: FROM node:20-alpine@sha256:<digest> AS builder
FROM node:20-alpine AS builder

WORKDIR /app
RUN corepack enable

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.base.json .prettierrc.json /app/
COPY apps/web/package.json /app/apps/web/package.json
COPY packages/api-client/package.json /app/packages/api-client/package.json

RUN echo "node-linker=hoisted" > .npmrc && \
    pnpm install --frozen-lockfile --filter @backtestforecast/web...

COPY apps/web /app/apps/web
COPY packages/api-client /app/packages/api-client

# IMPORTANT: NEXT_PUBLIC_* values are baked into the static JavaScript bundle
# at build time. A separate Docker image must be built for each environment
# (staging, production) with the correct values.
ARG NEXT_PUBLIC_APP_URL
ARG NEXT_PUBLIC_API_BASE_URL
ARG NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY
ARG NEXT_PUBLIC_CLERK_SIGN_IN_URL
ARG NEXT_PUBLIC_CLERK_SIGN_IN_FALLBACK_REDIRECT_URL
ARG NEXT_PUBLIC_CLERK_SIGN_UP_FALLBACK_REDIRECT_URL

RUN pnpm --filter @backtestforecast/web run build

FROM node:20-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production

RUN addgroup --system app && adduser --system --ingroup app app

COPY --from=builder --chown=app:app /app/apps/web/.next/standalone ./
COPY --from=builder --chown=app:app /app/apps/web/.next/static ./apps/web/.next/static
COPY --from=builder --chown=app:app /app/apps/web/public ./apps/web/public

USER app

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD wget -q --spider http://localhost:3000/ || exit 1

CMD ["node", "apps/web/server.js"]
