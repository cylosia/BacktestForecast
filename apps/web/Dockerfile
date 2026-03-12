FROM node:20-alpine AS base

WORKDIR /app
RUN corepack enable

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.base.json .prettierrc.json /app/
COPY apps/web/package.json /app/apps/web/package.json
COPY packages/api-client/package.json /app/packages/api-client/package.json

RUN pnpm install --frozen-lockfile --filter @backtestforecast/web...

COPY apps/web /app/apps/web
COPY packages/api-client /app/packages/api-client

FROM base AS builder
WORKDIR /app

ARG NEXT_PUBLIC_APP_URL
ARG NEXT_PUBLIC_API_BASE_URL
ARG NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY
ARG NEXT_PUBLIC_CLERK_SIGN_IN_URL
ARG NEXT_PUBLIC_CLERK_SIGN_IN_FALLBACK_REDIRECT_URL
ARG NEXT_PUBLIC_CLERK_SIGN_UP_FALLBACK_REDIRECT_URL

RUN pnpm --filter @backtestforecast/web exec next build

FROM node:20-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
RUN corepack enable

RUN addgroup --system app && adduser --system --ingroup app app

COPY --from=builder --chown=app:app /app/apps/web/.next/standalone ./
COPY --from=builder --chown=app:app /app/apps/web/.next/static ./apps/web/.next/static
COPY --from=builder --chown=app:app /app/apps/web/public ./apps/web/public

USER app

EXPOSE 3000

CMD ["node", "apps/web/server.js"]
