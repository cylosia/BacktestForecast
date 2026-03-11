FROM node:20-alpine AS base

WORKDIR /app
RUN corepack enable

COPY package.json pnpm-workspace.yaml tsconfig.base.json .prettierrc.json /app/
COPY apps/web/package.json /app/apps/web/package.json

RUN pnpm install --filter @backtestforecast/web...

COPY apps/web /app/apps/web

FROM base AS builder
WORKDIR /app
RUN pnpm --filter @backtestforecast/web exec next build

FROM node:20-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
RUN corepack enable

COPY --from=builder /app/apps/web/.next/standalone ./
COPY --from=builder /app/apps/web/.next/static ./apps/web/.next/static
COPY --from=builder /app/apps/web/public ./apps/web/public

EXPOSE 3000

CMD ["node", "apps/web/server.js"]
