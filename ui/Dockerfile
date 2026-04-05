FROM node:20-alpine AS deps
WORKDIR /app/ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci

FROM node:20-alpine AS builder
WORKDIR /app/ui
COPY --from=deps /app/ui/node_modules ./node_modules
COPY ui ./

ENV NEXT_PUBLIC_API_BASE_URL=http://localhost:8080
ENV NEXT_PUBLIC_WS_URL=ws://localhost:8080/ws

RUN npm run build

FROM node:20-alpine AS runner
WORKDIR /app/ui
ENV NODE_ENV=production
ENV PORT=3000

COPY --from=builder /app/ui/.next/standalone ./
COPY --from=builder /app/ui/.next/static ./.next/static
COPY --from=builder /app/ui/public ./public

EXPOSE 3000

CMD ["node", "server.js"]
