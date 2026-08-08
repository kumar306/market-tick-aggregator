FROM node:20-alpine AS deps
WORKDIR /app/ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci

FROM node:20-alpine AS builder
WORKDIR /app/ui
COPY --from=deps /app/ui/node_modules ./node_modules
COPY ui ./

# aws load balancer url for hitting backend - its baked in at build time into js bundles
ENV NEXT_PUBLIC_API_BASE_URL=http://aba782aab926e47b49c041b7bc02e42c-1313401351.us-east-1.elb.amazonaws.com:8080
ENV NEXT_PUBLIC_WS_URL=ws://aba782aab926e47b49c041b7bc02e42c-1313401351.us-east-1.elb.amazonaws.com:8080/ws

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
