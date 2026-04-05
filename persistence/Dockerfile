FROM golang:1.25.3-alpine AS builder
WORKDIR /src

COPY go.work go.work.sum ./
COPY adapter ./adapter
COPY aggregator ./aggregator
COPY normalizer ./normalizer
COPY orderbook ./orderbook
COPY persistence ./persistence
COPY shared ./shared
COPY ui-backend ./ui-backend

WORKDIR /src/persistence
RUN go build -o /out/persistence .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
WORKDIR /app/persistence

COPY --from=builder /out/persistence /app/persistence/persistence
COPY persistence/config ./config
COPY persistence/db/schema ./db/schema

EXPOSE 2116

CMD ["./persistence"]
