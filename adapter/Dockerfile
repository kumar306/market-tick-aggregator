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

WORKDIR /src/adapter
RUN go build -o /out/adapter .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
WORKDIR /app/adapter

COPY --from=builder /out/adapter /app/adapter/adapter
COPY adapter/config ./config

EXPOSE 2112

CMD ["./adapter"]
