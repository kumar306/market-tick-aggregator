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

WORKDIR /src/orderbook
RUN go build -o /out/orderbook .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
WORKDIR /app/orderbook

COPY --from=builder /out/orderbook /app/orderbook/orderbook
COPY orderbook/config ./config

EXPOSE 2115

CMD ["./orderbook"]
