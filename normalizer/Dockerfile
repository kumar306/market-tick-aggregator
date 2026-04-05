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

WORKDIR /src/normalizer
RUN go build -o /out/normalizer .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
WORKDIR /app/normalizer

COPY --from=builder /out/normalizer /app/normalizer/normalizer
COPY normalizer/config ./config

EXPOSE 2113

CMD ["./normalizer"]
