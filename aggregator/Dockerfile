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

WORKDIR /src/aggregator
RUN go build -o /out/aggregator .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
WORKDIR /app/aggregator

COPY --from=builder /out/aggregator /app/aggregator/aggregator
COPY aggregator/config ./config

EXPOSE 2114

CMD ["./aggregator"]
