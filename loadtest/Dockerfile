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
COPY loadtest ./loadtest

WORKDIR /src/loadtest
RUN go build -o /out/loadtest .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
WORKDIR /app/loadtest

COPY --from=builder /out/loadtest /app/loadtest/loadtest

CMD ["./loadtest"]
