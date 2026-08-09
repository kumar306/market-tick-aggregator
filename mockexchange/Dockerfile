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
COPY mockexchange ./mockexchange

WORKDIR /src/mockexchange
RUN go build -o /out/mockexchange .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
WORKDIR /app/mockexchange

COPY --from=builder /out/mockexchange /app/mockexchange/mockexchange

EXPOSE 8081

CMD ["./mockexchange"]
