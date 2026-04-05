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

WORKDIR /src/ui-backend
RUN go build -o /out/ui-backend .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
WORKDIR /app/ui-backend

COPY --from=builder /out/ui-backend /app/ui-backend/ui-backend
COPY ui-backend/config ./config

EXPOSE 8080

CMD ["./ui-backend"]
