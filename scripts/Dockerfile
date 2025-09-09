FROM golang:1.21-alpine AS builder

RUN apk add --no-cache git make

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o metrics-pipeline ./cmd/main.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/metrics-pipeline .
COPY --from=builder /app/config/config.yaml ./config.yaml

EXPOSE 4317 9201

CMD ["./metrics-pipeline", "-config", "/app/config.yaml"]