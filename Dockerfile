FROM golang:1.25.1-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/golang-es .

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/golang-es /app/golang-es
COPY data /app/data

ENV ES_URL=http://elasticsearch:9200
ENV ELASTICSEARCH_URL=http://elasticsearch:9200

ENTRYPOINT ["/app/golang-es"]
