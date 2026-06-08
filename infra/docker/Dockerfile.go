# Build stage — compiles the Go binary
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy dependency files first — Docker caches this layer
# so dependencies only re-download when go.mod changes
COPY go/go.mod go/go.sum ./
RUN go mod download

# Copy source and build
COPY go/ .
RUN go build -o /node ./cmd/node

# Runtime stage — minimal image with just the binary
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /app
COPY --from=builder /node .

ENTRYPOINT ["./node"]