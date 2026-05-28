# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:alpine AS builder
WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 produces a fully static binary that runs cleanly on Alpine
RUN CGO_ENABLED=0 go build -o server ./cmd/server

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/server .
ENTRYPOINT ["./server"]