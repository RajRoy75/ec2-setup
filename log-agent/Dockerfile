FROM golang:alpine AS builder

WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download || true

COPY *.go ./
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o log-agent .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/log-agent .
EXPOSE 8081
CMD ["./log-agent"]
