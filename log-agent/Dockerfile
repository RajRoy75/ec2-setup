FROM golang:1.24.0-alpine AS builder

WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY main.go ./
RUN go build -o log-agent .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/log-agent .
EXPOSE 8081
CMD ["./log-agent"]
