FROM golang:alpine AS builder

WORKDIR /app
COPY go.mod main.go ./
RUN go mod tidy
RUN go build -o log-agent .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/log-agent .
EXPOSE 8081
CMD ["./log-agent"]
