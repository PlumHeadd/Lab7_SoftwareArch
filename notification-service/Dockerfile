FROM golang:1.23 AS builder
WORKDIR /app
COPY . .
RUN go mod tidy && go mod download
RUN CGO_ENABLED=0 go build -o notification-service .

FROM debian:bookworm-slim
WORKDIR /root/
COPY --from=builder /app/notification-service .
CMD ["./notification-service"]
