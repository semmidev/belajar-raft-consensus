FROM golang:1.27-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o raft-server main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/raft-server .
EXPOSE 8080 12000
CMD ["./raft-server"]