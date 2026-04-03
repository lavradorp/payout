FROM golang:1.26.1-trixie AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o api-binary ./cmd/main.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/api-binary .

EXPOSE 8080

CMD ["./api-binary"]