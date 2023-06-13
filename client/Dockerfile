FROM golang:1.19 AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY . .

RUN go mod tidy
WORKDIR /build/client

RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/client

# Deploy the application binary into a lean image
FROM busybox:latest
COPY --from=builder /bin/client /client
ENTRYPOINT ["/bin/sh"]