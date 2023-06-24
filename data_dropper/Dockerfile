FROM golang:1.19 AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY . .

RUN go mod tidy
WORKDIR /build/data_dropper

RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/data_dropper

# Deploy the application binary into a lean image
FROM busybox:latest
COPY --from=builder /bin/data_dropper /data_dropper
ENTRYPOINT ["/bin/sh"]