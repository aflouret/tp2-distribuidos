FROM golang:1.19 AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY . .

RUN go mod tidy
WORKDIR /build/distance_merger

RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/distance_merger

# Deploy the application binary into a lean image
FROM busybox:latest
COPY --from=builder /bin/distance_merger /distance_merger
ENTRYPOINT ["/bin/sh"]