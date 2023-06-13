FROM golang:1.19 AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY . .

RUN go mod tidy
WORKDIR /build/distance_averager

RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/distance_averager

# Deploy the application binary into a lean image
FROM busybox:latest
COPY --from=builder /bin/distance_averager /distance_averager
ENTRYPOINT ["/bin/sh"]