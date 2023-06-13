FROM golang:1.19 AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY . .

RUN go mod tidy
WORKDIR /build/duration_averager

RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/duration_averager

# Deploy the application binary into a lean image
FROM busybox:latest
COPY --from=builder /bin/duration_averager /duration_averager
ENTRYPOINT ["/bin/sh"]