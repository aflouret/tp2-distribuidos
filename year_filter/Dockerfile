FROM golang:1.19 AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY . .

RUN go mod tidy
WORKDIR /build/year_filter

RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/year_filter

# Deploy the application binary into a lean image
FROM busybox:latest
COPY --from=builder /bin/year_filter /year_filter
ENTRYPOINT ["/bin/sh"]