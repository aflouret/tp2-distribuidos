FROM golang:1.19 AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY . .

RUN go mod tidy
WORKDIR /build/stations_joiner

RUN CGO_ENABLED=0 GOOS=linux GOGC=off go build -o /bin/stations_joiner

# Deploy the application binary into a lean image
FROM busybox:latest
COPY --from=builder /bin/stations_joiner /stations_joiner
ENTRYPOINT ["/bin/sh"]