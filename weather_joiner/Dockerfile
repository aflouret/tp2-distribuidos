FROM golang:1.19 AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY . .

RUN go mod tidy
WORKDIR /build/weather_joiner

RUN CGO_ENABLED=0 GOOS=linux GOGC=off go build -o /bin/weather_joiner

# Deploy the application binary into a lean image
FROM busybox:latest
COPY --from=builder /bin/weather_joiner /weather_joiner
ENTRYPOINT ["/bin/sh"]