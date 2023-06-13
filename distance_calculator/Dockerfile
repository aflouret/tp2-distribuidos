FROM golang:1.19 AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY . .

RUN go mod tidy
WORKDIR /build/distance_calculator

RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/distance_calculator

# Deploy the application binary into a lean image
FROM busybox:latest
COPY --from=builder /bin/distance_calculator /distance_calculator
ENTRYPOINT ["/bin/sh"]