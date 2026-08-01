# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26.5-alpine AS builder
# go.mod pins the toolchain. The golang base image sets GOTOOLCHAIN=local,
# which turns a `go` directive newer than the image into a hard build
# failure instead of a download.
ENV GOTOOLCHAIN=auto

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN apk add --no-cache git

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o /hanzo-amqp .

FROM alpine:3.20

LABEL org.opencontainers.image.source="https://github.com/hanzoai/amqp"
LABEL org.opencontainers.image.description="Hanzo AMQP - RabbitMQ-compatible AMQP 0-9-1 gateway over NATS"
LABEL org.opencontainers.image.licenses="MIT"

RUN apk add --no-cache ca-certificates
COPY --from=builder /hanzo-amqp /usr/local/bin/hanzo-amqp

EXPOSE 5672

ENTRYPOINT ["hanzo-amqp"]
CMD ["--pubsub-url", "nats://pubsub:4222", "--amqp-addr", "0.0.0.0:5672"]
