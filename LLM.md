# amqp

# Hanzo AMQP

RabbitMQ wire protocol (AMQP 0-9-1) adapter for Hanzo PubSub (NATS JetStream). Accepts standard AMQP clients and translates requests to NATS underneath. Built for the AML AML/fraud sidecar, which requires RabbitMQ but runs inside Hanzo infrastructure backed by NATS.

## Architecture
