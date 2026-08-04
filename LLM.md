# amqp

# Hanzo AMQP

RabbitMQ wire protocol (AMQP 0-9-1) adapter for Hanzo PubSub (NATS JetStream). Accepts standard AMQP clients and translates requests to NATS underneath. Built for the AML AML/fraud sidecar, which requires RabbitMQ but runs inside Hanzo infrastructure backed by NATS.

## Architecture

## Licensing

`MIT OR Apache-2.0`, at your option — per HIP-0137 (`hanzoai/hips`, `HIPs/hip-0137-one-license.md`). Relicensed from BSD-3-Clause,
which HIP-0137 puts out of scope for `hanzoai`. This is original Hanzo work: the
AMQP 0-9-1 framing and the NATS JetStream translation were written here, not
forked from `streadway/amqp` or any other AMQP library, so there is no upstream
licence to inherit.
