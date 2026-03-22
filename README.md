# Hanzo AMQP

RabbitMQ wire protocol (AMQP 0-9-1) adapter for Hanzo PubSub (NATS JetStream). Accepts standard AMQP clients and translates requests to NATS underneath. Built for the AML AML/fraud sidecar, which requires RabbitMQ but runs inside Hanzo infrastructure backed by NATS.

## Architecture

```
AML (C#) --AMQP 0-9-1--> hanzo-amqp --JetStream--> NATS
```

AML connects to this proxy on port 5672 thinking it is RabbitMQ. The proxy translates AMQP publish/consume operations to NATS JetStream subjects.

## Channel Mappings

| AMQP Name          | Type   | NATS Subject         | NATS Stream          |
|---------------------|--------|----------------------|----------------------|
| amlInbound        | queue  | aml.inbound         | aml-inbound         |
| amlOutbound       | fanout | aml.outbound        | aml-outbound        |
| amlActivations    | fanout | aml.activations     | aml-activations     |
| amlNotifications  | queue  | aml.notifications   | aml-notifications   |

## Quick Start

```bash
go run . --pubsub-url nats://localhost:4222 --amqp-addr 0.0.0.0:5672
```

## Kubernetes

In Hanzo infrastructure, this runs as a sidecar alongside AML:

```yaml
containers:
  - name: aml
    image: ghcr.io/hanzoai/aml:latest
    env:
      - name: AMQP_URL
        value: "amqp://localhost:5672"
  - name: amqp-proxy
    image: ghcr.io/hanzoai/amqp:latest
    args: ["--pubsub-url", "nats://pubsub.hanzo.svc:4222"]
    ports:
      - containerPort: 5672
```

## Running Tests

```bash
go test -v ./...
```

## Credits

Similar pattern to [Hanzo Stream](https://github.com/hanzoai/stream) (Kafka wire protocol gateway).

## License

MIT
