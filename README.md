<p align="center"><img src=".github/hero.svg" alt="amqp" width="880"></p>

# Hanzo AMQP

AMQP 0-9-1 on the platform bus: point a standard RabbitMQ client at `:5672` and
it works unchanged, sharing one bus with everything NATS-native beside it.

```
your client --AMQP 0-9-1--> hanzo-amqp --JetStream--> Hanzo PubSub
```

## The map

One table is the whole design. A routing key and a NATS subject are the same
idea — dot-separated words matched by wildcards — so most of AMQP translates
rather than being emulated.

| AMQP | JetStream |
|---|---|
| exchange + routing key | subject `amqp.<exchange>.<routing key>` |
| queue | a durable pull consumer on stream `AMQP` |
| binding | a filter subject on that consumer |
| `basic.ack` / `nack` / `reject` | `Ack` / `Nak` / `Term` |
| `basic.qos` prefetch | outstanding deliveries the gateway will hand out |
| exchanges, queues, bindings | a KV bucket every replica watches |

The default exchange is the subject word `_`; an empty routing key is spelled
by leaving the word off. Exchange and queue names are ONE subject word
(`[A-Za-z0-9_-]`), because a dotted name would open extra subject levels and
alias another exchange — dots belong in routing keys, which is where AMQP's own
hierarchy lives. A dotted name is refused at declare, never mis-routed.

Retention is the stream's; delivery is the consumer's. Many queues read one
stream at their own positions, which is what makes fanout free, and a message
leaves a queue when THAT queue's consumer acks it.

## What works

Connect · channels · `exchange.declare`/`delete` (direct, fanout, topic) ·
`queue.declare`/`bind`/`unbind`/`purge`/`delete`, server-named, exclusive and
auto-delete · `basic.publish` with the full property set · `basic.consume` /
`cancel` / `deliver` · `ack` / `nack` / `reject` / `recover` · `basic.get` ·
`basic.qos` prefetch · publisher confirms · the `mandatory` return · heartbeats
· bodies larger than frame-max.

## What does not, and says so

Headers exchanges, transactions (`tx.*`), byte-counted qos, `immediate`,
`channel.flow(false)`, exchange-to-exchange binding, and `#` anywhere but the
end of a binding key. Each answers with the AMQP error the spec defines for it,
on the channel, naming the method — so a client learns it asked for something
absent. Nothing is acknowledged and dropped.

## Run it

```bash
go run ./cmd/amqp --pubsub-url nats://127.0.0.1:4222 --addr 0.0.0.0:5672
```

Readiness is whether `:5672` accepts, which is what a TCP probe asks and what
every client asks. There is no second HTTP answer to the same question.

Inside Hanzo Cloud it is not a separate process at all: `apps/amqp` mounts this
package over the PubSub the host already runs, so the AMQP door and the NATS,
Kafka and MQ doors are all one bus.

## Licensing

`MIT OR Apache-2.0`, at your option — per HIP-0137 (`hanzoai/hips`,
`HIPs/hip-0137-one-license.md`).
