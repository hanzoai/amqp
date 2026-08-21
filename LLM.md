# amqp

AMQP 0-9-1 over Hanzo PubSub (NATS JetStream). Read README.md first — the
mapping table there IS the design.

## Layout

- `wire/` — the octet layer. Frames (`frame.go`) and the typed values a frame
  body carries (`value.go`: field tables, content properties). Knows the
  format, nothing about meaning. `EncodeProps`/`DecodeProps` serve both the
  content header and the bus, so a message's properties have one encoding.
- `protocol/` — the semantics.
  - `route.go` — the whole AMQP↔JetStream map. Start here.
  - `topology.go` — exchanges, queues, bindings in KV bucket `amqp`, mirrored
    per replica through a watch.
  - `broker.go` — listener, stream, per-queue consumers, `Serve`/`Ready`/`Shutdown`.
  - `session.go` — one connection: handshake, frame loop, faults.
  - `channel.go` — one channel: every method, publish assembly, delivery.
- `cmd/amqp/` — process lifecycle and nothing else.

## Things that will bite you

- **`Ready()`, not a sleep.** `Serve` blocks; everything that can fail has
  failed by the time `Ready` closes. A caller that must fail closed waits on it
  against a deadline.
- **A queue IS its consumer.** `queue.declare` creates the JetStream consumer,
  with `DeliverNew`. That is what makes a new queue start empty instead of
  replaying the stream's history.
- **Prefetch is enforced by a bucket in `channel.go`, not by the pull batch.**
  `PullMaxMessages` is a fetch size, not a ceiling on what is outstanding.
- **Delivery tags must arrive in order** or `basic.ack(multiple)` acks the
  wrong set — hence `channel.deliver`, held across the socket write. The state
  lock is released before that write, so a client that stops reading cannot
  stop its own acks from being processed.
- **A channel exception must not kill the connection.** After sending
  `channel.close` the session ignores that channel until `close-ok`
  (`session.closing`); the peer had frames in flight.
- **`wire.Reader` enforces frame-max.** The size field is 32 bits: without it,
  seven octets from a stranger ask for a 4 GiB allocation. A content HEADER is
  the same shape of claim one level up, so a declared body size is refused
  against the bus's own `MaxPayload` before a byte of it is read — the gateway
  names no second number.
- **Exclusive and auto-delete name DIFFERENT events, and conflating them deletes
  other people's queues.** Exclusive belongs to the connection that declared it
  (`Broker.claim`, refused elsewhere with 405); auto-delete dies when its LAST
  CONSUMER goes (`channel.reap`, off the consumer count). Making both die with
  the connection made two connections co-owners of one exclusive queue, so
  either one's close took the other's.
- **Exclusivity is enforced within ONE gateway.** A replica cannot see another
  replica's sockets, so two of them can each hold what the other thinks is
  exclusive. Say so; do not imply a guarantee the shape cannot make.

## Tests

`protocol/amqp_test.go` drives `github.com/rabbitmq/amqp091-go` — the reference
client, unmodified — against a real broker over an embedded NATS. That is the
only test that means anything for a wire protocol; a hand-rolled client would
agree with a hand-rolled server about a shared misreading of the spec. Add to
it before you add anywhere else.

```
GOWORK=off go test ./... -race
```

## Licensing

`MIT OR Apache-2.0`, at your option — per HIP-0137. Original Hanzo work: the
AMQP 0-9-1 framing and the JetStream translation were written here, not forked
from `streadway/amqp` or any other AMQP library, so there is no upstream
licence to inherit.
