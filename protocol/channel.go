package protocol

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hanzoai/amqp/wire"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

// Props is how a message's AMQP properties ride the bus: the same octets a
// content header carries, base64'd because a NATS header value is text. One
// encoding, so the gateway reads back exactly what it wrote and a NATS-native
// subscriber still gets a clean body and a subject that says where it came
// from.
const propsHeader = "Amqp-Props"

// work bounds any single call to the bus made while a client waits on a reply.
const work = 10 * time.Second

// channel is one AMQP channel: its consumers, what it has delivered and not
// yet had acked, and the content it is in the middle of receiving.
type channel struct {
	s  *session
	id uint16

	// deliver serialises outbound content. Delivery tags must arrive in the
	// order they were assigned, or basic.ack(multiple) acks the wrong set —
	// and two consumers on one channel are two goroutines.
	deliver sync.Mutex

	mu        sync.Mutex
	prefetch  uint16
	global    bool
	shared    chan struct{} // the channel-wide prefetch bucket, when global
	confirm   bool
	seq       uint64 // publisher confirm sequence
	tag       uint64 // delivery tags
	unacked   map[uint64]delivery
	consumers map[string]*consumer
	pending   *content
}

// delivery is a message handed to the client and not yet settled: what to ack
// on the bus, and the prefetch slot it holds until it is.
type delivery struct {
	msg   jetstream.Msg
	slots chan struct{}
}

// consumer is one basic.consume: a tag, the queue behind it, the prefetch it
// was given, and the pull loop feeding it.
type consumer struct {
	tag   string
	queue string
	noAck bool
	slots chan struct{}
	pull  jetstream.ConsumeContext
}

// content is a publish being assembled. AMQP sends the method, then a header,
// then body frames, and other channels interleave freely — so the partial
// message belongs to the channel, never to the read loop.
type content struct {
	exchange, key string
	subject       string
	mandatory     bool
	props         wire.Props
	size          uint64
	body          []byte
	header        bool
}

func newChannel(s *session, id uint16) *channel {
	return &channel{
		s:         s,
		id:        id,
		unacked:   map[uint64]delivery{},
		consumers: map[string]*consumer{},
	}
}

// shutdown stops this channel's consumers and returns everything it holds
// unacked, so another consumer gets those messages instead of waiting out the
// ack deadline.
func (c *channel) shutdown() {
	c.mu.Lock()
	cons := make([]*consumer, 0, len(c.consumers))
	for _, v := range c.consumers {
		cons = append(cons, v)
	}
	c.consumers = map[string]*consumer{}
	held := make([]delivery, 0, len(c.unacked))
	for _, d := range c.unacked {
		held = append(held, d)
	}
	c.unacked = map[uint64]delivery{}
	c.mu.Unlock()

	for _, v := range cons {
		if v.pull != nil {
			v.pull.Stop()
		}
		c.s.b.detach(v.queue)
	}
	for _, d := range held {
		_ = d.msg.Nak()
	}
}

func (c *channel) frame(f wire.Frame, class, method uint16) error {
	switch f.Type {
	case wire.Method:
		return c.method(class, method, wire.NewArgs(f.Payload))
	case wire.Header:
		return c.head(f.Payload)
	case wire.Body:
		return c.chunk(f.Payload)
	default:
		return connFault(unexpectedFrame, 0, 0, "frame type %d is not carried on a channel", f.Type)
	}
}

func (c *channel) method(class, method uint16, a *wire.Args) error {
	// A method arriving mid-content means the peer's framing is broken; the
	// half-built message is not something to guess at (§4.2.6.1).
	c.mu.Lock()
	mid := c.pending != nil
	c.mu.Unlock()
	if mid && !(class == classBasic && method == basicPublish) {
		return connFault(unexpectedFrame, class, method, "method arrived before the content body was complete")
	}

	switch class {
	case classChannel:
		return c.channel(method, a)
	case classExchange:
		return c.exchange(method, a)
	case classQueue:
		return c.queue(method, a)
	case classBasic:
		return c.basic(method, a)
	case classConfirm:
		if method != confirmSelect {
			return absent(class, method, fmt.Sprintf("confirm method %d", method))
		}
		noWait := a.Bit()
		c.mu.Lock()
		c.confirm, c.seq = true, 0
		c.mu.Unlock()
		if noWait {
			return nil
		}
		return c.send(wire.NewMethod(classConfirm, confirmSelectOK))
	case classTx:
		return absent(class, method, "transactions; a publish is confirmed one at a time through confirm.select")
	default:
		return absent(class, method, fmt.Sprintf("class %d", class))
	}
}

func (c *channel) send(b *wire.Buf) error { return c.s.w.Send(b.Frame(c.id)) }

// ---------------------------------------------------------------------------
// channel class
// ---------------------------------------------------------------------------

func (c *channel) channel(method uint16, a *wire.Args) error {
	switch method {
	case chanClose:
		c.shutdown()
		c.s.forget(c.id)
		return c.send(wire.NewMethod(classChannel, chanCloseOK))
	case chanCloseOK:
		return nil
	case chanFlow:
		if !a.Bit() {
			return absent(classChannel, chanFlow,
				"channel.flow(false); pause a consumer with basic.cancel or bound it with basic.qos")
		}
		return c.send(wire.NewMethod(classChannel, chanFlowOK).Bit(true))
	default:
		return absent(classChannel, method, fmt.Sprintf("channel method %d", method))
	}
}

// ---------------------------------------------------------------------------
// exchange class
// ---------------------------------------------------------------------------

func (c *channel) exchange(method uint16, a *wire.Args) error {
	ctx, cancel := context.WithTimeout(context.Background(), work)
	defer cancel()

	switch method {
	case exDeclare:
		a.Short() // reserved
		name := a.ShortStr()
		kind := a.ShortStr()
		passive, durable := a.Bit(), a.Bit()
		a.Bit() // auto-delete: an exchange here holds no state to reclaim
		a.Bit() // internal
		noWait := a.Bit()
		a.Table() // arguments
		if err := a.Err(); err != nil {
			return connFault(syntaxError, classExchange, method, "exchange.declare: %v", err)
		}
		if err := Name(name); err != nil {
			return chanFault(notAllowed, classExchange, method, "PRECONDITION_FAILED - %v", err)
		}

		have, ok := c.s.b.top.Exchange(name)
		if passive {
			if !ok {
				return chanFault(notFound, classExchange, method, "NOT_FOUND - no exchange %q", name)
			}
		} else {
			if kind == "" {
				kind = Direct
			}
			// Asked here, on the channel that declared it, so a kind with no
			// subject to be is an answer rather than a surprise at the first
			// publish.
			if err := Kind(kind); err != nil {
				return absent(classExchange, method, err.Error())
			}
			if ok && have.Kind != kind {
				return chanFault(preconditionFailed, classExchange, method,
					"PRECONDITION_FAILED - exchange %q is a %s exchange, redeclared as %s", name, have.Kind, kind)
			}
			if !ok {
				if err := c.s.b.top.PutExchange(ctx, name, Exchange{Kind: kind, Durable: durable}); err != nil {
					return err
				}
			}
		}
		if noWait {
			return nil
		}
		return c.send(wire.NewMethod(classExchange, exDeclareOK))

	case exDelete:
		a.Short()
		name := a.ShortStr()
		a.Bit() // if-unused
		noWait := a.Bit()
		if err := a.Err(); err != nil {
			return connFault(syntaxError, classExchange, method, "exchange.delete: %v", err)
		}
		if err := c.s.b.top.DropExchange(ctx, name); err != nil {
			return err
		}
		if noWait {
			return nil
		}
		return c.send(wire.NewMethod(classExchange, exDeleteOK))

	case exBind, exUnbind:
		return absent(classExchange, method,
			"exchange-to-exchange binding; bind the queue to each exchange it should read")

	default:
		return absent(classExchange, method, fmt.Sprintf("exchange method %d", method))
	}
}

// ---------------------------------------------------------------------------
// queue class
// ---------------------------------------------------------------------------

func (c *channel) queue(method uint16, a *wire.Args) error {
	ctx, cancel := context.WithTimeout(context.Background(), work)
	defer cancel()
	top := c.s.b.top

	switch method {
	case qDeclare:
		a.Short()
		name := a.ShortStr()
		passive, durable, exclusive, autoDelete := a.Bit(), a.Bit(), a.Bit(), a.Bit()
		noWait := a.Bit()
		a.Table()
		if err := a.Err(); err != nil {
			return connFault(syntaxError, classQueue, method, "queue.declare: %v", err)
		}
		if name == "" {
			// A server-named queue is the client saying it does not care, so
			// the name is one this gateway can always spell.
			name = "amq-gen-" + nuid.Next()
		}
		if err := Name(name); err != nil {
			return chanFault(notAllowed, classQueue, method, "PRECONDITION_FAILED - %v", err)
		}

		have, ok := top.Queue(name)
		if passive && !ok {
			return chanFault(notFound, classQueue, method, "NOT_FOUND - no queue %q", name)
		}
		if !passive && !ok {
			have = Queue{Durable: durable, Exclusive: exclusive, AutoDelete: autoDelete}
			if err := top.PutQueue(ctx, name, have); err != nil {
				return err
			}
		}
		// The consumer IS the queue: declaring it is what starts collecting,
		// which is why a queue receives what is published after it exists and
		// not the stream's whole history.
		info, err := c.s.b.ensure(ctx, name)
		if err != nil {
			return chanFault(internalError, classQueue, method, "INTERNAL_ERROR - queue %q: %v", name, err)
		}
		if have.Exclusive || have.AutoDelete {
			c.s.own(name)
		}
		if noWait {
			return nil
		}
		return c.send(wire.NewMethod(classQueue, qDeclareOK).
			ShortStr(name).
			Long(uint32(info.NumPending)).
			Long(uint32(c.s.b.consumers(name))))

	case qBind, qUnbind:
		a.Short()
		queue := a.ShortStr()
		exchange := a.ShortStr()
		key := a.ShortStr()
		noWait := method == qBind && a.Bit() // queue.unbind has no no-wait
		a.Table()
		if err := a.Err(); err != nil {
			return connFault(syntaxError, classQueue, method, "queue.bind: %v", err)
		}

		q, ok := top.Queue(queue)
		if !ok {
			return chanFault(notFound, classQueue, method, "NOT_FOUND - no queue %q", queue)
		}
		kind := Direct
		if exchange != "" {
			ex, ok := top.Exchange(exchange)
			if !ok {
				return chanFault(notFound, classQueue, method, "NOT_FOUND - no exchange %q", exchange)
			}
			kind = ex.Kind
		}
		// Validate against the exchange's kind now, on the channel that asked,
		// rather than discovering at delivery that the binding meant nothing.
		if _, err := Filters(kind, exchange, key); err != nil {
			return absent(classQueue, method, err.Error())
		}

		want := Binding{Exchange: exchange, Key: key}
		q.Bindings = drop(q.Bindings, want)
		if method == qBind {
			q.Bindings = append(q.Bindings, want)
		}
		if err := top.PutQueue(ctx, queue, q); err != nil {
			return err
		}
		if _, err := c.s.b.ensure(ctx, queue); err != nil {
			return chanFault(internalError, classQueue, method, "INTERNAL_ERROR - queue %q: %v", queue, err)
		}
		if noWait {
			return nil
		}
		if method == qBind {
			return c.send(wire.NewMethod(classQueue, qBindOK))
		}
		return c.send(wire.NewMethod(classQueue, qUnbindOK))

	case qPurge:
		a.Short()
		name := a.ShortStr()
		noWait := a.Bit()
		if err := a.Err(); err != nil {
			return connFault(syntaxError, classQueue, method, "queue.purge: %v", err)
		}
		n, err := c.s.b.purge(ctx, name)
		if err != nil {
			return chanFault(notFound, classQueue, method, "NOT_FOUND - queue %q: %v", name, err)
		}
		if noWait {
			return nil
		}
		return c.send(wire.NewMethod(classQueue, qPurgeOK).Long(n))

	case qDelete:
		a.Short()
		name := a.ShortStr()
		ifUnused, ifEmpty := a.Bit(), a.Bit()
		noWait := a.Bit()
		if err := a.Err(); err != nil {
			return connFault(syntaxError, classQueue, method, "queue.delete: %v", err)
		}
		if _, ok := top.Queue(name); !ok {
			return chanFault(notFound, classQueue, method, "NOT_FOUND - no queue %q", name)
		}
		info, err := c.s.b.ensure(ctx, name)
		if err != nil {
			return chanFault(internalError, classQueue, method, "INTERNAL_ERROR - queue %q: %v", name, err)
		}
		if ifUnused && c.s.b.consumers(name) > 0 {
			return chanFault(preconditionFailed, classQueue, method,
				"PRECONDITION_FAILED - queue %q has %d consumers", name, c.s.b.consumers(name))
		}
		if ifEmpty && info.NumPending > 0 {
			return chanFault(preconditionFailed, classQueue, method,
				"PRECONDITION_FAILED - queue %q holds %d messages", name, info.NumPending)
		}
		if err := c.s.b.dropConsumer(ctx, name); err != nil {
			return err
		}
		if err := top.DropQueue(ctx, name); err != nil {
			return err
		}
		c.s.disown(name)
		if noWait {
			return nil
		}
		return c.send(wire.NewMethod(classQueue, qDeleteOK).Long(uint32(info.NumPending)))

	default:
		return absent(classQueue, method, fmt.Sprintf("queue method %d", method))
	}
}

func drop(bs []Binding, b Binding) []Binding {
	out := bs[:0]
	for _, v := range bs {
		if v != b {
			out = append(out, v)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// basic class
// ---------------------------------------------------------------------------

func (c *channel) basic(method uint16, a *wire.Args) error {
	switch method {
	case basicQos:
		size := a.Long()
		count := a.Short()
		global := a.Bit()
		if err := a.Err(); err != nil {
			return connFault(syntaxError, classBasic, method, "basic.qos: %v", err)
		}
		if size != 0 {
			return absent(classBasic, method, "prefetch-size; bound the channel by message count instead")
		}
		c.mu.Lock()
		c.prefetch, c.global, c.shared = count, global, nil
		if global && count > 0 {
			c.shared = make(chan struct{}, count)
		}
		c.mu.Unlock()
		return c.send(wire.NewMethod(classBasic, basicQosOK))

	case basicConsume:
		return c.consume(a)

	case basicCancel:
		tag := a.ShortStr()
		noWait := a.Bit()
		if err := a.Err(); err != nil {
			return connFault(syntaxError, classBasic, method, "basic.cancel: %v", err)
		}
		c.mu.Lock()
		cons := c.consumers[tag]
		delete(c.consumers, tag)
		c.mu.Unlock()
		if cons != nil {
			if cons.pull != nil {
				cons.pull.Stop()
			}
			c.s.b.detach(cons.queue)
		}
		if noWait {
			return nil
		}
		return c.send(wire.NewMethod(classBasic, basicCancelOK).ShortStr(tag))

	case basicPublish:
		return c.publish(a)

	case basicGet:
		return c.get(a)

	case basicAck:
		tag, multiple := a.LongLong(), a.Bit()
		if err := a.Err(); err != nil {
			return connFault(syntaxError, classBasic, method, "basic.ack: %v", err)
		}
		return c.settle(tag, multiple, func(m jetstream.Msg) error { return m.Ack() })

	case basicNack:
		tag, multiple, requeue := a.LongLong(), a.Bit(), a.Bit()
		if err := a.Err(); err != nil {
			return connFault(syntaxError, classBasic, method, "basic.nack: %v", err)
		}
		return c.settle(tag, multiple, settler(requeue))

	case basicReject:
		tag, requeue := a.LongLong(), a.Bit()
		if err := a.Err(); err != nil {
			return connFault(syntaxError, classBasic, method, "basic.reject: %v", err)
		}
		return c.settle(tag, false, settler(requeue))

	case basicRecover:
		a.Bit() // requeue; the false case is basic.recover-async, which is gone
		c.mu.Lock()
		held := make([]delivery, 0, len(c.unacked))
		for _, d := range c.unacked {
			held = append(held, d)
		}
		c.unacked = map[uint64]delivery{}
		c.mu.Unlock()
		for _, d := range held {
			d.free()
			_ = d.msg.Nak()
		}
		return c.send(wire.NewMethod(classBasic, basicRecoverOK))

	case basicRecoverA:
		return absent(classBasic, method, "basic.recover-async; use basic.recover, which is answered")

	default:
		return absent(classBasic, method, fmt.Sprintf("basic method %d", method))
	}
}

// settler turns AMQP's requeue flag into the right JetStream disposition:
// requeue means give it to somebody else, and not-requeue means this message
// is finished whatever anyone thinks of it.
func settler(requeue bool) func(jetstream.Msg) error {
	if requeue {
		return func(m jetstream.Msg) error { return m.Nak() }
	}
	return func(m jetstream.Msg) error { return m.Term() }
}

// settle applies a disposition to one delivery tag, or to every tag up to it
// when the client said multiple.
func (c *channel) settle(tag uint64, multiple bool, apply func(jetstream.Msg) error) error {
	c.mu.Lock()
	var take []delivery
	if multiple {
		for t, d := range c.unacked {
			if tag == 0 || t <= tag {
				take = append(take, d)
				delete(c.unacked, t)
			}
		}
	} else if d, ok := c.unacked[tag]; ok {
		take = append(take, d)
		delete(c.unacked, tag)
	}
	c.mu.Unlock()

	if len(take) == 0 && !multiple {
		return chanFault(preconditionFailed, classBasic, basicAck,
			"PRECONDITION_FAILED - delivery tag %d is not outstanding on this channel", tag)
	}
	for _, d := range take {
		d.free()
		if err := apply(d.msg); err != nil {
			return chanFault(internalError, classBasic, basicAck, "INTERNAL_ERROR - %v", err)
		}
	}
	return nil
}

// free gives back the prefetch slot this delivery was holding.
func (d delivery) free() {
	if d.slots != nil {
		select {
		case <-d.slots:
		default:
		}
	}
}

func (c *channel) consume(a *wire.Args) error {
	a.Short()
	queue := a.ShortStr()
	tag := a.ShortStr()
	a.Bit() // no-local: this gateway has no notion of a message's publisher
	noAck, _, noWait := a.Bit(), a.Bit(), a.Bit()
	a.Table()
	if err := a.Err(); err != nil {
		return connFault(syntaxError, classBasic, basicConsume, "basic.consume: %v", err)
	}
	if _, ok := c.s.b.top.Queue(queue); !ok {
		return chanFault(notFound, classBasic, basicConsume, "NOT_FOUND - no queue %q", queue)
	}
	if tag == "" {
		tag = "ctag-" + nuid.Next()
	}
	c.mu.Lock()
	_, dup := c.consumers[tag]
	prefetch, shared := c.prefetch, c.shared
	c.mu.Unlock()
	if dup {
		return chanFault(notAllowed, classBasic, basicConsume,
			"NOT_ALLOWED - consumer tag %q is already in use on this channel", tag)
	}

	ctx, cancel := context.WithTimeout(context.Background(), work)
	defer cancel()
	if _, err := c.s.b.ensure(ctx, queue); err != nil {
		return chanFault(internalError, classBasic, basicConsume, "INTERNAL_ERROR - queue %q: %v", queue, err)
	}
	jsc, err := c.s.b.js.Consumer(ctx, Stream, queue)
	if err != nil {
		return chanFault(internalError, classBasic, basicConsume, "INTERNAL_ERROR - queue %q: %v", queue, err)
	}

	// consume-ok goes out BEFORE the first delivery, because a client that
	// sees a basic.deliver for a tag it has not been given yet has no consumer
	// to hand it to.
	if !noWait {
		if err := c.send(wire.NewMethod(classBasic, basicConsumeOK).ShortStr(tag)); err != nil {
			return err
		}
	}

	// The prefetch bucket. basic.qos(global=false) bounds each consumer on its
	// own; global=true bounds the channel, so every consumer draws on one
	// bucket. Nothing bounds a no-ack consumer, which has no outstanding
	// delivery to count. The bucket is the ONLY thing that enforces prefetch:
	// a pull batch is a fetch size, not a ceiling on what is outstanding, and
	// answering basic.qos with ok while relying on one would be a lie.
	cons := &consumer{tag: tag, queue: queue, noAck: noAck}
	switch {
	case noAck || prefetch == 0:
	case shared != nil:
		cons.slots = shared
	default:
		cons.slots = make(chan struct{}, prefetch)
	}
	var opts []jetstream.PullConsumeOpt
	if prefetch > 0 {
		opts = append(opts, jetstream.PullMaxMessages(int(prefetch)))
	}
	pull, err := jsc.Consume(func(m jetstream.Msg) { c.push(cons, m) }, opts...)
	if err != nil {
		return chanFault(internalError, classBasic, basicConsume, "INTERNAL_ERROR - consume %q: %v", queue, err)
	}
	cons.pull = pull

	c.mu.Lock()
	c.consumers[tag] = cons
	c.mu.Unlock()
	c.s.b.attach(queue)
	return nil
}

// push writes one delivery. The tag is assigned and recorded under the state
// lock, which is then RELEASED before the socket write: a client that stops
// reading must not be able to stop this channel from processing its acks.
func (c *channel) push(cons *consumer, m jetstream.Msg) {
	// Taken BEFORE the delivery lock, so a consumer that has filled its
	// prefetch stalls itself and not the channel's other consumers.
	if cons.slots != nil {
		select {
		case cons.slots <- struct{}{}:
		case <-c.s.done:
			return
		}
	}

	c.deliver.Lock()
	defer c.deliver.Unlock()

	c.mu.Lock()
	c.tag++
	tag := c.tag
	if !cons.noAck {
		c.unacked[tag] = delivery{msg: m, slots: cons.slots}
	}
	c.mu.Unlock()

	redelivered := false
	if md, err := m.Metadata(); err == nil && md.NumDelivered > 1 {
		redelivered = true
	}
	ex, key := parts(m.Subject())
	method := wire.NewMethod(classBasic, basicDeliver).
		ShortStr(cons.tag).LongLong(tag).Bit(redelivered).ShortStr(ex).ShortStr(key)

	if err := c.s.w.Content(c.id, classBasic, method.Bytes(), propsOf(m), m.Data()); err != nil {
		// The socket is gone. Leaving the message unacked is what makes it
		// somebody else's after the ack deadline.
		return
	}
	if cons.noAck {
		_ = m.Ack()
	}
}

func (c *channel) get(a *wire.Args) error {
	a.Short()
	queue := a.ShortStr()
	noAck := a.Bit()
	if err := a.Err(); err != nil {
		return connFault(syntaxError, classBasic, basicGet, "basic.get: %v", err)
	}
	if _, ok := c.s.b.top.Queue(queue); !ok {
		return chanFault(notFound, classBasic, basicGet, "NOT_FOUND - no queue %q", queue)
	}

	ctx, cancel := context.WithTimeout(context.Background(), work)
	defer cancel()
	if _, err := c.s.b.ensure(ctx, queue); err != nil {
		return chanFault(internalError, classBasic, basicGet, "INTERNAL_ERROR - queue %q: %v", queue, err)
	}
	jsc, err := c.s.b.js.Consumer(ctx, Stream, queue)
	if err != nil {
		return chanFault(internalError, classBasic, basicGet, "INTERNAL_ERROR - queue %q: %v", queue, err)
	}

	m, err := jsc.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil || m == nil {
		if err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) {
			return chanFault(internalError, classBasic, basicGet, "INTERNAL_ERROR - get %q: %v", queue, err)
		}
		return c.send(wire.NewMethod(classBasic, basicGetEmpty).ShortStr(""))
	}

	c.deliver.Lock()
	defer c.deliver.Unlock()
	c.mu.Lock()
	c.tag++
	tag := c.tag
	if !noAck {
		c.unacked[tag] = delivery{msg: m}
	}
	c.mu.Unlock()

	var left uint64
	redelivered := false
	if md, err := m.Metadata(); err == nil {
		left = md.NumPending
		redelivered = md.NumDelivered > 1
	}
	ex, key := parts(m.Subject())
	method := wire.NewMethod(classBasic, basicGetOK).
		LongLong(tag).Bit(redelivered).ShortStr(ex).ShortStr(key).Long(uint32(left))
	if err := c.s.w.Content(c.id, classBasic, method.Bytes(), propsOf(m), m.Data()); err != nil {
		return err
	}
	if noAck {
		_ = m.Ack()
	}
	return nil
}

// ---------------------------------------------------------------------------
// publishing: method, then header, then body
// ---------------------------------------------------------------------------

func (c *channel) publish(a *wire.Args) error {
	a.Short()
	exchange := a.ShortStr()
	key := a.ShortStr()
	mandatory, immediate := a.Bit(), a.Bit()
	if err := a.Err(); err != nil {
		return connFault(syntaxError, classBasic, basicPublish, "basic.publish: %v", err)
	}
	if immediate {
		return absent(classBasic, basicPublish,
			"the immediate flag; publish with mandatory to learn that nothing is bound")
	}
	if exchange != "" {
		if _, ok := c.s.b.top.Exchange(exchange); !ok {
			return chanFault(notFound, classBasic, basicPublish, "NOT_FOUND - no exchange %q", exchange)
		}
	}
	subject, err := Subject(exchange, key)
	if err != nil {
		return chanFault(preconditionFailed, classBasic, basicPublish, "PRECONDITION_FAILED - %v", err)
	}

	c.mu.Lock()
	c.pending = &content{exchange: exchange, key: key, subject: subject, mandatory: mandatory}
	c.mu.Unlock()
	return nil
}

func (c *channel) head(payload []byte) error {
	class, size, props, err := wire.ParseHeader(payload)
	if err != nil {
		return connFault(frameError, 0, 0, "content header: %v", err)
	}
	if class != classBasic {
		return connFault(frameError, class, 0, "content header names class %d, not basic", class)
	}
	if size > uint64(c.s.b.cfg.FrameMax)*4096 {
		return chanFault(311, classBasic, basicPublish, "CONTENT_TOO_LARGE - body of %d octets", size)
	}

	c.mu.Lock()
	p := c.pending
	if p == nil {
		c.mu.Unlock()
		return connFault(unexpectedFrame, 0, 0, "content header with no basic.publish before it")
	}
	p.props, p.size, p.header = props, size, true
	done := size == 0
	if done {
		c.pending = nil
	}
	c.mu.Unlock()

	if done {
		return c.emit(p)
	}
	return nil
}

func (c *channel) chunk(payload []byte) error {
	c.mu.Lock()
	p := c.pending
	if p == nil || !p.header {
		c.mu.Unlock()
		return connFault(unexpectedFrame, 0, 0, "content body with no header before it")
	}
	p.body = append(p.body, payload...)
	if uint64(len(p.body)) > p.size {
		c.pending = nil
		c.mu.Unlock()
		return connFault(frameError, 0, 0, "content body is longer than the %d octets its header declared", p.size)
	}
	done := uint64(len(p.body)) == p.size
	if done {
		c.pending = nil
	}
	c.mu.Unlock()

	if done {
		return c.emit(p)
	}
	return nil
}

// emit puts an assembled message on the bus, then answers the publisher if it
// asked to be answered — a confirm if it selected confirm mode, a return if it
// said mandatory and nothing is bound.
func (c *channel) emit(p *content) error {
	c.mu.Lock()
	confirm := c.confirm
	var seq uint64
	if confirm {
		c.seq++
		seq = c.seq
	}
	c.mu.Unlock()

	if p.mandatory && !c.s.b.top.Routable(p.subject) {
		// Unroutable: hand it back rather than store it where no queue reads.
		method := wire.NewMethod(classBasic, basicReturn).
			Short(noRoute).ShortStr("NO_ROUTE").ShortStr(p.exchange).ShortStr(p.key)
		if err := c.s.w.Content(c.id, classBasic, method.Bytes(), p.props, p.body); err != nil {
			return err
		}
		if confirm {
			// A returned message is still confirmed: the broker took
			// responsibility and discharged it (§1.3, publisher confirms).
			return c.send(wire.NewMethod(classBasic, basicAck).LongLong(seq).Bit(false))
		}
		return nil
	}

	msg := nats.NewMsg(p.subject)
	msg.Data = p.body
	msg.Header.Set(propsHeader, base64.StdEncoding.EncodeToString(wire.EncodeProps(p.props)))

	ctx, cancel := context.WithTimeout(context.Background(), work)
	defer cancel()
	if _, err := c.s.b.js.PublishMsg(ctx, msg); err != nil {
		if confirm {
			// nack, not ack: the publisher asked to be told, so tell it.
			_ = c.send(wire.NewMethod(classBasic, basicNack).LongLong(seq).Bit(false).Bit(false))
		}
		return chanFault(internalError, classBasic, basicPublish, "INTERNAL_ERROR - publish %q: %v", p.subject, err)
	}
	if confirm {
		return c.send(wire.NewMethod(classBasic, basicAck).LongLong(seq).Bit(false))
	}
	return nil
}

// parts reads a subject back as the exchange and routing key it was published
// under. Blank is the default exchange; a two-word subject had no routing key.
func parts(subject string) (exchange, key string) {
	rest, ok := strings.CutPrefix(subject, Root+".")
	if !ok {
		return "", subject
	}
	ex, key, _ := strings.Cut(rest, ".")
	if ex == Blank {
		ex = ""
	}
	return ex, key
}

func propsOf(m jetstream.Msg) wire.Props {
	h := m.Headers().Get(propsHeader)
	if h == "" {
		return wire.Props{}
	}
	raw, err := base64.StdEncoding.DecodeString(h)
	if err != nil {
		return wire.Props{}
	}
	p, err := wire.DecodeProps(raw)
	if err != nil {
		return wire.Props{}
	}
	return p
}
