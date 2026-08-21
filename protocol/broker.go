// Package protocol speaks AMQP 0-9-1 on a socket and JetStream underneath, so
// a client that only knows how to talk to RabbitMQ shares the platform bus
// with everything else on it.
//
// What it implements is a whole path, not a sample of one: connect, declare an
// exchange and a queue, bind, publish with properties, consume, ack, nack,
// reject, get, cancel, qos, publisher confirms, and the mandatory return. What
// it does NOT implement — headers exchanges, transactions, byte-counted qos —
// answers with the AMQP error the spec defines for it, on the channel, naming
// the method. A client learns it asked for something absent; it never watches a
// message vanish into a broker that said ok.
//
// See route.go for the whole map onto JetStream. It is small on purpose: a
// routing key and a subject are the same idea, so most of AMQP needs
// translation rather than emulation.
package protocol

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	luxlog "github.com/luxfi/log"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Config is what a Broker needs to know.
type Config struct {
	// Addr is the TCP address the AMQP listener binds. Empty means :5672.
	Addr string

	// PubSubURL is the bus. Empty means the local default.
	PubSubURL string

	// PubSubCreds is an optional NATS credentials file.
	PubSubCreds string

	// Product and Version identify this server in connection.start's
	// server-properties, which is what a client logs and a management tool
	// displays. Empty means "hanzo-amqp" and "dev".
	Product, Version string

	// Heartbeat is the interval offered in connection.tune. Zero means 60s;
	// a client is free to name a smaller one or turn it off.
	Heartbeat time.Duration

	// FrameMax is the largest frame payload accepted, and the ceiling on what
	// a client may negotiate. Zero means 128 KiB.
	FrameMax uint32
}

// Broker is the gateway: one listener speaking AMQP, one connection to the bus.
type Broker struct {
	cfg Config
	nc  *nats.Conn
	js  jetstream.JetStream
	top *Topology

	ready chan struct{}
	quit  chan struct{}
	stop  sync.Once
	wg    sync.WaitGroup

	mu   sync.Mutex
	ln   net.Listener
	live map[*session]struct{}
	cons map[string]int
}

// Defaults for what Config leaves empty.
const (
	DefaultAddr      = ":5672"
	DefaultFrameMax  = 128 << 10
	DefaultHeartbeat = 60 * time.Second
	DefaultProduct   = "hanzo-amqp"
)

// NewBroker builds a Broker. It opens nothing; Serve does that.
func NewBroker(cfg Config) *Broker {
	if cfg.Addr == "" {
		cfg.Addr = DefaultAddr
	}
	if cfg.FrameMax < 8192 {
		cfg.FrameMax = DefaultFrameMax
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = DefaultHeartbeat
	}
	if cfg.Product == "" {
		cfg.Product = DefaultProduct
	}
	if cfg.Version == "" {
		cfg.Version = "dev"
	}
	return &Broker{
		cfg:   cfg,
		ready: make(chan struct{}),
		quit:  make(chan struct{}),
		live:  map[*session]struct{}{},
		cons:  map[string]int{},
	}
}

// Serve connects to the bus, declares the stream and the topology bucket,
// binds the listener, and then accepts until Shutdown. It blocks.
//
// Everything that can fail has failed by the time Ready closes, so a caller
// that must fail closed waits on Ready against a deadline rather than guessing
// with a sleep.
func (b *Broker) Serve() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var opts []nats.Option
	if b.cfg.PubSubCreds != "" {
		opts = append(opts, nats.UserCredentials(b.cfg.PubSubCreds))
	}
	nc, err := nats.Connect(b.cfg.PubSubURL, opts...)
	if err != nil {
		return fmt.Errorf("connect %s: %w", b.cfg.PubSubURL, err)
	}
	b.nc = nc

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return fmt.Errorf("jetstream: %w", err)
	}
	b.js = js

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        Stream,
		Description: "AMQP 0-9-1 gateway traffic",
		Subjects:    []string{Root + ".>"},
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
	}); err != nil {
		nc.Close()
		return fmt.Errorf("stream %s: %w", Stream, err)
	}

	top, err := OpenTopology(ctx, js)
	if err != nil {
		nc.Close()
		return err
	}
	b.top = top

	ln, err := net.Listen("tcp", b.cfg.Addr)
	if err != nil {
		top.Close()
		nc.Close()
		return fmt.Errorf("listen %s: %w", b.cfg.Addr, err)
	}
	b.mu.Lock()
	b.ln = ln
	b.mu.Unlock()
	close(b.ready)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-b.quit:
				b.wg.Wait()
				return nil
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			b.wg.Wait()
			return fmt.Errorf("accept: %w", err)
		}
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			b.serve(conn)
		}()
	}
}

// Ready closes once the listener is bound and the bus is answering.
func (b *Broker) Ready() <-chan struct{} { return b.ready }

// Addr is where the listener actually landed, which is the question worth
// asking when Config named port 0.
func (b *Broker) Addr() net.Addr {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ln == nil {
		return nil
	}
	return b.ln.Addr()
}

// Topology is what is declared right now, for an operator's status view.
func (b *Broker) Topology() *Topology { return b.top }

// Connections counts the AMQP sessions open.
func (b *Broker) Connections() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.live)
}

// Shutdown stops accepting, drops every open connection, and closes the bus
// link. Idempotent, and safe on a Broker that never served.
func (b *Broker) Shutdown() {
	b.stop.Do(func() {
		close(b.quit)
		b.mu.Lock()
		ln := b.ln
		live := make([]*session, 0, len(b.live))
		for s := range b.live {
			live = append(live, s)
		}
		b.mu.Unlock()

		if ln != nil {
			ln.Close()
		}
		for _, s := range live {
			s.conn.Close()
		}
		b.wg.Wait()
		if b.top != nil {
			b.top.Close()
		}
		if b.nc != nil {
			b.nc.Close()
		}
	})
}

func (b *Broker) serve(conn net.Conn) {
	s := newSession(b, conn)
	b.mu.Lock()
	b.live[s] = struct{}{}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.live, s)
		b.mu.Unlock()
		s.close()
	}()
	s.run()
}

// ensure makes a queue's JetStream consumer match what the queue is bound to,
// creating it if this is the declare that brought the queue into being.
//
// DeliverNew is the AMQP rule: a queue holds what was published after it was
// declared, never the stream's history. It is also why declaring the queue —
// not consuming from it — is what starts collecting.
//
// MaxAckPending is unlimited on purpose. AMQP's prefetch is a property of a
// CHANNEL, and several channels in several processes consume one queue; a
// queue-wide ceiling would let one idle consumer stall the others. The
// per-channel limit is the pull batch, which is where it belongs.
func (b *Broker) ensure(ctx context.Context, queue string) (*jetstream.ConsumerInfo, error) {
	c, err := b.js.CreateOrUpdateConsumer(ctx, Stream, jetstream.ConsumerConfig{
		Durable:        queue,
		Name:           queue,
		FilterSubjects: b.top.Filters(queue),
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverNewPolicy,
		MaxAckPending:  -1,
		AckWait:        5 * time.Minute,
	})
	if err != nil {
		return nil, err
	}
	return c.CachedInfo(), nil
}

// dropConsumer removes a queue's consumer. A queue that was never declared on
// this stream has none, and that is not an error.
func (b *Broker) dropConsumer(ctx context.Context, queue string) error {
	err := b.js.DeleteConsumer(ctx, Stream, queue)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil
	}
	return err
}

// purge empties a queue by moving its consumer past everything waiting. There
// is no per-queue delete to make: the stream is shared, so purging by message
// would take those messages from every other queue bound to the same subjects.
func (b *Broker) purge(ctx context.Context, queue string) (uint32, error) {
	c, err := b.js.Consumer(ctx, Stream, queue)
	if err != nil {
		return 0, err
	}
	info, err := c.Info(ctx)
	if err != nil {
		return 0, err
	}
	if err := b.js.DeleteConsumer(ctx, Stream, queue); err != nil {
		return 0, err
	}
	if _, err := b.ensure(ctx, queue); err != nil {
		return 0, err
	}
	return uint32(info.NumPending), nil
}

// attach, detach and consumers count the live basic.consume calls per queue.
// It is what queue.declare-ok reports and what queue.delete(if-unused) tests,
// and it is this gateway's own view — one replica cannot see another's
// sockets, so a fleet of them reports a lower bound.
func (b *Broker) attach(queue string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cons[queue]++
}

func (b *Broker) detach(queue string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cons[queue] <= 1 {
		delete(b.cons, queue)
		return
	}
	b.cons[queue]--
}

func (b *Broker) consumers(queue string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cons[queue]
}

func logf(format string, a ...any) {
	luxlog.Default().Info(fmt.Sprintf(format, a...), "subsystem", "amqp")
}
