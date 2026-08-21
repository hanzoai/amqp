package protocol_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hanzoai/amqp/protocol"
	"github.com/hanzoai/amqp/wire"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Everything below drives github.com/rabbitmq/amqp091-go — the reference
// client, unmodified — against the gateway. That is the only test that means
// anything for a wire protocol: a hand-rolled client would agree with a
// hand-rolled server about a shared misreading of the spec.

// bus starts an embedded NATS with JetStream and returns its URL.
func bus(t *testing.T) string {
	t.Helper()
	s, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatalf("nats server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(20 * time.Second) {
		t.Fatal("nats server did not come up")
	}
	t.Cleanup(s.Shutdown)
	return s.ClientURL()
}

// gateway starts a Broker on a free port over its own bus and returns the URL
// a client dials.
func gateway(t *testing.T) string {
	t.Helper()
	return gatewayOn(t, bus(t))
}

func gatewayOn(t *testing.T, url string) string {
	t.Helper()
	b := protocol.NewBroker(protocol.Config{Addr: "127.0.0.1:0", PubSubURL: url})
	errc := make(chan error, 1)
	go func() { errc <- b.Serve() }()
	select {
	case err := <-errc:
		t.Fatalf("broker serve returned before it was ready: %v", err)
	case <-b.Ready():
	case <-time.After(30 * time.Second):
		t.Fatal("broker never became ready")
	}
	t.Cleanup(b.Shutdown)
	return "amqp://" + b.Addr().String()
}

func dial(t *testing.T, url string) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	return conn, ch
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return c
}

func next(t *testing.T, d <-chan amqp.Delivery) amqp.Delivery {
	t.Helper()
	select {
	case m, ok := <-d:
		if !ok {
			t.Fatal("delivery channel closed before a message arrived")
		}
		return m
	case <-time.After(15 * time.Second):
		t.Fatal("no delivery within 15s")
		return amqp.Delivery{}
	}
}

// TestPublishConsumeAck is the whole point: a standard client declares a
// queue, publishes to it, consumes it, and acks — and the message went through
// JetStream on the way, which the last assertion checks by reading the stream
// with a NATS client that has never heard of AMQP.
func TestPublishConsumeAck(t *testing.T) {
	url := bus(t)
	_, ch := dial(t, gatewayOn(t, url))

	q, err := ch.QueueDeclare("orders", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("queue.declare: %v", err)
	}
	if q.Name != "orders" {
		t.Fatalf("declare-ok named %q, want orders", q.Name)
	}

	deliveries, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("basic.consume: %v", err)
	}

	body := []byte(`{"id":1}`)
	if err := ch.PublishWithContext(ctx(t), "", "orders", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	}); err != nil {
		t.Fatalf("basic.publish: %v", err)
	}

	m := next(t, deliveries)
	if string(m.Body) != string(body) {
		t.Fatalf("body = %q, want %q", m.Body, body)
	}
	if m.ContentType != "application/json" {
		t.Fatalf("content-type = %q, want application/json", m.ContentType)
	}
	if m.RoutingKey != "orders" {
		t.Fatalf("routing key = %q, want orders", m.RoutingKey)
	}
	if err := m.Ack(false); err != nil {
		t.Fatalf("basic.ack: %v", err)
	}

	// The ack must reach JetStream, or "acked" means nothing: read the
	// consumer back and insist nothing is outstanding.
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	c, err := js.Consumer(ctx(t), protocol.Stream, "orders")
	if err != nil {
		t.Fatalf("the gateway did not create a JetStream consumer for the queue: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		info, err := c.Info(ctx(t))
		if err != nil {
			t.Fatalf("consumer info: %v", err)
		}
		if info.NumAckPending == 0 && info.NumPending == 0 && info.Delivered.Consumer >= 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after ack: %d pending, %d awaiting ack, %d delivered — the AMQP ack did not reach JetStream",
				info.NumPending, info.NumAckPending, info.Delivered.Consumer)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestPublishReachesTheBusAsPlainNATS proves the translation is real and not a
// private format: a NATS-native subscriber reads the body byte for byte off a
// subject it can work out from the exchange and routing key alone.
func TestPublishReachesTheBusAsPlainNATS(t *testing.T) {
	url := bus(t)
	_, ch := dial(t, gatewayOn(t, url))

	if err := ch.ExchangeDeclare("events", "topic", true, false, false, false, nil); err != nil {
		t.Fatalf("exchange.declare: %v", err)
	}
	if _, err := ch.QueueDeclare("audit", true, false, false, false, nil); err != nil {
		t.Fatalf("queue.declare: %v", err)
	}
	if err := ch.QueueBind("audit", "order.#", "events", false, nil); err != nil {
		t.Fatalf("queue.bind: %v", err)
	}

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("amqp.events.order.created")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if err := ch.PublishWithContext(ctx(t), "events", "order.created", false, false,
		amqp.Publishing{Body: []byte("hello")}); err != nil {
		t.Fatalf("basic.publish: %v", err)
	}
	msg, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatalf("a NATS subscriber saw nothing on amqp.events.order.created: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("bus body = %q, want hello", msg.Data)
	}
}

// TestTopicWildcards checks the two wildcard sets line up, including the case
// they disagree on: AMQP's '#' matches zero words and NATS's '>' does not.
func TestTopicWildcards(t *testing.T) {
	_, ch := dial(t, gateway(t))
	if err := ch.ExchangeDeclare("logs", "topic", true, false, false, false, nil); err != nil {
		t.Fatalf("exchange.declare: %v", err)
	}
	for _, q := range []struct{ name, key string }{
		{"deep", "app.#"},
		{"one", "app.*"},
		{"exact", "app.boot"},
	} {
		if _, err := ch.QueueDeclare(q.name, true, false, false, false, nil); err != nil {
			t.Fatalf("declare %s: %v", q.name, err)
		}
		if err := ch.QueueBind(q.name, q.key, "logs", false, nil); err != nil {
			t.Fatalf("bind %s: %v", q.name, err)
		}
	}

	sub := map[string]<-chan amqp.Delivery{}
	for _, name := range []string{"deep", "one", "exact"} {
		d, err := ch.Consume(name, "", true, false, false, false, nil)
		if err != nil {
			t.Fatalf("consume %s: %v", name, err)
		}
		sub[name] = d
	}

	// app.boot reaches all three; app.a.b reaches only the '#' binding; the
	// bare key "app" reaches '#' too, which is the zero-word case.
	for _, key := range []string{"app.boot", "app.a.b", "app"} {
		if err := ch.PublishWithContext(ctx(t), "logs", key, false, false,
			amqp.Publishing{Body: []byte(key)}); err != nil {
			t.Fatalf("publish %s: %v", key, err)
		}
	}

	want := map[string][]string{
		"deep":  {"app.boot", "app.a.b", "app"},
		"one":   {"app.boot"},
		"exact": {"app.boot"},
	}
	for name, keys := range want {
		for _, k := range keys {
			m := next(t, sub[name])
			if string(m.Body) != k {
				t.Fatalf("queue %s got %q, want %q", name, m.Body, k)
			}
		}
		select {
		case m := <-sub[name]:
			t.Fatalf("queue %s got an extra message %q", name, m.Body)
		case <-time.After(400 * time.Millisecond):
		}
	}
}

// TestFanoutReachesEveryQueue: one publish, two queues, two copies — the shape
// a shared stream with per-queue consumers gets for free.
func TestFanoutReachesEveryQueue(t *testing.T) {
	_, ch := dial(t, gateway(t))
	if err := ch.ExchangeDeclare("broadcast", "fanout", true, false, false, false, nil); err != nil {
		t.Fatalf("exchange.declare: %v", err)
	}
	subs := map[string]<-chan amqp.Delivery{}
	for _, name := range []string{"left", "right"} {
		if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
			t.Fatalf("declare %s: %v", name, err)
		}
		if err := ch.QueueBind(name, "", "broadcast", false, nil); err != nil {
			t.Fatalf("bind %s: %v", name, err)
		}
		d, err := ch.Consume(name, "", true, false, false, false, nil)
		if err != nil {
			t.Fatalf("consume %s: %v", name, err)
		}
		subs[name] = d
	}
	if err := ch.PublishWithContext(ctx(t), "broadcast", "", false, false,
		amqp.Publishing{Body: []byte("all")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	for name, d := range subs {
		if m := next(t, d); string(m.Body) != "all" {
			t.Fatalf("queue %s got %q, want all", name, m.Body)
		}
	}
}

// TestConfirms: confirm.select, then every publish is acked by sequence.
func TestConfirms(t *testing.T) {
	_, ch := dial(t, gateway(t))
	if _, err := ch.QueueDeclare("confirmed", true, false, false, false, nil); err != nil {
		t.Fatalf("queue.declare: %v", err)
	}
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}
	acks := ch.NotifyPublish(make(chan amqp.Confirmation, 4))

	for i := range 3 {
		if err := ch.PublishWithContext(ctx(t), "", "confirmed", false, false,
			amqp.Publishing{Body: fmt.Appendf(nil, "%d", i)}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	for i := range 3 {
		select {
		case c := <-acks:
			if !c.Ack {
				t.Fatalf("publish %d was nacked", i)
			}
			if c.DeliveryTag != uint64(i+1) {
				t.Fatalf("confirm %d carried tag %d, want %d", i, c.DeliveryTag, i+1)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("no confirm for publish %d", i)
		}
	}
}

// TestMandatoryReturnsWhatNothingIsBoundTo: the publisher asked to be told, so
// it is told — rather than the message being stored where no queue reads.
func TestMandatoryReturnsWhatNothingIsBoundTo(t *testing.T) {
	_, ch := dial(t, gateway(t))
	if err := ch.ExchangeDeclare("void", "direct", true, false, false, false, nil); err != nil {
		t.Fatalf("exchange.declare: %v", err)
	}
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))

	if err := ch.PublishWithContext(ctx(t), "void", "nobody", true, false,
		amqp.Publishing{Body: []byte("lost")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case r := <-returns:
		if string(r.Body) != "lost" {
			t.Fatalf("returned body = %q, want lost", r.Body)
		}
		if r.ReplyCode != 312 {
			t.Fatalf("returned code = %d, want 312 NO_ROUTE", r.ReplyCode)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("mandatory publish to an unbound exchange was not returned")
	}
}

// TestNackRequeues: a nacked message comes back, and says so. Without that,
// "at least once" is a claim with nothing behind it.
func TestNackRequeues(t *testing.T) {
	_, ch := dial(t, gateway(t))
	if _, err := ch.QueueDeclare("retry", true, false, false, false, nil); err != nil {
		t.Fatalf("queue.declare: %v", err)
	}
	d, err := ch.Consume("retry", "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err := ch.PublishWithContext(ctx(t), "", "retry", false, false,
		amqp.Publishing{Body: []byte("work")}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	m := next(t, d)
	if m.Redelivered {
		t.Fatal("the first delivery claimed to be a redelivery")
	}
	if err := m.Nack(false, true); err != nil {
		t.Fatalf("basic.nack: %v", err)
	}

	again := next(t, d)
	if string(again.Body) != "work" {
		t.Fatalf("redelivered body = %q, want work", again.Body)
	}
	if !again.Redelivered {
		t.Fatal("the redelivery did not carry the redelivered flag")
	}
	if err := again.Ack(false); err != nil {
		t.Fatalf("basic.ack: %v", err)
	}
}

// TestCompetingConsumersSplitTheQueue: two connections on one queue share the
// work and never duplicate it, which is what makes a queue a queue rather than
// a topic.
func TestCompetingConsumersSplitTheQueue(t *testing.T) {
	url := gateway(t)
	_, first := dial(t, url)
	if _, err := first.QueueDeclare("work", true, false, false, false, nil); err != nil {
		t.Fatalf("queue.declare: %v", err)
	}
	if err := first.Qos(1, 0, false); err != nil {
		t.Fatalf("basic.qos: %v", err)
	}
	a, err := first.Consume("work", "a", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume a: %v", err)
	}

	_, second := dial(t, url)
	if err := second.Qos(1, 0, false); err != nil {
		t.Fatalf("basic.qos: %v", err)
	}
	b, err := second.Consume("work", "b", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume b: %v", err)
	}

	const n = 6
	for i := range n {
		if err := first.PublishWithContext(ctx(t), "", "work", false, false,
			amqp.Publishing{Body: fmt.Appendf(nil, "%d", i)}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	seen := map[string]int{}
	for range n {
		select {
		case m := <-a:
			seen[string(m.Body)]++
			_ = m.Ack(false)
		case m := <-b:
			seen[string(m.Body)]++
			_ = m.Ack(false)
		case <-time.After(20 * time.Second):
			t.Fatalf("only %d of %d messages arrived: %v", len(seen), n, seen)
		}
	}
	for i := range n {
		if c := seen[fmt.Sprint(i)]; c != 1 {
			t.Fatalf("message %d was delivered %d times, want once (%v)", i, c, seen)
		}
	}
}

// TestRejectWithoutRequeueDropsIt: reject(requeue=false) means finished, and
// finished has to be observable or a poison message loops forever.
func TestRejectWithoutRequeueDropsIt(t *testing.T) {
	_, ch := dial(t, gateway(t))
	if _, err := ch.QueueDeclare("poison", true, false, false, false, nil); err != nil {
		t.Fatalf("queue.declare: %v", err)
	}
	d, err := ch.Consume("poison", "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	for _, body := range []string{"bad", "good"} {
		if err := ch.PublishWithContext(ctx(t), "", "poison", false, false,
			amqp.Publishing{Body: []byte(body)}); err != nil {
			t.Fatalf("publish %s: %v", body, err)
		}
	}
	bad := next(t, d)
	if err := bad.Reject(false); err != nil {
		t.Fatalf("basic.reject: %v", err)
	}
	good := next(t, d)
	if string(good.Body) != "good" {
		t.Fatalf("second delivery = %q, want good", good.Body)
	}
	_ = good.Ack(false)

	select {
	case m := <-d:
		t.Fatalf("the rejected message came back as %q", m.Body)
	case <-time.After(2 * time.Second):
	}
}

// TestPropertiesRoundTrip: every property a publisher sets is what the
// consumer reads, headers table included.
func TestPropertiesRoundTrip(t *testing.T) {
	_, ch := dial(t, gateway(t))
	if _, err := ch.QueueDeclare("props", true, false, false, false, nil); err != nil {
		t.Fatalf("queue.declare: %v", err)
	}
	d, err := ch.Consume("props", "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	stamp := time.Unix(1700000000, 0).UTC()
	sent := amqp.Publishing{
		ContentType:     "application/octet-stream",
		ContentEncoding: "gzip",
		DeliveryMode:    amqp.Persistent,
		Priority:        7,
		CorrelationId:   "corr-1",
		ReplyTo:         "back",
		Expiration:      "60000",
		MessageId:       "msg-1",
		Timestamp:       stamp,
		Type:            "order.created",
		UserId:          "",
		AppId:           "checkout",
		Headers: amqp.Table{
			"retry":  int32(3),
			"trace":  "abc",
			"ratio":  float64(0.5),
			"ok":     true,
			"raw":    []byte{1, 2, 3},
			"nested": amqp.Table{"deep": int64(9)},
		},
		Body: []byte("payload"),
	}
	if err := ch.PublishWithContext(ctx(t), "", "props", false, false, sent); err != nil {
		t.Fatalf("publish: %v", err)
	}

	got := next(t, d)
	for _, c := range []struct {
		field      string
		have, want any
	}{
		{"content-type", got.ContentType, sent.ContentType},
		{"content-encoding", got.ContentEncoding, sent.ContentEncoding},
		{"delivery-mode", got.DeliveryMode, sent.DeliveryMode},
		{"priority", got.Priority, sent.Priority},
		{"correlation-id", got.CorrelationId, sent.CorrelationId},
		{"reply-to", got.ReplyTo, sent.ReplyTo},
		{"expiration", got.Expiration, sent.Expiration},
		{"message-id", got.MessageId, sent.MessageId},
		{"timestamp", got.Timestamp.UTC(), stamp},
		{"type", got.Type, sent.Type},
		{"app-id", got.AppId, sent.AppId},
	} {
		if c.have != c.want {
			t.Errorf("%s = %v, want %v", c.field, c.have, c.want)
		}
	}
	for k, want := range sent.Headers {
		have, ok := got.Headers[k]
		if !ok {
			t.Errorf("header %q did not survive", k)
			continue
		}
		if fmt.Sprint(have) != fmt.Sprint(want) {
			t.Errorf("header %q = %#v, want %#v", k, have, want)
		}
	}
}

// TestBodyLargerThanOneFrame: a body over frame-max is split on the way out
// and reassembled on the way in, both directions.
func TestBodyLargerThanOneFrame(t *testing.T) {
	_, ch := dial(t, gateway(t))
	if _, err := ch.QueueDeclare("big", true, false, false, false, nil); err != nil {
		t.Fatalf("queue.declare: %v", err)
	}
	d, err := ch.Consume("big", "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	body := []byte(strings.Repeat("x", 512<<10)) // 4x the default frame-max
	if err := ch.PublishWithContext(ctx(t), "", "big", false, false,
		amqp.Publishing{Body: body}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	m := next(t, d)
	if len(m.Body) != len(body) {
		t.Fatalf("body came back %d octets, sent %d", len(m.Body), len(body))
	}
	if string(m.Body) != string(body) {
		t.Fatal("body came back the right length and the wrong bytes")
	}
}

// TestGet takes one message synchronously, and reports an empty queue as empty
// rather than blocking.
func TestGet(t *testing.T) {
	_, ch := dial(t, gateway(t))
	if _, err := ch.QueueDeclare("pull", true, false, false, false, nil); err != nil {
		t.Fatalf("queue.declare: %v", err)
	}
	if _, ok, err := ch.Get("pull", true); err != nil || ok {
		t.Fatalf("get on an empty queue: ok=%v err=%v, want ok=false err=nil", ok, err)
	}
	if err := ch.PublishWithContext(ctx(t), "", "pull", false, false,
		amqp.Publishing{Body: []byte("one")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		m, ok, err := ch.Get("pull", false)
		if err != nil {
			t.Fatalf("basic.get: %v", err)
		}
		if ok {
			if string(m.Body) != "one" {
				t.Fatalf("got %q, want one", m.Body)
			}
			if err := m.Ack(false); err != nil {
				t.Fatalf("ack: %v", err)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("basic.get never saw the published message")
		}
	}
}

// TestQueueOutlivesTheConnectionThatDeclaredIt: the topology is on the bus, so
// a durable queue is not a property of whoever happened to declare it.
func TestQueueOutlivesTheConnectionThatDeclaredIt(t *testing.T) {
	url := gateway(t)

	first, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ch, err := first.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	if _, err := ch.QueueDeclare("durable", true, false, false, false, nil); err != nil {
		t.Fatalf("queue.declare: %v", err)
	}
	if err := ch.PublishWithContext(ctx(t), "", "durable", false, false,
		amqp.Publishing{Body: []byte("kept")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, second := dial(t, url)
	q, err := second.QueueDeclare("durable", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("redeclare after reconnect: %v", err)
	}
	if q.Messages != 1 {
		t.Fatalf("declare-ok reported %d messages, want 1", q.Messages)
	}
	d, err := second.Consume("durable", "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if m := next(t, d); string(m.Body) != "kept" {
		t.Fatalf("body = %q, want kept", m.Body)
	}
}

// TestQosBoundsInFlight: prefetch 1 means one unacked delivery at a time.
func TestQosBoundsInFlight(t *testing.T) {
	_, ch := dial(t, gateway(t))
	if _, err := ch.QueueDeclare("paced", true, false, false, false, nil); err != nil {
		t.Fatalf("queue.declare: %v", err)
	}
	if err := ch.Qos(1, 0, false); err != nil {
		t.Fatalf("basic.qos: %v", err)
	}
	d, err := ch.Consume("paced", "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	for i := range 3 {
		if err := ch.PublishWithContext(ctx(t), "", "paced", false, false,
			amqp.Publishing{Body: fmt.Appendf(nil, "%d", i)}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	for i := range 3 {
		m := next(t, d)
		if string(m.Body) != fmt.Sprint(i) {
			t.Fatalf("delivery %d = %q, want %d", i, m.Body, i)
		}
		select {
		case extra := <-d:
			t.Fatalf("prefetch 1 delivered %q before %d was acked", extra.Body, i)
		case <-time.After(300 * time.Millisecond):
		}
		if err := m.Ack(false); err != nil {
			t.Fatalf("ack %d: %v", i, err)
		}
	}
}

// TestAbsentMethodIsAChannelError is the promise the package doc makes: what
// is not implemented SAYS so, on the channel, and the connection lives.
func TestAbsentMethodIsAChannelError(t *testing.T) {
	url := gateway(t)
	conn, ch := dial(t, url)

	err := ch.ExchangeDeclare("byheaders", "headers", true, false, false, false, nil)
	if err == nil {
		t.Fatal("a headers exchange was accepted; it has no subject to be")
	}
	var ae *amqp.Error
	if !errors.As(err, &ae) {
		t.Fatalf("error was %T (%v), want an *amqp.Error", err, err)
	}
	if ae.Code != 540 {
		t.Fatalf("reply code = %d, want 540 NOT_IMPLEMENTED", ae.Code)
	}
	if ae.Server != true {
		t.Fatal("the error did not come from the server")
	}

	// The connection survives a channel exception: a fresh channel works.
	again, err := conn.Channel()
	if err != nil {
		t.Fatalf("the connection died with the channel: %v", err)
	}
	if _, err := again.QueueDeclare("still-here", true, false, false, false, nil); err != nil {
		t.Fatalf("declare on a new channel: %v", err)
	}
}

// TestDottedNameIsRefused: a name with a dot would open subject levels and
// alias another exchange, so it is refused where the client can see it rather
// than mis-routed where it cannot.
func TestDottedNameIsRefused(t *testing.T) {
	_, ch := dial(t, gateway(t))
	_, err := ch.QueueDeclare("a.b", true, false, false, false, nil)
	var ae *amqp.Error
	if !errors.As(err, &ae) {
		t.Fatalf("error was %T (%v), want an *amqp.Error", err, err)
	}
	if ae.Code != 530 {
		t.Fatalf("reply code = %d, want 530 NOT_ALLOWED", ae.Code)
	}
	if !strings.Contains(ae.Reason, "a.b") {
		t.Fatalf("the refusal did not name the offending queue: %q", ae.Reason)
	}
}

// TestOversizedFrameIsRefused is the DoS guard. The size field is 32 bits, so
// a peer can ask for a 4 GiB allocation in seven octets; the server must
// refuse on the header, before it reads or allocates a byte of the payload.
func TestOversizedFrameIsRefused(t *testing.T) {
	url := strings.TrimPrefix(gateway(t), "amqp://")
	conn, err := net.Dial("tcp", url)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}); err != nil {
		t.Fatalf("write protocol header: %v", err)
	}
	// Read connection.start so the server is past the header and into frames.
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var hdr [7]byte
	if _, err := readFull(conn, hdr[:]); err != nil {
		t.Fatalf("read connection.start header: %v", err)
	}
	size := binary.BigEndian.Uint32(hdr[3:7])
	if _, err := readFull(conn, make([]byte, size+1)); err != nil {
		t.Fatalf("read connection.start payload: %v", err)
	}

	// Now claim a 4 GiB frame.
	var huge [7]byte
	huge[0] = 1 // method
	binary.BigEndian.PutUint32(huge[3:7], 0xFFFFFFFF)
	if _, err := conn.Write(huge[:]); err != nil {
		t.Fatalf("write oversized frame header: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := readFull(conn, make([]byte, 1)); err == nil {
		t.Fatal("the server kept the connection after a frame claiming 4 GiB")
	}
}

func readFull(c net.Conn, b []byte) (int, error) {
	n := 0
	for n < len(b) {
		m, err := c.Read(b[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// TestExclusiveQueueBelongsToOneConnection: a second connection may not take,
// consume or get an exclusive queue. Before this held, both connections were
// made owners and EITHER one's close deleted the other's queue.
func TestExclusiveQueueBelongsToOneConnection(t *testing.T) {
	url := gateway(t)
	_, first := dial(t, url)
	if _, err := first.QueueDeclare("mine", false, false, true, false, nil); err != nil {
		t.Fatalf("queue.declare: %v", err)
	}

	for _, c := range []struct {
		what string
		try  func(*amqp.Channel) error
	}{
		{"declare", func(ch *amqp.Channel) error {
			_, err := ch.QueueDeclare("mine", false, false, true, false, nil)
			return err
		}},
		{"consume", func(ch *amqp.Channel) error {
			_, err := ch.Consume("mine", "", true, false, false, false, nil)
			return err
		}},
		{"get", func(ch *amqp.Channel) error {
			_, _, err := ch.Get("mine", true)
			return err
		}},
	} {
		// A fresh connection each time: a channel exception closes the channel.
		_, other := dial(t, url)
		err := c.try(other)
		var ae *amqp.Error
		if !errors.As(err, &ae) {
			t.Fatalf("%s from another connection: err = %v, want an *amqp.Error", c.what, err)
		}
		if ae.Code != 405 {
			t.Fatalf("%s from another connection: code = %d, want 405 RESOURCE_LOCKED", c.what, ae.Code)
		}
	}

	// The owner still holds it.
	if _, err := first.QueueDeclare("mine", false, false, true, false, nil); err != nil {
		t.Fatalf("the declaring connection lost its own exclusive queue: %v", err)
	}
}

// TestRedeclareWithDifferentFlagsIsRefused: a client told "ok" for a durable
// queue that is not durable believes its messages survive a restart.
func TestRedeclareWithDifferentFlagsIsRefused(t *testing.T) {
	_, ch := dial(t, gateway(t))
	if _, err := ch.QueueDeclare("flags", false, false, false, false, nil); err != nil {
		t.Fatalf("queue.declare: %v", err)
	}
	_, err := ch.QueueDeclare("flags", true, false, false, false, nil) // now durable
	var ae *amqp.Error
	if !errors.As(err, &ae) {
		t.Fatalf("redeclare with different flags: err = %v, want an *amqp.Error", err)
	}
	if ae.Code != 406 {
		t.Fatalf("code = %d, want 406 PRECONDITION_FAILED", ae.Code)
	}
}

// TestAutoDeleteOutlivesItsDeclarerAndDiesWithItsLastConsumer: auto-delete
// names the last consumer leaving, which is NOT the connection closing —
// deleting on close took queues other connections were still consuming.
func TestAutoDeleteOutlivesItsDeclarerAndDiesWithItsLastConsumer(t *testing.T) {
	url := gateway(t)

	declarer, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	dch, err := declarer.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	if _, err := dch.QueueDeclare("transient", false, true, false, false, nil); err != nil {
		t.Fatalf("queue.declare: %v", err)
	}

	_, consumer := dial(t, url)
	if _, err := consumer.Consume("transient", "c", false, false, false, false, nil); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err := declarer.Close(); err != nil {
		t.Fatalf("close the declaring connection: %v", err)
	}

	// Still there, because a consumer is still on it.
	if _, err := consumer.QueueDeclarePassive("transient", false, true, false, false, nil); err != nil {
		t.Fatalf("the auto-delete queue died with the connection that declared it: %v", err)
	}

	if err := consumer.Cancel("c", false); err != nil {
		t.Fatalf("basic.cancel: %v", err)
	}
	_, spare := dial(t, url)
	if _, err := spare.QueueDeclarePassive("transient", false, true, false, false, nil); err == nil {
		t.Fatal("the auto-delete queue outlived its last consumer")
	}
}

// TestABodyLargerThanTheBusIsRefusedOnTheHeader: a body size is a claim, and a
// broker that believes one before reading it has been handed an allocation.
func TestABodyLargerThanTheBusIsRefusedOnTheHeader(t *testing.T) {
	url := strings.TrimPrefix(gateway(t), "amqp://")
	conn, err := net.Dial("tcp", url)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))

	if _, err := conn.Write([]byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}); err != nil {
		t.Fatalf("write protocol header: %v", err)
	}
	r := wire.NewReader(conn, protocol.DefaultFrameMax)
	w := wire.NewWriter(conn, protocol.DefaultFrameMax)
	read := func(what string) wire.Frame {
		t.Helper()
		f, err := r.Read()
		if err != nil {
			t.Fatalf("read %s: %v", what, err)
		}
		return f
	}
	send := func(what string, b *wire.Buf, ch uint16) {
		t.Helper()
		if err := w.Send(b.Frame(ch)); err != nil {
			t.Fatalf("write %s: %v", what, err)
		}
	}

	read("connection.start")
	send("connection.start-ok", wire.NewMethod(10, 11).
		Table(wire.Table{"product": "probe"}).ShortStr("PLAIN").LongStr("\x00g\x00g").ShortStr("en_US"), 0)
	read("connection.tune")
	send("connection.tune-ok", wire.NewMethod(10, 31).Short(64).Long(protocol.DefaultFrameMax).Short(0), 0)
	send("connection.open", wire.NewMethod(10, 40).ShortStr("/").ShortStr("").Bit(false), 0)
	read("connection.open-ok")
	send("channel.open", wire.NewMethod(20, 10).ShortStr(""), 1)
	read("channel.open-ok")

	// basic.publish, then a content header claiming 4 GiB.
	send("basic.publish", wire.NewMethod(60, 40).Short(0).ShortStr("").ShortStr("anywhere").Bit(false).Bit(false), 1)
	if err := w.Send(wire.HeaderFrame(1, 60, 4<<30, wire.Props{})); err != nil {
		t.Fatalf("write content header: %v", err)
	}

	f := read("the refusal")
	class, method, err := wire.ClassMethod(f.Payload)
	if err != nil || class != 20 || method != 40 {
		t.Fatalf("answer was class %d method %d (%v), want channel.close", class, method, err)
	}
	if code := wire.NewArgs(f.Payload).Short(); code != 311 {
		t.Fatalf("reply code = %d, want 311 CONTENT_TOO_LARGE", code)
	}
}
