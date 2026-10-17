package protocol_test

import (
	"net"
	"testing"
	"time"

	"github.com/hanzoai/amqp/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

// TestAcceptServesAHandedListener: a host that owns the port connects the
// gateway to the bus first and then hands it a listener. A unix socket stands in
// for the host's relay, and the reference client publishes and consumes over it.
func TestAcceptServesAHandedListener(t *testing.T) {
	b := protocol.NewBroker(protocol.Config{PubSubURL: bus(t)})
	t.Cleanup(b.Shutdown)
	if err := b.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	ln, err := net.Listen("unix", t.TempDir()+"/amqp.wire")
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- b.Accept(ln) }()
	select {
	case err := <-errc:
		t.Fatalf("accept returned before it was ready: %v", err)
	case <-b.Ready():
	case <-time.After(10 * time.Second):
		t.Fatal("accept never became ready")
	}

	conn, err := amqp.DialConfig("amqp://wire/", amqp.Config{Dial: func(string, string) (net.Conn, error) {
		return net.Dial("unix", ln.Addr().String())
	}})
	if err != nil {
		t.Fatalf("dial the handed listener: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	if _, err := ch.QueueDeclare("handed", true, false, false, false, nil); err != nil {
		t.Fatalf("queue.declare: %v", err)
	}
	if err := ch.PublishWithContext(ctx(t), "", "handed", false, false, amqp.Publishing{Body: []byte("hi")}); err != nil {
		t.Fatalf("basic.publish: %v", err)
	}
	d, err := ch.Consume("handed", "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("basic.consume: %v", err)
	}
	if m := next(t, d); string(m.Body) != "hi" {
		t.Fatalf("body %q, want hi", m.Body)
	}

	b.Shutdown()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("accept after shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("accept did not return after shutdown")
	}
}

// TestConnectFailsClosedAndBindsNothing: an unreachable bus is an error from
// Connect, before any listener exists to accept a client it cannot serve.
func TestConnectFailsClosedAndBindsNothing(t *testing.T) {
	b := protocol.NewBroker(protocol.Config{PubSubURL: "nats://127.0.0.1:1"})
	t.Cleanup(b.Shutdown)
	if err := b.Connect(); err == nil {
		t.Fatal("an unreachable bus must fail Connect")
	}
	if b.Addr() != nil {
		t.Fatalf("a failed Connect bound %v", b.Addr())
	}
}
