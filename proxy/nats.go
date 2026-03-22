package proxy

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

// PubSub wraps a NATS JetStream connection for the proxy.
type PubSub struct {
	NC *nats.Conn
	JS nats.JetStreamContext
}

// NewPubSub connects to Hanzo PubSub and obtains a JetStream context.
func NewPubSub(url string, opts ...nats.Option) (*PubSub, error) {
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to pubsub %s: %w", url, err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("get jetstream context: %w", err)
	}
	Log("INFO", "connected to Hanzo PubSub at %s", url)
	return &PubSub{NC: nc, JS: js}, nil
}

// EnsureStreams creates JetStream streams for all Jube channel mappings.
func (p *PubSub) EnsureStreams(mappings []ChannelMapping) error {
	for _, m := range mappings {
		_, err := p.JS.AddStream(&nats.StreamConfig{
			Name:     m.NATSStream,
			Subjects: []string{m.NATSSubject},
			Storage:  nats.FileStorage,
			Replicas: 1,
		})
		if err != nil {
			return fmt.Errorf("create stream %s: %w", m.NATSStream, err)
		}
		Log("INFO", "ensured stream %s -> %s", m.NATSStream, m.NATSSubject)
	}
	return nil
}

// Publish publishes a message to a NATS JetStream subject.
func (p *PubSub) Publish(subject string, data []byte) error {
	_, err := p.JS.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("publish to %s: %w", subject, err)
	}
	return nil
}

// Subscribe creates a pull-based consumer for a NATS subject and delivers
// messages to the callback. The consumer name is derived from the stream name.
func (p *PubSub) Subscribe(streamName, subject string, handler func([]byte)) (*nats.Subscription, error) {
	sub, err := p.JS.Subscribe(subject, func(msg *nats.Msg) {
		handler(msg.Data)
		msg.Ack()
	}, nats.Durable(streamName+"-consumer"), nats.ManualAck())
	if err != nil {
		return nil, fmt.Errorf("subscribe to %s: %w", subject, err)
	}
	return sub, nil
}

// Close closes the NATS connection.
func (p *PubSub) Close() {
	if p.NC != nil {
		p.NC.Close()
		Log("INFO", "Hanzo PubSub connection closed")
	}
}
