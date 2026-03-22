package proxy

import (
	"fmt"
	"net"
	"sync"

	"github.com/nats-io/nats.go"
)

// Config holds configuration for the AMQP->NATS proxy.
type Config struct {
	AMQPAddr   string // Listen address for AMQP connections (default: 0.0.0.0:5672)
	PubSubURL  string // NATS server URL
	PubSubCreds string // Optional NATS credentials file
}

// Proxy is the AMQP-to-NATS gateway. It accepts AMQP 0-9-1 connections
// and translates publish/consume operations to NATS JetStream.
type Proxy struct {
	cfg      Config
	pubsub   *PubSub
	listener net.Listener
	wg       sync.WaitGroup
	quit     chan struct{}
}

// New creates a new Proxy. It connects to NATS and ensures JetStream
// streams exist for all AML channel mappings.
func New(cfg Config) (*Proxy, error) {
	var opts []nats.Option
	if cfg.PubSubCreds != "" {
		opts = append(opts, nats.UserCredentials(cfg.PubSubCreds))
	}

	pubsub, err := NewPubSub(cfg.PubSubURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to pubsub: %w", err)
	}

	if err := pubsub.EnsureStreams(DefaultMappings()); err != nil {
		pubsub.Close()
		return nil, fmt.Errorf("ensure streams: %w", err)
	}

	return &Proxy{
		cfg:    cfg,
		pubsub: pubsub,
		quit:   make(chan struct{}),
	}, nil
}

// Start begins accepting AMQP connections. Blocks until Shutdown is called.
func (p *Proxy) Start() error {
	ln, err := net.Listen("tcp", p.cfg.AMQPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", p.cfg.AMQPAddr, err)
	}
	p.listener = ln
	Log("INFO", "AMQP proxy listening on %s", p.cfg.AMQPAddr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-p.quit:
				return nil // Clean shutdown
			default:
				Log("ERROR", "accept: %v", err)
				continue
			}
		}

		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			HandleConnection(conn, p.pubsub)
		}()
	}
}

// Shutdown gracefully shuts down the proxy.
func (p *Proxy) Shutdown() {
	close(p.quit)
	if p.listener != nil {
		p.listener.Close()
	}
	p.wg.Wait()
	p.pubsub.Close()
	Log("INFO", "AMQP proxy shut down")
}
