// Command amqp runs the AMQP 0-9-1 gateway as its own process.
//
// It owns process lifecycle and nothing else: protocol.Broker is the gateway,
// this binary starts it and drains it. There is no HTTP surface, because a
// broker's readiness is whether its port accepts — which is what a TCP probe
// asks and what every client asks.
package main

import (
	"cmp"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/luxfi/log"

	"github.com/hanzoai/amqp/protocol"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	var cfg protocol.Config
	flag.StringVar(&cfg.Addr, "addr", env("AMQP_BIND", protocol.DefaultAddr), "AMQP 0-9-1 listen address")
	flag.StringVar(&cfg.PubSubURL, "pubsub-url", env("PUBSUB_URL", "nats://127.0.0.1:4222"), "the bus to translate to and from")
	flag.StringVar(&cfg.PubSubCreds, "pubsub-creds", env("PUBSUB_CREDS", ""), "NATS credentials file")
	flag.Parse()
	cfg.Version = version

	b := protocol.NewBroker(cfg)
	done := make(chan error, 1)
	go func() { done <- b.Serve() }()

	select {
	case err := <-done:
		log.Crit("amqp: serve", "err", err)
	case <-b.Ready():
		log.Info("amqp: serving", "addr", b.Addr(), "pubsub", cfg.PubSubURL, "version", version)
	case <-time.After(30 * time.Second):
		log.Crit("amqp: the bus did not answer within 30s", "pubsub", cfg.PubSubURL)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sig:
		log.Info("amqp: shutting down", "signal", s)
	case err := <-done:
		log.Crit("amqp: serve exited", "err", err)
	}
	b.Shutdown()
}

func env(key, dflt string) string { return cmp.Or(os.Getenv(key), dflt) }
