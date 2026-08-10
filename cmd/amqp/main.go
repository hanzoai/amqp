// hanzo-amqp runs the AMQP 0-9-1 gateway as its own process.
//
// It owns process lifecycle and nothing else: amqp.App builds the gateway
// and its probes, this binary listens and drains. The same app composes into
// a host binary unchanged.
package main

import (
	"cmp"
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/luxfi/log"

	"github.com/hanzoai/amqp"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	var cfg amqp.Config
	flag.StringVar(&cfg.AMQPAddr, "amqp-addr", "", "AMQP 0-9-1 listen address ($AMQP_BIND, else 0.0.0.0:5672)")
	flag.StringVar(&cfg.PubSubURL, "pubsub-url", "", "NATS server URL ($NATS_URL, else nats://localhost:4222)")
	flag.StringVar(&cfg.PubSubCreds, "pubsub-creds", "", "NATS credentials file ($NATS_CREDS)")
	httpAddr := flag.String("http-addr", cmp.Or(os.Getenv("HTTP_BIND"), ":8080"), "health/readyz listen address")
	flag.Parse()

	amqp.Version = version
	app, err := amqp.App(cfg)
	if err != nil {
		log.Crit("amqp: build", "err", err)
	}

	// zip reads the transport off the address scheme and a bare one means ZAP.
	// These are kubelet probes, so say HTTP.
	addr := *httpAddr
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}

	listenErr := make(chan error, 1)
	go func() {
		log.Info("amqp: HTTP listening", "addr", addr)
		listenErr <- app.Listen(addr)
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sig:
		log.Info("amqp: shutting down", "signal", s)
	case err := <-listenErr:
		log.Crit("amqp: HTTP listen failed", "err", err)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	_ = amqp.Shutdown(stopCtx)
	_ = app.ShutdownWithContext(stopCtx)
}
