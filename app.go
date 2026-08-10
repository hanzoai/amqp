// Package amqp is a RabbitMQ-compatible front door for Hanzo PubSub: it
// speaks AMQP 0-9-1 on the wire and NATS JetStream underneath, so a client
// that can only talk to RabbitMQ runs unchanged on Hanzo infrastructure.
//
// The gateway is a TCP listener, not an HTTP surface. What it publishes over
// HTTP is two probes — /v1/amqp/health (liveness) and /v1/amqp/readyz (ready
// only once NATS is connected and the listener is bound). The listener is
// opt-in: it binds when AMQP_BIND is set (default 0.0.0.0:5672) and NATS
// answers. When NATS does not answer the probes stay honest — health 200,
// readyz 503 — rather than killing the process, because an operator often
// brings the broker up after the gateway.
//
// App builds and returns its own zip.App. It takes nothing from whoever runs
// it, so the same app serves standalone (cmd/amqp) and composed into a host
// binary, and no host can claim it exclusively.
package amqp

import (
	"cmp"
	"context"
	"net/http"
	"os"
	"strings"
	"sync"

	luxlog "github.com/luxfi/log"
	"github.com/zap-proto/zip"
	"github.com/zap-proto/zip/middleware"

	"github.com/hanzoai/amqp/proxy"
)

// Version is overridden at build time via -ldflags
// "-X github.com/hanzoai/amqp.Version=...".
var Version = "dev"

var (
	mu      sync.RWMutex
	current *proxy.Proxy
	running bool
)

// Config overrides the env-driven defaults App reads at startup. Empty
// fields fall back to env.
type Config struct {
	// AMQPAddr — TCP listen address for the AMQP 0-9-1 gateway.
	// Empty → $AMQP_BIND or 0.0.0.0:5672.
	AMQPAddr string

	// PubSubURL — NATS server URL. Empty → $NATS_URL or
	// nats://localhost:4222. When NATS is unreachable, App emits a warning
	// and leaves the gateway disabled.
	PubSubURL string

	// PubSubCreds — optional NATS credentials file. Empty → $NATS_CREDS.
	PubSubCreds string

	// DisableListener — do not bind a TCP listener. Used by tests and by
	// control-plane-only deployments that want the probes and nothing else.
	DisableListener bool
}

// App builds the AMQP gateway and returns it.
//
// The returned app carries its own middleware and its own probes, and — when
// a listener is wanted — spawns the AMQP 0-9-1 gateway in the background.
// zip.Config.Eager records that fact: this app's work is not request-driven,
// so a host must start it rather than wait for a request to arrive.
//
// The logger comes from luxfi/log's process default. It used to arrive as a
// field on the host's dependency struct, which is what made this package
// importable by exactly one host; a package-level default cannot be nil and
// cannot be owned.
func App(cfg Config) (*zip.App, error) {
	log := luxlog.Default().New("subsystem", "amqp")

	cfg.AMQPAddr = cmp.Or(cfg.AMQPAddr, envOr("AMQP_BIND", "0.0.0.0:5672"))
	cfg.PubSubURL = cmp.Or(cfg.PubSubURL, envOr("NATS_URL", "nats://localhost:4222"))
	cfg.PubSubCreds = cmp.Or(cfg.PubSubCreds, envOr("NATS_CREDS", ""))

	app := zip.New(zip.Config{
		Logger:  log,
		AppName: "amqp",
		Eager:   !cfg.DisableListener,
	})
	app.Use(middleware.Recover(), middleware.RequestID(), middleware.Logger(log))
	routes(app)

	if cfg.DisableListener {
		log.Info("amqp ready (listener disabled)", "version", Version)
		return app, nil
	}

	// Boot the gateway in the background. A connect failure does not fail the
	// build: the operator may bring NATS up after this binary is already
	// serving, and health stays green so k8s does not restart-loop a pod that
	// is waiting on purpose. readyz is what reports the truth.
	go func() {
		p, err := proxy.New(proxy.Config{
			AMQPAddr:    cfg.AMQPAddr,
			PubSubURL:   cfg.PubSubURL,
			PubSubCreds: cfg.PubSubCreds,
		})
		if err != nil {
			log.Warn("amqp: pubsub unreachable — gateway disabled",
				"err", err,
				"pubsub", cfg.PubSubURL)
			return
		}
		mu.Lock()
		current, running = p, true
		mu.Unlock()
		log.Info("amqp listener starting",
			"amqp_bind", cfg.AMQPAddr,
			"pubsub", cfg.PubSubURL)
		// Start blocks until Shutdown is called.
		if err := p.Start(); err != nil {
			log.Error("amqp listener exited", "err", err)
		}
		mu.Lock()
		running = false
		mu.Unlock()
	}()

	log.Info("amqp ready",
		"version", Version,
		"amqp_bind", cfg.AMQPAddr,
		"pubsub", cfg.PubSubURL)
	return app, nil
}

// routes wires the probes. No infrastructure, so it is what the route test
// exercises; App adds the gateway on top.
func routes(app *zip.App) {
	app.Get("/v1/amqp/health", func(c *zip.Ctx) error {
		return c.JSON(http.StatusOK, map[string]any{
			"status":  "ok",
			"service": "amqp",
			"version": Version,
		})
	})

	app.Get("/v1/amqp/readyz", func(c *zip.Ctx) error {
		mu.RLock()
		up := running
		mu.RUnlock()
		if !up {
			return c.JSON(http.StatusServiceUnavailable, map[string]any{
				"status":  "not_ready",
				"service": "amqp",
				"reason":  "AMQP listener not running (check NATS connectivity)",
			})
		}
		return c.JSON(http.StatusOK, map[string]any{
			"status":  "ready",
			"service": "amqp",
			"version": Version,
		})
	})
}

// Shutdown drains the gateway. Idempotent, and safe when no listener was
// ever bound.
func Shutdown(_ context.Context) error {
	mu.Lock()
	defer mu.Unlock()
	if current == nil {
		return nil
	}
	current.Shutdown()
	current = nil
	running = false
	return nil
}

func envOr(key, dflt string) string {
	return cmp.Or(strings.TrimSpace(os.Getenv(key)), dflt)
}
