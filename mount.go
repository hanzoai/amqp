// Package amqp wraps the proxy package in the HIP-0106 Mount() shape.
// Lets cmd/cloud import this package and register the AMQP -> NATS
// JetStream gateway with the shared zip.App.
//
// Wire shape:
//
//	import _ "github.com/hanzoai/amqp"  // init() registers
//
// AMQP is broker-only — there is no HTTP surface today. The Mount()
// exposes /v1/amqp/health and /v1/amqp/readyz on the parent zip.App
// and spawns the AMQP 0-9-1 listener in a background goroutine. The
// listener is opt-in: it starts only when AMQP_BIND is set on the
// process env (default ":5672") and NATS connectivity succeeds. When
// disabled, readyz reports "not_ready".
package amqp

import (
	"context"
	"net/http"
	"strings"
	"sync"

	"github.com/hanzoai/amqp/proxy"
	"github.com/hanzoai/cloud"
	"github.com/zap-proto/zip"
)

// Version is overridden at build time via -ldflags
// "-X github.com/hanzoai/amqp/pkg/amqp.Version=...".
var Version = "dev"

var (
	mu             sync.RWMutex
	mountedProxy   *proxy.Proxy
	mountedRunning bool
)

// MountConfig overrides the env-driven defaults Mount() reads at
// startup. Empty fields fall back to env. Tests use this to inject
// a no-op proxy.
type MountConfig struct {
	// AMQPAddr — TCP listen address for the AMQP 0-9-1 gateway.
	// Empty → $AMQP_BIND or :5672.
	AMQPAddr string

	// PubSubURL — NATS server URL. Empty → $NATS_URL or
	// nats://localhost:4222. When NATS is unreachable, Mount() emits
	// a warning and leaves the proxy disabled.
	PubSubURL string

	// PubSubCreds — optional NATS credentials file.
	// Empty → $NATS_CREDS.
	PubSubCreds string

	// DisableListener — true means do not actually bind a TCP
	// listener. Used by tests and by the cloud binary in
	// "control-plane-only" deployments.
	DisableListener bool
}

// Mount registers AMQP routes with the shared cloud zip.App per HIP-0106.
//
// Routes:
//
//	GET /v1/amqp/health   — liveness; always 200 if the process is up
//	GET /v1/amqp/readyz   — ready when NATS connected + listener bound
//
// In a separate goroutine, Mount() boots the AMQP 0-9-1 proxy when
// NATS connectivity succeeds. Failure to connect to NATS is logged
// and treated as "data plane disabled" — the binary continues to
// serve /v1/amqp/health for k8s liveness while the operator fixes the
// upstream.
func Mount(app *zip.App, deps cloud.Deps) error {
	return mountWithConfig(app, deps, MountConfig{})
}

// MountWithConfig is the explicit-config variant used by the
// standalone shim and tests.
func MountWithConfig(app *zip.App, deps cloud.Deps, cfg MountConfig) error {
	return mountWithConfig(app, deps, cfg)
}

func mountWithConfig(app *zip.App, deps cloud.Deps, cfg MountConfig) error {
	logger := deps.Logger.New("subsystem", "amqp")

	app.Get("/v1/amqp/health", func(c *zip.Ctx) error {
		return c.JSON(http.StatusOK, map[string]any{
			"status":  "ok",
			"service": "amqp",
			"version": Version,
		})
	})

	app.Get("/v1/amqp/readyz", func(c *zip.Ctx) error {
		mu.RLock()
		running := mountedRunning
		mu.RUnlock()
		if !running {
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

	if cfg.AMQPAddr == "" {
		cfg.AMQPAddr = envOr("AMQP_BIND", "0.0.0.0:5672")
	}
	if cfg.PubSubURL == "" {
		cfg.PubSubURL = envOr("NATS_URL", "nats://localhost:4222")
	}
	if cfg.PubSubCreds == "" {
		cfg.PubSubCreds = strings.TrimSpace(envOr("NATS_CREDS", ""))
	}

	if cfg.DisableListener {
		logger.Info("amqp mounted (listener disabled)", "version", Version)
		return nil
	}

	// Boot the proxy in a goroutine. Connect failures don't fail the
	// Mount(); the operator may bring NATS up after the cloud binary
	// is already serving traffic. Health probes stay green so k8s
	// doesn't restart-loop a pod that's intentionally waiting.
	go func() {
		p, err := proxy.New(proxy.Config{
			AMQPAddr:    cfg.AMQPAddr,
			PubSubURL:   cfg.PubSubURL,
			PubSubCreds: cfg.PubSubCreds,
		})
		if err != nil {
			logger.Warn("amqp: pubsub unreachable — data plane disabled",
				"err", err,
				"pubsub", cfg.PubSubURL)
			return
		}
		mu.Lock()
		mountedProxy = p
		mountedRunning = true
		mu.Unlock()
		logger.Info("amqp listener starting",
			"amqp_bind", cfg.AMQPAddr,
			"pubsub", cfg.PubSubURL)
		// Start blocks until Shutdown is called.
		if err := p.Start(); err != nil {
			logger.Error("amqp listener exited", "err", err)
		}
		mu.Lock()
		mountedRunning = false
		mu.Unlock()
	}()

	logger.Info("amqp mounted",
		"version", Version,
		"amqp_bind", cfg.AMQPAddr,
		"pubsub", cfg.PubSubURL)
	return nil
}

// Shutdown drains the AMQP proxy. Idempotent. Safe to call when
// Mount() never bound a listener.
func Shutdown(_ context.Context) error {
	mu.Lock()
	defer mu.Unlock()
	if mountedProxy == nil {
		return nil
	}
	mountedProxy.Shutdown()
	mountedProxy = nil
	mountedRunning = false
	return nil
}

// envOr is a small helper to keep the Mount() body terse.
func envOr(key, dflt string) string {
	if v := strings.TrimSpace(osGetenv(key)); v != "" {
		return v
	}
	return dflt
}

// osGetenv exists as a tiny indirection so tests can stub env reads
// without pulling in os.* directly into every file in this package.
var osGetenv = func(key string) string {
	return osGetenvImpl(key)
}

// Mount and Shutdown are the whole contract. cloud/apps.Wire lists this adaptor
// as a MountSpec in mount order, the same as the Kafka adaptor beside it — one
// way to attach a subsystem, no self-registering init and no separate ordering
// number to keep in sync with the list.
