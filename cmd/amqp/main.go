// hanzo-amqp — HIP-0106 thin shim.
//
// Mounts pkg/amqp into a zip.App via the same Mount() the unified
// cloud binary calls. The AMQP 0-9-1 listener and NATS JetStream
// bridge are wired by pkg/amqp.Mount — this binary only handles
// process lifecycle.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/luxfi/log"

	"github.com/hanzoai/amqp"
	"github.com/hanzoai/cloud"
	"github.com/hanzoai/zip"
	"github.com/hanzoai/zip/middleware"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	amqp.Version = version

	cfg := cloud.LoadConfig()
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}
	deps := cloud.BuildDeps(cfg)

	app := zip.New(zip.Config{
		Logger:  deps.Logger,
		AppName: "amqp",
	})
	app.Use(middleware.Recover())
	app.Use(middleware.RequestID())
	app.Use(middleware.Logger(deps.Logger))

	if err := amqp.Mount(app, deps); err != nil {
		log.Crit("amqp: mount", "err", err)
	}

	listenErr := make(chan error, 1)
	go func() {
		log.Info("amqp shim: HTTP listening", "addr", cfg.ListenAddr)
		listenErr <- app.Listen(cfg.ListenAddr)
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sig:
		log.Info("amqp shim: shutting down", "signal", s)
	case err := <-listenErr:
		log.Crit("amqp shim: HTTP listen failed", "err", err)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	_ = amqp.Shutdown(stopCtx)
	_ = app.ShutdownWithContext(stopCtx)
}
