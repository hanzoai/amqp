package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/hanzoai/amqp/proxy"
	"github.com/spf13/cobra"
)

func main() {
	cfg := proxy.Config{
		AMQPAddr:   "0.0.0.0:5672",
		PubSubURL:  "nats://localhost:4222",
	}

	var rootCmd = &cobra.Command{
		Use:   "hanzo-amqp",
		Short: "Hanzo AMQP -- RabbitMQ wire protocol gateway for Hanzo PubSub",
		Run: func(cmd *cobra.Command, args []string) {
			p, err := proxy.New(cfg)
			if err != nil {
				proxy.Log("FATAL", "failed to create proxy: %v", err)
				os.Exit(1)
			}

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				sig := <-sigCh
				proxy.Log("INFO", "received signal: %s, shutting down...", sig)
				p.Shutdown()
				os.Exit(0)
			}()

			if err := p.Start(); err != nil {
				proxy.Log("FATAL", "proxy failed: %v", err)
				os.Exit(1)
			}
		},
	}

	rootCmd.Flags().StringVar(&cfg.AMQPAddr, "amqp-addr", "0.0.0.0:5672", "AMQP listener address")
	rootCmd.Flags().StringVar(&cfg.PubSubURL, "pubsub-url", "nats://localhost:4222", "Hanzo PubSub server URL")
	rootCmd.Flags().StringVar(&cfg.PubSubCreds, "pubsub-creds", "", "Hanzo PubSub credentials file")

	if err := rootCmd.Execute(); err != nil {
		proxy.Log("FATAL", "failed to execute: %v", err)
		os.Exit(1)
	}
}
