# Hanzo AMQP — ZAP Schema
#
# Server: amqp (Go) embedded via pkg/amqp.Mount(app, deps) per HIP-0106.
#
# The data plane is the AMQP 0-9-1 wire protocol (RabbitMQ-compatible)
# bridged to NATS JetStream — that surface is not ZAP. This schema
# captures only the management-plane health + readiness ZAP RPCs so
# the cloud binary can probe AMQP through the canonical Mount() shape.
#
# Code generation:
#   zapc generate schema/amqp.zap --lang go --out ./gen/zap/

# ── Health ────────────────────────────────────────────────────────────────

struct HealthRequest

struct HealthResponse
  status   Text
  service  Text
  version  Text

struct ReadyResponse
  status   Text
  service  Text
  version  Text
  reason   Text

# ── Service interface ────────────────────────────────────────────────────

interface AMQPService
  health (request HealthRequest) -> (response HealthResponse)
  ready  (request HealthRequest) -> (response ReadyResponse)
