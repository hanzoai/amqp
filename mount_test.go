package amqp_test

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	luxlog "github.com/luxfi/log"

	"github.com/hanzoai/amqp"
	"github.com/hanzoai/cloud"
	"github.com/hanzoai/zip"
)

func TestMount_HealthReadyz(t *testing.T) {
	app := zip.New(zip.Config{Logger: luxlog.New("test")})
	deps := cloud.Deps{Logger: luxlog.New("test")}

	// DisableListener=true keeps the test from trying to bind 5672 or
	// reach a NATS server. Health stays 200, readyz stays 503.
	if err := amqp.MountWithConfig(app, deps, amqp.MountConfig{
		DisableListener: true,
	}); err != nil {
		t.Fatalf("Mount: %v", err)
	}

	req := httptest.NewRequest("GET", "/v1/amqp/health", nil)
	resp, err := app.Fiber().Test(req)
	if err != nil {
		t.Fatalf("health test: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("health status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var hb map[string]any
	_ = json.Unmarshal(body, &hb)
	if hb["service"] != "amqp" {
		t.Fatalf("health body service = %v, want amqp", hb["service"])
	}

	req = httptest.NewRequest("GET", "/v1/amqp/readyz", nil)
	resp, err = app.Fiber().Test(req)
	if err != nil {
		t.Fatalf("readyz test: %v", err)
	}
	if resp.StatusCode != 503 {
		t.Fatalf("readyz (no listener) status = %d, want 503", resp.StatusCode)
	}
}
