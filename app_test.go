package amqp_test

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/hanzoai/amqp"
)

func TestApp_HealthReadyz(t *testing.T) {
	// DisableListener keeps the test off port 5672 and off any NATS server.
	// Health stays 200, readyz stays 503.
	app, err := amqp.App(amqp.Config{DisableListener: true})
	if err != nil {
		t.Fatalf("App: %v", err)
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
