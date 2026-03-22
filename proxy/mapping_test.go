package proxy

import "testing"

func TestDefaultMappings(t *testing.T) {
	mappings := DefaultMappings()
	if len(mappings) != 4 {
		t.Fatalf("expected 4 mappings, got %d", len(mappings))
	}

	expected := map[string]string{
		"jubeInbound":       "jube.inbound",
		"jubeOutbound":      "jube.outbound",
		"jubeActivations":   "jube.activations",
		"jubeNotifications": "jube.notifications",
	}

	for _, m := range mappings {
		nats, ok := expected[m.AMQPName]
		if !ok {
			t.Errorf("unexpected AMQP name: %q", m.AMQPName)
			continue
		}
		if m.NATSSubject != nats {
			t.Errorf("mapping %s: expected NATS subject %q, got %q", m.AMQPName, nats, m.NATSSubject)
		}
		if m.NATSStream == "" {
			t.Errorf("mapping %s: empty stream name", m.AMQPName)
		}
	}
}

func TestMappingByAMQP(t *testing.T) {
	tests := []struct {
		name     string
		expected string
	}{
		{"jubeInbound", "jube.inbound"},
		{"jubeOutbound", "jube.outbound"},
		{"jubeActivations", "jube.activations"},
		{"jubeNotifications", "jube.notifications"},
		{"unknown", ""},
	}

	for _, tt := range tests {
		m := MappingByAMQP(tt.name)
		if tt.expected == "" {
			if m != nil {
				t.Errorf("MappingByAMQP(%q): expected nil, got %+v", tt.name, m)
			}
			continue
		}
		if m == nil {
			t.Errorf("MappingByAMQP(%q): expected mapping, got nil", tt.name)
			continue
		}
		if m.NATSSubject != tt.expected {
			t.Errorf("MappingByAMQP(%q): expected %q, got %q", tt.name, tt.expected, m.NATSSubject)
		}
	}
}

func TestMappingByNATS(t *testing.T) {
	m := MappingByNATS("jube.outbound")
	if m == nil {
		t.Fatal("expected mapping for jube.outbound")
	}
	if m.AMQPName != "jubeOutbound" {
		t.Errorf("expected AMQPName jubeOutbound, got %q", m.AMQPName)
	}

	if MappingByNATS("nonexistent") != nil {
		t.Error("expected nil for nonexistent subject")
	}
}

func TestMappingTypes(t *testing.T) {
	for _, m := range DefaultMappings() {
		switch m.AMQPName {
		case "jubeInbound", "jubeNotifications":
			if m.AMQPType != "queue" {
				t.Errorf("%s: expected type queue, got %q", m.AMQPName, m.AMQPType)
			}
		case "jubeOutbound", "jubeActivations":
			if m.AMQPType != "fanout" {
				t.Errorf("%s: expected type fanout, got %q", m.AMQPName, m.AMQPType)
			}
		}
	}
}
