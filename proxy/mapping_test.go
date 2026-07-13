package proxy

import "testing"

func TestDefaultMappings(t *testing.T) {
	mappings := DefaultMappings()
	if len(mappings) != 4 {
		t.Fatalf("expected 4 mappings, got %d", len(mappings))
	}

	expected := map[string]string{
		"amlInbound":       "aml.inbound",
		"amlOutbound":      "aml.outbound",
		"amlActivations":   "aml.activations",
		"amlNotifications": "aml.notifications",
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
		{"amlInbound", "aml.inbound"},
		{"amlOutbound", "aml.outbound"},
		{"amlActivations", "aml.activations"},
		{"amlNotifications", "aml.notifications"},
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
	m := MappingByNATS("aml.outbound")
	if m == nil {
		t.Fatal("expected mapping for aml.outbound")
	}
	if m.AMQPName != "amlOutbound" {
		t.Errorf("expected AMQPName amlOutbound, got %q", m.AMQPName)
	}

	if MappingByNATS("nonexistent") != nil {
		t.Error("expected nil for nonexistent subject")
	}
}

func TestMappingTypes(t *testing.T) {
	for _, m := range DefaultMappings() {
		switch m.AMQPName {
		case "amlInbound", "amlNotifications":
			if m.AMQPType != "queue" {
				t.Errorf("%s: expected type queue, got %q", m.AMQPName, m.AMQPType)
			}
		case "amlOutbound", "amlActivations":
			if m.AMQPType != "fanout" {
				t.Errorf("%s: expected type fanout, got %q", m.AMQPName, m.AMQPType)
			}
		}
	}
}
