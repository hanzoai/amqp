package proxy

// ChannelMapping maps an AMQP exchange/queue to a NATS subject.
type ChannelMapping struct {
	AMQPName     string // AMQP queue or exchange name
	AMQPType     string // "queue" or "fanout"
	NATSSubject  string // NATS JetStream subject
	NATSStream   string // NATS JetStream stream name
}

// DefaultMappings returns the 4 Jube AMQP channel mappings.
func DefaultMappings() []ChannelMapping {
	return []ChannelMapping{
		{
			AMQPName:    "jubeInbound",
			AMQPType:    "queue",
			NATSSubject: "jube.inbound",
			NATSStream:  "jube-inbound",
		},
		{
			AMQPName:    "jubeOutbound",
			AMQPType:    "fanout",
			NATSSubject: "jube.outbound",
			NATSStream:  "jube-outbound",
		},
		{
			AMQPName:    "jubeActivations",
			AMQPType:    "fanout",
			NATSSubject: "jube.activations",
			NATSStream:  "jube-activations",
		},
		{
			AMQPName:    "jubeNotifications",
			AMQPType:    "queue",
			NATSSubject: "jube.notifications",
			NATSStream:  "jube-notifications",
		},
	}
}

// MappingByAMQP returns the mapping for a given AMQP name, or nil if not found.
func MappingByAMQP(name string) *ChannelMapping {
	for _, m := range DefaultMappings() {
		if m.AMQPName == name {
			return &m
		}
	}
	return nil
}

// MappingByNATS returns the mapping for a given NATS subject, or nil if not found.
func MappingByNATS(subject string) *ChannelMapping {
	for _, m := range DefaultMappings() {
		if m.NATSSubject == subject {
			return &m
		}
	}
	return nil
}
