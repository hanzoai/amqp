package proxy

// ChannelMapping maps an AMQP exchange/queue to a NATS subject.
type ChannelMapping struct {
	AMQPName     string // AMQP queue or exchange name
	AMQPType     string // "queue" or "fanout"
	NATSSubject  string // NATS JetStream subject
	NATSStream   string // NATS JetStream stream name
}

// DefaultMappings returns the 4 AML AMQP channel mappings.
func DefaultMappings() []ChannelMapping {
	return []ChannelMapping{
		{
			AMQPName:    "amlInbound",
			AMQPType:    "queue",
			NATSSubject: "aml.inbound",
			NATSStream:  "aml-inbound",
		},
		{
			AMQPName:    "amlOutbound",
			AMQPType:    "fanout",
			NATSSubject: "aml.outbound",
			NATSStream:  "aml-outbound",
		},
		{
			AMQPName:    "amlActivations",
			AMQPType:    "fanout",
			NATSSubject: "aml.activations",
			NATSStream:  "aml-activations",
		},
		{
			AMQPName:    "amlNotifications",
			AMQPType:    "queue",
			NATSSubject: "aml.notifications",
			NATSStream:  "aml-notifications",
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
