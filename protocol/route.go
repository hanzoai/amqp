package protocol

import (
	"fmt"
	"strings"
)

// The map from the AMQP 0-9-1 model onto JetStream, in one place:
//
//	exchange + routing key    ->  subject   amqp.<exchange>.<routing key>
//	queue                     ->  a durable pull consumer on stream AMQP
//	binding                   ->  a filter subject on that consumer
//	basic.ack / nack / reject ->  Ack / Nak / Term
//
// The correspondence is exact where it exists. A routing key and a subject are
// the same idea — dot-separated words, matched by wildcards — so a topic
// binding becomes a filter and nothing is approximated. Where it does NOT
// exist, the declare is refused rather than approximated: see Filters on
// headers exchanges and on '#' in the middle of a binding key.
//
// Retention is the stream's; delivery is the consumer's. Many queues read one
// stream at their own positions, which is what makes fanout free, and a message
// leaves a queue when THAT queue's consumer acks it — which is the AMQP rule.
const (
	// Stream holds every message published through the gateway.
	Stream = "AMQP"

	// Root is the first subject word, so the gateway's traffic is one subtree
	// of the bus that a NATS-native client can read directly.
	Root = "amqp"

	// Blank is the default exchange, whose AMQP name is the empty string and
	// whose subject word cannot be. It is spelled only in the EXCHANGE
	// position: an empty routing key is spelled by leaving the word off
	// entirely, so a message published under the literal key "_" and one
	// published under no key at all stay distinguishable.
	Blank = "_"
)

// Exchange kinds. Headers exchanges route on a table rather than a name and
// have no subject to be, so Filters refuses one instead of pretending.
const (
	Direct = "direct"
	Fanout = "fanout"
	Topic  = "topic"
)

// MaxName bounds an exchange, queue or consumer name. AMQP's own limit is 255
// (a short string); JetStream durable names are shorter in practice, and a
// name is an identifier, not a payload.
const MaxName = 128

// Name reports whether s can be an exchange or queue name here.
//
// One subject word, and that restriction is real: a dotted name would open
// extra subject levels, so exchange "a" with key "b.c" and exchange "a.b" with
// key "c" would land on one subject and leak into each other. Dots belong in
// routing keys, which is where AMQP's own hierarchy lives. The empty name is
// the default exchange and is spelled by leaving it empty, never by writing
// Blank.
func Name(s string) error {
	switch {
	case s == "":
		return fmt.Errorf("name is empty")
	case len(s) > MaxName:
		return fmt.Errorf("name %q is %d octets, over the %d limit", s, len(s), MaxName)
	case s == Blank:
		return fmt.Errorf("name %q is reserved for the default exchange", Blank)
	}
	for _, r := range s {
		if !(r == '-' || r == '_' ||
			('0' <= r && r <= '9') ||
			('a' <= r && r <= 'z') ||
			('A' <= r && r <= 'Z')) {
			return fmt.Errorf("name %q holds %q; one subject word of letters, digits, '-' and '_' only", s, r)
		}
	}
	return nil
}

// Subject is where a publish to exchange with key lands.
func Subject(exchange, key string) (string, error) {
	ex := exchange
	if ex == "" {
		ex = Blank
	} else if err := Name(ex); err != nil {
		return "", fmt.Errorf("exchange: %w", err)
	}
	if key == "" {
		return Root + "." + ex, nil
	}
	words := strings.SplitSeq(key, ".")
	for w := range words {
		if w == "" {
			return "", fmt.Errorf("routing key %q has an empty word", key)
		}
		if strings.ContainsAny(w, "*#> \t") {
			return "", fmt.Errorf("routing key %q holds a wildcard or space; a published key is literal", key)
		}
	}
	return Root + "." + ex + "." + key, nil
}

// Kind reports whether an exchange kind maps onto subjects at all. Headers
// exchanges route on a table rather than a name, so there is no subject for a
// binding to be and no honest translation to make.
func Kind(kind string) error {
	switch kind {
	case Direct, Fanout, Topic:
		return nil
	default:
		return fmt.Errorf("exchange kind %q routes on something other than a name and has no subject to be", kind)
	}
}

// Filters is what a binding becomes: the subjects a queue's consumer reads.
//
// It returns two for a key ending in '#', because AMQP's '#' matches ZERO or
// more words and NATS's '>' matches one or more — so the zero case needs the
// prefix on its own. That is the difference the two wildcard sets actually
// have, and covering it is cheaper than documenting a gap.
func Filters(kind, exchange, key string) ([]string, error) {
	ex := exchange
	if ex == "" {
		ex = Blank
	} else if err := Name(ex); err != nil {
		return nil, fmt.Errorf("exchange: %w", err)
	}
	stem := Root + "." + ex

	switch kind {
	case Fanout:
		// The routing key is ignored, so every key under the exchange binds —
		// and stem alone is the message published with no routing key, which
		// '>' does not reach because it needs at least one word.
		return []string{stem + ".>", stem}, nil

	case Direct:
		if strings.ContainsAny(key, "*#>") {
			return nil, fmt.Errorf("direct binding key %q holds a wildcard; a direct key matches literally", key)
		}
		s, err := Subject(exchange, key)
		if err != nil {
			return nil, err
		}
		return []string{s}, nil

	case Topic:
		if key == "" {
			return []string{stem}, nil
		}
		words := strings.Split(key, ".")
		out := make([]string, 0, len(words))
		for i, w := range words {
			switch {
			case w == "#":
				if i != len(words)-1 {
					return nil, fmt.Errorf("binding key %q has '#' before the end; NATS matches a rest-wildcard only in last position", key)
				}
			case w == "*":
			case w == "":
				return nil, fmt.Errorf("binding key %q has an empty word", key)
			case strings.ContainsAny(w, "*#> \t"):
				return nil, fmt.Errorf("binding key %q holds a partial wildcard in %q; a word is a wildcard or it is literal", key, w)
			}
			out = append(out, w)
		}
		if out[len(out)-1] != "#" {
			return []string{stem + "." + strings.Join(out, ".")}, nil
		}
		head := out[:len(out)-1]
		if len(head) == 0 {
			return []string{stem + ".>", stem}, nil // '#' alone: everything
		}
		prefix := stem + "." + strings.Join(head, ".")
		return []string{prefix + ".>", prefix}, nil

	default:
		return nil, Kind(kind)
	}
}

// Match reports whether a NATS filter selects a subject. '*' takes one word,
// '>' takes the rest and must be last.
func Match(filter, subject string) bool {
	f := strings.Split(filter, ".")
	s := strings.Split(subject, ".")
	for i, w := range f {
		if w == ">" {
			return i < len(s)
		}
		if i >= len(s) {
			return false
		}
		if w != "*" && w != s[i] {
			return false
		}
	}
	return len(f) == len(s)
}
