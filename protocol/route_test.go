package protocol

import (
	"slices"
	"strings"
	"testing"
)

func TestName(t *testing.T) {
	for _, ok := range []string{"orders", "a", "A-b_9", strings.Repeat("x", MaxName)} {
		if err := Name(ok); err != nil {
			t.Errorf("Name(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"", "a.b", "a b", "a*", "a>", "a#", "_", strings.Repeat("x", MaxName+1)} {
		if err := Name(bad); err == nil {
			t.Errorf("Name(%q) = nil; a name that is not one subject word must be refused", bad)
		}
	}
}

func TestSubject(t *testing.T) {
	for _, c := range []struct{ exchange, key, want string }{
		{"", "orders", "amqp._.orders"},
		{"", "", "amqp._"},
		{"events", "order.created", "amqp.events.order.created"},
		{"events", "", "amqp.events"},
		{"", "_", "amqp._._"}, // a literal "_" key is not the empty key
	} {
		got, err := Subject(c.exchange, c.key)
		if err != nil {
			t.Errorf("Subject(%q, %q) = %v", c.exchange, c.key, err)
			continue
		}
		if got != c.want {
			t.Errorf("Subject(%q, %q) = %q, want %q", c.exchange, c.key, got, c.want)
		}
	}
	for _, c := range []struct{ exchange, key string }{
		{"", "a..b"}, {"", "a.*"}, {"", "a.>"}, {"", "a #"}, {"a.b", "k"},
	} {
		if _, err := Subject(c.exchange, c.key); err == nil {
			t.Errorf("Subject(%q, %q) = nil error; a published key is literal", c.exchange, c.key)
		}
	}
}

// TestSubjectRoundTrip: a subject says which exchange and key it came from,
// which is what lets a delivery carry them without a second copy on the wire.
func TestSubjectRoundTrip(t *testing.T) {
	for _, c := range []struct{ exchange, key string }{
		{"", "orders"}, {"", ""}, {"events", "order.created"}, {"events", ""}, {"", "_"},
	} {
		s, err := Subject(c.exchange, c.key)
		if err != nil {
			t.Fatalf("Subject(%q, %q): %v", c.exchange, c.key, err)
		}
		ex, key := parts(s)
		if ex != c.exchange || key != c.key {
			t.Errorf("%q read back as (%q, %q), want (%q, %q)", s, ex, key, c.exchange, c.key)
		}
	}
}

func TestFilters(t *testing.T) {
	for _, c := range []struct {
		kind, exchange, key string
		want                []string
	}{
		{Direct, "", "orders", []string{"amqp._.orders"}},
		{Direct, "d", "a.b", []string{"amqp.d.a.b"}},
		{Direct, "d", "", []string{"amqp.d"}},
		{Fanout, "f", "ignored", []string{"amqp.f.>", "amqp.f"}},
		{Topic, "t", "a.*", []string{"amqp.t.a.*"}},
		{Topic, "t", "a.b", []string{"amqp.t.a.b"}},
		{Topic, "t", "*.b", []string{"amqp.t.*.b"}},
		// '#' matches zero words too, which '>' does not — hence the second.
		{Topic, "t", "a.#", []string{"amqp.t.a.>", "amqp.t.a"}},
		{Topic, "t", "#", []string{"amqp.t.>", "amqp.t"}},
		{Topic, "t", "", []string{"amqp.t"}},
	} {
		got, err := Filters(c.kind, c.exchange, c.key)
		if err != nil {
			t.Errorf("Filters(%s, %q, %q) = %v", c.kind, c.exchange, c.key, err)
			continue
		}
		if !slices.Equal(got, c.want) {
			t.Errorf("Filters(%s, %q, %q) = %q, want %q", c.kind, c.exchange, c.key, got, c.want)
		}
	}

	for _, c := range []struct{ kind, exchange, key string }{
		{"headers", "h", "x"}, // routes on a table; no subject to be
		{"fanfare", "x", ""},  // not a kind at all
		{Topic, "t", "a.#.b"}, // '>' matches only in last position
		{Topic, "t", "a.b*"},  // a word is a wildcard or it is literal
		{Direct, "d", "a.*"},  // a direct key matches literally
		{Direct, "d", "#"},    // same
	} {
		if _, err := Filters(c.kind, c.exchange, c.key); err == nil {
			t.Errorf("Filters(%s, %q, %q) = nil error; it has no honest translation",
				c.kind, c.exchange, c.key)
		}
	}
}

// TestFiltersReachWhatTheyClaim ties the two halves together: what Filters
// produces for a binding must select exactly the subjects Subject produces for
// the keys that binding is meant to catch.
func TestFiltersReachWhatTheyClaim(t *testing.T) {
	hit, err := Filters(Topic, "t", "a.#")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		key  string
		want bool
	}{
		{"a", true}, {"a.b", true}, {"a.b.c", true}, {"b", false}, {"ab", false},
	} {
		subject, err := Subject("t", c.key)
		if err != nil {
			t.Fatal(err)
		}
		got := slices.ContainsFunc(hit, func(f string) bool { return Match(f, subject) })
		if got != c.want {
			t.Errorf("binding a.# %s key %q (subject %q), want %v",
				map[bool]string{true: "matched", false: "missed"}[got], c.key, subject, c.want)
		}
	}
}

func TestMatch(t *testing.T) {
	for _, c := range []struct {
		filter, subject string
		want            bool
	}{
		{"a.b", "a.b", true},
		{"a.b", "a.c", false},
		{"a.*", "a.b", true},
		{"a.*", "a.b.c", false},
		{"a.>", "a.b", true},
		{"a.>", "a.b.c", true},
		{"a.>", "a", false},
		{"a", "a", true},
		{"a", "a.b", false},
	} {
		if got := Match(c.filter, c.subject); got != c.want {
			t.Errorf("Match(%q, %q) = %v, want %v", c.filter, c.subject, got, c.want)
		}
	}
}
